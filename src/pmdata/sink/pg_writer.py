"""把实时指标写进 Postgres，给 Grafana 看板用。

一个 consumer（group = pm-pg-sink）读三个 topic：
- pm.metrics.book   → 表 metrics_book（主键 asset_id + second）
- pm.metrics.trades → 表 metrics_trades（主键 asset_id + minute）
- pm.raw.markets    → 表 tokens（asset_id → 市场问题、Yes/No），看板上显示市场名字用

可靠性：每批先写数据库、提交事务，成功后才提交 Kafka offset（at-least-once）。
上游 metrics 程序本身也可能重复输出，所以全部用 upsert（INSERT ... ON CONFLICT DO UPDATE）：
同一个主键写几次结果都一样，重复的自然被覆盖——合起来效果等于 exactly-once。
写数据库出错直接退出，让 Docker 重启，从上次提交的位置重读。

数据只留 RETENTION_DAYS 天（和 Kafka 一样），更早的随时可以从 bronze 重算。
"""
import argparse
import json
import logging
import signal
import time
from datetime import UTC, datetime

import psycopg
from confluent_kafka import Consumer

from pmdata.common.config import KAFKA_BOOTSTRAP, PG_DSN, TOPIC_MARKETS, TOPIC_METRICS_BOOK, TOPIC_METRICS_TRADES
from pmdata.common.log import setup_logging
from pmdata.common.timeutil import parse_time
from pmdata.ingest.select_markets import get_token_ids

log = logging.getLogger("pg-sink")

GROUP_ID = "pm-pg-sink"
BATCH_SIZE = 5000
RETENTION_DAYS = 7
CLEANUP_SECONDS = 3600

BOOK_COLS = ["asset_id", "second", "market_id", "best_bid", "best_ask", "mid", "spread", "microprice",
             "imbalance_1", "imbalance_5", "bid_depth_1c", "ask_depth_1c", "bid_depth_5c", "ask_depth_5c",
             "logodds_chg_1m", "logodds_chg_5m", "logodds_chg_15m", "logodds_chg_60m",
             "exchange_ts", "received_at", "computed_at"]
TRADE_COLS = ["asset_id", "minute", "market_id", "trades", "volume", "notional", "buy_volume", "sell_volume",
              "exchange_ts", "received_at", "computed_at"]
TOKEN_COLS = ["asset_id", "market_id", "question", "outcome", "outcome_index", "slug", "end_date"]

SCHEMA = f"""
create table if not exists metrics_book (
    asset_id text not null,
    second timestamptz not null,
    market_id text not null,
    {", ".join(f"{c} double precision" for c in BOOK_COLS[3:-3])},
    exchange_ts timestamptz,
    received_at timestamptz,
    computed_at timestamptz,
    inserted_at timestamptz not null default now(),   -- 写进数据库的时间，测端到端延迟用
    primary key (asset_id, second)
);
create index if not exists metrics_book_market_second on metrics_book (market_id, second);
create index if not exists metrics_book_second on metrics_book (second);

create table if not exists metrics_trades (
    asset_id text not null,
    minute timestamptz not null,
    market_id text not null,
    trades integer not null,
    volume double precision not null,
    notional double precision not null,
    buy_volume double precision not null,
    sell_volume double precision not null,
    exchange_ts timestamptz,
    received_at timestamptz,
    computed_at timestamptz,
    inserted_at timestamptz not null default now(),
    primary key (asset_id, minute)
);
create index if not exists metrics_trades_minute on metrics_trades (minute);

create table if not exists tokens (
    asset_id text primary key,
    market_id text not null,
    question text,
    outcome text,
    outcome_index integer,       -- 1 = 第一个选项（二元市场是 Yes），2 = 第二个
    slug text,
    end_date timestamptz,
    updated_at timestamptz not null default now()
);
create index if not exists tokens_market on tokens (market_id);
"""


def ms(x):
    return datetime.fromtimestamp(x / 1000, UTC) if x is not None else None


def book_row(rec):
    r = dict(rec, second=ms(rec["second"]), market_id=rec["market"], exchange_ts=ms(rec["exchange_ts"]),
             received_at=parse_time(rec["received_at"]), computed_at=parse_time(rec["computed_at"]))
    return tuple(r.get(c) for c in BOOK_COLS)


def trade_row(rec):
    r = dict(rec, minute=ms(rec["minute"]), market_id=rec["market"], exchange_ts=ms(rec["exchange_ts"]),
             received_at=parse_time(rec["received_at"]), computed_at=parse_time(rec["computed_at"]))
    return tuple(r.get(c) for c in TRADE_COLS)


def token_rows(market):
    """一个 Gamma 市场 → 两个 token 的行；拿不到 token ID 的市场返回空。"""
    tokens = get_token_ids(market) or []
    end = market.get("endDate")
    return [(t["token_id"], market["conditionId"], market.get("question"), t["outcome"], i,
             market.get("slug"), datetime.fromisoformat(end) if end else None)
            for i, t in enumerate(tokens, 1)]


def upsert_sql(table, cols, keys, extra_set=""):
    updates = ", ".join(f"{c} = excluded.{c}" for c in cols if c not in keys)
    return (f"insert into {table} ({', '.join(cols)}) values ({', '.join(['%s'] * len(cols))}) "
            f"on conflict ({', '.join(keys)}) do update set {updates}{extra_set}")


UPSERTS = {
    "metrics_book": upsert_sql("metrics_book", BOOK_COLS, ["asset_id", "second"], ", inserted_at = now()"),
    "metrics_trades": upsert_sql("metrics_trades", TRADE_COLS, ["asset_id", "minute"], ", inserted_at = now()"),
    "tokens": upsert_sql("tokens", TOKEN_COLS, ["asset_id"], ", updated_at = now()"),
}


def to_rows(msgs):
    """一批 Kafka 消息 → {表名: {主键: 行}}。同一批里同一个主键只留最后一条（后到的覆盖先到的）。"""
    rows = {t: {} for t in UPSERTS}
    for msg in msgs:
        if msg.value() is None:      # pm.raw.markets 的 tombstone（市场关闭）：tokens 保留，历史数据还要显示名字
            continue
        try:
            rec = json.loads(msg.value())
        except ValueError:
            log.warning("%s p%d offset %d 不是合法 JSON，跳过", msg.topic(), msg.partition(), msg.offset())
            continue
        topic = msg.topic()
        if topic == TOPIC_METRICS_BOOK:
            row = book_row(rec)
            rows["metrics_book"][row[:2]] = row
        elif topic == TOPIC_METRICS_TRADES:
            row = trade_row(rec)
            rows["metrics_trades"][row[:2]] = row
        elif topic == TOPIC_MARKETS:
            for row in token_rows(rec["payload"]):
                rows["tokens"][row[0]] = row
    return rows


def write(conn, rows):
    with conn.transaction(), conn.cursor() as cur:
        for table, by_key in rows.items():
            if by_key:
                cur.executemany(UPSERTS[table], list(by_key.values()))


def cleanup(conn):
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(f"delete from metrics_book where second < now() - interval '{RETENTION_DAYS} days'")
        n_book = cur.rowcount
        cur.execute(f"delete from metrics_trades where minute < now() - interval '{RETENTION_DAYS} days'")
        log.info("清理 %d 天前的数据：metrics_book %d 行，metrics_trades %d 行", RETENTION_DAYS, n_book, cur.rowcount)


def main():
    parser = argparse.ArgumentParser(description="实时指标 → Postgres")
    parser.add_argument("--group-id", default=GROUP_ID, help="consumer group（试运行时换一个，不影响正式进度）")
    parser.add_argument("--duration", type=int, help="运行多少秒后退出；不填则一直运行")
    args = parser.parse_args()
    setup_logging()

    conn = psycopg.connect(PG_DSN)
    with conn.transaction():
        conn.execute(SCHEMA)
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": args.group_id,
        "enable.auto.commit": False,       # 数据库提交成功后手动提交
        "auto.offset.reset": "earliest",   # 第一次启动把 topic 里现有的都写进去
    })

    stopping = False

    def stop(signum, frame):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    consumer.subscribe([TOPIC_METRICS_BOOK, TOPIC_METRICS_TRADES, TOPIC_MARKETS])
    log.info("开始（group=%s）", args.group_id)
    deadline = time.monotonic() + args.duration if args.duration else None
    last_cleanup = last_report = time.monotonic()
    counts = {t: 0 for t in UPSERTS}
    try:
        while not stopping and (deadline is None or time.monotonic() < deadline):
            msgs = [m for m in consumer.consume(num_messages=BATCH_SIZE, timeout=1.0) if not m.error()]
            if msgs:
                rows = to_rows(msgs)
                write(conn, rows)
                consumer.commit(asynchronous=False)   # 提交这批之后的位置
                for t, by_key in rows.items():
                    counts[t] += len(by_key)
            now = time.monotonic()
            if now - last_cleanup >= CLEANUP_SECONDS:
                cleanup(conn)
                last_cleanup = now
            if now - last_report >= 60:
                log.info("累计写入 %s", counts)
                last_report = now
        log.info("停止")
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
