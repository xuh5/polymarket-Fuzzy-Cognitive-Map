"""把实时指标写进 Postgres，给 Grafana 看板用。

一个 consumer（group = pm-pg-sink）读七个 topic：
- pm.metrics.book   → 表 metrics_book（主键 asset_id + second）
- pm.metrics.trades → 表 metrics_trades（主键 asset_id + minute）
- pm.metrics.depth  → 表 depth_quality（主键 market_id + minute），网站 Depth quality 用
- pm.metrics.execution → 表 execution_quality（主键 tx_hash + asset_id），每笔吃单的成交前盘口、成本和 markout
- pm.raw.markets    → 表 tokens（asset_id → 市场问题、Yes/No），看板上显示市场名字用；
                      顺便按问题文字给市场打标签 → 表 market_labels（话题、公司，规则见 pmdata.topics.rules）
- pm.events         → 表 signal_events（主键 event_id），网站 Signals 页用；永久保留
- pm.twins          → 表 twin_judgments（每组事件对一行）+ market_relations（每对孪生市场一行），网站 Related 用；
                      一组重新判断后，这组 AI 写的旧关系先标退役（retired_at），新结果里还有的再恢复；
                      tombstone（这组不再是候选）= 这组的关系全部退役。不删，以后还能回头查。永久保留

可靠性：每批先写数据库、提交事务，成功后才提交 Kafka offset（at-least-once）。
上游 metrics 程序本身也可能重复输出，所以全部用 upsert（INSERT ... ON CONFLICT DO UPDATE）：
同一个主键写几次结果都一样，重复的自然被覆盖——合起来效果等于 exactly-once。
写数据库出错直接退出，让 Docker 重启，从上次提交的位置重读。

指标只留 RETENTION_DAYS 天（和 Kafka 一样），更早的随时可以从 bronze 重算。
"""
import argparse
import json
import logging
import signal
import time
from datetime import UTC, datetime

import psycopg
from confluent_kafka import Consumer
from psycopg.types.json import Jsonb

from pmdata.common.config import (
    KAFKA_BOOTSTRAP,
    PG_DSN,
    TOPIC_EVENTS,
    TOPIC_MARKETS,
    TOPIC_METRICS_BOOK,
    TOPIC_METRICS_DEPTH,
    TOPIC_METRICS_EXECUTION,
    TOPIC_METRICS_TRADES,
    TOPIC_TWINS,
)
from pmdata.common.log import setup_logging
from pmdata.common.timeutil import parse_time
from pmdata.ingest.select_markets import get_token_ids
from pmdata.topics.rules import classify

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
DEPTH_COLS = ["market_id", "minute", "depth_usd", "suspicious_usd", "levels", "computed_at"]
# 成交质量，字段含义见 pmdata.execution.tracker
EXEC_HORIZONS = [1, 10, 60, 300]
EXEC_COLS = (["tx_hash", "asset_id", "market_id", "ts", "side", "fee_rate_bps", "price", "size", "notional", "levels",
              "worst_price", "pre_bid", "pre_ask", "pre_mid", "pre_spread", "pre_match", "cost"]
             + [f"mid_{h}s" for h in EXEC_HORIZONS] + [f"maker_pnl_{h}s" for h in EXEC_HORIZONS] + ["computed_at"])
TOKEN_COLS = ["asset_id", "market_id", "question", "outcome", "outcome_index", "slug", "end_date"]
LABEL_COLS = ["market_id", "topic", "company", "source"]
# 跳动事件，字段含义见 pmdata.signals.detector
EVENT_TIMES = ["start_at", "peak_at", "last_at", "book_since"]
EVENT_FLOATS = ["mid_before", "mid_peak", "change_points", "change_logodds", "bid_before", "ask_before", "bid_peak",
                "ask_peak", "spread_before", "spread_peak", "depth_before", "notional", "bullish_volume",
                "bearish_volume", "flow", "baseline_notional_24h", "volume_ratio", "mid_after_5m", "mid_after_30m"]
EVENT_COLS = (["event_id", "kind", "status", "market_id", "asset_id"] + EVENT_TIMES + EVENT_FLOATS
              + ["move_type", "reversals", "trades", "baseline_source", "computed_at"])
STATUS_ORDER = "array['open', 'closed', 'final']"

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

-- 假深度识别（规则见 pmdata.depth.tracker）：第一个选项的盘口，离最优价 5 分内
create table if not exists depth_quality (
    market_id text not null,
    minute timestamptz not null,
    depth_usd double precision not null,
    suspicious_usd double precision not null,
    levels jsonb not null,                   -- 可疑价位列表：side / price / usd / flashes / filled_usd
    computed_at timestamptz,
    inserted_at timestamptz not null default now(),
    primary key (market_id, minute)
);
create index if not exists depth_quality_minute on depth_quality (minute);

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

-- 市场标签。source = 谁打的：rules（pmdata.topics.rules，按问题文字）；以后别的打法（ai / manual）写同一张表，
-- 规则不覆盖它们
create table if not exists market_labels (
    market_id text primary key,
    topic text,
    company text,
    source text not null,
    updated_at timestamptz not null default now()
);

create table if not exists signal_events (
    event_id text primary key,             -- 市场 ID:开始那一秒
    kind text not null,                    -- 目前只有 jump
    status text not null,                  -- open / closed / final
    market_id text not null,
    asset_id text not null,
    {", ".join(f"{c} timestamptz" for c in EVENT_TIMES)},
    {", ".join(f"{c} double precision" for c in EVENT_FLOATS)},
    move_type text,
    reversals integer,
    trades integer,
    baseline_source text,
    computed_at timestamptz,
    inserted_at timestamptz not null default now(),   -- 第一次写进来的时间（open），之后的版本不改它
    updated_at timestamptz not null default now()
);
create index if not exists signal_events_peak on signal_events (peak_at);

-- 成交质量（规则见 pmdata.execution.tracker）：每笔吃单一条，成交 5 分钟后写入
create table if not exists execution_quality (
    tx_hash text not null,
    asset_id text not null,
    market_id text not null,
    ts timestamptz not null,                 -- 成交消息的交易所时间
    side text not null,                      -- 吃单方向
    fee_rate_bps integer,
    price double precision not null,         -- 吃单均价（交易所四舍五入到 0.001）
    size double precision not null,
    notional double precision not null,
    levels integer,                          -- 吃了几档；null = 没认出来
    worst_price double precision,
    pre_bid double precision,
    pre_ask double precision,
    pre_mid double precision,
    pre_spread double precision,
    pre_match text not null,                 -- exact / sweep / none
    cost double precision,                   -- 吃单方比成交前中间价多付多少（每股）
    mid_1s double precision, mid_10s double precision, mid_60s double precision, mid_300s double precision,
    maker_pnl_1s double precision, maker_pnl_10s double precision,     -- 挂单方按之后的中间价算，每股赚多少
    maker_pnl_60s double precision, maker_pnl_300s double precision,
    computed_at timestamptz,
    inserted_at timestamptz not null default now(),
    primary key (tx_hash, asset_id)
);
create index if not exists execution_quality_market_ts on execution_quality (market_id, ts);
create index if not exists execution_quality_ts on execution_quality (ts);

-- 孪生市场（pmdata.twins）：每组事件对一次判断一行（event_a < event_b）
create table if not exists twin_judgments (
    event_a text not null,
    event_b text not null,
    fingerprint text not null,
    status text not null,            -- ok / error / retired（不再是候选）
    error text,
    relations int,
    model text,
    cost_usd double precision,       -- 按 API 价格折算（订阅额度，不实际扣钱）
    judged_at timestamptz not null default now(),
    primary key (event_a, event_b)
);
-- 每对孪生市场一行（market_a 属于 event_a）。relation 从 A 的角度说（implies = A 是 → B 一定是）。
-- source = ai / manual：AI 的结果只动 ai 的行。retired_at：市场关闭或重新判断后不再成立，不删
create table if not exists market_relations (
    market_a text not null,
    market_b text not null,
    event_a text not null,
    event_b text not null,
    relation text not null,          -- identical / complement / exclusive / implies / implied_by
    strength text not null,          -- strict / conditional
    condition text,
    differences jsonb not null default '[]',
    confidence text,
    reason text,
    source text not null,
    model text,
    judged_at timestamptz not null default now(),
    retired_at timestamptz,
    primary key (market_a, market_b)
);
create index if not exists market_relations_b on market_relations (market_b);
create index if not exists market_relations_events on market_relations (event_a, event_b);
-- 2026-11-20 前 twins 自己建表，那时这一列叫 closed_at
do $$ begin
    if exists (select 1 from information_schema.columns
               where table_name = 'market_relations' and column_name = 'closed_at') then
        alter table market_relations rename column closed_at to retired_at;
    end if;
end $$;
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


def depth_row(rec):
    return (rec["market"], ms(rec["ts"]), rec["depth_usd"], rec["suspicious_usd"], Jsonb(rec["levels"]),
            parse_time(rec["computed_at"]))


def exec_row(rec):
    r = dict(rec, market_id=rec["market"], ts=ms(rec["ts"]), computed_at=parse_time(rec["computed_at"]))
    return tuple(r.get(c) for c in EXEC_COLS)


def event_row(rec):
    r = dict(rec, market_id=rec["market"], start_at=ms(rec["start"]), peak_at=ms(rec["peak"]),
             last_at=ms(rec["last"]), book_since=ms(rec["book_since"]), computed_at=parse_time(rec["computed_at"]))
    return tuple(r.get(c) for c in EVENT_COLS)


def token_rows(market):
    """一个 Gamma 市场 → 两个 token 的行；拿不到 token ID 的市场返回空。"""
    tokens = get_token_ids(market) or []
    end = market.get("endDate")
    return [(t["token_id"], market["conditionId"], market.get("question"), t["outcome"], i,
             market.get("slug"), datetime.fromisoformat(end) if end else None)
            for i, t in enumerate(tokens, 1)]


def label_row(market_id, question):
    return (market_id, *classify(question), "rules")


def upsert_sql(table, cols, keys, extra_set=""):
    updates = ", ".join(f"{c} = excluded.{c}" for c in cols if c not in keys)
    return (f"insert into {table} ({', '.join(cols)}) values ({', '.join(['%s'] * len(cols))}) "
            f"on conflict ({', '.join(keys)}) do update set {updates}{extra_set}")


UPSERTS = {
    "metrics_book": upsert_sql("metrics_book", BOOK_COLS, ["asset_id", "second"], ", inserted_at = now()"),
    "metrics_trades": upsert_sql("metrics_trades", TRADE_COLS, ["asset_id", "minute"], ", inserted_at = now()"),
    "depth_quality": upsert_sql("depth_quality", DEPTH_COLS, ["market_id", "minute"], ", inserted_at = now()"),
    "execution_quality": upsert_sql("execution_quality", EXEC_COLS, ["tx_hash", "asset_id"], ", inserted_at = now()"),
    "tokens": upsert_sql("tokens", TOKEN_COLS, ["asset_id"], ", updated_at = now()"),
    # 只覆盖规则自己打的标签，别的来源（ai / manual）优先
    "market_labels": upsert_sql("market_labels", LABEL_COLS, ["market_id"], ", updated_at = now()")
    + " where market_labels.source = 'rules'",
    # 重启后上游可能重发旧版本（比如 final 之后又来一条 open）：状态只往前走，不倒退
    "signal_events": upsert_sql("signal_events", EVENT_COLS, ["event_id"], ", updated_at = now()")
    + f" where array_position({STATUS_ORDER}, excluded.status) >= array_position({STATUS_ORDER}, signal_events.status)",
}


JUDGMENT_UPSERT = upsert_sql("twin_judgments", ["event_a", "event_b", "fingerprint", "status", "error", "relations",
                                                "model", "cost_usd", "judged_at"], ["event_a", "event_b"])
RELATION_COLS = ["market_a", "market_b", "event_a", "event_b", "relation", "strength", "condition", "differences",
                 "confidence", "reason", "source", "model", "judged_at"]
# 只覆盖 AI 写的行（人工改过的 manual 不碰）；新结果里有的恢复成「在用」
RELATION_UPSERT = (upsert_sql("market_relations", RELATION_COLS, ["market_a", "market_b"], ", retired_at = null")
                   + " where market_relations.source = 'ai'")
RETIRE_PAIR = ("update market_relations set retired_at = now() "
               "where event_a = %s and event_b = %s and source = 'ai' and retired_at is null")


def write_twins(cur, by_key):
    """pm.twins 的消息：{「事件A:事件B」: payload，tombstone 为 None}。"""
    for key, p in by_key.items():
        a, b = key.split(":", 1)
        if p is None:      # 这组不再是候选
            cur.execute(RETIRE_PAIR, (a, b))
            cur.execute("update twin_judgments set status = 'retired', judged_at = now() "
                        "where event_a = %s and event_b = %s", (a, b))
            continue
        cur.execute(JUDGMENT_UPSERT, (a, b, p["fingerprint"], p["status"], p.get("error"),
                                      len(p["relations"]) if p["status"] == "ok" else None, p.get("model"),
                                      p.get("cost_usd"), parse_time(p["judged_at"])))
        if p["status"] != "ok":     # 判断失败：旧结果先留着，下一轮重试
            continue
        cur.execute(RETIRE_PAIR, (a, b))
        cur.executemany(RELATION_UPSERT, [
            (r["market_a"], r["market_b"], a, b, r["relation"], r["strength"], r.get("condition"),
             Jsonb(r.get("differences") or []), r.get("confidence"), r.get("reason"), "ai", p.get("model"),
             parse_time(p["judged_at"]))
            for r in p["relations"]])


def to_rows(msgs):
    """一批 Kafka 消息 → {表名: {主键: 行}}。同一批里同一个主键只留最后一条（后到的覆盖先到的）。
    pm.twins 单独放在 "twins"：{key: payload 或 None（tombstone）}，由 write_twins 处理。"""
    rows = {t: {} for t in UPSERTS} | {"twins": {}}
    for msg in msgs:
        if msg.topic() == TOPIC_TWINS:
            key = msg.key().decode() if msg.key() else None
            if not key or ":" not in key:
                continue
            rows["twins"][key] = None if msg.value() is None else json.loads(msg.value())["payload"]
            continue
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
        elif topic == TOPIC_METRICS_DEPTH:
            row = depth_row(rec)
            rows["depth_quality"][row[:2]] = row
        elif topic == TOPIC_METRICS_EXECUTION:
            if not rec.get("tx_hash"):     # 交易所一直都带交易哈希；万一没有就没法做主键，跳过
                log.warning("成交质量记录没有交易哈希，跳过：%s %s", rec.get("market"), rec.get("ts"))
                continue
            row = exec_row(rec)
            rows["execution_quality"][row[:2]] = row
        elif topic == TOPIC_EVENTS:
            row = event_row(rec)
            rows["signal_events"][row[0]] = row
        elif topic == TOPIC_MARKETS:
            m = rec["payload"]
            for row in token_rows(m):
                rows["tokens"][row[0]] = row
            if m.get("conditionId"):
                rows["market_labels"][m["conditionId"]] = label_row(m["conditionId"], m.get("question"))
    return rows


def write(conn, rows):
    with conn.transaction(), conn.cursor() as cur:
        for table, by_key in rows.items():
            if table == "twins":
                write_twins(cur, by_key)
            elif by_key:
                cur.executemany(UPSERTS[table], list(by_key.values()))


def relabel_all(conn):
    """启动时按现在的规则把数据库里所有市场重新打一遍标签：规则改了、已经关闭（不会再收到消息）的老市场也跟着更新。"""
    # 查询也放在事务里：连接不是 autocommit，单独一句查询会隐式开一个事务，之后的写入就都没提交
    with conn.transaction(), conn.cursor() as cur:
        markets = cur.execute("select distinct on (market_id) market_id, question from tokens").fetchall()
        cur.executemany(UPSERTS["market_labels"], [label_row(m, q) for m, q in markets])
    log.info("按规则重新打标签：%d 个市场", len(markets))


def cleanup(conn):
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(f"delete from metrics_book where second < now() - interval '{RETENTION_DAYS} days'")
        n_book = cur.rowcount
        cur.execute(f"delete from metrics_trades where minute < now() - interval '{RETENTION_DAYS} days'")
        n_trades = cur.rowcount
        cur.execute(f"delete from depth_quality where minute < now() - interval '{RETENTION_DAYS} days'")
        n_depth = cur.rowcount
        cur.execute(f"delete from execution_quality where ts < now() - interval '{RETENTION_DAYS} days'")
        log.info("清理 %d 天前的数据：metrics_book %d 行，metrics_trades %d 行，depth_quality %d 行，"
                 "execution_quality %d 行", RETENTION_DAYS, n_book, n_trades, n_depth, cur.rowcount)


def main():
    parser = argparse.ArgumentParser(description="实时指标 → Postgres")
    parser.add_argument("--group-id", default=GROUP_ID, help="consumer group（试运行时换一个，不影响正式进度）")
    parser.add_argument("--duration", type=int, help="运行多少秒后退出；不填则一直运行")
    args = parser.parse_args()
    setup_logging()

    conn = psycopg.connect(PG_DSN)
    with conn.transaction():
        conn.execute(SCHEMA)
    relabel_all(conn)
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

    consumer.subscribe([TOPIC_METRICS_BOOK, TOPIC_METRICS_TRADES, TOPIC_METRICS_DEPTH, TOPIC_METRICS_EXECUTION,
                        TOPIC_MARKETS, TOPIC_EVENTS, TOPIC_TWINS])
    log.info("开始（group=%s）", args.group_id)
    deadline = time.monotonic() + args.duration if args.duration else None
    last_cleanup = last_report = time.monotonic()
    counts = {t: 0 for t in UPSERTS} | {"twins": 0}
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
