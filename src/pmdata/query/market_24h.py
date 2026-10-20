"""查询某个市场一段时间内（默认过去 24 小时）的成交和盘口变化——阶段 2 的验收查询。

只读 dbt 建好的 gold 表（data/warehouse.duckdb），时间一律按交易所时间（exchange_ts）筛选。
数据新不新取决于上次 dbt build 的时间：查询前先在 dbt/ 目录下跑一次 dbt build。

用法：
    python -m pmdata.query.market_24h --list                       # 先列出这段时间有数据的市场
    python -m pmdata.query.market_24h --market google            # 问题里含 google 的市场（多个会列出来让你选）
    python -m pmdata.query.market_24h --market <slug / conditionId / Gamma 市场 ID>
    python -m pmdata.query.market_24h --market <...> --hours 6 --end 2026-10-19T12:00 --limit 0
"""
import argparse
from datetime import UTC, datetime, timedelta

import duckdb

from pmdata.common.config import DATA_DIR

WAREHOUSE = DATA_DIR / "warehouse.duckdb"
SEARCH_LIMIT = 20                     # 模糊搜索最多列出多少个市场
STALE_AFTER = timedelta(minutes=30)   # 仓库最新数据比查询终点早这么多，就提醒先跑 dbt build


def connect(path=WAREHOUSE):
    con = duckdb.connect(str(path), read_only=True)   # 只读：不和正在跑的 dbt 抢写锁
    con.execute("set TimeZone = 'UTC'")
    return con


def rows(con, sql, params=None):
    cur = con.execute(sql, params or {})
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]


def find_markets(con, text):
    """先按 conditionId / Gamma 市场 ID / slug 精确匹配；都不中再在问题文本里模糊搜索（按 24 小时成交额排序）。"""
    cols = "market_id, gamma_market_id, slug, question, end_date, volume_24hr, is_delisted"
    exact = rows(con, f"select {cols} from gold.dim_markets where $t in (market_id, gamma_market_id, slug)",
                 {"t": text})
    if exact:
        return exact
    return rows(con, f"""
        select {cols} from gold.dim_markets
        where question ilike $p or slug ilike $p
        order by volume_24hr desc limit {SEARCH_LIMIT}
    """, {"p": f"%{text}%"})


def data_freshness(con):
    """仓库里最新一条盘口/成交数据的交易所时间，用来判断是不是该先跑 dbt build。"""
    return con.execute("""
        select max(ts) from (
            select max(exchange_ts) as ts from gold.fct_top_of_book
            union all
            select max(exchange_ts) from gold.fct_trades
        )
    """).fetchone()[0]


def outcome_summary(con, market_id, start, end):
    """每个选项一行：成交笔数/份数/金额、首末成交价、窗口内盘口变化次数、开始和结束那一刻的盘口。

    fct_top_of_book 只记录买一/卖一变了的时刻，所以「某一刻的盘口」= 那一刻之前最后一次变化后的值（as-of）。
    冷门市场可能一整天买一卖一都不变（变化全在更深的价位），窗口内 0 次变化，但盘口照样有值。
    book_as_of 是结束时盘口的时间：市场早就被移出订阅名单的话，从这里能看出盘口已经过时。
    """
    return rows(con, """
        with t as (
            select asset_id, count(*) as trades, sum(size) as shares, sum(notional) as notional,
                   arg_min(price, exchange_ts) as first_price, arg_max(price, exchange_ts) as last_price
            from gold.fct_trades
            where market_id = $m and exchange_ts >= $start and exchange_ts < $end
            group by 1
        ),
        b as (
            select asset_id,
                   count(*) filter (where exchange_ts >= $start) as book_changes,
                   arg_max(mid_price, exchange_ts) filter (where exchange_ts < $start) as mid_at_start,
                   arg_max(mid_price, exchange_ts) as mid_at_end,
                   arg_max(best_bid, exchange_ts) as bid_at_end,
                   arg_max(best_ask, exchange_ts) as ask_at_end,
                   max(exchange_ts) as book_as_of
            from gold.fct_top_of_book
            where market_id = $m and exchange_ts < $end
            group by 1
        )
        select k.outcome, coalesce(t.trades, 0) as trades, t.shares, t.notional, t.first_price, t.last_price,
               coalesce(b.book_changes, 0) as book_changes, b.mid_at_start, b.mid_at_end,
               b.bid_at_end, b.ask_at_end, b.book_as_of
        from gold.dim_tokens as k
        left join t using (asset_id)
        left join b using (asset_id)
        where k.market_id = $m
        order by k.outcome_index
    """, {"m": market_id, "start": start, "end": end})


def trades(con, market_id, start, end):
    return rows(con, """
        select f.exchange_ts, k.outcome, f.side, f.price, f.size, f.notional
        from gold.fct_trades as f
        join gold.dim_tokens as k using (asset_id)
        where f.market_id = $m and f.exchange_ts >= $start and f.exchange_ts < $end
        order by f.exchange_ts, f.trade_id
    """, {"m": market_id, "start": start, "end": end})


def book_changes(con, market_id, start, end):
    return rows(con, """
        select b.exchange_ts, k.outcome, b.best_bid, b.best_ask, b.spread, b.mid_price, b.source_event
        from gold.fct_top_of_book as b
        join gold.dim_tokens as k using (asset_id)
        where b.market_id = $m and b.exchange_ts >= $start and b.exchange_ts < $end
        order by b.exchange_ts, b.tob_id
    """, {"m": market_id, "start": start, "end": end})


def markers(con, market_id, start, end):
    """这段时间内的缺口/退订标记，外加窗口开始前最后一个标记（说明窗口开始时这个市场在不在采集）。"""
    return rows(con, """
        select received_at, marker_type, reason, disconnected_at, reconnected_at,
               received_at < $start as before_window
        from silver.stg_clob__markers
        where market_id = $m and received_at < $end
        qualify received_at >= $start
             or row_number() over (partition by received_at < $start order by received_at desc) = 1
        order by received_at
    """, {"m": market_id, "start": start, "end": end})


def fmt(v):
    if v is None:
        return "-"
    if isinstance(v, datetime):
        return v.strftime("%m-%d %H:%M:%S.%f")[:-3]
    if isinstance(v, float):
        return f"{v:,.4f}"
    if hasattr(v, "normalize"):          # Decimal：去掉末尾多余的 0
        return f"{v.normalize():f}" if v == v.to_integral() or abs(v) < 1 else f"{v:,.2f}"
    return str(v)


def print_table(items, columns, limit=0):
    """简单的对齐表格。limit > 0 时只显示最后 limit 行（最近的）。"""
    if not items:
        print("  （无）")
        return
    shown = items[-limit:] if limit else items
    if len(shown) < len(items):
        print(f"  共 {len(items)} 行，只显示最近 {len(shown)} 行（--limit 0 显示全部）")
    cells = [[fmt(r[c]) for c in columns] for r in shown]
    widths = [max(len(c), *(len(row[i]) for row in cells)) for i, c in enumerate(columns)]
    print("  " + "  ".join(c.ljust(w) for c, w in zip(columns, widths, strict=True)))
    for row in cells:
        print("  " + "  ".join(v.ljust(w) for v, w in zip(row, widths, strict=True)))


def report(con, market, start, end, limit):
    m = market
    print(f"\n市场：{m['question']}")
    status = "已关闭（从开放列表消失）" if m["is_delisted"] else "开放"
    print(f"  conditionId={m['market_id']}  Gamma ID={m['gamma_market_id']}  到期={fmt(m['end_date'])}  状态={status}")
    print(f"时间范围（UTC，按交易所时间）：{fmt(start)} ~ {fmt(end)}")

    freshness = data_freshness(con)
    if freshness is None or end - freshness > STALE_AFTER:
        print(f"  注意：仓库数据只到 {fmt(freshness)}，要看最新数据先在 dbt/ 目录下跑 dbt build")

    summary = outcome_summary(con, m["market_id"], start, end)
    print("\n【成交汇总】")
    print_table(summary, ["outcome", "trades", "shares", "notional", "first_price", "last_price"])
    print("\n【盘口汇总】mid_at_start / *_at_end = 窗口开始 / 结束那一刻的盘口；book_as_of = 结束时盘口来自什么时候")
    print_table(summary, ["outcome", "book_changes", "mid_at_start", "mid_at_end", "bid_at_end", "ask_at_end",
                          "book_as_of"])

    print("\n【采集状态】缺口 / 退订标记（第一行可能是窗口开始前的最后一个标记）")
    print_table(markers(con, m["market_id"], start, end),
                ["received_at", "marker_type", "reason", "disconnected_at", "reconnected_at"])

    print("\n【成交明细】")
    print_table(trades(con, m["market_id"], start, end),
                ["exchange_ts", "outcome", "side", "price", "size", "notional"], limit)

    print("\n【盘口变化】买一 / 卖一变了才算一行（0 行不代表没数据：可能只是更深的价位在变，盘口见上面的盘口汇总）")
    print_table(book_changes(con, m["market_id"], start, end),
                ["exchange_ts", "outcome", "best_bid", "best_ask", "spread", "mid_price", "source_event"], limit)


def active_markets(con, start, end):
    """这段时间内有数据（成交或盘口变化）的市场——不知道查哪个时先列出来挑。

    采集状态看窗口结束前这个市场的最后一个标记：unsubscribed = 已移出订阅名单，否则还在采集。
    """
    return rows(con, """
        with activity as (
            select market_id, count(*) filter (where kind = 'trade') as trades,
                   count(*) filter (where kind = 'book') as book_changes,
                   min(exchange_ts) as first_data, max(exchange_ts) as last_data
            from (
                select market_id, exchange_ts, 'trade' as kind from gold.fct_trades
                union all
                select market_id, exchange_ts, 'book' from gold.fct_top_of_book
            )
            where exchange_ts >= $start and exchange_ts < $end
            group by 1
        ),
        last_marker as (
            select market_id, marker_type
            from silver.stg_clob__markers
            where received_at < $end
            qualify row_number() over (partition by market_id order by received_at desc) = 1
        )
        select m.slug, left(m.question, 60) as question, a.trades, a.book_changes, a.first_data, a.last_data,
               case when m.is_delisted then '已关闭'
                    when l.marker_type = 'unsubscribed' then '已移出名单'
                    else '采集中' end as status
        from activity as a
        join gold.dim_markets as m using (market_id)
        left join last_marker as l using (market_id)
        order by a.trades desc, a.book_changes desc
    """, {"start": start, "end": end})


def main():
    parser = argparse.ArgumentParser(description="查询某个市场一段时间内的成交和盘口变化（读 gold 表）")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--market", help="slug、conditionId、Gamma 市场 ID，或问题里的关键词")
    group.add_argument("--list", action="store_true", help="列出这段时间内有数据的市场（不知道查哪个时用）")
    parser.add_argument("--hours", type=float, default=24, help="往前查多少小时，默认 24")
    parser.add_argument("--end", help="结束时间（UTC，如 2026-10-19T12:00），默认现在")
    parser.add_argument("--limit", type=int, default=20, help="明细最多显示最近多少行，0 = 全部")
    args = parser.parse_args()

    end = datetime.fromisoformat(args.end).replace(tzinfo=UTC) if args.end else datetime.now(UTC)
    start = end - timedelta(hours=args.hours)

    con = connect()
    if args.list:
        found = active_markets(con, start, end)
        print(f"{fmt(start)} ~ {fmt(end)}（UTC）有数据的市场：{len(found)} 个，按成交笔数排序")
        print_table(found, ["slug", "question", "trades", "book_changes", "first_data", "last_data", "status"])
        return

    found = find_markets(con, args.market)
    if not found:
        print(f"找不到和「{args.market}」匹配的市场")
        return
    if len(found) > 1:
        many = f"至少 {len(found)}" if len(found) == SEARCH_LIMIT else str(len(found))
        print(f"「{args.market}」匹配到 {many} 个市场，请用 slug 或 conditionId 指定一个：")
        print_table(found, ["slug", "question", "volume_24hr", "is_delisted"])
        return
    report(con, found[0], start, end, args.limit)


if __name__ == "__main__":
    main()
