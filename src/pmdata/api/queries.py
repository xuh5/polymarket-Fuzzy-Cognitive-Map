"""网站后端用到的 SQL，全部只读 Postgres（pg-sink 写进去的 metrics_book / metrics_trades / tokens）。

每个函数接收一个数据库连接（行格式是 dict），返回 list[dict] 或 dict，直接给 FastAPI 转成 JSON。
市场的价格一律用第一个选项（outcome_index = 1，二元市场是 Yes）的 token。
"""


def latest_metric(conn):
    """最近一条盘口指标的时间和距今秒数，健康检查用。"""
    return conn.execute(
        "select max(computed_at) as latest, extract(epoch from now() - max(computed_at)) as age_seconds "
        "from metrics_book where second > now() - interval '1 day'"
    ).fetchone()


def list_markets(conn, hours):
    """过去 hours 小时有盘口指标的市场，每个市场带第一个选项的最新盘口。

    tokens 表里有几千个 token，但只有订阅的那几十个有指标：对每个 token 用主键索引（asset_id, second）
    取最新一条，比扫一遍 metrics_book 再去重快得多。
    """
    return conn.execute(
        """
        select t.market_id, t.question, t.slug, t.outcome, t.end_date, t.asset_id,
               b.mid, b.best_bid, b.best_ask, b.spread, b.logodds_chg_60m, b.second as updated_at
        from tokens t
        join lateral (
            select * from metrics_book b
            where b.asset_id = t.asset_id and b.second > now() - make_interval(hours => %s)
            order by b.second desc
            limit 1
        ) b on true
        where t.outcome_index = 1
        order by t.question
        """,
        (hours,),
    ).fetchall()


def market_history(conn, market_id, minutes, bucket_seconds=1):
    """某个市场第一个选项过去 minutes 分钟的盘口（只有盘口变了的秒才有行）。

    bucket_seconds > 1 时按这么多秒一格取每格最后的值（画迷你走势图不需要逐秒的几千个点）。
    """
    return conn.execute(
        """
        select distinct on (1)
               date_bin(make_interval(secs => %s), b.second, timestamptz '2000-01-01') as second,
               b.best_bid, b.best_ask, b.mid, b.microprice
        from metrics_book b join tokens t using (asset_id)
        where t.market_id = %s and t.outcome_index = 1
          and b.second > now() - make_interval(mins => %s)
        order by 1, b.second desc
        """,
        (bucket_seconds, market_id, minutes),
    ).fetchall()


def market_tokens(conn, market_id):
    """某个市场的所有选项（按 outcome_index 排），每个带最新一条盘口指标（过去 24 小时没有就是 null）。"""
    return conn.execute(
        """
        select t.market_id, t.question, t.slug, t.end_date, t.outcome, t.outcome_index, t.asset_id,
               b.mid, b.best_bid, b.best_ask, b.spread, b.microprice, b.imbalance_5,
               b.bid_depth_5c, b.ask_depth_5c, b.logodds_chg_60m, b.second as updated_at
        from tokens t
        left join lateral (
            select * from metrics_book b
            where b.asset_id = t.asset_id and b.second > now() - interval '24 hours'
            order by b.second desc
            limit 1
        ) b on true
        where t.market_id = %s
        order by t.outcome_index
        """,
        (market_id,),
    ).fetchall()


def sparklines(conn, hours, bucket_minutes):
    """所有市场第一个选项过去 hours 小时的走势，每 bucket_minutes 分钟取一格最后的中间价。

    返回 {market_id: [[格子开始时间毫秒, 中间价], ...]}（按时间排）。首页表格每行一条小走势图，一次查完所有市场。
    """
    rows = conn.execute(
        """
        select t.market_id,
               date_bin(make_interval(mins => %s), b.second, timestamptz '2000-01-01') as bucket,
               (array_agg(b.mid order by b.second desc))[1] as mid
        from metrics_book b join tokens t using (asset_id)
        where t.outcome_index = 1 and b.second > now() - make_interval(hours => %s) and b.mid is not null
        group by 1, 2
        order by 1, 2
        """,
        (bucket_minutes, hours),
    ).fetchall()
    out = {}
    for r in rows:
        out.setdefault(r["market_id"], []).append([int(r["bucket"].timestamp() * 1000), r["mid"]])
    return out
