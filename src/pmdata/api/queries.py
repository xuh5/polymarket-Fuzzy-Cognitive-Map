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

    topic / company 是市场标签（表 market_labels，规则或以后别的打法写的），可能为空。
    tokens 表里有几千个 token，但只有订阅的那几十个有指标：对每个 token 用主键索引（asset_id, second）
    取最新一条，比扫一遍 metrics_book 再去重快得多。
    """
    return conn.execute(
        """
        select t.market_id, t.question, t.slug, t.outcome, t.end_date, t.asset_id, l.topic, l.company,
               b.mid, b.best_bid, b.best_ask, b.spread, b.logodds_chg_60m, b.second as updated_at
        from tokens t
        left join market_labels l using (market_id)
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


def market_trades(conn, market_id, minutes, bucket_seconds=60):
    """某个市场过去 minutes 分钟的成交，按 bucket_seconds 一格汇总（只有有成交的格才有行）。

    两个选项合并成第一个选项（Yes）的视角，和 Grafana 的「成交流向」一样：
    看涨 = 主动买 Yes + 主动卖 No，看跌 = 主动卖 Yes + 主动买 No。单位是份数。
    """
    return conn.execute(
        """
        select date_bin(make_interval(secs => %s), tr.minute, timestamptz '2000-01-01') as t,
               sum(tr.trades) as trades,
               sum(case when t.outcome_index = 1 then tr.buy_volume else tr.sell_volume end) as bullish,
               sum(case when t.outcome_index = 1 then tr.sell_volume else tr.buy_volume end) as bearish
        from metrics_trades tr join tokens t using (asset_id)
        where tr.market_id = %s and tr.minute > now() - make_interval(mins => %s)
        group by 1
        order by 1
        """,
        (bucket_seconds, market_id, minutes),
    ).fetchall()


def market_tokens(conn, market_id):
    """某个市场的所有选项（按 outcome_index 排），每个带最新一条盘口指标（过去 24 小时没有就是 null）。"""
    return conn.execute(
        """
        select t.market_id, t.question, t.slug, t.end_date, t.outcome, t.outcome_index, t.asset_id, l.topic, l.company,
               b.mid, b.best_bid, b.best_ask, b.spread, b.microprice, b.imbalance_5,
               b.bid_depth_5c, b.ask_depth_5c, b.logodds_chg_60m, b.second as updated_at
        from tokens t
        left join market_labels l using (market_id)
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



def list_signals(conn, hours):
    """过去 hours 小时（按峰值时间）的所有 signal，最新的在前，带市场问题和第一个选项的名字。

    kind 是类型（目前只有 jump = 价格跳动）。signals 服务写的是候选（门槛放低），筛选在网页上做：
    一周几百条，全部给前端就行；量大了再改成按条件查（规则格式见 web/src/signals.ts）。
    """
    return conn.execute(
        """
        select e.event_id, e.kind, e.status, e.market_id, t.question, t.slug, t.outcome,
               e.start_at, e.peak_at, e.last_at, e.mid_before, e.mid_peak, e.change_points,
               e.bid_before, e.ask_before, e.bid_peak, e.ask_peak, e.spread_before, e.spread_peak, e.depth_before,
               e.move_type, e.reversals, e.trades, e.notional, e.bullish_volume, e.bearish_volume, e.flow,
               e.baseline_source, e.volume_ratio, e.mid_after_5m, e.mid_after_30m
        from signal_events e
        left join tokens t on t.asset_id = e.asset_id
        where e.peak_at > now() - make_interval(hours => %s)
        order by e.peak_at desc
        """,
        (hours,),
    ).fetchall()
