from datetime import UTC, datetime

import duckdb
import pytest

from pmdata.query import market_24h as q

START = datetime(2026, 10, 19, 0, 0, tzinfo=UTC)
END = datetime(2026, 10, 20, 0, 0, tzinfo=UTC)


@pytest.fixture
def con():
    """内存里的迷你仓库：只建查询用到的表和列，塞几行假数据。"""
    c = duckdb.connect()
    c.execute("set TimeZone = 'UTC'")
    c.execute("create schema gold; create schema silver")
    c.execute("""
        create table gold.dim_markets as select * from (values
            ('0xA', '1', 'google-best-ai', 'Will Google have the best AI model?', null::timestamptz, 100.0, false),
            ('0xB', '2', 'openai-best-ai', 'Will OpenAI have the best AI model?', null::timestamptz, 50.0, true)
        ) t(market_id, gamma_market_id, slug, question, end_date, volume_24hr, is_delisted)
    """)
    c.execute("""
        create table gold.dim_tokens as select * from (values
            ('a-yes', '0xA', 'Yes', 1), ('a-no', '0xA', 'No', 2)
        ) t(asset_id, market_id, outcome, outcome_index)
    """)
    c.execute("""
        create table gold.fct_trades as select * from (values
            ('0-1', '0xA', 'a-yes', '2026-10-18 23:00:00+00'::timestamptz, 'BUY', 0.60, 10.0, 6.0),  -- 窗口之前
            ('0-2', '0xA', 'a-yes', '2026-10-19 01:00:00+00'::timestamptz, 'BUY', 0.70, 10.0, 7.0),
            ('0-3', '0xA', 'a-yes', '2026-10-19 02:00:00+00'::timestamptz, 'SELL', 0.72, 5.0, 3.6),
            ('0-4', '0xA', 'a-yes', '2026-10-20 00:00:00+00'::timestamptz, 'BUY', 0.80, 1.0, 0.8)   -- 终点不含
        ) t(trade_id, market_id, asset_id, exchange_ts, side, price, size, notional)
    """)
    c.execute("""
        create table gold.fct_top_of_book as select * from (values
            ('0-0-1', '0xA', 'a-yes', '2026-10-18 20:00:00+00'::timestamptz, 0.59, 0.61, 0.02, 0.60, 'book'),  -- 窗口前
            ('0-2-1', '0xA', 'a-yes', '2026-10-19 01:00:00+00'::timestamptz, 0.69, 0.71, 0.02, 0.70, 'price_change'),
            ('0-5-1', '0xA', 'a-yes', '2026-10-19 03:00:00+00'::timestamptz, 0.71, 0.73, 0.02, 0.72, 'price_change'),
            ('0-5-2', '0xA', 'a-no',  '2026-10-19 03:00:00+00'::timestamptz, 0.27, 0.29, 0.02, 0.28, 'price_change')
        ) t(tob_id, market_id, asset_id, exchange_ts, best_bid, best_ask, spread, mid_price, source_event)
    """)
    c.execute("""
        create table silver.stg_clob__markers as select * from (values
            ('0xA', '2026-10-18 10:00:00+00'::timestamptz, 'gap', 'collector 启动', null::timestamptz,
             '2026-10-18 10:00:00+00'::timestamptz),
            ('0xA', '2026-10-18 12:00:00+00'::timestamptz, 'gap', '断线', '2026-10-18 11:59:58+00'::timestamptz,
             '2026-10-18 12:00:00+00'::timestamptz),
            ('0xA', '2026-10-19 05:00:00+00'::timestamptz, 'gap', '断线', '2026-10-19 04:59:58+00'::timestamptz,
             '2026-10-19 05:00:00+00'::timestamptz)
        ) t(market_id, received_at, marker_type, reason, disconnected_at, reconnected_at)
    """)
    return c


class TestFindMarkets:
    def test_exact_match_by_slug_or_ids(self, con):
        for text in ["google-best-ai", "0xA", "1"]:
            assert [m["market_id"] for m in q.find_markets(con, text)] == ["0xA"]

    def test_fuzzy_match_on_question_sorted_by_volume(self, con):
        assert [m["market_id"] for m in q.find_markets(con, "BEST ai")] == ["0xA", "0xB"]

    def test_no_match(self, con):
        assert q.find_markets(con, "bitcoin") == []


def test_summary_only_counts_window_and_lists_every_outcome(con):
    summary = {r["outcome"]: r for r in q.outcome_summary(con, "0xA", START, END)}
    yes, no = summary["Yes"], summary["No"]
    assert yes["trades"] == 2                       # 窗口前那笔和恰好在终点那笔都不算
    assert float(yes["first_price"]) == 0.70 and float(yes["last_price"]) == 0.72
    assert float(yes["notional"]) == pytest.approx(10.6)
    assert no["trades"] == 0 and no["book_changes"] == 1   # 没成交的选项也要有一行


def test_book_summary_is_as_of_window_start_and_end(con):
    summary = {r["outcome"]: r for r in q.outcome_summary(con, "0xA", START, END)}
    yes, no = summary["Yes"], summary["No"]
    assert yes["book_changes"] == 2                          # 窗口前那次变化不算「窗口内变化」
    assert float(yes["mid_at_start"]) == 0.60                # 但它决定了窗口开始那一刻的盘口
    assert float(yes["mid_at_end"]) == 0.72 and float(yes["bid_at_end"]) == 0.71
    assert no["mid_at_start"] is None and float(no["mid_at_end"]) == 0.28


def test_book_summary_when_top_of_book_never_changes_in_window(con):
    # 冷门市场：窗口内买一卖一一次没变，盘口仍然取窗口前最后一次的值
    late_start = datetime(2026, 10, 19, 4, 0, tzinfo=UTC)
    yes = next(r for r in q.outcome_summary(con, "0xA", late_start, END) if r["outcome"] == "Yes")
    assert yes["book_changes"] == 0
    assert float(yes["mid_at_start"]) == 0.72 and float(yes["mid_at_end"]) == 0.72


def test_details_are_in_time_order_with_outcome(con):
    trades = q.trades(con, "0xA", START, END)
    assert [(r["outcome"], float(r["price"])) for r in trades] == [("Yes", 0.70), ("Yes", 0.72)]
    assert [r["outcome"] for r in q.book_changes(con, "0xA", START, END)] == ["Yes", "Yes", "No"]


def test_markers_include_last_one_before_window(con):
    result = q.markers(con, "0xA", START, END)
    assert [(r["reason"], r["before_window"]) for r in result] == [("断线", True), ("断线", False)]
