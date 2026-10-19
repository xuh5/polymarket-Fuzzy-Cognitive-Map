import json
from datetime import UTC, datetime

from pmdata.ingest.select_markets import get_token_ids, select_markets

NOW = datetime(2026, 10, 18, tzinfo=UTC)


def market(id_, volume=None, end="2026-12-31T00:00:00Z", **overrides):
    m = {
        "id": id_,
        "conditionId": f"0xcond{id_}",
        "question": f"Q{id_}",
        "version": "v1",
        "clobTokenIds": json.dumps([f"{id_}1", f"{id_}2"]),
        "outcomes": json.dumps(["Yes", "No"]),
        "acceptingOrders": True,
        "enableOrderBook": True,
        "endDate": end,
        "volume24hr": volume,
    }
    m.update(overrides)
    return m


class TestGetTokenIds:
    def test_v1_parses_json_string_and_pairs_with_outcomes(self):
        assert get_token_ids(market(1)) == [
            {"outcome": "Yes", "token_id": "11"},
            {"outcome": "No", "token_id": "12"},
        ]

    def test_v2_uses_position_ids_even_if_clob_token_ids_present(self):
        m = market(1, version="v2", positionIds=["901", "902"])
        assert [t["token_id"] for t in get_token_ids(m)] == ["901", "902"]

    def test_unknown_version_is_rejected(self):
        assert get_token_ids(market(1, version="v3")) is None
        assert get_token_ids(market(1, version=None)) is None

    def test_non_decimal_id_is_rejected(self):
        assert get_token_ids(market(1, clobTokenIds=json.dumps(["0xabc", "12"]))) is None

    def test_outcome_count_must_be_two(self):
        assert get_token_ids(market(1, outcomes=json.dumps(["A", "B", "C"]))) is None

    def test_missing_ids_are_rejected(self):
        assert get_token_ids(market(1, clobTokenIds=None)) is None


class TestSelectMarkets:
    def test_sorts_by_volume_and_takes_top_n(self):
        markets = [market(1, volume=10), market(2, volume=30), market(3, volume=20)]
        selected = select_markets(markets, top_n=2, now=NOW)
        assert [s["market_id"] for s in selected] == ["2", "3"]

    def test_missing_volume_counts_as_zero(self):
        selected = select_markets([market(1, volume=None), market(2, volume=5)], now=NOW)
        assert [s["market_id"] for s in selected] == ["2", "1"]

    def test_excludes_markets_not_accepting_orders_or_without_book(self):
        markets = [market(1, acceptingOrders=False), market(2, enableOrderBook=False), market(3)]
        assert [s["market_id"] for s in select_markets(markets, now=NOW)] == ["3"]

    def test_excludes_expired_soon_and_no_end_date(self):
        markets = [
            market(1, end="2026-10-17T00:00:00Z"),   # 已过期
            market(2, end="2026-10-19T12:00:00Z"),   # 只剩 1.5 天
            market(3, end=None),                     # 没有 endDate
            market(4, end="2026-10-21T00:00:00Z"),   # 还剩 3 天
        ]
        assert [s["market_id"] for s in select_markets(markets, min_days_left=2, now=NOW)] == ["4"]

    def test_output_carries_condition_id_for_kafka_key(self):
        (s,) = select_markets([market(7)], now=NOW)
        assert s["condition_id"] == "0xcond7"
