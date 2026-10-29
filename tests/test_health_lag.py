from datetime import UTC, datetime

from pmdata.health.lag import lag_rows

TS = datetime(2026, 10, 29, tzinfo=UTC)


def test_lag_per_partition():
    committed = {"pm-metrics": {("pm.raw.clob", 1): 90, ("pm.raw.clob", 0): 100}}
    ends = {("pm.raw.clob", 0): 100, ("pm.raw.clob", 1): 120}
    assert lag_rows(TS, committed, ends) == [
        (TS, "pm-metrics", "pm.raw.clob", 0, 100, 100, 0),
        (TS, "pm-metrics", "pm.raw.clob", 1, 90, 120, 30),
    ]


def test_skips_uncommitted_and_unknown_partitions():
    committed = {"g": {("t", 0): -1001, ("t", 1): 5, ("other", 0): 3}}   # -1001 = 从没提交过
    ends = {("t", 0): 10, ("t", 1): 8}
    assert lag_rows(TS, committed, ends) == [(TS, "g", "t", 1, 5, 8, 3)]


def test_negative_lag_clamped_to_zero():
    # 已提交比末尾还大：两次查询之间又写了消息、又被处理了
    assert lag_rows(TS, {"g": {("t", 0): 12}}, {("t", 0): 10})[0][-1] == 0
