import json
from datetime import UTC, datetime

from pmdata.health.lag import lag_rows
from pmdata.health.reconnects import reconnect_rows

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


def env(payload, conn_id="c1", source="collector"):
    return json.dumps({"source": source, "received_at": "x", "conn_id": conn_id, "seq": None,
                       "frame_index": 0, "payload": payload}).encode()


def gap(market, reason, down="2026-10-29T10:00:00.000+00:00", back="2026-10-29T10:00:01.600+00:00"):
    return {"event_type": "gap", "market": market, "disconnected_at": down, "reconnected_at": back, "reason": reason}


def test_reconnect_rows_one_per_connection():
    rows = reconnect_rows([
        env(gap("m1", "ConnectionClosedError: x")),
        env(gap("m2", "ConnectionClosedError: x")),               # 同一次重连的另一个市场
        env({"event_type": "book", "market": "m1"}, source="clob_ws"),
        env(gap("m3", "加入订阅名单", down=None)),                  # 名单刷新，不是断线
        env(gap("m1", "collector 启动", down=None), conn_id="c2"),  # 刚启动：没有断开时间
        None,
    ])
    assert set(rows) == {"c1", "c2"}
    assert rows["c1"][3] == 1.6 and rows["c1"][4] == "ConnectionClosedError: x"
    assert rows["c2"][2] is None and rows["c2"][3] is None
