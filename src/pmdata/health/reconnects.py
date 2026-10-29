"""采集器断线重连记录：从 pm.raw.clob 里挑出缺口标记，一次重连一行。

采集器每次连上 WebSocket，都会给每个订阅市场各写一条缺口标记（同一个 conn_id），这里按 conn_id 合成一次重连。
「加入订阅名单」也会写缺口标记，但那是名单刷新、不是断线，不算。
停了多久 = reconnected_at − disconnected_at；disconnected_at 是采集器发现断线的时间，
「30 秒没收到消息」这种情况真正断开还要再往前 30 秒。采集器刚启动时 disconnected_at 为空。
"""
import json

from pmdata.common.timeutil import parse_time

SCHEMA = """
create table if not exists health_reconnects (
    conn_id text primary key,           -- 重连后的新连接
    reconnected_at timestamptz not null,
    disconnected_at timestamptz,        -- 采集器刚启动时为空
    gap_seconds double precision,
    reason text
);
create index if not exists health_reconnects_at on health_reconnects (reconnected_at);
"""

# 同一次重连每个市场一条标记，分在不同分区、不同批次里，重复的直接忽略
INSERT = ("insert into health_reconnects (conn_id, reconnected_at, disconnected_at, gap_seconds, reason) "
          "values (%s, %s, %s, %s, %s) on conflict (conn_id) do nothing")

# 采集器自己写的消息，信封开头就是这个；先比字节、不用每条都解析 JSON（clob 每秒上百条，缺口标记一天才几十条）
COLLECTOR_PREFIX = b'{"source": "collector"'
NOT_RECONNECT = "加入订阅名单"


def reconnect_rows(values):
    """一批 pm.raw.clob 消息原文 → {conn_id: 行}。"""
    rows = {}
    for v in values:
        if not v or not v.startswith(COLLECTOR_PREFIX):
            continue
        env = json.loads(v)
        p = env["payload"]
        if p.get("event_type") != "gap" or p.get("reason") == NOT_RECONNECT:
            continue
        conn = env["conn_id"]
        back = parse_time(p["reconnected_at"])
        down = parse_time(p["disconnected_at"]) if p.get("disconnected_at") else None
        gap = (back - down).total_seconds() if down else None
        rows[conn] = (conn, back, down, gap, p.get("reason"))
    return rows
