from datetime import UTC, datetime, timedelta

from pmdata.api.status import archive_stalls, describe, incidents, lag_seconds, live_gaps, overall, uptime

T0 = datetime(2026, 10, 29, 10, 0, tzinfo=UTC)


def mins(*ms):
    return [T0 + timedelta(minutes=m) for m in ms]


def test_live_gaps():
    assert live_gaps(mins(0, 1, 2, 5, 6), T0, T0 + timedelta(minutes=8)) == [
        (T0 + timedelta(minutes=3), T0 + timedelta(minutes=5)),
        (T0 + timedelta(minutes=7), T0 + timedelta(minutes=8)),
    ]


def test_lag_seconds_uses_topic_write_rate():
    lag = [{"group_id": "pm-metrics", "lag": 100}, {"group_id": "pm-pg-sink", "lag": 0},
           {"group_id": "pm-archiver", "lag": 50}]
    rates = [{"group_id": "pm-metrics", "topic": "pm.raw.clob", "written_per_s": 50.0},
             {"group_id": "pm-archiver", "topic": "pm.raw.clob", "written_per_s": 0.0}]
    assert lag_seconds(lag, rates) == {"pm-metrics": 2.0, "pm-pg-sink": 0.0, "pm-archiver": None}


def test_overall_levels():
    good_q = {"checked": 100, "mismatched": 0}
    assert overall(1.0, {"pm-metrics": 2.0}, {"end_to_end_p50": 2000}, good_q, 30)[0] == "ok"
    assert overall(None, {}, None, good_q, 30)[0] == "down"
    level, _, reasons = overall(1.0, {"pm-metrics": 60.0}, {"end_to_end_p50": 2000},
                                {"checked": 100, "mismatched": 5}, 30)
    assert level == "degraded" and len(reasons) == 2
    assert overall(1.0, {}, None, good_q, None)[0] == "degraded"   # 监控本身没在报告


def test_describe_reasons():
    assert describe("SSLCertVerificationError: x") == "TLS certificate errors on the network"
    assert describe("TimeoutError? 30 秒没有收到消息") == "Exchange feed went silent"
    assert describe(None) == "Exchange connection dropped"


def test_archive_stall_needs_15_quiet_minutes():
    rows = [{"ts": t, "processed": 0, "written": 10} for t in mins(*range(1, 17))]
    rows.append({"ts": T0 + timedelta(minutes=17), "processed": 5000, "written": 10})
    assert archive_stalls(rows, T0) == [(T0, T0 + timedelta(minutes=16))]
    assert archive_stalls(rows[:5] + rows[-1:], T0) == []   # 5 分钟一提交，正常


def test_incidents_merge_and_fill_live_gaps():
    def rc(start_min, secs, reason):
        down = T0 + timedelta(minutes=start_min)
        return {"disconnected_at": down, "reconnected_at": down + timedelta(seconds=secs),
                "gap_seconds": secs, "reason": reason}
    reconnects = [rc(0, 64, "SSLCertVerificationError"), rc(20, 30, "SSLCertVerificationError"),
                  rc(25, 1.6, "ConnectionClosedError"), rc(30, 70, "SSLCertVerificationError")]
    host_off = (T0 + timedelta(hours=2), T0 + timedelta(hours=5))
    out = incidents(reconnects, [host_off])
    assert [(i["title"], i["count"]) for i in out] == [
        ("Live data interrupted", 1), ("TLS certificate errors on the network", 2)]
    assert out[1]["down_seconds"] == 134


def test_uptime_cells():
    now = T0 + timedelta(minutes=30)
    live = mins(*range(-UPTIME_MINUTES, 28))
    live = [m for m in live if not (T0 - timedelta(minutes=50) <= m < T0 - timedelta(minutes=20))]   # 停了 30 分钟
    up = uptime(now, live, [], [], None)
    cells = up["live"]["hours"]
    assert len(cells) == 168
    assert cells[-2]["level"] == "down" and cells[-2]["down_seconds"] == 1800
    assert cells[-1]["level"] == "ok"
    assert up["archive"]["hours"][-1]["level"] == "none"   # 还没有归档监控数据


UPTIME_MINUTES = 168 * 60


def test_stage_levels():
    from pmdata.api.status import stage_levels
    ok = stage_levels(1.0, {"pm-metrics": 3.0, "pm-pg-sink": 1.0, "pm-archiver": 200.0}, 60.0, 20)
    assert set(ok.values()) == {"ok"}
    down = stage_levels(None, {}, 0.0, None)
    assert down["collector"] == "down" and down["metrics"] == "down" and down["kafka"] == "none"
    assert stage_levels(1.0, {"pm-archiver": 2000.0}, 60.0, 20)["archive"] == "degraded"
