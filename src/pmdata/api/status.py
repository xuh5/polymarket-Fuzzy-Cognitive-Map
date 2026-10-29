"""Status 页面的数据：读 health 程序写的几张表（pmdata.health），算好总体状态、可用率、事故列表给前端。

SQL 和纯逻辑分开：fetch_* 只查数据库，其他函数只做计算（测试不用连数据库）。
「现在」一律由调用方传进来（now），方便测试。

几个口径：
- 实时数据（live）：某一分钟有盘口指标写进数据库就算这一分钟正常（health_latency 的 rows > 0，一直保留）。
- 采集器（collector）：断线重连那几秒 + 实时数据中断的分钟。
- 归档（archive）：archiver 每 5 分钟才提交一次，连续 ARCHIVE_STALL_MINUTES 分钟已提交位置没动、
  而 topic 一直在写，才算停了。  积压监控 2026-10-29 才开始，更早的格子显示「没有数据」。
- 落后几秒 = 积压条数 ÷ 这个 topic 每秒写进多少条（「还有几秒的新数据没处理」）。不用 group 自己的处理速度：
  archiver 一阵一阵地提交，它的处理速度一会儿 0 一会儿很大。
"""
from collections import defaultdict
from datetime import timedelta

UPTIME_HOURS = 7 * 24
LIVE_STALE_SECONDS = 120        # 超过这么久没有新指标写进数据库 = 实时数据中断
LAG_SLOW_SECONDS = 30           # metrics / pg-sink 落后超过这么多秒 = 变慢
LATENCY_SLOW_MS = 10_000        # 最近 10 分钟端到端 p50 超过 10 秒 = 变慢
MISMATCH_SLOW = 0.01            # 盘口对不上超过 1% = 变慢
MONITOR_STALE_SECONDS = 300     # health 程序超过 5 分钟没写积压 = 监控本身停了
ARCHIVE_STALL_MINUTES = 15
INCIDENT_MIN_SECONDS = 60       # 停了不到 1 分钟的不列进事故
MERGE_GAP = timedelta(minutes=30)   # 同一类事故前后隔不到 30 分钟合成一条

# 断线原因（Python 异常名）→ 给外部用户看的说法
REASONS = [
    ("SSLCertVerificationError", "TLS certificate errors on the network"),
    ("Network is unreachable", "Network outage"),
    ("gaierror", "Network outage"),
    ("秒没有收到消息", "Exchange feed went silent"),
    ("ConnectionClosed", "Exchange connection dropped"),
]

# 管道各环节 → 用哪个 consumer group 的哪个 topic 算积压和处理速度
LAG_TOPICS = {"pm-metrics": "pm.raw.clob", "pm-pg-sink": "pm.metrics.book", "pm-archiver": "pm.raw.clob"}


# ---------- 查数据库 ----------

def fetch_live_age(conn):
    """最近一条指标写进数据库距今几秒；最近 10 分钟一条都没有为 None。"""
    return conn.execute(
        "select extract(epoch from now() - max(inserted_at)) as age from metrics_book "
        "where second > now() - interval '10 minutes'").fetchone()["age"]


def fetch_lag(conn):
    """各 group 最近一次的积压条数（只算 LAG_TOPICS 里的主 topic）和那次记录距今几秒。"""
    return conn.execute(
        """
        select group_id, sum(lag)::bigint as lag, extract(epoch from now() - max(ts)) as age
        from health_consumer_lag
        where ts = (select max(ts) from health_consumer_lag)
          and (group_id, topic) in (select * from unnest(%s::text[], %s::text[]))
        group by group_id
        """, (list(LAG_TOPICS), list(LAG_TOPICS.values()))).fetchall()


def fetch_rates(conn, minutes=10):
    """最近 minutes 分钟每个 (group, topic) 每秒处理多少条、topic 每秒写进多少条。"""
    return conn.execute(
        """
        select group_id, topic,
               sum(processed)::float / nullif(sum(seconds), 0) as processed_per_s,
               sum(written)::float / nullif(sum(seconds), 0) as written_per_s
        from health_throughput
        where ts > now() - make_interval(mins => %s)
        group by group_id, topic
        """, (minutes,)).fetchall()


def fetch_latency(conn, minutes):
    """最近 minutes 分钟各段延迟：每分钟的 p50 / p99 再取中位数（一分钟一行，取中位数不受个别分钟影响）。"""
    segs = ["exch_recv", "recv_comp", "comp_ins", "end_to_end"]
    cols = ", ".join(f"percentile_cont(0.5) within group (order by {s}_{q}_ms) as {s}_{q}"
                     for s in segs for q in ("p50", "p99"))
    return conn.execute(
        f"select {cols} from health_latency where minute > now() - make_interval(mins => %s) and rows > 0",
        (minutes,)).fetchone()


def fetch_quality(conn, minutes):
    return conn.execute(
        """
        select coalesce(sum(checked), 0)::bigint as checked, coalesce(sum(mismatched), 0)::bigint as mismatched,
               coalesce(sum(out_of_order), 0)::bigint as out_of_order, coalesce(sum(ignored), 0)::bigint as ignored
        from health_stream_quality where minute > now() - make_interval(mins => %s)
        """, (minutes,)).fetchone()


def fetch_live_minutes(conn, since):
    """since 之后每一分钟写进了多少条指标（没有行的分钟就是没数据）。"""
    return [r["minute"] for r in conn.execute(
        "select minute from health_latency where minute >= %s and rows > 0 order by minute", (since,)).fetchall()]


def fetch_reconnects(conn, since):
    return conn.execute(
        "select reconnected_at, disconnected_at, gap_seconds, reason from health_reconnects "
        "where reconnected_at >= %s order by reconnected_at", (since,)).fetchall()


def fetch_archiver_progress(conn, since):
    """archiver 在 pm.raw.clob 上每次记录时处理了多少、topic 写进了多少（每分钟一行）。"""
    return conn.execute(
        "select ts, processed, written from health_throughput "
        "where group_id = 'pm-archiver' and topic = 'pm.raw.clob' and ts >= %s order by ts", (since,)).fetchall()


# ---------- 计算 ----------

def lag_seconds(lag_rows, rate_rows):
    """{group: 落后几秒}。topic 没在写入而积压不为 0 时为 None（算不出来，前端显示 —）。"""
    rates = {(r["group_id"], r["topic"]): r["written_per_s"] for r in rate_rows}
    out = {}
    for r in lag_rows:
        rate = rates.get((r["group_id"], LAG_TOPICS[r["group_id"]]))
        out[r["group_id"]] = 0.0 if not r["lag"] else (r["lag"] / rate if rate else None)
    return out


def overall(live_age, lag_s, latency_10m, quality_10m, monitor_age):
    """总体状态：(level, title, 原因列表)。level：ok / degraded / down。"""
    if live_age is None or live_age > LIVE_STALE_SECONDS:
        return "down", "Live data interrupted", ["No new order book metrics in the last 2 minutes."]
    reasons = []
    for group, label in (("pm-metrics", "Metrics engine"), ("pm-pg-sink", "Database writer")):
        if group in lag_s and (lag_s[group] is None or lag_s[group] > LAG_SLOW_SECONDS):
            reasons.append(f"{label} is falling behind.")
    p50 = latency_10m.get("end_to_end_p50") if latency_10m else None
    if p50 is not None and p50 > LATENCY_SLOW_MS:
        reasons.append("End-to-end latency is higher than usual.")
    if quality_10m["checked"] and quality_10m["mismatched"] / quality_10m["checked"] > MISMATCH_SLOW:
        reasons.append("Some rebuilt order books disagree with the exchange.")
    if monitor_age is None or monitor_age > MONITOR_STALE_SECONDS:
        reasons.append("Pipeline monitoring is not reporting.")
    if reasons:
        return "degraded", "Degraded performance", reasons
    return "ok", "All systems operational", []


def stage_levels(live_age, lag_s, ingest_per_s, monitor_age):
    """管道图上每个环节的状态灯：ok / degraded / down / none（不知道）。"""
    live_down = live_age is None or live_age > LIVE_STALE_SECONDS

    def behind(group, limit):
        if group not in lag_s:
            return "none"
        s = lag_s[group]
        return "degraded" if s is None or s > limit else "ok"

    return {
        "collector": "down" if live_down and not ingest_per_s else "ok" if ingest_per_s else "none",
        "kafka": "ok" if monitor_age is not None and monitor_age <= MONITOR_STALE_SECONDS else "none",
        "metrics": "down" if live_down else behind("pm-metrics", LAG_SLOW_SECONDS),
        "database": "down" if live_down else behind("pm-pg-sink", LAG_SLOW_SECONDS),
        "archive": behind("pm-archiver", ARCHIVE_STALL_MINUTES * 60),
    }


def describe(reason):
    for needle, text in REASONS:
        if reason and needle in reason:
            return text
    return "Exchange connection dropped"


def live_gaps(minutes, start, end):
    """[start, end) 里没有实时数据的时间段 [(开始, 结束)]。minutes 是有数据的分钟（升序）。"""
    gaps, cursor = [], start
    for m in minutes:
        if m - cursor >= timedelta(minutes=1):
            gaps.append((cursor, m))
        cursor = max(cursor, m + timedelta(minutes=1))
    if end - cursor >= timedelta(minutes=1):
        gaps.append((cursor, end))
    return gaps


def archive_stalls(progress, start):
    """archiver 停了的时间段：连续 ARCHIVE_STALL_MINUTES 分钟没有处理任何消息、而 topic 一直在写。"""
    stalls, run = [], []
    for r in progress:
        if r["processed"] == 0 and r["written"] > 0:
            run.append(r["ts"])
            continue
        if len(run) >= ARCHIVE_STALL_MINUTES:
            stalls.append((run[0] - timedelta(minutes=1), run[-1]))
        run = []
    if len(run) >= ARCHIVE_STALL_MINUTES:
        stalls.append((run[0] - timedelta(minutes=1), run[-1]))
    return [(max(a, start), b) for a, b in stalls]


def _overlap(intervals, a, b):
    return sum(max(0.0, (min(y, b) - max(x, a)).total_seconds()) for x, y in intervals)


def uptime(now, live_minutes, reconnects, archive_progress, monitor_since):
    """过去 UPTIME_HOURS 小时每个组件每小时一格，加上总可用率。

    每格 {start, level, down_seconds}；level：ok（可用率 ≥ 99.9%）/ degraded（≥ 95%）/ down / none（那时还没有监控）。
    最后一格是还没过完的这一小时，只算到 now。
    """
    end = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    start = end - timedelta(hours=UPTIME_HOURS)
    # health_latency 每分钟算一次上一分钟：最近两分钟可能还没算，不当成没数据
    settled = now.replace(second=0, microsecond=0) - timedelta(minutes=2)
    first_live = live_minutes[0] if live_minutes else settled
    live_down = live_gaps(live_minutes, max(start, first_live), settled)
    conn_down = [(r["disconnected_at"], r["reconnected_at"]) for r in reconnects if r["disconnected_at"]]
    components = {
        "collector": (live_down + conn_down, first_live),
        "live": (live_down, first_live),
        "archive": (archive_stalls(archive_progress, start), monitor_since or settled),
    }
    out = {}
    for name, (down, since) in components.items():
        down = _merge(down)
        cells, total_down, total_covered = [], 0.0, 0.0
        for h in range(UPTIME_HOURS):
            a = start + timedelta(hours=h)
            b = min(a + timedelta(hours=1), settled)
            covered_from = max(a, since)
            covered = max(0.0, (b - covered_from).total_seconds())
            if covered <= 0:
                cells.append({"start": a, "level": "none", "down_seconds": 0})
                continue
            d = min(_overlap(down, covered_from, b), covered)
            ratio = 1 - d / covered
            level = "ok" if ratio >= 0.999 else "degraded" if ratio >= 0.95 else "down"
            cells.append({"start": a, "level": level, "down_seconds": round(d)})
            total_down += d
            total_covered += covered
        out[name] = {"uptime": 1 - total_down / total_covered if total_covered else None, "hours": cells}
    return out


def _merge(intervals):
    """把重叠的时间段合并，避免同一段停机算两遍。"""
    merged = []
    for a, b in sorted(intervals):
        if merged and a <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], b))
        else:
            merged.append((a, b))
    return merged


def incidents(reconnects, live_down, limit=20):
    """停了超过 INCIDENT_MIN_SECONDS 秒的事故，新的在前。同一类事故前后隔不到 MERGE_GAP 合成一条。

    实时数据中断（整机或 Docker 停了）在重连记录里只有一条「采集器启动」，没有断开时间，所以从 live_down 里补上。
    """
    events = []
    for r in reconnects:
        if r["gap_seconds"] is not None and r["gap_seconds"] >= INCIDENT_MIN_SECONDS:
            events.append((r["disconnected_at"], r["reconnected_at"], describe(r["reason"])))
    covered = [(a, b) for a, b, _ in events]
    for a, b in live_down:
        if (b - a).total_seconds() >= INCIDENT_MIN_SECONDS and _overlap(covered, a, b) < (b - a).total_seconds() / 2:
            events.append((a, b, "Live data interrupted"))
    events.sort()

    merged = []
    for a, b, title in events:
        last = merged[-1] if merged else None
        if last and last["title"] == title and a - last["end"] <= MERGE_GAP:
            last["end"] = max(last["end"], b)
            last["down_seconds"] += (b - a).total_seconds()
            last["count"] += 1
        else:
            merged.append({"title": title, "start": a, "end": b, "down_seconds": (b - a).total_seconds(), "count": 1})
    for m in merged:
        m["down_seconds"] = round(m["down_seconds"])
    return list(reversed(merged))[:limit]


def summary(conn, now):
    """/api/status 的全部内容。"""
    rates = fetch_rates(conn)
    lag_rows = fetch_lag(conn)
    lag_s = lag_seconds(lag_rows, rates)
    monitor_age = min((r["age"] for r in lag_rows), default=None)
    live_age = fetch_live_age(conn)
    latency_1h, latency_24h = fetch_latency(conn, 60), fetch_latency(conn, 1440)
    quality_1h = fetch_quality(conn, 60)
    level, title, reasons = overall(live_age, lag_s, fetch_latency(conn, 10), fetch_quality(conn, 10), monitor_age)

    since = now - timedelta(hours=UPTIME_HOURS + 1)
    live_minutes = fetch_live_minutes(conn, since)
    reconnects = fetch_reconnects(conn, since)
    progress = fetch_archiver_progress(conn, since)
    first_monitor = progress[0]["ts"] if progress else None
    up = uptime(now, live_minutes, reconnects, progress, first_monitor)
    settled = now.replace(second=0, microsecond=0) - timedelta(minutes=2)
    down = live_gaps(live_minutes, live_minutes[0], settled) if live_minutes else []

    rate = {(r["group_id"], r["topic"]): r for r in rates}
    ingest = rate.get(("pm-metrics", "pm.raw.clob"), {}).get("written_per_s")
    checked = quality_1h["checked"]
    return {
        "generated_at": now,
        "level": level, "title": title, "reasons": reasons,
        "live_age_seconds": live_age,
        "latency_24h": latency_24h,
        "latency_1h": latency_1h,
        "throughput": {
            "ingest_per_s": ingest,
            "metrics_per_s": rate.get(("pm-pg-sink", "pm.metrics.book"), {}).get("written_per_s"),
        },
        "lag_seconds": lag_s,
        "stages": stage_levels(live_age, lag_s, ingest, monitor_age),
        "quality_1h": {**quality_1h, "accuracy": 1 - quality_1h["mismatched"] / checked if checked else None},
        "uptime": up,
        "incidents": incidents(reconnects, down),
    }


RANGES = {"1h": (60, 60), "24h": (1440, 600), "7d": (10080, 3600)}   # 范围 → (多少分钟, 每格几秒)


def charts(conn, range_):
    """/api/status/charts：延迟（各段 p50 + 端到端 p99）、吞吐量、各程序落后几秒，按格取平均。"""
    minutes, bucket = RANGES[range_]
    p = {"mins": minutes, "secs": bucket, "groups": list(LAG_TOPICS), "topics": list(LAG_TOPICS.values())}
    bin_ = "date_bin(make_interval(secs => %(secs)s), {col}, timestamptz '2000-01-01')"
    latency = conn.execute(f"""
        select {bin_.format(col="minute")} as t,
               avg(exch_recv_p50_ms) as network, avg(recv_comp_p50_ms) as processing,
               avg(comp_ins_p50_ms) as storage, avg(end_to_end_p50_ms) as p50, avg(end_to_end_p99_ms) as p99
        from health_latency where minute > now() - make_interval(mins => %(mins)s) and rows > 0
        group by 1 order by 1""", p).fetchall()
    throughput = conn.execute(f"""
        select {bin_.format(col="ts")} as t,
               sum(written) filter (where group_id = 'pm-metrics' and topic = 'pm.raw.clob')::float
                 / nullif(sum(seconds) filter (where group_id = 'pm-metrics' and topic = 'pm.raw.clob'), 0) as ingest,
               sum(written) filter (where group_id = 'pm-pg-sink' and topic = 'pm.metrics.book')::float
                 / nullif(sum(seconds) filter (where group_id = 'pm-pg-sink' and topic = 'pm.metrics.book'), 0)
                 as metrics
        from health_throughput where ts > now() - make_interval(mins => %(mins)s)
        group by 1 order by 1""", p).fetchall()
    lag = conn.execute(f"""
        with main as (select * from unnest(%(groups)s::text[], %(topics)s::text[]) as m(group_id, topic)),
        l as (select ts, group_id, sum(lag) as lag from health_consumer_lag join main using (group_id, topic)
              where ts > now() - make_interval(mins => %(mins)s) group by 1, 2),
        r as (select ts, group_id, sum(written)::float / nullif(sum(seconds), 0) as rate
              from health_throughput join main using (group_id, topic)
              where ts > now() - make_interval(mins => %(mins)s) group by 1, 2)
        select {bin_.format(col="ts")} as bucket, group_id, avg(l.lag / nullif(r.rate, 0)) as value
        from l join r using (ts, group_id) group by 1, 2 order by 1""", p).fetchall()
    return {"range": range_, "bucket_seconds": bucket, "latency": latency, "throughput": throughput,
            "lag": chart_rows_by_bucket(lag, "group_id")}


def chart_rows_by_bucket(rows, key):
    """[{bucket, key, value}] → {bucket: {key: value}}，前端按时间画多条线用。"""
    out = defaultdict(dict)
    for r in rows:
        out[r["bucket"]][r[key]] = r["value"]
    return [{"t": t, **v} for t, v in sorted(out.items())]
