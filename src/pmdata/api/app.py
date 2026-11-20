"""网站后端（FastAPI）：只读 Postgres（指标、市场名录）和 Redis（当前订单簿），给前端提供 JSON 接口。

启动：python -m pmdata.api.app（本机开发默认 127.0.0.1:8000；容器里加 --host 0.0.0.0）
接口文档：启动后打开 http://localhost:8000/docs（FastAPI 自动生成）

用同步的 psycopg 连接池 + 普通 def 接口：FastAPI 会把 def 接口放进线程池执行，不会卡住其他请求；
不用 async 版 psycopg，是因为它在 Windows 默认的事件循环上跑不起来，宿主机开发不方便。
"""
import argparse
import json
import threading
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Annotated, Literal

import redis
import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from psycopg import Connection
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from pmdata.api import queries, status
from pmdata.book.cache import KEY_PREFIX
from pmdata.common.config import API_CORS_ORIGINS, PG_DSN, REDIS_URL
from pmdata.replay import bronze as replay_bronze
from pmdata.twins import edge

# open=False：导入模块时不连数据库（测试不需要 Postgres），启动服务时在 lifespan 里再打开
pool = ConnectionPool(PG_DSN, min_size=1, max_size=10, kwargs={"row_factory": dict_row}, open=False)
# redis 客户端第一次用的时候才连；超时设短，Redis 卡住时接口快速报错，不挂着
redis_client = redis.Redis.from_url(REDIS_URL, socket_timeout=1, socket_connect_timeout=1)


@asynccontextmanager
async def lifespan(app):
    pool.open()
    stop = threading.Event()
    threading.Thread(target=replay_bronze.keep_periods_fresh, args=(stop,), daemon=True).start()
    yield
    stop.set()
    pool.close()


app = FastAPI(title="Polymarket 订单簿分析 API", lifespan=lifespan)

# 前端放在 Cloudflare Pages，和后端不是同一个域名，浏览器要后端明确允许才让网页调用（CORS）
app.add_middleware(CORSMiddleware, allow_origins=API_CORS_ORIGINS, allow_methods=["GET"])
# 回放一次返回几千条变化，JSON 重复度高，压缩后小很多
app.add_middleware(GZipMiddleware, minimum_size=1000)


def get_conn():
    with pool.connection() as conn:
        yield conn


def get_redis():
    return redis_client


# 每个请求从连接池借一个连接，请求结束自动还回去
Conn = Annotated[Connection, Depends(get_conn)]
Redis = Annotated[redis.Redis, Depends(get_redis)]


@app.get("/api/health")
def health(conn: Conn):
    row = queries.latest_metric(conn)
    return {"status": "ok", "latest_metric_at": row["latest"], "age_seconds": row["age_seconds"]}


@app.get("/api/markets")
def markets(conn: Conn, r: Redis, hours: Annotated[int, Query(ge=1, le=168)] = 24):
    """twin_gap / twin_mid_gap：这个市场所有孪生里最大的缺口（吃单 / 按中间价，同 /twins 的 edge / mid_edge），
    列表上一眼看出 Related 里有没有正的。"""
    rows = queries.list_markets(conn, hours)
    best = best_twin_gaps(conn, r) if any(m.get("twins") for m in rows) else {}
    return [{**m, "twin_gap": best.get(m["market_id"], {}).get("taker"),
             "twin_mid_gap": best.get(m["market_id"], {}).get("mid")} for m in rows]


@app.get("/api/markets/{market_id}/history")
def history(conn: Conn, market_id: str, minutes: Annotated[int, Query(ge=1, le=1440)] = 60,
            bucket_seconds: Annotated[int, Query(ge=1, le=3600)] = 1):
    rows = queries.market_history(conn, market_id, minutes, bucket_seconds)
    if not rows:
        raise HTTPException(404, "这个市场在这段时间内没有盘口数据")
    return rows


@app.get("/api/markets/{market_id}/trades")
def trades(conn: Conn, market_id: str, minutes: Annotated[int, Query(ge=1, le=1440)] = 60,
           bucket_seconds: Annotated[int, Query(ge=60, le=3600)] = 60):
    """每格的成交笔数和看涨 / 看跌份数。没有成交是常态（冷门市场大部分分钟 0 成交），返回空列表，不报 404。"""
    return queries.market_trades(conn, market_id, minutes, bucket_seconds)


def tokens_or_404(conn, market_id):
    rows = queries.market_tokens(conn, market_id)
    if not rows:
        raise HTTPException(404, "没有这个市场")
    return rows


@app.get("/api/markets/{market_id}")
def market(conn: Conn, market_id: str):
    """市场的问题、截止时间，和每个选项（Yes/No）的最新盘口指标。"""
    rows = tokens_or_404(conn, market_id)
    first = rows[0]
    keep = ("outcome", "outcome_index", "asset_id", "mid", "best_bid", "best_ask", "spread", "microprice",
            "imbalance_5", "bid_depth_5c", "ask_depth_5c", "logodds_chg_60m", "updated_at")
    return {"market_id": market_id, "question": first["question"], "slug": first["slug"],
            "end_date": first["end_date"], "topic": first["topic"], "company": first["company"],
            "outcomes": [{k: r[k] for k in keep} for r in rows]}


@app.get("/api/markets/{market_id}/book")
def book(conn: Conn, r: Redis, market_id: str, outcome_index: Annotated[int, Query(ge=1)] = 1):
    """某个选项当前的完整订单簿（metrics 每秒写进 Redis 的那份）。

    盘口暂时不可用（断线后还没收到新快照、metrics 没在跑）时 available = false，不报错：前端显示「暂时不可用」。
    """
    rows = tokens_or_404(conn, market_id)
    token = next((t for t in rows if t["outcome_index"] == outcome_index), None)
    if token is None:
        raise HTTPException(404, "这个市场没有这个选项")
    try:
        raw = r.get(KEY_PREFIX + token["asset_id"])
    except redis.RedisError as e:
        raise HTTPException(503, "订单簿服务暂时不可用") from e
    base = {"asset_id": token["asset_id"], "outcome": token["outcome"]}
    if raw is None:
        return {**base, "available": False, "bids": [], "asks": [], "exchange_ts": None, "written_at": None}
    b = json.loads(raw)
    # Redis 里是字符串（Decimal 原样），给前端转成数字，画图和算滑点方便；显示用的精度足够
    return {**base, "available": True, "exchange_ts": b["exchange_ts"], "written_at": b["written_at"],
            "bids": [[float(p), float(s)] for p, s in b["bids"]],
            "asks": [[float(p), float(s)] for p, s in b["asks"]]}


@app.get("/api/markets/{market_id}/replay")
def replay(conn: Conn, market_id: str, start: datetime, minutes: Annotated[int, Query(ge=1, le=60)] = 20):
    """回放：start 起 minutes 分钟内两个选项的订单簿和成交——起点那一刻的完整订单簿 + 之后每一次变化（毫秒级）。

    格式见 pmdata.replay.timeline。数据来自 bronze，归档有几分钟延迟：这段还没归档完时 complete = false，
    只有已归档的部分。start 不带时区时按 UTC。
    """
    rows = tokens_or_404(conn, market_id)
    if start.tzinfo is None:
        start = start.replace(tzinfo=UTC)
    end = start + timedelta(minutes=minutes)
    result, complete = replay_bronze.replay(market_id, {r["asset_id"]: r["outcome_index"] for r in rows}, start, end)
    return {"market_id": market_id, "start": start, "end": end, "complete": complete,
            "outcomes": [{"outcome_index": r["outcome_index"], "outcome": r["outcome"], "asset_id": r["asset_id"],
                          "initial": result["initial"][r["outcome_index"]]} for r in rows],
            "events": result["events"]}


@app.get("/api/markets/{market_id}/replay/periods")
def replay_periods(conn: Conn, market_id: str):
    """能回放的时间段：这个市场在订阅名单里、bronze 录到数据的几段（按时间排）。回放的开始时间只能在这里面选。"""
    tokens_or_404(conn, market_id)
    return [{"start": a, "end": b} for a, b in replay_bronze.periods(market_id)]


@app.get("/api/markets/{market_id}/depth-quality")
def depth_quality(conn: Conn, market_id: str, hours: Annotated[int, Query(ge=1, le=168)] = 24):
    """假深度识别：最新一分钟的深度、可疑金额和可疑价位，加走势。这个市场最近没数据时 latest = null，不报错。"""
    tokens_or_404(conn, market_id)
    return queries.depth_quality(conn, market_id, hours)


@app.get("/api/markets/{market_id}/executions")
def executions(conn: Conn, market_id: str, hours: Annotated[int, Query(ge=1, le=168)] = 24):
    """成交质量：每笔吃单的成交前盘口、吃单成本和挂单方事后赚亏（markout）。"""
    tokens_or_404(conn, market_id)
    return queries.executions(conn, market_id, hours)


def top_of_book(r, asset_ids):
    """Redis 里当前盘口的 (买一, 卖一)；不可用的给 None。"""
    out = {}
    for aid in asset_ids:
        raw = r.get(KEY_PREFIX + aid)
        if raw is None:
            out[aid] = None
            continue
        b = json.loads(raw)
        out[aid] = (float(b["bids"][0][0]) if b["bids"] else None, float(b["asks"][0][0]) if b["asks"] else None)
    return out


def best_twin_gaps(conn, r):
    """每个市场所有孪生里最大的 edge：{市场: {"taker": .., "mid": ..}}（同一组从哪边看都一样）。
    Redis 不可用时返回空，不让列表报错。"""
    rows = queries.twins(conn)
    try:
        quotes = top_of_book(r, {x["asset_a"] for x in rows} | {x["asset_b"] for x in rows})
    except redis.RedisError:
        return {}
    best = {}
    for x in rows:
        qa, qb = quotes[x["asset_a"]], quotes[x["asset_b"]]
        if not (qa and qb):
            continue
        for kind, g in (("taker", edge.gap(x["relation"], qa, qb)), ("mid", edge.mid_gap(x["relation"], qa, qb))):
            if g is None:
                continue
            for m in (x["market_a"], x["market_b"]):
                b = best.setdefault(m, {})
                b[kind] = max(b.get(kind, g["edge"]), g["edge"])
    return best


@app.get("/api/markets/{market_id}/twins")
def twins(conn: Conn, r: Redis, market_id: str):
    """孪生市场：别的事件里、按结算规则和这个市场有严格逻辑关系的市场（AI 判断，见 pmdata.twins）。

    只列两边都在订阅、都还开着的。relation 换成从当前市场的角度（implies = 这个是 Yes → 那个一定是 Yes）。
    gap：按两边当前 Yes 的买一卖一，买一组「保证至少拿回 1」的组合要多少钱（吃单、不含手续费），
    edge > 0 就是有缺口；盘口暂时不可用时为 null。mid_edge 是同样的组合按两边中间价算的缺口。按 edge 从大到小排。
    """
    rows = queries.twins(conn, market_id)
    try:
        quotes = top_of_book(r, {x["asset_a"] for x in rows} | {x["asset_b"] for x in rows})
    except redis.RedisError as e:
        raise HTTPException(503, "订单簿服务暂时不可用") from e
    out = []
    for x in rows:
        mine = "a" if x["market_a"] == market_id else "b"
        other = "b" if mine == "a" else "a"
        relation = x["relation"] if mine == "a" else edge.flip(x["relation"])
        qm, qo = quotes[x[f"asset_{mine}"]], quotes[x[f"asset_{other}"]]
        g = edge.gap(relation, qm, qo) if qm and qo else None
        gm = edge.mid_gap(relation, qm, qo) if qm and qo else None
        out.append({
            "market_id": x[f"market_{other}"], "question": x[f"question_{other}"], "slug": x[f"slug_{other}"],
            "relation": relation, "strength": x["strength"], "condition": x["condition"],
            "differences": x["differences"], "confidence": x["confidence"], "reason": x["reason"],
            "source": x["source"], "model": x.get("model"), "judged_at": x["judged_at"],
            "this_quote": qm and {"bid": qm[0], "ask": qm[1]}, "other_quote": qo and {"bid": qo[0], "ask": qo[1]},
            "gap": g and {"this": g["a"], "other": g["b"], "cost": g["cost"], "edge": g["edge"]},
            "mid_edge": gm and gm["edge"],
        })
    out.sort(key=lambda t: (t["gap"] is None, -(t["gap"] or {}).get("edge", 0), t["question"]))
    return {"market_id": market_id, "twins": out}


@app.get("/api/signals")
def signals(conn: Conn, hours: Annotated[int, Query(ge=1, le=720)] = 168):
    """过去 hours 小时的价格跳动（候选门槛 3 个点，网页自己筛）。字段含义见 pmdata.signals.detector。"""
    return queries.list_signals(conn, hours)


@app.get("/api/status")
def pipeline_status(conn: Conn):
    """Status 页面：总体状态、各段延迟、吞吐量、积压、行情质量、过去 7 天每小时的可用率、事故列表。"""
    return status.summary(conn, datetime.now(UTC))


@app.get("/api/status/charts")
def pipeline_charts(conn: Conn, range: Literal["1h", "24h", "7d"] = "24h"):
    return status.charts(conn, range)


def main():
    parser = argparse.ArgumentParser(description="网站后端 API")
    parser.add_argument("--host", default="127.0.0.1", help="容器里用 0.0.0.0")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()
    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
