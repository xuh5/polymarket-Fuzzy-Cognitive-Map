"""网站后端（FastAPI）：只读 Postgres（指标、市场名录）和 Redis（当前订单簿），给前端提供 JSON 接口。

启动：python -m pmdata.api.app（本机开发默认 127.0.0.1:8000；容器里加 --host 0.0.0.0）
接口文档：启动后打开 http://localhost:8000/docs（FastAPI 自动生成）

用同步的 psycopg 连接池 + 普通 def 接口：FastAPI 会把 def 接口放进线程池执行，不会卡住其他请求；
不用 async 版 psycopg，是因为它在 Windows 默认的事件循环上跑不起来，宿主机开发不方便。
"""
import argparse
import json
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Annotated, Literal

import redis
import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from psycopg import Connection
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from pmdata.api import queries, status
from pmdata.book.cache import KEY_PREFIX
from pmdata.common.config import API_CORS_ORIGINS, PG_DSN, REDIS_URL

# open=False：导入模块时不连数据库（测试不需要 Postgres），启动服务时在 lifespan 里再打开
pool = ConnectionPool(PG_DSN, min_size=1, max_size=10, kwargs={"row_factory": dict_row}, open=False)
# redis 客户端第一次用的时候才连；超时设短，Redis 卡住时接口快速报错，不挂着
redis_client = redis.Redis.from_url(REDIS_URL, socket_timeout=1, socket_connect_timeout=1)


@asynccontextmanager
async def lifespan(app):
    pool.open()
    yield
    pool.close()


app = FastAPI(title="Polymarket 订单簿分析 API", lifespan=lifespan)

# 前端放在 Cloudflare Pages，和后端不是同一个域名，浏览器要后端明确允许才让网页调用（CORS）
app.add_middleware(CORSMiddleware, allow_origins=API_CORS_ORIGINS, allow_methods=["GET"])


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
def markets(conn: Conn, hours: Annotated[int, Query(ge=1, le=168)] = 24):
    return queries.list_markets(conn, hours)


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
            "end_date": first["end_date"], "outcomes": [{k: r[k] for k in keep} for r in rows]}


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
