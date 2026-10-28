"""网站后端（FastAPI）：只读 Postgres，给前端提供 JSON 接口。

启动：python -m pmdata.api.app（本机开发默认 127.0.0.1:8000；容器里加 --host 0.0.0.0）
接口文档：启动后打开 http://localhost:8000/docs（FastAPI 自动生成）

用同步的 psycopg 连接池 + 普通 def 接口：FastAPI 会把 def 接口放进线程池执行，不会卡住其他请求；
不用 async 版 psycopg，是因为它在 Windows 默认的事件循环上跑不起来，宿主机开发不方便。
"""
import argparse
from contextlib import asynccontextmanager
from typing import Annotated

import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from psycopg import Connection
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from pmdata.api import queries
from pmdata.common.config import API_CORS_ORIGINS, PG_DSN

# open=False：导入模块时不连数据库（测试不需要 Postgres），启动服务时在 lifespan 里再打开
pool = ConnectionPool(PG_DSN, min_size=1, max_size=10, kwargs={"row_factory": dict_row}, open=False)


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


# 每个请求从连接池借一个连接，请求结束自动还回去
Conn = Annotated[Connection, Depends(get_conn)]


@app.get("/api/health")
def health(conn: Conn):
    row = queries.latest_metric(conn)
    return {"status": "ok", "latest_metric_at": row["latest"], "age_seconds": row["age_seconds"]}


@app.get("/api/markets")
def markets(conn: Conn, hours: Annotated[int, Query(ge=1, le=168)] = 24):
    return queries.list_markets(conn, hours)


@app.get("/api/markets/{market_id}/history")
def history(conn: Conn, market_id: str, minutes: Annotated[int, Query(ge=1, le=1440)] = 60):
    rows = queries.market_history(conn, market_id, minutes)
    if not rows:
        raise HTTPException(404, "这个市场在这段时间内没有盘口数据")
    return rows


def main():
    parser = argparse.ArgumentParser(description="网站后端 API")
    parser.add_argument("--host", default="127.0.0.1", help="容器里用 0.0.0.0")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()
    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
