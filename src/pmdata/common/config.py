"""全局配置：Kafka 地址、topic 名、外部接口地址，只在这里写一次。"""
import os
from pathlib import Path

from dotenv import load_dotenv

# 宿主机直接跑用 localhost:9092；容器里由 docker-compose 设成 kafka:19092
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")

TOPIC_MARKETS = "pm.raw.markets"
TOPIC_CLOB = "pm.raw.clob"
TOPIC_COMMENTS = "pm.raw.comments"
TOPIC_METRICS_BOOK = "pm.metrics.book"       # 盘口类指标，按 token 每秒最多一条
TOPIC_METRICS_TRADES = "pm.metrics.trades"   # 成交类指标，按 token 每分钟一条

GAMMA_URL = "https://gamma-api.polymarket.com"
CLOB_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
CLOB_REST_URL = "https://clob.polymarket.com"

# Postgres（阶段 3 看板用）。密码只放在项目根目录的 .env 里（不进 Git）：宿主机程序由 load_dotenv 读进来，
# 容器由 docker-compose 从同一个 .env 传进去。宿主机连 localhost，容器里 PG_HOST 设成 postgres
load_dotenv(Path(__file__).resolve().parents[3] / ".env")
PG_DSN = (f"host={os.environ.get('PG_HOST', 'localhost')} port=5432 dbname=pm user=pm "
          f"password={os.environ.get('POSTGRES_PASSWORD', '')}")

# Redis：当前订单簿的缓存（metrics 写、网站后端读）。容器里设成 redis://redis:6379/0
# 宿主机用 127.0.0.1 不用 localhost：Windows 上 localhost 先试 IPv6，Redis 只绑了 IPv4，每次连接白等约 2 秒
REDIS_URL = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379/0")

# 网站后端允许哪些网页来调用（逗号分隔）；默认是本机前端开发服务器的地址
API_CORS_ORIGINS = os.environ.get("API_CORS_ORIGINS", "http://localhost:5173").split(",")

# 归档的 Parquet 文件放在这里（宿主机默认是项目下的 data/，容器里由 docker-compose 设成挂载进来的目录）
DATA_DIR = Path(os.environ.get("PM_DATA_DIR", "data"))
