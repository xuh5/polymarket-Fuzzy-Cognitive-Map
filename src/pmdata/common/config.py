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

# 归档的 Parquet 文件放在这里（宿主机默认是项目下的 data/，容器里由 docker-compose 设成挂载进来的目录）
DATA_DIR = Path(os.environ.get("PM_DATA_DIR", "data"))
