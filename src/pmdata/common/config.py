"""全局配置：Kafka 地址、topic 名、外部接口地址，只在这里写一次。"""
import os

# 宿主机直接跑用 localhost:9092；容器里由 docker-compose 设成 kafka:19092
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")

TOPIC_MARKETS = "pm.raw.markets"
TOPIC_CLOB = "pm.raw.clob"
TOPIC_COMMENTS = "pm.raw.comments"

GAMMA_URL = "https://gamma-api.polymarket.com"
CLOB_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
CLOB_REST_URL = "https://clob.polymarket.com"
