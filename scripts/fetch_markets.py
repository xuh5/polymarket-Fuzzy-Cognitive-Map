"""从 Gamma API 拉取科技/AI 类的开放市场，原样写入 Kafka 的 pm.raw.markets。"""
import json
import time
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer

GAMMA = "https://gamma-api.polymarket.com"
BOOTSTRAP = "localhost:9092"
TOPIC = "pm.raw.markets"
TAG_SLUGS = ["tech", "ai"]   # 要抓的分类标签
PAGE_SIZE = 100

session = requests.Session()


def get_json(path, params=None):
    """调用接口；遇到限流(429)或服务器错误(5xx)就等一会儿重试。"""
    for attempt in range(5):
        resp = session.get(f"{GAMMA}{path}", params=params, timeout=15)
        if resp.status_code == 429 or resp.status_code >= 500:
            wait = 2 ** attempt
            print(f"  收到 {resp.status_code}，{wait} 秒后重试")
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp.json()
    resp.raise_for_status()


def resolve_tag_id(slug):
    """把标签名（如 'ai'）换成数字 ID。"""
    try:
        return get_json(f"/tags/slug/{slug}")["id"]
    except requests.HTTPError as e:
        print(f"找不到标签 {slug}：{e}")
        return None


def fetch_open_markets(tag_id):
    """按标签一页页翻，拿到所有开放中的市场。"""
    markets, cursor = [], None
    while True:
        params = {"tag_id": tag_id, "closed": "false", "limit": PAGE_SIZE}
        if cursor:
            params["after_cursor"] = cursor
        page = get_json("/markets/keyset", params)
        batch = page.get("markets", [])
        markets.extend(batch)
        cursor = page.get("next_cursor")
        if not cursor or not batch:
            return markets


def main():
    # 同一个市场可能同时有 tech 和 ai 两个标签，用市场 ID 去重
    by_id = {}
    for slug in TAG_SLUGS:
        tag_id = resolve_tag_id(slug)
        if tag_id is None:
            continue
        found = fetch_open_markets(tag_id)
        print(f"标签 {slug} (id={tag_id})：{len(found)} 个开放市场")
        for m in found:
            entry = by_id.setdefault(str(m["id"]), {"market": m, "tags": set()})
            entry["tags"].add(slug)

    producer = Producer({
        "bootstrap.servers": BOOTSTRAP,
        "enable.idempotence": True,   # 防止网络重试导致重复写入
        "compression.type": "lz4",
    })
    failures = []

    def on_delivery(err, msg):
        if err:
            failures.append(err)

    received_at = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
    for market_id, entry in by_id.items():
        envelope = {
            "source": "gamma",
            "received_at": received_at,
            "tag_slugs": sorted(entry["tags"]),
            "payload": entry["market"],          # 交易所给的原始数据，一个字都不改
        }
        producer.produce(TOPIC, key=market_id, value=json.dumps(envelope), on_delivery=on_delivery)
        producer.poll(0)

    producer.flush(30)
    print(f"写入 {len(by_id)} 条，失败 {len(failures)} 条")
    print("前 10 个市场：")
    for entry in list(by_id.values())[:10]:
        print("  -", entry["market"].get("question"))


if __name__ == "__main__":
    main()