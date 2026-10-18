"""从 Gamma API 拉取科技/AI 类的开放市场，原样写入 Kafka 的 pm.raw.markets。

已经不在开放列表里的市场（关闭了），写一条 tombstone（value 为空）把它从 compact topic 里删掉。
加 --every 3600 就每小时拉一次，一直运行；不加只跑一次。
"""
import argparse
import json
import os
import time
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer

from select_markets import load_latest_markets

GAMMA = "https://gamma-api.polymarket.com"
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")   # 容器里设成 kafka:19092
TOPIC = "pm.raw.markets"
TAG_SLUGS = ["tech", "ai"]   # 要抓的分类标签
PAGE_SIZE = 100
MAX_GONE_RATIO = 0.2         # 一轮里消失的市场超过这个比例，就怀疑是 API 出错，不写 tombstone

session = requests.Session()


def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


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


def fetch_all():
    """拉所有标签的开放市场，按市场 ID 去重。任何一步出错都直接抛异常，不返回不完整的结果。"""
    # 同一个市场可能同时有 tech 和 ai 两个标签，用市场 ID 去重
    by_id = {}
    for slug in TAG_SLUGS:
        tag_id = get_json(f"/tags/slug/{slug}")["id"]
        found = fetch_open_markets(tag_id)
        print(f"  标签 {slug} (id={tag_id})：{len(found)} 个开放市场")
        for m in found:
            entry = by_id.setdefault(str(m["id"]), {"market": m, "tags": set()})
            entry["tags"].add(slug)
    return by_id


def run_once(producer, known_ids):
    """拉一轮并写入 Kafka，返回这一轮之后 topic 里应有的市场 ID 集合。"""
    print(f"[{now_iso()}] 开始拉取")
    by_id = fetch_all()

    failures = []

    def on_delivery(err, msg):
        if err:
            failures.append(err)

    received_at = now_iso()
    for market_id, entry in by_id.items():
        envelope = {
            "source": "gamma",
            "received_at": received_at,
            "tag_slugs": sorted(entry["tags"]),
            "payload": entry["market"],          # 交易所给的原始数据，一个字都不改
        }
        producer.produce(TOPIC, key=market_id, value=json.dumps(envelope), on_delivery=on_delivery)
        producer.poll(0)

    # 上一轮还在、这一轮没了的市场 = 已关闭
    gone = known_ids - by_id.keys()
    if known_ids and len(gone) > MAX_GONE_RATIO * len(known_ids):
        print(f"  警告：{len(gone)}/{len(known_ids)} 个市场同时消失，超过 {MAX_GONE_RATIO:.0%}，"
              f"怀疑 API 出错，这一轮不写 tombstone")
        gone = set()
    for market_id in gone:
        producer.produce(TOPIC, key=market_id, value=None, on_delivery=on_delivery)
        producer.poll(0)

    producer.flush(30)
    print(f"  写入 {len(by_id)} 条，tombstone {len(gone)} 条，失败 {len(failures)} 条")
    if failures:
        # 有写失败的，下一轮按 topic 实际状态重新对比，不能假设都写进去了
        return {str(m["id"]) for m in load_latest_markets()}
    return set(by_id) | (known_ids - by_id.keys() - gone)


def main(every):
    producer = Producer({
        "bootstrap.servers": BOOTSTRAP,
        "enable.idempotence": True,   # 防止网络重试导致重复写入
        "compression.type": "lz4",
    })
    # 启动时先读一遍 topic，知道目前名录里有哪些市场，才能判断哪些消失了
    known_ids = {str(m["id"]) for m in load_latest_markets()}
    print(f"名录里现有 {len(known_ids)} 个市场")
    while True:
        try:
            known_ids = run_once(producer, known_ids)
        except Exception as e:   # 一轮出错（断网、API 挂了）不影响下一轮
            if every is None:
                raise
            print(f"  这一轮失败：{type(e).__name__}: {e}")
        if every is None:
            return
        time.sleep(every)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--every", type=float, default=None, help="每隔多少秒拉一次，不填只跑一次")
    args = parser.parse_args()
    main(args.every)
