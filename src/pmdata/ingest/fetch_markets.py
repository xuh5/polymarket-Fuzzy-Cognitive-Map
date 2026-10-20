"""从 Gamma API 拉取科技/AI 类的开放市场，原样写入 Kafka 的 pm.raw.markets。

已经不在开放列表里的市场（关闭了），写一条 tombstone（value 为空）把它从 compact topic 里删掉。
加 --every 3600 就每小时拉一次，一直运行；不加只跑一次。
"""
import argparse
import logging
import time

from pmdata.common.config import TOPIC_MARKETS
from pmdata.common.gamma import get_json
from pmdata.common.kafka import envelope, make_producer
from pmdata.common.log import setup_logging
from pmdata.common.timeutil import now_iso
from pmdata.ingest.select_markets import load_latest_markets

log = logging.getLogger("fetch_markets")

TAG_SLUGS = ["tech", "ai"]   # 要抓的分类标签
PAGE_SIZE = 100
MAX_GONE_RATIO = 0.2         # 一轮里消失的市场超过这个比例，就怀疑是 API 出错，不写 tombstone


def find_gone(known_ids, fetched_ids, max_ratio=MAX_GONE_RATIO):
    """上一轮还在、这一轮没了的市场 = 已关闭，要写 tombstone。

    返回 (要写 tombstone 的 ID 集合, 是否因为消失太多而放弃)。一次消失超过 max_ratio 时
    怀疑是 API 出错（返回了不完整的列表），这一轮一个都不删。
    """
    gone = set(known_ids) - set(fetched_ids)
    if known_ids and len(gone) > max_ratio * len(known_ids):
        return set(), True
    return gone, False


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
        log.info(f"标签 {slug} (id={tag_id})：{len(found)} 个开放市场")
        for m in found:
            entry = by_id.setdefault(str(m["id"]), {"market": m, "tags": set()})
            entry["tags"].add(slug)
    return by_id


def run_once(producer, known_ids):
    """拉一轮并写入 Kafka，返回这一轮之后 topic 里应有的市场 ID 集合。"""
    log.info("开始拉取")
    by_id = fetch_all()

    failures = []

    def on_delivery(err, msg):
        if err:
            failures.append(err)

    received_at = now_iso()
    for market_id, entry in by_id.items():
        # payload 是交易所给的原始数据，一个字都不改
        value = envelope("gamma", received_at, entry["market"], tag_slugs=sorted(entry["tags"]))
        producer.produce(TOPIC_MARKETS, key=market_id, value=value, on_delivery=on_delivery)
        producer.poll(0)

    gone, suspicious = find_gone(known_ids, by_id.keys())
    if suspicious:
        log.warning(f"{len(known_ids - by_id.keys())}/{len(known_ids)} 个市场同时消失，超过 {MAX_GONE_RATIO:.0%}，"
                    f"怀疑 API 出错，这一轮不写 tombstone")
    for market_id in gone:
        producer.produce(TOPIC_MARKETS, key=market_id, value=None, on_delivery=on_delivery)
        producer.poll(0)

    producer.flush(30)
    log.info(f"写入 {len(by_id)} 条，tombstone {len(gone)} 条，失败 {len(failures)} 条")
    if failures:
        # 有写失败的，下一轮按 topic 实际状态重新对比，不能假设都写进去了
        return {str(m["id"]) for m in load_latest_markets()}
    return set(by_id) | (known_ids - by_id.keys() - gone)


def main(every):
    producer = make_producer()
    # 启动时先读一遍 topic，知道目前名录里有哪些市场，才能判断哪些消失了
    known_ids = {str(m["id"]) for m in load_latest_markets()}
    log.info(f"名录里现有 {len(known_ids)} 个市场")
    while True:
        try:
            known_ids = run_once(producer, known_ids)
        except Exception as e:   # 一轮出错（断网、API 挂了）不影响下一轮
            if every is None:
                raise
            log.warning(f"这一轮失败：{type(e).__name__}: {e}")
        if every is None:
            return
        time.sleep(every)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--every", type=float, default=None, help="每隔多少秒拉一次，不填只跑一次")
    args = parser.parse_args()
    setup_logging()
    main(args.every)
