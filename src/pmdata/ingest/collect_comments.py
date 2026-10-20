"""定时轮询 Gamma API 的评论接口，把科技/AI 事件下的新评论和有变化的评论原样写入 pm.raw.comments。

官方的实时评论推送正在下线且没有替代，所以用 REST 轮询。
每轮：从 pm.raw.markets 取出所有事件 → 逐个拉该事件的全部评论 → 和已存的比对 → 只写新增或变化的。
pm.raw.comments 是 compact topic、key 为评论 ID，启动时读一遍就知道哪些评论已经存过。
"""
import argparse
import json
import logging
import time
from collections import Counter

from pmdata.common.config import TOPIC_COMMENTS
from pmdata.common.gamma import get_json
from pmdata.common.kafka import envelope, latest_by_key, make_producer
from pmdata.common.log import setup_logging
from pmdata.common.timeutil import now_iso
from pmdata.ingest.select_markets import load_latest_markets

log = logging.getLogger("collect_comments")

PAGE_SIZE = 100         # 每页 100 条主楼（limit 和 offset 都按主楼数，回复跟在主楼后面一起返回）
EVERY = 600             # 默认每 10 分钟一轮
PAUSE = 0.2             # 两个事件之间停一下，对接口温和一点


def fingerprint(comment):
    """判断一条评论有没有变化：内容、编辑时间、点赞数、举报数任一变了就算新版本。"""
    return json.dumps([comment.get(k) for k in ("body", "updatedAt", "reactionCount", "reportCount")])


def load_seen():
    """读一遍 pm.raw.comments，得到 {评论 ID: 指纹}。"""
    return {cid: fingerprint(e["payload"]) for cid, e in latest_by_key(TOPIC_COMMENTS, "collect-comments").items()}


def load_events():
    """从市场名录里取出所有事件 {事件 ID: 标题}。"""
    events = {}
    for m in load_latest_markets():
        for e in m.get("events") or []:
            events[str(e["id"])] = e.get("title")
    return events


def fetch_event_comments(event_id):
    """一页页拉某个事件的全部评论（主楼 + 回复）。"""
    comments, offset = [], 0
    while True:
        page = get_json("/comments", {
            "parent_entity_type": "Event",
            "parent_entity_id": event_id,
            "limit": PAGE_SIZE,
            "offset": offset,
            "order": "createdAt",
            "ascending": "false",
        })
        comments.extend(page)
        top_level = sum(1 for c in page if not c.get("parentCommentID"))
        if top_level < PAGE_SIZE:
            return comments
        offset += PAGE_SIZE


def run_once(producer, seen):
    started = time.time()
    events = load_events()
    stats = Counter()
    failures = []

    def on_delivery(err, msg):
        if err:
            failures.append(err)

    log.info(f"开始一轮：{len(events)} 个事件，已存评论 {len(seen)} 条")
    for event_id in events:
        try:
            comments = fetch_event_comments(event_id)
        except Exception as e:   # 一个事件失败不影响其他事件，下一轮会再拉
            stats["事件拉取失败"] += 1
            log.warning(f"事件 {event_id} 拉取失败：{type(e).__name__}: {e}")
            continue
        received_at = now_iso()
        stats["事件"] += 1
        stats["评论"] += len(comments)
        for c in comments:
            cid, fp = str(c["id"]), fingerprint(c)
            old = seen.get(cid)
            if old == fp:
                continue
            stats["新增" if old is None else "有变化"] += 1
            # payload 是接口给的原始数据，一个字都不改
            value = envelope("gamma_comments", received_at, c, event_id=event_id)
            producer.produce(TOPIC_COMMENTS, key=cid, value=value, on_delivery=on_delivery)
            producer.poll(0)
            seen[cid] = fp
        time.sleep(PAUSE)

    producer.flush(30)
    stats["写入失败"] = len(failures)
    log.info(f"一轮结束，用时 {time.time() - started:.0f} 秒  "
             + "  ".join(f"{k}={v}" for k, v in stats.items()))
    if failures:
        # 有写失败的，按 topic 实际内容重建，下一轮会把没写进去的再写一次
        return load_seen()
    return seen


def main(every):
    producer = make_producer()
    seen = load_seen()
    log.info(f"pm.raw.comments 里已有 {len(seen)} 条评论")
    while True:
        try:
            seen = run_once(producer, seen)
        except Exception as e:   # 一轮出错（读 Kafka 失败等）不影响下一轮
            if every is None:
                raise
            log.warning(f"这一轮失败：{type(e).__name__}: {e}")
        if every is None:
            return
        time.sleep(every)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--every", type=float, default=None,
                        help=f"每隔多少秒拉一轮（正式运行用 {EVERY}），不填只跑一轮")
    args = parser.parse_args()
    setup_logging()
    main(args.every)
