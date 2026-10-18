"""订阅 CLOB WebSocket market 频道，把订单簿和成交原样写入 Kafka 的 pm.raw.clob。

订阅名单启动时用 select_markets 筛一次，之后每隔 --refresh-every 秒从 pm.raw.markets 重新筛，
用动态 subscribe/unsubscribe 增减 token，不断开连接。
"""
import argparse
import asyncio
import json
import os
import time
import uuid
from collections import Counter
from datetime import datetime, timezone

import requests
import websockets
from confluent_kafka import Producer

from select_markets import load_latest_markets, select_markets

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
REST_BOOKS_URL = "https://clob.polymarket.com/books"
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")   # 容器里设成 kafka:19092
TOPIC = "pm.raw.clob"

PING_EVERY = 10        # 官方要求每 10 秒发一次 PING
SILENCE_TIMEOUT = 30   # 这么久什么都没收到，就当连接已经死了
BOOK_WAIT = 10         # 订阅后等这么久还没收到 book 快照，就用 REST 补拉
BACKOFF_MAX = 60       # 重连等待最长 60 秒
REST_BATCH = 500       # POST /books 一次最多 500 个
STATS_EVERY = 60       # 每分钟打印一次统计
REFRESH_EVERY = 3600   # 默认每小时重新筛一次订阅名单


def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def to_asset_map(selected):
    """筛选结果 → {token: 市场 condition ID}"""
    return {t["token_id"]: s["condition_id"] for s in selected for t in s["tokens"]}


class Collector:
    def __init__(self, selected):
        # Kafka key 用市场 ID：一条 price_change 里经常同时有 Yes/No 两个 token，按市场分区才能保证同一市场的顺序
        self.subscribed = to_asset_map(selected)        # 当前订阅的 token → 市场
        self.asset_to_market = dict(self.subscribed)    # 见过的所有 token → 市场，只增不减（退订后迟到的消息也能换算 key）
        self.questions = {s["condition_id"]: s["question"] for s in selected}
        self.producer = Producer({
            "bootstrap.servers": BOOTSTRAP,
            "enable.idempotence": True,
            "compression.type": "lz4",
        })
        self.http = requests.Session()
        self.stats = Counter()
        self.ws = None                # 当前连接；没连上时为 None
        self.conn_id = None
        self.seq = 0                  # 当前连接内收到的第几帧（PONG 不算）
        self.books_seen = set()       # 当前连接内收到过 book 快照的 token
        self.disconnected_at = None   # 上次断开的时间；刚启动时为 None
        self.disconnect_reason = "collector 启动"
        self.background = set()       # 后台任务的引用，防止被垃圾回收

    @property
    def markets(self):
        return sorted(set(self.subscribed.values()))

    # ---------- 写 Kafka ----------

    def on_delivery(self, err, msg):
        if err:
            self.stats["投递失败"] += 1
            print(f"写入 Kafka 失败：{err}")

    def send(self, key, source, payload, received_at, seq, frame_index):
        envelope = {
            "source": source,
            "received_at": received_at,
            "conn_id": self.conn_id,
            "seq": seq,
            "frame_index": frame_index,
            "payload": payload,          # 交易所给的原始数据，一个字都不改
        }
        self.producer.produce(TOPIC, key=key, value=json.dumps(envelope), on_delivery=self.on_delivery)
        self.producer.poll(0)

    def write_gap_markers(self, markets, disconnected_at, reason):
        """给每个市场写一条缺口标记，告诉下游：之前的订单簿状态作废，等新快照。"""
        reconnected_at = now_iso()
        for i, market in enumerate(markets):
            payload = {
                "event_type": "gap",
                "market": market,
                "disconnected_at": disconnected_at,   # 刚启动或新加入订阅时为 None
                "reconnected_at": reconnected_at,
                "reason": reason,
            }
            self.send(market, "collector", payload, reconnected_at, None, i)

    def write_unsubscribed_markers(self, markets, reason):
        """被移出名单的市场写一条标记，告诉下游：后面没数据是正常停止，不是丢了。"""
        at = now_iso()
        for i, market in enumerate(markets):
            payload = {"event_type": "unsubscribed", "market": market, "at": at, "reason": reason}
            self.send(market, "collector", payload, at, None, i)

    # ---------- 处理 WebSocket 消息 ----------

    def handle_frame(self, raw, received_at):
        self.seq += 1
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            self.stats["非 JSON 帧"] += 1
            print(f"收到非 JSON 帧：{raw[:200]}")
            return
        # 一帧可能是一个事件，也可能是一个事件数组（比如订阅后的那批 book）；拆成一个事件一条消息
        events = data if isinstance(data, list) else [data]
        for i, event in enumerate(events):
            event_type = event.get("event_type", "未知")
            self.stats[event_type] += 1
            if event_type == "new_market":
                # 全平台的新市场都会推过来，跟订阅名单无关；新市场随时能从 Gamma API 拉回来，只计数不存
                continue
            if event_type == "book":
                self.books_seen.add(event.get("asset_id"))
            self.send(self.market_key(event), "clob_ws", event, received_at, self.seq, i)

    def market_key(self, event):
        """Kafka key 一律是市场 ID：优先用事件里的 market，没有就用 asset_id 换算。

        都拿不到时计数报警，key 为 None（消息照样写入，但不保证和该市场其他消息同分区有序）。
        """
        key = event.get("market") or self.asset_to_market.get(event.get("asset_id"))
        if key is None:
            self.stats["无法确定市场"] += 1
            print(f"警告：事件没有 market 字段，asset_id 也不在订阅名单里：{json.dumps(event)[:200]}")
        return key

    # ---------- REST 补拉快照 ----------

    def fetch_rest_books(self, asset_ids):
        """调 POST /books 补拉订单簿快照，写进同一个 topic（source=clob_rest）。"""
        for start in range(0, len(asset_ids), REST_BATCH):
            batch = asset_ids[start:start + REST_BATCH]
            body = [{"token_id": a} for a in batch]
            for attempt in range(3):
                try:
                    resp = self.http.post(REST_BOOKS_URL, json=body, timeout=15)
                    if resp.status_code == 429 or resp.status_code >= 500:
                        raise requests.HTTPError(f"HTTP {resp.status_code}")
                    resp.raise_for_status()
                    break
                except requests.RequestException as e:
                    print(f"  REST 补拉失败（{e}），{2 ** attempt} 秒后重试")
                    time.sleep(2 ** attempt)
            else:
                self.stats["REST 补拉失败"] += len(batch)
                continue
            received_at = now_iso()
            for i, book in enumerate(resp.json()):
                self.stats["REST 补拉"] += 1
                self.books_seen.add(book.get("asset_id"))
                self.send(self.market_key(book), "clob_rest", book, received_at, None, i)

    async def check_books(self, asset_ids):
        """订阅后等 BOOK_WAIT 秒，这些 token 里还没收到 book 快照的，用 REST 补拉。"""
        await asyncio.sleep(BOOK_WAIT)
        missing = [a for a in asset_ids if a not in self.books_seen and a in self.subscribed]
        if missing:
            print(f"{BOOK_WAIT} 秒内有 {len(missing)} 个 token 没收到 book 快照，用 REST 补拉")
            await asyncio.to_thread(self.fetch_rest_books, missing)

    def in_background(self, coro):
        task = asyncio.create_task(coro)
        self.background.add(task)
        task.add_done_callback(self.background.discard)

    # ---------- 订阅名单刷新 ----------

    async def refresh_loop(self, every):
        while True:
            await asyncio.sleep(every)
            try:
                selected = await asyncio.to_thread(lambda: select_markets(load_latest_markets()))
            except Exception as e:   # 读 Kafka 出错就继续用旧名单
                print(f"[{now_iso()}] 刷新名单失败，继续用旧名单：{type(e).__name__}: {e}")
                continue
            await self.apply_selection(selected)

    async def apply_selection(self, selected):
        """新名单和旧名单比一比：新增的 subscribe，被挤掉的 unsubscribe。"""
        new = to_asset_map(selected)
        if not new:
            print(f"[{now_iso()}] 新名单是空的，可能是数据有问题，继续用旧名单")
            return
        added = [a for a in new if a not in self.subscribed]
        removed = [a for a in self.subscribed if a not in new]
        if not added and not removed:
            print(f"[{now_iso()}] 刷新名单：没有变化")
            return

        old_markets, new_markets = set(self.subscribed.values()), set(new.values())
        added_markets, removed_markets = sorted(new_markets - old_markets), sorted(old_markets - new_markets)
        self.questions.update({s["condition_id"]: s["question"] for s in selected})
        self.subscribed = new
        self.asset_to_market.update(new)
        self.stats["名单刷新"] += 1
        print(f"[{now_iso()}] 刷新名单：加入 {len(added_markets)} 个市场，移出 {len(removed_markets)} 个")
        for m in added_markets:
            print(f"  + {self.questions.get(m)}")
        for m in removed_markets:
            print(f"  - {self.questions.get(m)}")

        self.write_unsubscribed_markers(removed_markets, "移出订阅名单")
        self.write_gap_markers(added_markets, None, "加入订阅名单")
        if self.ws is None:
            return   # 没连着；下次连上时会按新名单整体订阅
        try:
            if removed:
                await self.ws.send(json.dumps({"assets_ids": removed, "operation": "unsubscribe"}))
            if added:
                await self.ws.send(json.dumps({"assets_ids": added, "operation": "subscribe"}))
                self.in_background(self.check_books(added))
        except Exception as e:   # 连接刚好断了；重连时会按新名单整体订阅
            print(f"  发送订阅变更失败（{type(e).__name__}），等重连后按新名单订阅")

    # ---------- 连接与重连 ----------

    async def pinger(self, ws):
        while True:
            await asyncio.sleep(PING_EVERY)
            await ws.send("PING")

    async def session(self, ws):
        """一次连接的完整生命周期：写缺口标记 → 订阅 → 收消息，直到出错。"""
        self.conn_id = uuid.uuid4().hex[:12]
        self.seq = 0
        self.books_seen = set()
        self.stats["连接次数"] += 1
        print(f"[{now_iso()}] 已连接 conn_id={self.conn_id}，订阅 {len(self.subscribed)} 个 token")
        self.write_gap_markers(self.markets, self.disconnected_at, self.disconnect_reason)
        self.disconnected_at = None
        assets = list(self.subscribed)
        await ws.send(json.dumps({
            "assets_ids": assets,
            "type": "market",
            "custom_feature_enabled": True,   # 多推 best_bid_ask / new_market / market_resolved（new_market 不存）
        }))
        self.ws = ws
        helpers = [asyncio.create_task(self.pinger(ws)), asyncio.create_task(self.check_books(assets))]
        try:
            while True:
                # 超过 SILENCE_TIMEOUT 秒没有任何消息（包括 PONG）就抛 TimeoutError，触发重连
                raw = await asyncio.wait_for(ws.recv(), SILENCE_TIMEOUT)
                if raw == "PONG":
                    continue
                self.handle_frame(raw, now_iso())
        finally:
            self.ws = None
            for t in helpers:
                t.cancel()

    async def run(self):
        backoff = 1
        while True:
            try:
                # 关掉库自带的协议层 ping，只用官方要求的文本 PING
                async with websockets.connect(WS_URL, ping_interval=None, max_size=None) as ws:
                    backoff = 1
                    await self.session(ws)
            except asyncio.TimeoutError:
                self.disconnect_reason = f"{SILENCE_TIMEOUT} 秒没有收到消息"
            except Exception as e:   # 断网、服务器关连接、握手失败等，一律重连
                self.disconnect_reason = f"{type(e).__name__}: {e}"
            # 连续重连失败时保留最早那次断开的时间，缺口才算得准
            self.disconnected_at = self.disconnected_at or now_iso()
            self.stats["断线次数"] += 1
            print(f"[{now_iso()}] 连接断开（{self.disconnect_reason}），{backoff} 秒后重连")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, BACKOFF_MAX)

    async def report(self):
        start = time.time()
        while True:
            await asyncio.sleep(STATS_EVERY)
            self.producer.poll(0)
            minutes = (time.time() - start) / 60
            print(f"[{now_iso()}] 运行 {minutes:.0f} 分钟  订阅 {len(self.subscribed)} 个 token  "
                  + "  ".join(f"{k}={v}" for k, v in sorted(self.stats.items())))


async def main(duration, refresh_every):
    selected = select_markets(load_latest_markets())
    collector = Collector(selected)
    print(f"订阅 {len(selected)} 个市场，{len(collector.subscribed)} 个 token；每 {refresh_every:.0f} 秒刷新一次名单")
    side_tasks = [
        asyncio.create_task(collector.report()),
        asyncio.create_task(collector.refresh_loop(refresh_every)),
    ]
    try:
        await asyncio.wait_for(collector.run(), duration)   # duration=None 表示一直跑
    except asyncio.TimeoutError:
        print(f"已运行 {duration} 秒，停止")
    finally:
        for t in side_tasks:
            t.cancel()
        left = collector.producer.flush(30)
        print("最终统计：" + "  ".join(f"{k}={v}" for k, v in sorted(collector.stats.items())))
        if left:
            print(f"还有 {left} 条消息没写进 Kafka")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", type=float, default=None, help="运行多少秒后自动停止，不填就一直跑")
    parser.add_argument("--refresh-every", type=float, default=REFRESH_EVERY, help="每隔多少秒重新筛一次订阅名单")
    args = parser.parse_args()
    try:
        asyncio.run(main(args.duration, args.refresh_every))
    except KeyboardInterrupt:
        print("手动停止")
