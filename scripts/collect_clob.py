"""订阅 CLOB WebSocket market 频道，把订单簿和成交原样写入 Kafka 的 pm.raw.clob。

订阅名单启动时用 select_markets 筛一次，运行期间不变。
"""
import argparse
import asyncio
import json
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
BOOTSTRAP = "localhost:9092"
TOPIC = "pm.raw.clob"

PING_EVERY = 10        # 官方要求每 10 秒发一次 PING
SILENCE_TIMEOUT = 30   # 这么久什么都没收到，就当连接已经死了
BOOK_WAIT = 10         # 订阅后等这么久还没收到 book 快照，就用 REST 补拉
BACKOFF_MAX = 60       # 重连等待最长 60 秒
REST_BATCH = 500       # POST /books 一次最多 500 个
STATS_EVERY = 60       # 每分钟打印一次统计


def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


class Collector:
    def __init__(self, selected):
        # token → 市场（condition ID）。Kafka key 用市场 ID：
        # 一条 price_change 里经常同时有 Yes/No 两个 token，按市场分区才能保证同一市场的顺序
        self.asset_to_market = {
            t["token_id"]: s["condition_id"] for s in selected for t in s["tokens"]
        }
        self.markets = sorted(set(self.asset_to_market.values()))
        self.producer = Producer({
            "bootstrap.servers": BOOTSTRAP,
            "enable.idempotence": True,
            "compression.type": "lz4",
        })
        self.http = requests.Session()
        self.stats = Counter()
        self.conn_id = None
        self.seq = 0                  # 当前连接内收到的第几帧（PONG 不算）
        self.books_seen = set()       # 当前连接内收到过 book 快照的 token
        self.disconnected_at = None   # 上次断开的时间；刚启动时为 None
        self.disconnect_reason = "collector 启动"

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

    def write_gap_markers(self):
        """每次连上都给每个市场写一条缺口标记，告诉下游：之前的订单簿状态作废，等新快照。"""
        reconnected_at = now_iso()
        for i, market in enumerate(self.markets):
            payload = {
                "event_type": "gap",
                "market": market,
                "disconnected_at": self.disconnected_at,   # 刚启动时为 None
                "reconnected_at": reconnected_at,
                "reason": self.disconnect_reason,
            }
            self.send(market, "collector", payload, reconnected_at, None, i)
        self.disconnected_at = None

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

    async def check_books(self):
        await asyncio.sleep(BOOK_WAIT)
        missing = [a for a in self.asset_to_market if a not in self.books_seen]
        if missing:
            print(f"{BOOK_WAIT} 秒内有 {len(missing)} 个 token 没收到 book 快照，用 REST 补拉")
            await asyncio.to_thread(self.fetch_rest_books, missing)

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
        print(f"[{now_iso()}] 已连接 conn_id={self.conn_id}，订阅 {len(self.asset_to_market)} 个 token")
        self.write_gap_markers()
        await ws.send(json.dumps({
            "assets_ids": list(self.asset_to_market),
            "type": "market",
            "custom_feature_enabled": True,   # 多推 best_bid_ask / new_market / market_resolved（new_market 不存）
        }))
        helpers = [asyncio.create_task(self.pinger(ws)), asyncio.create_task(self.check_books())]
        try:
            while True:
                # 超过 SILENCE_TIMEOUT 秒没有任何消息（包括 PONG）就抛 TimeoutError，触发重连
                raw = await asyncio.wait_for(ws.recv(), SILENCE_TIMEOUT)
                if raw == "PONG":
                    continue
                self.handle_frame(raw, now_iso())
        finally:
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
            print(f"[{now_iso()}] 运行 {minutes:.0f} 分钟  " + "  ".join(f"{k}={v}" for k, v in sorted(self.stats.items())))


async def main(duration):
    selected = select_markets(load_latest_markets())
    collector = Collector(selected)
    print(f"订阅 {len(selected)} 个市场，{len(collector.asset_to_market)} 个 token")
    reporter = asyncio.create_task(collector.report())
    try:
        await asyncio.wait_for(collector.run(), duration)   # duration=None 表示一直跑
    except asyncio.TimeoutError:
        print(f"已运行 {duration} 秒，停止")
    finally:
        reporter.cancel()
        left = collector.producer.flush(30)
        print("最终统计：" + "  ".join(f"{k}={v}" for k, v in sorted(collector.stats.items())))
        if left:
            print(f"还有 {left} 条消息没写进 Kafka")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", type=float, default=None, help="运行多少秒后自动停止，不填就一直跑")
    args = parser.parse_args()
    try:
        asyncio.run(main(args.duration))
    except KeyboardInterrupt:
        print("手动停止")
