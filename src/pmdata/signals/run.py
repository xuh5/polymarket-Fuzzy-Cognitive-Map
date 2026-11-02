"""跳动检测程序：读 pm.metrics.book / pm.metrics.trades，用 JumpDetector 找跳动，写进 pm.events。

做法和实时指标程序一样（细节见 pmdata.metrics.run）：
- 状态在内存里，每次启动往回预热 WARMUP_HOURS 小时：只更新状态、不输出，读到上次提交的位置才开始输出。
  预热 25 小时：「成交是平时的几倍」要前 24 小时的成交。
- 事件确认写进 Kafka（producer.flush）之后才提交 offset；写失败直接退出让 Docker 重启。
- 所有分区追上之后，每秒 flush(现在 − GRACE_MS)：冷门市场很久没有新指标，靠它让跳动按时结束、补上事后价格。
- 市场名录（哪个 token 是第一个选项、Gamma 的 24 小时成交额）启动时和之后每小时从 pm.raw.markets 读一次。

输出：pm.events，key = event_id，同一次跳动的 open → closed → final 落在同一个分区、按顺序到达；
compact topic，每次跳动只留最新一版、永久保留。格式见 detector.py 的 _event。
"""
import argparse
import json
import logging
import signal
import time

from confluent_kafka import Consumer, TopicPartition

from pmdata.common.config import KAFKA_BOOTSTRAP, TOPIC_EVENTS, TOPIC_MARKETS, TOPIC_METRICS_BOOK, TOPIC_METRICS_TRADES
from pmdata.common.kafka import latest_by_key, make_producer
from pmdata.common.log import setup_logging
from pmdata.ingest.select_markets import get_token_ids
from pmdata.metrics.run import plan_start
from pmdata.signals.detector import JumpDetector

log = logging.getLogger("signals")

GROUP_ID = "pm-signals"
TOPICS = [TOPIC_METRICS_BOOK, TOPIC_METRICS_TRADES]
WARMUP_HOURS = 25
GRACE_MS = 30_000        # 指标本身比交易所晚 2～5 秒到，多等一会儿再按电脑时间推进
COMMIT_SECONDS = 5
MARKETS_SECONDS = 3600


def load_markets():
    """pm.raw.markets → ({asset_id: 选项序号（1 起）}, {市场: Gamma 24 小时成交额})"""
    outcomes, volume = {}, {}
    for env in latest_by_key(TOPIC_MARKETS, f"{GROUP_ID}-markets").values():
        m = env["payload"]
        for i, t in enumerate(get_token_ids(m) or [], 1):
            outcomes[t["token_id"]] = i
        if m.get("volume24hr") is not None:
            volume[m["conditionId"]] = float(m["volume24hr"])
    return outcomes, volume


class Runner:
    def __init__(self, consumer, producer, warmup_hours=WARMUP_HOURS):
        self.consumer = consumer
        self.producer = producer
        self.warmup_ms = int(warmup_hours * 3600 * 1000)
        self.detector = JumpDetector()
        self.resume = {}       # (topic, 分区) → 从哪个 offset 开始输出
        self.position = {}     # (topic, 分区) → 下一条要读的 offset
        self.counts = {"指标": 0, "事件": 0}

    def set_markets(self, outcomes, volume):
        self.detector.outcomes, self.detector.volume24h = outcomes, volume

    def on_assign(self, consumer, partitions):
        committed = {(tp.topic, tp.partition): tp.offset for tp in consumer.committed(partitions, timeout=10)}
        since = int(time.time() * 1000) - self.warmup_ms
        warm = {(tp.topic, tp.partition): tp.offset for tp in consumer.offsets_for_times(
            [TopicPartition(tp.topic, tp.partition, since) for tp in partitions], timeout=10)}
        markets = (self.detector.outcomes, self.detector.volume24h)
        self.detector = JumpDetector()   # 重新预热，旧状态不要了
        self.set_markets(*markets)
        self.resume, self.position = {}, {}
        assign = []
        for tp in partitions:
            k = (tp.topic, tp.partition)
            low, high = consumer.get_watermark_offsets(tp, timeout=10)
            start, resume = plan_start(committed[k] if committed[k] >= 0 else None,
                                       warm[k] if warm[k] >= 0 else None, low, high)
            self.resume[k], self.position[k] = resume, start
            assign.append(TopicPartition(tp.topic, tp.partition, start))
            log.info("%s 分区 %d：从 %d 开始预热，从 %d 开始输出（预热 %d 条）", tp.topic, tp.partition, start, resume,
                     resume - start)
        consumer.assign(assign)

    def caught_up(self):
        return bool(self.resume) and all(self.position[k] >= r for k, r in self.resume.items())

    def handle(self, msg):
        k = (msg.topic(), msg.partition())
        events = self.detector.apply(msg.topic(), json.loads(msg.value()))
        if msg.offset() >= self.resume[k]:
            self.produce(events)
        self.position[k] = msg.offset() + 1
        self.counts["指标"] += 1

    def flush_idle(self, now_ms):
        if self.caught_up():
            self.produce(self.detector.flush(up_to_ms=now_ms - GRACE_MS))

    def produce(self, events):
        for ev in events:
            self.producer.produce(TOPIC_EVENTS, key=ev["event_id"], value=json.dumps(ev), on_delivery=self.on_delivery)
            self.counts["事件"] += 1
            log.info("跳动 %s %s：%+.1f 个点（%s）", ev["status"], ev["event_id"], ev["change_points"], ev["move_type"])
        self.producer.poll(0)

    @staticmethod
    def on_delivery(err, msg):
        if err is not None:
            raise SystemExit(f"写 Kafka 失败：{err}")

    def commit(self):
        """事件都确认写进 Kafka 之后，才提交读到的位置。预热中的分区不提交。"""
        if self.producer.flush(30) > 0:
            log.warning("还有事件没确认写入，这次不提交")
            return
        offsets = [TopicPartition(t, p, pos) for (t, p), pos in self.position.items() if pos >= self.resume[(t, p)]]
        if offsets:
            self.consumer.commit(offsets=offsets, asynchronous=False)


def main():
    parser = argparse.ArgumentParser(description="跳动检测：pm.metrics.book / pm.metrics.trades → pm.events")
    parser.add_argument("--group-id", default=GROUP_ID, help="consumer group（试运行时换一个，不影响正式进度）")
    parser.add_argument("--warmup-hours", type=float, default=WARMUP_HOURS, help="启动时往回重放多少小时")
    parser.add_argument("--duration", type=int, help="运行多少秒后退出；不填则一直运行")
    args = parser.parse_args()
    setup_logging()

    consumer = Consumer({"bootstrap.servers": KAFKA_BOOTSTRAP, "group.id": args.group_id,
                         "enable.auto.commit": False})
    runner = Runner(consumer, make_producer(), args.warmup_hours)
    runner.set_markets(*load_markets())
    log.info("市场名录：%d 个 token", len(runner.detector.outcomes))

    stopping = False

    def stop(signum, frame):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    consumer.subscribe(TOPICS, on_assign=runner.on_assign)
    log.info("开始（group=%s，预热 %s 小时）", args.group_id, args.warmup_hours)
    deadline = time.monotonic() + args.duration if args.duration else None
    last_tick = last_commit = last_report = last_markets = time.monotonic()
    was_caught_up = False
    try:
        while not stopping and (deadline is None or time.monotonic() < deadline):
            msg = consumer.poll(0.2)
            if msg is not None:
                if msg.error():
                    log.warning("Kafka 错误：%s", msg.error())
                else:
                    runner.handle(msg)
            now = time.monotonic()
            if now - last_tick >= 1:
                runner.flush_idle(int(time.time() * 1000))
                last_tick = now
            if now - last_commit >= COMMIT_SECONDS:
                runner.commit()
                last_commit = now
            if now - last_markets >= MARKETS_SECONDS:
                try:
                    runner.set_markets(*load_markets())
                except Exception:
                    log.exception("读市场名录失败，沿用旧的")
                last_markets = now
            if runner.caught_up() and not was_caught_up:
                log.info("预热完成，开始输出")
                was_caught_up = True
            if now - last_report >= 60:
                log.info("累计 %s", runner.counts)
                last_report = now
        log.info("停止，提交后退出")
        runner.commit()
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
