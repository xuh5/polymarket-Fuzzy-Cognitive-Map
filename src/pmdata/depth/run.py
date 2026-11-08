"""假深度程序：持续读 pm.raw.clob，用 DepthTracker 算每个市场的可疑深度，写进 pm.metrics.depth。

做法和实时指标程序一样（细节见 pmdata.metrics.run）：
- 状态在内存里，每次启动往回预热 WARMUP_HOURS 小时：只更新状态、不输出，读到上次提交的位置才开始输出。
- 指标确认写进 Kafka（producer.flush）之后才提交 offset；写失败直接退出让 Docker 重启。
- 所有分区追上之后，每秒 advance(现在 − GRACE_MS)：冷门市场很久没有新消息，靠它按时输出。
- 市场名录（哪个 token 是第一个选项）启动时和之后每小时从 pm.raw.markets 读一次。

输出：pm.metrics.depth，key = 市场 ID，每个市场每分钟一条。格式见 tracker.py 的 _summary，再加 computed_at。
"""
import argparse
import json
import logging
import signal
import time

from confluent_kafka import TopicPartition

from pmdata.common.config import TOPIC_CLOB, TOPIC_MARKETS, TOPIC_METRICS_DEPTH
from pmdata.common.kafka import latest_by_key, make_producer
from pmdata.common.log import setup_logging
from pmdata.common.timeutil import now_iso
from pmdata.depth.tracker import DepthTracker
from pmdata.ingest.select_markets import get_token_ids
from pmdata.metrics.run import make_consumer, plan_start

log = logging.getLogger("depth")

GROUP_ID = "pm-depth"
WARMUP_HOURS = 24      # 要覆盖到每个市场最近一次快照，和实时指标程序一样
GRACE_MS = 3000        # 用电脑时间推进时多等 3 秒：给网络延迟 + 电脑和交易所的时钟差留余量
COMMIT_SECONDS = 5
MARKETS_SECONDS = 3600


def load_outcomes():
    """pm.raw.markets → {asset_id: 选项序号（1 起）}"""
    outcomes = {}
    for env in latest_by_key(TOPIC_MARKETS, f"{GROUP_ID}-markets").values():
        for i, t in enumerate(get_token_ids(env["payload"]) or [], 1):
            outcomes[t["token_id"]] = i
    return outcomes


class Runner:
    def __init__(self, consumer, producer, warmup_hours=WARMUP_HOURS):
        self.consumer = consumer
        self.producer = producer
        self.warmup_ms = int(warmup_hours * 3600 * 1000)
        self.tracker = DepthTracker()
        self.resume = {}       # 分区 → 从哪个 offset 开始输出
        self.position = {}     # 分区 → 下一条要读的 offset
        self.counts = {"消息": 0, "输出": 0}

    def on_assign(self, consumer, partitions):
        committed = {tp.partition: tp.offset for tp in consumer.committed(partitions, timeout=10)}
        since = int(time.time() * 1000) - self.warmup_ms
        warm = {tp.partition: tp.offset for tp in consumer.offsets_for_times(
            [TopicPartition(TOPIC_CLOB, tp.partition, since) for tp in partitions], timeout=10)}
        self.tracker = DepthTracker(outcomes=self.tracker.outcomes)   # 重新预热，旧状态不要了
        self.resume, self.position = {}, {}
        assign = []
        for tp in partitions:
            p = tp.partition
            low, high = consumer.get_watermark_offsets(tp, timeout=10)
            start, resume = plan_start(committed[p] if committed[p] >= 0 else None,
                                       warm[p] if warm[p] >= 0 else None, low, high)
            self.resume[p], self.position[p] = resume, start
            assign.append(TopicPartition(TOPIC_CLOB, p, start))
            log.info("分区 %d：从 %d 开始预热，从 %d 开始输出（预热 %d 条）", p, start, resume, resume - start)
        consumer.assign(assign)

    def caught_up(self):
        return bool(self.resume) and all(self.position[p] >= r for p, r in self.resume.items())

    def handle(self, msg):
        p = msg.partition()
        outs = self.tracker.apply(json.loads(msg.value()))
        if msg.offset() >= self.resume[p]:
            self.produce(outs)
        self.position[p] = msg.offset() + 1
        self.counts["消息"] += 1

    def flush_idle(self, now_ms):
        if self.caught_up():
            self.produce(self.tracker.advance(now_ms - GRACE_MS))

    def produce(self, outs):
        for summary in outs:
            value = json.dumps(dict(summary, computed_at=now_iso()))
            self.producer.produce(TOPIC_METRICS_DEPTH, key=summary["market"], value=value,
                                  on_delivery=self.on_delivery)
            self.counts["输出"] += 1
        self.producer.poll(0)

    @staticmethod
    def on_delivery(err, msg):
        if err is not None:
            raise SystemExit(f"写 Kafka 失败：{err}")

    def commit(self):
        """指标都确认写进 Kafka 之后，才提交读到的位置。预热中的分区不提交。"""
        if self.producer.flush(30) > 0:
            log.warning("还有指标没确认写入，这次不提交")
            return
        offsets = [TopicPartition(TOPIC_CLOB, p, pos) for p, pos in self.position.items() if pos >= self.resume[p]]
        if offsets:
            self.consumer.commit(offsets=offsets, asynchronous=False)


def main():
    parser = argparse.ArgumentParser(description="假深度识别：pm.raw.clob → pm.metrics.depth")
    parser.add_argument("--group-id", default=GROUP_ID, help="consumer group（试运行时换一个，不影响正式进度）")
    parser.add_argument("--warmup-hours", type=float, default=WARMUP_HOURS, help="启动时往回重放多少小时")
    parser.add_argument("--duration", type=int, help="运行多少秒后退出；不填则一直运行")
    args = parser.parse_args()
    setup_logging()

    consumer = make_consumer(args.group_id)
    runner = Runner(consumer, make_producer(), args.warmup_hours)
    runner.tracker.outcomes = load_outcomes()
    log.info("市场名录：%d 个 token", len(runner.tracker.outcomes))

    stopping = False

    def stop(signum, frame):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    consumer.subscribe([TOPIC_CLOB], on_assign=runner.on_assign)
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
                    runner.tracker.outcomes = load_outcomes()
                except Exception:
                    log.exception("读市场名录失败，沿用旧的")
                last_markets = now
            if runner.caught_up() and not was_caught_up:
                log.info("预热完成，开始输出")
                was_caught_up = True
            if now - last_report >= 60:
                log.info("累计 %s；盘口不可用时忽略的变化 %d 条", runner.counts, runner.tracker.ignored)
                last_report = now
        log.info("停止，提交后退出")
        runner.commit()
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
