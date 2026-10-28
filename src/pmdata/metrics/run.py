"""实时指标程序：持续读 pm.raw.clob，用 MetricsEngine 算指标，写进 pm.metrics.book / pm.metrics.trades。

没有用流处理框架，下面几件事是自己做的：
- 状态恢复：订单簿和「N 分钟前的中间价」都在内存里，重启就没了，而快照只在采集器重连时才有。
  所以每次启动先往回倒 WARMUP_HOURS 小时重放「预热」：只更新内存状态、不输出，
  读到上次提交的位置之后才开始输出。engine 是确定性的（同样的输入 → 同样的输出），所以重启后的结果和没重启一样。
  第一次启动（group 还没提交过）：预热到当前末尾，之后的新消息才输出。
- 不丢不重复（at-least-once）：先确认指标写进 Kafka（producer.flush），再提交 offset。
  崩溃时最多重复输出一点，下游按（asset_id, second）/（asset_id, minute）upsert 就去掉了。
  写 Kafka 失败直接退出，让 Docker 重启，重启后预热重放，不会丢。
- 冷门市场的兜底输出：每秒用电脑时间 flush(现在 − GRACE_MS)。只在所有分区都追上之后才做，
  预热时数据是几小时前的，用现在的时间 flush 会把还没到齐的秒提前输出。
- 只跑一个实例：分区重新分配时直接清空 engine、按新分配重新预热。
- 顺带把当前订单簿每秒同步进 Redis 给网站用（pmdata.book.cache）。只在追上之后同步：预热时的盘口是几小时前的。

输出格式（下游依赖它，改字段要同步改下游）：见 engine.py 的 _book_metrics / _add_trade。
key 是市场 ID；时间字段 second / minute / exchange_ts 是交易所时间毫秒，received_at / computed_at 是 UTC ISO 字符串。
"""
import argparse
import json
import logging
import signal
import time

import redis
from confluent_kafka import Consumer, TopicPartition

from pmdata.book.cache import BookCache
from pmdata.common.config import KAFKA_BOOTSTRAP, REDIS_URL, TOPIC_CLOB
from pmdata.common.kafka import make_producer
from pmdata.common.log import setup_logging
from pmdata.metrics.engine import MetricsEngine

log = logging.getLogger("metrics")

GROUP_ID = "pm-metrics"
WARMUP_HOURS = 24      # 启动时往回重放多久（要覆盖到每个市场最近一次快照；现在一天断线十几次，24 小时基本都有）
GRACE_MS = 3000        # 兜底输出时多等 3 秒：给网络延迟 + 电脑和交易所的时钟差留余量
COMMIT_SECONDS = 5


def plan_start(committed, warm, low, high):
    """一个分区从哪开始读、从哪开始输出。

    committed：上次提交的位置（没有为 None）；warm：预热起点（offsets_for_times 的结果，没有更晚的消息为 None）。
    返回 (start, resume)：从 start 开始读，offset >= resume 的消息才输出。
    """
    resume = committed if committed is not None else high   # 第一次启动：从当前末尾开始输出
    start = warm if warm is not None else high
    start = min(start, resume)        # 上次提交比 24 小时还早（停了很久）：至少从提交位置读起，不漏
    start = max(start, low)           # 太早的已被保留策略删掉
    return start, resume


class Runner:
    def __init__(self, consumer, producer, warmup_hours=WARMUP_HOURS, cache=None):
        self.consumer = consumer
        self.producer = producer
        self.cache = cache     # BookCache；None = 不写 Redis（测试、试跑）
        self.warmup_ms = int(warmup_hours * 3600 * 1000)
        self.engine = MetricsEngine()
        self.resume = {}       # 分区 → 从哪个 offset 开始输出
        self.position = {}     # 分区 → 下一条要读的 offset（处理完一条才更新）
        self.counts = {"消息": 0, "盘口指标": 0, "成交指标": 0}

    def on_assign(self, consumer, partitions):
        committed = {tp.partition: tp.offset for tp in consumer.committed(partitions, timeout=10)}
        since = int(time.time() * 1000) - self.warmup_ms
        warm = {tp.partition: tp.offset for tp in consumer.offsets_for_times(
            [TopicPartition(TOPIC_CLOB, tp.partition, since) for tp in partitions], timeout=10)}
        self.engine = MetricsEngine()   # 重新预热，旧状态不要了
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
        outs = self.engine.apply(json.loads(msg.value()))
        if msg.offset() >= self.resume[p]:
            self.produce(outs)
        self.position[p] = msg.offset() + 1
        self.counts["消息"] += 1

    def flush_idle(self, now_ms):
        if self.caught_up():
            self.produce(self.engine.flush(up_to_ms=now_ms - GRACE_MS))

    def sync_cache(self, now):
        if self.cache is not None and self.caught_up():
            self.cache.sync(self.engine.store, now)

    def produce(self, outs):
        for topic, key, rec in outs:
            self.producer.produce(topic, key=key, value=json.dumps(rec), on_delivery=self.on_delivery)
            self.counts["盘口指标" if rec.get("second") is not None else "成交指标"] += 1
        self.producer.poll(0)

    @staticmethod
    def on_delivery(err, msg):
        if err is not None:
            # 退出让 Docker 重启：没写成功的那些 offset 还没提交，重启后预热重放会重新算出来
            raise SystemExit(f"写 Kafka 失败：{err}")

    def commit(self):
        """指标都确认写进 Kafka 之后，才提交读到的位置。预热中的分区不提交（不能把提交位置往回拨）。"""
        if self.producer.flush(30) > 0:
            log.warning("还有指标没确认写入，这次不提交")
            return
        offsets = [TopicPartition(TOPIC_CLOB, p, pos) for p, pos in self.position.items() if pos >= self.resume[p]]
        if offsets:
            self.consumer.commit(offsets=offsets, asynchronous=False)


def make_consumer(group_id):
    return Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": group_id,
        "enable.auto.commit": False,   # 指标确认写入后手动提交
    })


def main():
    parser = argparse.ArgumentParser(description="实时指标：pm.raw.clob → pm.metrics.book / pm.metrics.trades")
    parser.add_argument("--group-id", default=GROUP_ID, help="consumer group（试运行时换一个，不影响正式进度）")
    parser.add_argument("--warmup-hours", type=float, default=WARMUP_HOURS, help="启动时往回重放多少小时")
    parser.add_argument("--duration", type=int, help="运行多少秒后退出；不填则一直运行")
    parser.add_argument("--no-redis", action="store_true", help="不把订单簿写进 Redis（试跑时用，别覆盖正式的盘口）")
    args = parser.parse_args()
    setup_logging()

    consumer = make_consumer(args.group_id)
    # 超时设短：Redis 卡住时最多耽误 1 秒，不拖住读 Kafka
    cache = None if args.no_redis else BookCache(redis.Redis.from_url(REDIS_URL, socket_timeout=1,
                                                                       socket_connect_timeout=1))
    runner = Runner(consumer, make_producer(), args.warmup_hours, cache)

    stopping = False

    def stop(signum, frame):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    consumer.subscribe([TOPIC_CLOB], on_assign=runner.on_assign)
    log.info("开始（group=%s，预热 %s 小时）", args.group_id, args.warmup_hours)
    deadline = time.monotonic() + args.duration if args.duration else None
    last_tick = last_commit = last_report = time.monotonic()
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
                runner.sync_cache(now)
                last_tick = now
            if now - last_commit >= COMMIT_SECONDS:
                runner.commit()
                last_commit = now
            if runner.caught_up() and not was_caught_up:
                log.info("预热完成，开始输出")
                was_caught_up = True
            if now - last_report >= 60:
                log.info("累计 %s；盘口不可用时忽略的变化 %d 条", runner.counts, runner.engine.store.ignored)
                last_report = now
        # 不 flush engine 里还没结束的秒：没输出的部分对应的 offset 没提交，重启预热后会重新算出来
        log.info("停止，提交后退出")
        runner.commit()
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
