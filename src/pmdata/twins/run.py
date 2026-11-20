"""孪生市场的运行程序：每小时读一次市场名录，挑候选事件对，交给 AI 判断，结果写进 Kafka topic pm.twins。

和其他计算服务（metrics、depth、execution）一个模式：只读写 Kafka，不连数据库。pm.twins 的读者：
- pg-sink：写进 Postgres 表 twin_judgments / market_relations，网站从那里读
- collect-clob：把订阅中市场的孪生也加进订阅名单（见 pmdata.ingest.select_markets）
- twins 自己：启动和每一轮都读一遍，就知道哪些组判断过、用的什么指纹

每轮：
1. 读 pm.raw.markets 的最新名录（fetch-markets 每小时写），只看开放的市场
2. pmdata.twins.candidates 挑候选事件对
3. 默认（--scope all）判断全部开放市场：首次 650 多组按 API 价格折算约 $6～7、之后每周几十组，订阅额度够用；
   全量结果以后提醒推送、Fuzzy Map 都要用。和订阅名单里的市场有关的事件对排最前（网站只显示订阅中的，第一轮就能判完），
   其余按成交额在后台慢慢补。--scope live 只判断订阅名单相关的
4. 只判断「需要判断的」：没判断过、规则文字或提示词改了（指纹变了）、上次失败的；按两边 24 小时成交额从大到小，
   每轮最多 --limit 组
5. 每组判断完写一条消息（key = 「事件A:事件B」，成功、失败都写，失败的下一轮重试）
6. 不再是候选的组（事件关闭了、市场全下架了）写 tombstone，pg-sink 把这组的关系标成退役（不删，以后还能回头查）。
   保护：一轮里超过 MAX_GONE_RATIO 的已判断组同时消失，视为读名录出了问题，这一轮不写 tombstone

调用 AI：--ai claude 用 Claude Code 的非交互模式（`claude -p`），扣的是订阅额度、不用 API key。
容器里用 `claude setup-token` 生成的长期令牌登录（.env 的 CLAUDE_CODE_OAUTH_TOKEN）。
2026-11-19 实测 Haiku：两组真实事件对 165 条关系和人工判断全部一致，一组 30～110 秒，按 API 价格约 $0.006～0.018。
要关掉用户设置和 MCP（--setting-sources "" --strict-mcp-config），否则每次多带三万多 token 的个人设置，
理由还会按个人语言设置写成中文。
"""
import argparse
import hashlib
import json
import logging
import subprocess
import time
from collections import defaultdict
from datetime import UTC, datetime

from pmdata.common.config import TOPIC_TWINS
from pmdata.common.kafka import envelope, latest_by_key, make_producer
from pmdata.common.log import setup_logging
from pmdata.common.timeutil import now_iso
from pmdata.ingest.select_markets import load_latest_markets, select_markets
from pmdata.twins.candidates import candidate_events, market_info
from pmdata.twins.judge import PROMPT_VERSION, JudgeError, judge

log = logging.getLogger("pmdata.twins")

GROUP_ID = "pm-twins"          # 只用来读 pm.twins（不提交 offset，每次从头读）
MAX_GONE_RATIO = 0.2           # 一轮里超过这个比例的已判断组同时不再是候选，就不写 tombstone
SYSTEM_PROMPT = "You are a careful analyst of prediction market rules. Follow the user's instructions exactly."


def pair_key(a, b):
    return f"{a}:{b}"


def fingerprint(markets_a, markets_b):
    """两个事件的市场（ID、问题、规则）+ 提示词版本 → 指纹；有市场新增、下架、规则或提示词改了就会变，要重判。"""
    def part(ms):
        return sorted((m["market_id"], m["question"], m["description"]) for m in ms)
    payload = [PROMPT_VERSION, part(markets_a), part(markets_b)]
    return hashlib.sha256(json.dumps(payload).encode()).hexdigest()[:16]


def plan(pairs, by_event, done, limit, first=frozenset()):
    """这一轮要判断哪些事件对 → [(event_a, event_b, 指纹)]。done：{(a, b): (指纹, status)}。
    没判断过、指纹变了、上次失败的要判；first 里的事件对优先（订阅名单相关的，网站马上要用），
    其次按两边 24 小时成交额从大到小，最多 limit 组。"""
    todo = []
    for a, b in pairs:
        fp = fingerprint(by_event[a], by_event[b])
        if done.get((a, b)) != (fp, "ok"):
            vol = sum(m["volume"] for m in by_event[a] + by_event[b])
            todo.append(((a, b) not in first, -vol, a, b, fp))
    todo.sort()
    return [(a, b, fp) for *_, a, b, fp in todo[:limit]]


def in_scope(pairs, by_event, watched):
    """只留至少一边事件里有订阅名单内市场的事件对。"""
    watched_events = {e for e, ms in by_event.items() if any(m["market_id"] in watched for m in ms)}
    return [(a, b) for a, b in pairs if a in watched_events or b in watched_events]


def gone(done, pairs):
    """判断过、但这一轮不再是候选的组 → 要写 tombstone 的；消失太多（读名录出问题？）返回 None。"""
    current = set(pairs)
    out = [p for p in done if p not in current]
    if done and len(out) > MAX_GONE_RATIO * len(done):
        return None
    return out


def judgment(a, b, fp, status, model, cost, relations=(), error=None):
    """一组的判断结果 → pm.twins 消息的 payload（pg-sink、采集器都读它）。"""
    return {"event_a": a, "event_b": b, "fingerprint": fp, "status": status, "error": error, "model": model,
            "cost_usd": cost, "judged_at": now_iso(), "relations": list(relations)}


class ClaudeCLI:
    """用 `claude -p` 问一次。cost 记下最近一次的花费（按 API 价格折算，订阅不实际扣钱）。"""

    def __init__(self, model="haiku", timeout=900, binary="claude"):
        self.model, self.timeout, self.binary = model, timeout, binary
        self.cost = None

    def __call__(self, prompt):
        cmd = [self.binary, "-p", "--model", self.model, "--tools", "", "--strict-mcp-config",
               "--setting-sources", "", "--disable-slash-commands", "--no-session-persistence",
               "--output-format", "json", "--system-prompt", SYSTEM_PROMPT]
        self.cost = None
        try:
            p = subprocess.run(cmd, input=prompt, capture_output=True, text=True, encoding="utf-8",
                               timeout=self.timeout)
        except subprocess.TimeoutExpired as e:
            raise JudgeError(f"claude -p 超时（{self.timeout} 秒）") from e
        try:
            out = json.loads(p.stdout)
        except json.JSONDecodeError as e:
            raise JudgeError(f"claude -p 退出码 {p.returncode}：{(p.stderr or p.stdout).strip()[:300]}") from e
        self.cost = out.get("total_cost_usd")
        if out.get("is_error") or p.returncode != 0:
            raise JudgeError(f"claude -p 出错：{str(out.get('result'))[:300]}")
        return out.get("result") or ""


def load_done(group_id=GROUP_ID):
    """读完 pm.twins → {(a, b): (指纹, status)}：哪些组判断过。"""
    done = {}
    for key, env in latest_by_key(TOPIC_TWINS, group_id).items():
        p = env["payload"]
        done[tuple(key.split(":", 1))] = (p["fingerprint"], p["status"])
    return done


def publish(producer, a, b, payload):
    """写一条（payload 为 None 是 tombstone），等 Kafka 确认才返回：写失败直接抛错，让 Docker 重启。"""
    value = None if payload is None else envelope("twins", now_iso(), payload)
    errors = []
    producer.produce(TOPIC_TWINS, key=pair_key(a, b), value=value,
                     on_delivery=lambda err, msg: err and errors.append(err))
    producer.flush(30)
    if errors:
        raise RuntimeError(f"写 {TOPIC_TWINS} 失败：{errors[0]}")


def run_once(producer, ask, model, limit, scope="all", dry_run=False):
    raw = load_latest_markets()
    markets = [x for x in map(market_info, raw) if x]
    if not markets:
        log.warning("市场名录为空（读 Kafka 失败？），这一轮跳过")
        return
    by_event = defaultdict(list)
    for m in markets:
        by_event[m["event_id"]].append(m)
    pairs = candidate_events(markets)
    # 「订阅名单」用和采集器同一个函数算（按成交额的前 N 个），不用连数据库
    watched = {s["condition_id"] for s in select_markets(raw)}
    watched_pairs = set(in_scope(pairs, by_event, watched))
    if scope == "live":
        pairs = [p for p in pairs if p in watched_pairs]
    done = load_done()
    todo = plan(pairs, by_event, done, limit, first=watched_pairs)
    retire = gone(done, pairs) if scope == "all" else []   # 只判断一部分时，不在范围里的组不算消失
    log.info("开放市场 %d 个，候选事件对 %d 组（其中和订阅名单有关的 %d 组，优先），范围 %s，"
             "这一轮判断 %d 组，不再是候选的 %s 组", len(markets), len(pairs), len(watched_pairs), scope, len(todo),
             "?" if retire is None else len(retire))
    if dry_run:
        for a, b, _ in todo:
            log.info("  待判断：%s  ||  %s", by_event[a][0]["event_title"], by_event[b][0]["event_title"])
        return
    if retire is None:
        log.warning("超过 %.0f%% 的已判断组同时消失，可能是名录读得不完整，这一轮不写 tombstone", MAX_GONE_RATIO * 100)
        retire = []
    for a, b in retire:
        publish(producer, a, b, None)

    today = datetime.now(UTC).date().isoformat()
    ok = failed = 0
    for a, b, fp in todo:
        t0 = time.monotonic()
        titles = f"{by_event[a][0]['event_title']} || {by_event[b][0]['event_title']}"
        try:
            rels = judge(ask, by_event[a], by_event[b], today)
        except JudgeError as e:
            failed += 1
            log.warning("判断失败：%s：%s", titles, e)
            publish(producer, a, b, judgment(a, b, fp, "error", model, getattr(ask, "cost", None), error=str(e)[:500]))
            continue
        publish(producer, a, b, judgment(a, b, fp, "ok", model, getattr(ask, "cost", None), rels))
        ok += 1
        log.info("%s：%d 条关系，%.0f 秒", titles, len(rels), time.monotonic() - t0)
    log.info("这一轮完成：成功 %d 组，失败 %d 组；退役 %d 组", ok, failed, len(retire))


def main():
    ap = argparse.ArgumentParser(description="孪生市场：候选 → AI 判断 → Kafka pm.twins")
    ap.add_argument("--every", type=int, default=3600, help="每隔几秒跑一轮；0 = 只跑一轮")
    ap.add_argument("--limit", type=int, default=40, help="每轮最多判断几组事件对")
    ap.add_argument("--ai", choices=["claude"], default="claude", help="用哪个 AI（以后加 codex）")
    ap.add_argument("--model", default="haiku")
    ap.add_argument("--scope", choices=["live", "all"], default="all",
                    help="all = 全部开放市场；live = 只判断至少一边有订阅名单内市场的事件对")
    ap.add_argument("--dry-run", action="store_true", help="只打印这一轮要判断哪些，不调用 AI、不写 Kafka")
    args = ap.parse_args()
    setup_logging()
    ask = ClaudeCLI(model=args.model)
    model = f"{args.ai}:{args.model}"
    producer = make_producer()
    while True:
        try:
            run_once(producer, ask, model, args.limit, args.scope, args.dry_run)
        except RuntimeError:
            raise   # 写 Kafka 失败直接退出，让 Docker 重启
        except Exception:
            log.exception("这一轮出错，下一轮再试")
        if args.every <= 0:
            break
        time.sleep(args.every)


if __name__ == "__main__":
    main()
