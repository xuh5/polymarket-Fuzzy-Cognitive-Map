"""孪生市场的候选：从开放市场里挑出「可能有严格逻辑关系」的事件对，交给 AI 判断（pmdata.twins.judge）。

孪生 = 不同事件里、结果之间有严格逻辑关系的市场，比如「收盘市值 < 1.25T」和「> 1.25T」（互补）、
同一个榜「第一」和「第三」（互斥）。3000 个市场两两配有 450 万对，几乎都无关，全让 AI 看又慢又贵，
所以这里先用便宜的规则缩小范围，宁可多放、不能漏；判断准不准交给 AI。

按事件对筛（AI 一次看两个事件的全部市场），三个条件都要满足：
1. 两边有市场提到同一个名字（问题里首字母大写的词，去掉句首疑问词、月份和 STOP_NAMES）
2. 这两个市场的截止日期相差不超过 max_gap_hours（默认 12 小时）
3. 事件标题去掉名次词、比较词、数字后，词的重合度 ≥ min_title_sim（默认 0.3）

2026-11-19 用全部开放市场试过：只用 1+2 有约 4000 组事件对（同一家公司出现在几十个不相干的排行榜里），
加上 3 剩 778 组；抽 14 组人工看，4 组真有关系，被 3 去掉的基本都无关（「谁敲上市钟」vs「上市收盘市值」）。
已知漏掉的：截止日期不同的跨事件关系（「明年 3 月前上市」vs「2027 年前不上市」），以后再放宽。
"""
import re
from collections import defaultdict
from datetime import datetime

MONTHS = {"january", "february", "march", "april", "may", "june", "july", "august", "september", "october",
          "november", "december"}
# 问题开头的疑问词不是名字
QUESTION_WORDS = {"will", "which", "who", "what", "is", "does", "did", "can", "no", "how", "the"}
# 首字母大写、但不是公司 / 人名的词
STOP_NAMES = {"ipo", "no", "yes", "ai", "low", "high", "t", "b", "us", "q1", "q2", "q3", "q4", "index", "next",
              "top", "most", "searched", "lab", "labs", "arena", "text", "math", "code", "webdev", "agent",
              "chinese", "pro", "et", "pm", "am"}
# 事件标题里不说明「问的是什么」的词：名次、比较、区间、虚词
TITLE_STOP = {"best", "second-best", "third-best", "fourth-best", "second", "third", "fourth", "2nd", "3rd", "4th",
              "1st", "largest", "highest", "top", "lower", "middle", "higher", "brackets", "above", "below", "less",
              "more", "over", "under", "between", "by", "the", "of", "a", "an", "in", "on", "at", "end", "will", "be",
              "what", "which", "who", "how", "many", "much", "is", "has", "have", "for", "to", "s", "and", "or"}


def names(question):
    """问题里提到的公司 / 人名（小写）。粗糙但够用：只用来缩小范围，后面还有 AI 判断。"""
    words = re.findall(r"[A-Za-z][A-Za-z0-9.'\-]*", (question or "").replace("’", "'"))
    if words and words[0].lower() in QUESTION_WORDS:
        words = words[1:]
    out = set()
    for w in words:
        if not w[0].isupper():
            continue
        w = w.lower().removesuffix("'s").rstrip(".")
        if w and w not in MONTHS and w not in STOP_NAMES:
            out.add(w)
    return out


def title_words(title):
    t = re.sub(r"[#\d$%.,?_()|:!]+", " ", (title or "").lower().replace("’", "'"))
    return {w.removesuffix("'s") for w in t.split()} - TITLE_STOP - MONTHS


def title_similarity(a, b):
    wa, wb = title_words(a), title_words(b)
    return len(wa & wb) / len(wa | wb) if wa | wb else 0.0


def parse_time(s):
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")) if s else None
    except ValueError:
        return None


def market_info(m):
    """Gamma 的市场数据 → 这里用到的几个字段；已关闭、缺事件或问题的返回 None。"""
    events = m.get("events") or []
    if m.get("closed") or not m.get("conditionId") or not m.get("question") or not events or not events[0].get("id"):
        return None
    return dict(market_id=m["conditionId"], question=m["question"], description=m.get("description") or "",
                event_id=str(events[0]["id"]), event_title=events[0].get("title") or "",
                end=parse_time(m.get("endDate")), volume=float(m.get("volume24hr") or 0))


def candidate_events(markets, max_gap_hours=12, min_title_sim=0.3):
    """markets：market_info() 的结果列表 → 候选事件对 [(事件 ID, 事件 ID)]（小的在前，排好序）。"""
    by_name = defaultdict(list)
    titles = {}
    for m in markets:
        titles[m["event_id"]] = m["event_title"]
        for n in names(m["question"]):
            by_name[n].append(m)
    out = set()
    for ms in by_name.values():
        for i, a in enumerate(ms):
            for b in ms[i + 1:]:
                ea, eb = a["event_id"], b["event_id"]
                if ea == eb or (min(ea, eb), max(ea, eb)) in out or a["end"] is None or b["end"] is None:
                    continue
                if abs((a["end"] - b["end"]).total_seconds()) > max_gap_hours * 3600:
                    continue
                if title_similarity(titles[ea], titles[eb]) >= min_title_sim:
                    out.add((min(ea, eb), max(ea, eb)))
    return sorted(out)
