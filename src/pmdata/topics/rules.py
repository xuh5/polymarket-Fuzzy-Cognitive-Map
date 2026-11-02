"""市场标签（规则版）：按问题文字给每个市场一个话题、一个公司（有的话）。

只看问题文字，不用 Polymarket 自带的标签：它的标签按整个事件打（百度那个市场也挂着 Anthropic）、
每个市场 4～12 个且有平台内部用的，不准也太乱（2026-11-02 看过）。只看问题文字的另一个好处：
已经关闭的老市场也能分类（数据库里有问题文字就行）。

这是第一版，留位置给以后更好的打法（比如让 AI 理解市场背后的话题，Fuzzy Map 也要用）：
标签存在 Postgres 表 market_labels，每行记来源 source；规则写的是 'rules'，
别的来源（'ai'、'manual'）写进同一张表，规则不会覆盖它们（见 pmdata.sink.pg_writer）。
"""
import re

# 话题：按顺序匹配，第一个命中的算；都不中是 Other
TOPICS = [
    # 「财报会上会不会说某个词」「谷歌搜索 / App Store 排第几」放最前面：里面常带 #2、公司名，别被后面的规则抢走
    ("Mentions & charts", r"\bsays?\b|\bsaid\b|\bpost\b|mention|podcast|episode|searched|App Store"),
    ("Biggest company", r"largest company"),
    ("Policy", r"moratorium|regulat|\bban\b|banned|\blaw\b|\bbill\b|court|lawsuit|\bsue|antitrust|\bFTC\b|\bDOJ\b"
               r"|military compan|export control|tariff|congress|executive order|\bspy\b|\bFBI\b|indict|senate"),
    ("AI rankings", r"\bbest\b.*\b(AI|model|lab)\b|-best\b|\bAI (lab|agent)\b|#\d|\barena\b|leaderboard|\bscore\b"
                    r"|benchmark|\bHLE\b|livebench|market share|\branked?\b"),
    ("Releases", r"releas|\bdebut|\blaunch|announce[sd]? .*\bmodel|\bmodel\b.*\bby\b"),
    ("IPO & valuation", r"\bIPO|public offering|ticker|valuation|market cap|\braise\b|funding|acquir|bankrupt"),
    ("Compute", r"data cent|\bGPU|\bchip|\bA100|\bH100|\bOrnn\b|compute|\btokens\b"),
    ("Company news", r"earnings|headcount|\bCEO\b|margin|revenue|usage limit|go down|outage|incident|deliver"
                     r"|\bQ[1-4]\b"),
    ("AI milestones", r"\bAGI\b|millennium prize|navier|\bsolved?\b|\bdetermine\b"),
]

# 公司：问题里最先提到的那个
COMPANIES = {
    "OpenAI": r"OpenAI|ChatGPT|\bGPT|Sam Altman|\bAstra\b|Codex",
    "Anthropic": r"Anthropic|Claude|\bFable\b|Dario",
    "Google": r"Google|Alphabet|Gemini|DeepMind",
    "xAI": r"\bxAI\b|Grok|SpaceXAI",
    "Meta": r"\bMeta\b|Llama",
    "Microsoft": r"Microsoft|Copilot",
    "Apple": r"\bApple\b",
    "Amazon": r"Amazon",
    "NVIDIA": r"NVIDIA",
    "Tesla": r"Tesla|Optimus",
    "DeepSeek": r"DeepSeek",
    "Moonshot": r"Moonshot|\bKimi\b",
    "Alibaba": r"Alibaba|\bQwen",
    "ByteDance": r"ByteDance|TikTok",
    "Baidu": r"Baidu",
    "Xiaomi": r"Xiaomi",
    "Mistral": r"Mistral",
}

_TOPICS = [(name, re.compile(p, re.I)) for name, p in TOPICS]
# Meta、Apple 要区分大小写（meta、apple 是普通单词）
_COMPANIES = [(name, re.compile(p, 0 if name in ("Meta", "Apple") else re.I)) for name, p in COMPANIES.items()]


def classify(question):
    """问题文字 → (话题, 公司或 None)"""
    q = question or ""
    topic = next((name for name, p in _TOPICS if p.search(q)), "Other")
    hits = [(m.start(), name) for name, p in _COMPANIES if (m := p.search(q))]
    return topic, min(hits)[1] if hits else None
