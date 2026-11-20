from datetime import UTC, datetime

import pytest

from pmdata.twins.candidates import candidate_events, market_info, names, title_similarity


@pytest.mark.parametrize("question, expected", [
    ("Will Anthropic's market cap be less than $1.25T at market close on IPO day?", {"anthropic"}),
    ("Anthropic IPO closing market cap above $1.25T?", {"anthropic"}),      # 句首就是公司名，不能去掉
    ("Will Jensen Huang rejoin NVIDIA by December 31, 2026?", {"jensen", "huang", "nvidia"}),
    ("Next Claude Haiku (5.6+) released by December 31, 2026?", {"claude", "haiku"}),   # Next 不是名字
    ("Will Z.ai be the third-best Math AI lab at the end of November 2026?", {"z.ai"}),
])
def test_names(question, expected):
    assert names(question) == expected


def test_title_similarity_ignores_rank_words():
    assert title_similarity("Second-best AI Agent Lab end of December?",
                            "Third-best AI Agent Lab end of December?") == 1
    assert title_similarity("Anthropic IPO: Who will ring the bell?", "Anthropic IPO Closing Market Cap") < 0.3


def mk(mid, q, ev, title, end="2026-12-01T04:59:00Z", **extra):
    return market_info({"conditionId": mid, "question": q, "events": [{"id": ev, "title": title}], "endDate": end,
                        **extra})


def test_market_info_skips_closed_and_eventless():
    assert mk("m1", "Q?", 1, "T", closed=True) is None
    assert market_info({"conditionId": "m1", "question": "Q?", "events": []}) is None
    info = mk("m1", "Q?", 7, "T", description="rules")
    assert info["event_id"] == "7" and info["end"] == datetime(2026, 12, 1, 4, 59, tzinfo=UTC)


def test_candidate_events():
    ms = [
        mk("a1", "Will Anthropic's market cap be less than $1.25T at market close on IPO day?", 1,
           "Anthropic IPO Closing Market Cap"),
        mk("a2", "Will Anthropic's market cap be between $1.25T and $1.5T at market close on IPO day?", 1,
           "Anthropic IPO Closing Market Cap"),
        mk("b1", "Anthropic IPO closing market cap above $1.25T?", 2, "Anthropic IPO closing market cap above ___ ?"),
        # 同一家公司、标题不像：不配
        mk("c1", "Will Dario Amodei ring the bell for Anthropic?", 3, "Anthropic IPO: Who will ring the bell?"),
        # 标题像、截止日期差一个月：不配
        mk("d1", "Anthropic IPO closing market cap above $2T?", 4, "Anthropic IPO closing market cap above ___ ?",
           end="2027-01-01T04:59:00Z"),
        # 标题一样、没有共同的名字：不配
        mk("e1", "Next GPT Luna (6.1+) released by November 30, 2026?", 5, "Next GPT Luna (6.1+) released by...?"),
        mk("f1", "Next Claude Haiku (5.6+) released by November 30, 2026?", 6,
           "Next Claude Haiku (5.6+) released by...?"),
    ]
    assert candidate_events(ms) == [("1", "2")]
