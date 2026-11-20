import json

import pytest

from pmdata.twins.judge import JudgeError, build_prompt, derive, judge, parse


def P(yy, yn, ny, nn):
    return {"yes_yes": yy, "yes_no": yn, "no_yes": ny, "no_no": nn}


@pytest.mark.parametrize("possible, expected", [
    (P(True, False, False, True), "identical"),
    (P(False, True, True, False), "complement"),     # 「< 1.25T」vs「> 1.25T」
    (P(False, True, True, True), "exclusive"),       # 同一个榜「第一」vs「第三」
    (P(True, False, True, True), "implies"),         # A 是 → B 一定是
    (P(True, True, False, True), "implied_by"),
    (P(True, True, True, True), None),               # 没关系
    (P(False, False, True, True), None),             # A 一定是否：不算关系
    (P(True, False, False, False), None),
])
def test_derive(possible, expected):
    assert derive(possible) == expected


A = [dict(market_id="ma1", question="Will DeepSeek have the best Math AI model at the end of November 2026?",
          description="Rank column, style control off.", event_title="Best Math AI model end of November?"),
     dict(market_id="ma2", question="Will Google have the best Math AI model at the end of November 2026?",
          description="Rank column, style control off.", event_title="Best Math AI model end of November?")]
B = [dict(market_id="mb1", question="Will DeepSeek be the third-best Math AI lab at the end of November 2026?",
          description="Lab Rank column, style control off.", event_title="Third-best Math AI Lab end of November?")]


def test_build_prompt_lists_shared_rules_once():
    p = build_prompt(A, B, "2026-11-19")
    assert "A1: Will DeepSeek" in p and "A2: Will Google" in p and "B1: Will DeepSeek" in p
    assert p.count("Rules RA") == 1 and p.count("[rules RA1]") == 2   # 两个市场规则一样，只写一次
    assert "Today is 2026-11-19" in p


def reply(*rels):
    return "```json\n" + json.dumps({"relations": list(rels)}) + "\n```"


def rel(a="A1", b="B1", possible=None, strength="strict", **extra):
    return dict(a=a, b=b, possible=possible or P(False, True, True, True), strength=strength, **extra)


def test_parse_maps_labels_and_derives_relation():
    out = parse(reply(rel(differences=["Models view vs. Labs view"], confidence="high", reason="Can't be #1 and #3.")),
                A, B)
    assert out == [dict(market_a="ma1", market_b="mb1", relation="exclusive", strength="strict", condition="",
                        differences=["Models view vs. Labs view"], confidence="high", reason="Can't be #1 and #3.")]


def test_parse_skips_bad_entries():
    out = parse(reply(
        rel(a="A9"),                                   # 没有这个市场
        rel(a="B1", b="A1"),                           # 方向反了
        rel(a="A2", possible=P(True, True, True, True)),   # 四种都可能 = 没关系
        rel(a="A2", strength="maybe"),                 # 不认识的成立程度
        rel(a="A2", possible={"yes_yes": "no"}),        # possible 不是布尔
        rel(), rel(),                                  # 重复的只留一条
    ), A, B)
    assert [(r["market_a"], r["market_b"]) for r in out] == [("ma1", "mb1")]
    assert out[0]["confidence"] == "low"               # 没给就算 low


@pytest.mark.parametrize("text", ["I could not decide.", "{not json}", '{"answer": 1}'])
def test_parse_rejects_bad_reply(text):
    with pytest.raises(JudgeError):
        parse(text, A, B)


def test_judge_calls_ask_with_prompt():
    seen = []

    def ask(prompt):
        seen.append(prompt)
        return reply()

    assert judge(ask, A, B, "2026-11-19") == []
    assert seen and seen[0].startswith("You compare prediction markets")
