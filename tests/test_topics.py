import pytest

from pmdata.topics.rules import classify


@pytest.mark.parametrize("question, expected", [
    ("Will Anthropic's market cap be between $1.5T and $1.75T at market close on IPO day?",
     ("IPO & valuation", "Anthropic")),
    ("Will Baidu have the best AI model at the end of December 2026?", ("AI rankings", "Baidu")),
    ("Will Moonshot be the second-best AI lab at the end of January 2027?", ("AI rankings", "Moonshot")),
    ("Will Gemini Carbon be released on November 10, 2026?", ("Releases", "Google")),
    ("Will Apple be the largest company in the world by market cap on December 31?", ("Biggest company", "Apple")),
    ("Will Texas enact a data center moratorium by December 31, 2027?", ("Policy", None)),
    ("Will Matt Damon be the #1 searched person on Google this year?", ("Mentions & charts", "Google")),  # 不是 AI 排名
    ("Will Microsoft say \"Gaming\" during earnings call?", ("Mentions & charts", "Microsoft")),
    ("Will the Ornn A100 Index be between $0.75 and $1.00 on November 30, 2026?", ("Compute", None)),
    ("AI bubble burst in 2026?", ("Other", None)),
    ("Will the OpenAI spy be accused of working for Meta by December 31?", ("Policy", "OpenAI")),   # 最先提到的公司
    ("Will Claude go down 8-10 times in October?", ("Company news", "Anthropic")),
    ("Will meta-analysis show apple consumption helps?", ("Other", None)),   # 小写 meta / apple 不算公司
])
def test_classify(question, expected):
    assert classify(question) == expected
