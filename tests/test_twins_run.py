import json
import subprocess

import pytest

from pmdata.twins import run
from pmdata.twins.judge import JudgeError


def m(mid, ev, q="Q?", desc="rules", vol=0.0):
    return dict(market_id=mid, event_id=ev, question=q, description=desc, event_title=f"E{ev}", volume=vol)


BY = {"1": [m("a1", "1", vol=5)], "2": [m("b1", "2", vol=1)], "3": [m("c1", "3", vol=100)]}


def test_fingerprint_changes_with_rules_and_markets():
    fp = run.fingerprint(BY["1"], BY["2"])
    assert fp == run.fingerprint(list(BY["1"]), list(BY["2"]))
    assert fp != run.fingerprint([m("a1", "1", desc="rules v2")], BY["2"])          # 规则改了
    assert fp != run.fingerprint(BY["1"] + [m("a2", "1")], BY["2"])                  # 新加了市场


def test_plan_skips_done_and_orders_by_volume():
    pairs = [("1", "2"), ("1", "3"), ("2", "3")]
    done = {("1", "2"): (run.fingerprint(BY["1"], BY["2"]), "ok"),        # 判断过、没变：跳过
            ("2", "3"): (run.fingerprint(BY["2"], BY["3"]), "error")}     # 上次失败：重判
    todo = run.plan(pairs, BY, done, limit=10)
    assert [(a, b) for a, b, _ in todo] == [("1", "3"), ("2", "3")]       # 成交额 105 在前，101 在后
    assert len(run.plan(pairs, BY, {}, limit=1)) == 1
    # 和订阅中市场有关的优先，哪怕成交额小
    assert [(a, b) for a, b, _ in run.plan(pairs, BY, {}, limit=10, first={("1", "2")})][0] == ("1", "2")


def test_in_scope_keeps_pairs_touching_watched_markets():
    assert run.in_scope([("1", "2"), ("2", "3")], BY, {"a1"}) == [("1", "2")]


def test_gone_and_guard():
    done = {(str(i), str(i + 100)): ("fp", "ok") for i in range(10)}
    pairs = list(done)
    assert run.gone(done, pairs[1:]) == [pairs[0]]          # 少了 1 组（10%）：写 tombstone
    assert run.gone(done, pairs[3:]) is None                 # 少了 3 组（30%）：当成读名录出问题，不写
    assert run.gone({}, []) == []


def test_judgment_payload():
    p = run.judgment("1", "2", "fp", "ok", "claude:haiku", 0.01, [{"market_a": "a1"}])
    assert p["event_a"] == "1" and p["status"] == "ok" and p["relations"] == [{"market_a": "a1"}]
    assert p["judged_at"] and p["error"] is None


class FakeProducer:
    def __init__(self):
        self.sent = []

    def produce(self, topic, key, value, on_delivery):
        self.sent.append((topic, key, value))
        on_delivery(None, None)

    def flush(self, timeout):
        return 0


def test_publish_and_tombstone():
    p = FakeProducer()
    run.publish(p, "1", "2", {"x": 1})
    run.publish(p, "1", "3", None)
    assert p.sent[0][:2] == ("pm.twins", "1:2") and json.loads(p.sent[0][2])["payload"] == {"x": 1}
    assert p.sent[1] == ("pm.twins", "1:3", None)


class Done:
    def __init__(self, stdout, returncode=0, stderr=""):
        self.stdout, self.returncode, self.stderr = stdout, returncode, stderr


def test_claude_cli_returns_result_and_cost(monkeypatch):
    seen = {}

    def fake_run(cmd, **kw):
        seen["cmd"], seen["input"] = cmd, kw["input"]
        return Done(json.dumps({"is_error": False, "result": '{"relations": []}', "total_cost_usd": 0.006}))

    monkeypatch.setattr(subprocess, "run", fake_run)
    ask = run.ClaudeCLI(model="haiku")
    assert ask("PROMPT") == '{"relations": []}' and ask.cost == 0.006
    assert seen["input"] == "PROMPT"
    assert "--setting-sources" in seen["cmd"] and "--strict-mcp-config" in seen["cmd"]   # 不带个人设置


@pytest.mark.parametrize("result", [
    Done(json.dumps({"is_error": True, "result": "Not logged in"}), returncode=1),
    Done("garbage", returncode=1, stderr="boom"),
])
def test_claude_cli_errors(monkeypatch, result):
    monkeypatch.setattr(subprocess, "run", lambda cmd, **kw: result)
    with pytest.raises(JudgeError):
        run.ClaudeCLI()("PROMPT")


def test_claude_cli_timeout(monkeypatch):
    def slow(cmd, **kw):
        raise subprocess.TimeoutExpired(cmd, 1)
    monkeypatch.setattr(subprocess, "run", slow)
    with pytest.raises(JudgeError):
        run.ClaudeCLI(timeout=1)("PROMPT")
