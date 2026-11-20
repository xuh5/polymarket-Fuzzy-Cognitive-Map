from pmdata.twins import watch


def env(status, *pairs):
    return {"payload": {"status": status, "relations": [{"market_a": a, "market_b": b} for a, b in pairs]}}


def test_twin_map_both_directions_only_ok():
    m = watch.twin_map([env("ok", ("A", "B"), ("A", "C")), env("error", ("A", "D"))])
    assert m == {"A": {"B", "C"}, "B": {"A"}, "C": {"A"}}


def test_expander_one_level_and_first_reason_wins():
    expand = watch.expander({"A": {"B", "C"}, "B": {"A", "Z"}, "C": {"A"}})
    assert expand({"A"}) == {"B": "twin of A", "C": "twin of A"}          # 孪生的孪生（Z）不加
    assert expand({"A", "B"}) == {"B": "twin of A", "C": "twin of A", "A": "twin of B", "Z": "twin of B"}


def test_twin_expander_survives_kafka_errors(monkeypatch):
    def boom(topic, group):
        raise TimeoutError("kafka down")
    monkeypatch.setattr(watch, "latest_by_key", boom)
    assert watch.twin_expander() is None
