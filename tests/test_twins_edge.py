import pytest

from pmdata.twins.edge import flip, gap, mid_gap


def test_exclusive_buys_both_no():
    # 「< 1.25T」Yes 买一 0.41、「> 1.25T」Yes 买一 0.61：两个 No 共 0.59 + 0.39 = 0.98，缺口 2 分
    assert gap("exclusive", (0.41, 0.42), (0.61, 0.62)) == {"a": "No", "b": "No", "cost": 0.98, "edge": 0.02}


def test_implies_buys_no_on_a_and_yes_on_b():
    # A 是 → B 是，但 A 的 Yes 买一 0.50 比 B 的卖一 0.45 还高：买 A 的 No 0.50 + B 的 Yes 0.45 = 0.95
    assert gap("implies", (0.50, 0.52), (0.44, 0.45)) == {"a": "No", "b": "Yes", "cost": 0.95, "edge": 0.05}
    assert gap("implied_by", (0.44, 0.45), (0.50, 0.52)) == {"a": "Yes", "b": "No", "cost": 0.95, "edge": 0.05}


@pytest.mark.parametrize("relation, qa, qb, expected", [
    ("complement", (0.40, 0.41), (0.55, 0.57), {"a": "Yes", "b": "Yes", "cost": 0.98, "edge": 0.02}),
    ("complement", (0.45, 0.47), (0.58, 0.60), {"a": "No", "b": "No", "cost": 0.97, "edge": 0.03}),
    ("identical", (0.30, 0.31), (0.35, 0.36), {"a": "Yes", "b": "No", "cost": 0.96, "edge": 0.04}),
])
def test_two_way_relations_pick_cheaper(relation, qa, qb, expected):
    assert gap(relation, qa, qb) == expected


def test_missing_prices():
    assert gap("exclusive", (None, 0.1), (0.5, 0.6)) is None   # 没有买一，买不到 No
    assert gap("unknown", (0.5, 0.6), (0.5, 0.6)) is None


def test_flip():
    assert flip("implies") == "implied_by" and flip("exclusive") == "exclusive"


def test_mid_gap():
    # 吃单 0.41+0.42 的 No：(1−0.41)+(1−0.61)=0.98；按中间价 (1−0.415)+(1−0.615)=0.97
    assert mid_gap("exclusive", (0.41, 0.42), (0.61, 0.62))["edge"] == 0.03
    # 价差很宽时吃单差很多，按中间价正好打平
    assert gap("exclusive", (0.40, 0.50), (0.50, 0.60))["edge"] == -0.1
    assert mid_gap("exclusive", (0.40, 0.50), (0.50, 0.60))["edge"] == 0.0
    assert mid_gap("exclusive", (None, 0.5), (0.5, 0.6)) is None
