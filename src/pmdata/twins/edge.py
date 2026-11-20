"""孪生市场的缺口：按两边当前的 Yes 买一 / 卖一，算「买一组保证至少拿回 1 美元的组合」要花多少。

都按吃单算（在卖一买进），不含手续费：
- exclusive（不能都是 Yes）：两边各买 No，最少一个 No 赢 → 成本 (1 − A 买一) + (1 − B 买一)
- implies（A 是 → B 一定是）：买 A 的 No + B 的 Yes。A 是时 B 也是（B 的 Yes 赢），A 否时 A 的 No 赢
  → 成本 (1 − A 买一) + B 卖一；implied_by 反过来
- complement（正好一个是 Yes）：两边买 Yes 或两边买 No 都保证拿回 1，取便宜的
- identical（同一个问题）：一边 Yes 一边 No，两个方向取便宜的
No 的价格用 Yes 的镜像：买 No 的价格 = 1 − Yes 买一（盘口本来就是镜像的）。
edge = 1 − 成本：大于 0 就是扣手续费之前有缺口。
mid_gap 按中间价算（买 Yes = 中间价，买 No = 1 − 中间价）：吃单没缺口、中间价有，说明挂单等成交可能有机会。
"""


def _buy(side, q):
    """在卖一买进 Yes 或 No 的价格；q = (Yes 买一, Yes 卖一)。没有对应的挂单返回 None。"""
    bid, ask = q
    if side == "Yes":
        return ask
    return None if bid is None else 1 - bid


def gap(relation, qa, qb):
    """relation 从 A 的角度说；qa / qb = (Yes 买一, Yes 卖一)，没有的价给 None。
    返回最便宜的那一组：{"a": "Yes"/"No", "b": "Yes"/"No", "cost": 成本, "edge": 1 − 成本}；算不出来返回 None。"""
    options = {
        "exclusive": [("No", "No")],
        "implies": [("No", "Yes")],
        "implied_by": [("Yes", "No")],
        "complement": [("Yes", "Yes"), ("No", "No")],
        "identical": [("Yes", "No"), ("No", "Yes")],
    }.get(relation, [])
    best = None
    for sa, sb in options:
        pa, pb = _buy(sa, qa), _buy(sb, qb)
        if pa is None or pb is None:
            continue
        cost = round(pa + pb, 6)
        if best is None or cost < best["cost"]:
            best = {"a": sa, "b": sb, "cost": cost, "edge": round(1 - cost, 6)}
    return best


def mid_gap(relation, qa, qb):
    """同 gap，但两边都按 Yes 中间价算；任一边缺买一或卖一（没有中间价）返回 None。"""
    if None in (*qa, *qb):
        return None
    ma, mb = (qa[0] + qa[1]) / 2, (qb[0] + qb[1]) / 2
    return gap(relation, (ma, ma), (mb, mb))


FLIP = {"implies": "implied_by", "implied_by": "implies"}


def flip(relation):
    """换成从 B 的角度说（网站上从当前市场的角度显示）。"""
    return FLIP.get(relation, relation)
