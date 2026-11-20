"""把孪生市场加进订阅名单：读 pm.twins，给 pmdata.ingest.select_markets 提供一个「额外理由」。

订阅中的市场（按成交额选的前 N 个）如果有孪生，把孪生也订阅上：网站的 Related 只显示两边都有实时盘口的孪生，
缺口要用两边的实时买一卖一算。只扩一层（孪生的孪生不加），数量由 select_markets 的 max_extra 封顶。
读 pm.twins 失败时返回 None：名单照旧只有前 N 个，采集不受影响。
"""
import logging
from collections import defaultdict

from pmdata.common.config import TOPIC_TWINS
from pmdata.common.kafka import latest_by_key

log = logging.getLogger("pmdata.twins.watch")

GROUP_ID = "pm-twins-watch"   # 只读、不提交 offset


def twin_map(entries):
    """pm.twins 的最新消息（latest_by_key 的值）→ {市场 ID: {孪生市场 ID}}。只看判断成功的组。"""
    out = defaultdict(set)
    for env in entries:
        p = env["payload"]
        if p.get("status") != "ok":
            continue
        for r in p["relations"]:
            out[r["market_a"]].add(r["market_b"])
            out[r["market_b"]].add(r["market_a"])
    return out


def expander(twins):
    """{市场: 孪生} → select_markets 用的 expand(选中的市场) -> {孪生市场: 理由}。"""
    def expand(chosen):
        out = {}
        for m in sorted(chosen):
            for t in sorted(twins.get(m, ())):
                out.setdefault(t, f"twin of {m}")
        return out
    return expand


def twin_expander(group_id=GROUP_ID):
    """读 pm.twins 做成 expand；读失败返回 None（只订阅前 N 个）。"""
    try:
        return expander(twin_map(latest_by_key(TOPIC_TWINS, group_id).values()))
    except Exception as e:
        log.warning("读 %s 失败，这次名单不加孪生：%s: %s", TOPIC_TWINS, type(e).__name__, e)
        return None
