import pytest

from pmdata.common.config import TOPIC_METRICS_BOOK, TOPIC_METRICS_TRADES
from pmdata.signals.detector import JumpDetector

DAY = 86400


@pytest.fixture
def det():
    d = JumpDetector(clock=lambda: "NOW")
    d.outcomes = {"Y": 1, "N": 2}
    return d


def book(det, sec, bid, ask, since=0, asset="Y"):
    rec = {"market": "0xM", "asset_id": asset, "second": sec * 1000, "book_since": since,
           "best_bid": bid, "best_ask": ask, "mid": (bid + ask) / 2, "spread": ask - bid,
           "bid_depth_5c": 100.0, "ask_depth_5c": 50.0}
    return det.apply(TOPIC_METRICS_BOOK, rec)


def trades(det, minute_sec, notional, buy=0.0, sell=0.0, asset="Y", n=1):
    rec = {"market": "0xM", "asset_id": asset, "minute": minute_sec * 1000, "trades": n, "volume": buy + sell,
           "notional": notional, "buy_volume": buy, "sell_volume": sell}
    return det.apply(TOPIC_METRICS_TRADES, rec)


def test_small_moves_are_ignored(det):
    book(det, 0, 0.40, 0.42)
    assert book(det, 300, 0.42, 0.44) == []          # 2 个点


def test_lifecycle_open_closed_final(det):
    book(det, 0, 0.40, 0.42)                          # 中间价 41¢
    [ev] = book(det, 300, 0.44, 0.46)                 # 5 分钟后 45¢：+4 个点，刚过门槛就写
    assert (ev["status"], ev["change_points"]) == ("open", pytest.approx(4))
    assert ev["mid_after_5m"] is None
    assert book(det, 310, 0.49, 0.51) == []           # 涨到 50¢：峰值更新，不另写
    assert book(det, 600, 0.47, 0.49) == []           # 48¢，还在门槛以上（和 5 分钟前比）
    [ev] = det.flush(911 * 1000)                      # 最后一次过门槛后 5 分钟没再过：结束
    assert ev["status"] == "closed"
    assert ev["peak"] == 310_000 and ev["change_points"] == pytest.approx(9)
    assert ev["mid_after_5m"] == pytest.approx(0.48)  # 峰值 5 分钟后（610 秒）那一刻的价格
    assert ev["move_type"] == "both"
    assert ev["event_id"] == "0xM:300"
    assert det.flush(2109 * 1000) == []
    [ev] = det.flush(2110 * 1000)                     # 峰值 30 分钟后补事后价格
    assert ev["status"] == "final" and ev["mid_after_30m"] == pytest.approx(0.48)


def test_wider_spread_when_one_side_pulled(det):
    book(det, 0, 0.40, 0.42)
    [ev] = book(det, 300, 0.40, 0.60)                 # 卖一的挂单撤了，卖一跳到 60¢，中间价 +9 个点
    assert ev["move_type"] == "wider"


def test_back_and_forth_merges_into_one_jump(det):
    book(det, 0, 0.40, 0.42)
    assert len(book(det, 300, 0.40, 0.60)) == 1       # 往上跳
    assert book(det, 350, 0.40, 0.42) == []           # 撤回去
    assert book(det, 590, 0.30, 0.32) == []           # 和 5 分钟前的 41¢ 比往下跳了 10 个点：算同一次，反向一次
    [ev] = det.flush(891 * 1000)
    assert ev["status"] == "closed"
    assert ev["reversals"] == 1 and ev["change_points"] == pytest.approx(-10)


def test_not_compared_across_book_stretches(det):
    book(det, 0, 0.40, 0.42, since=0)
    assert book(det, 300, 0.60, 0.62, since=200_000) == []   # 新的一段（比如重新订阅）：不和上一段比
    assert book(det, 600, 0.61, 0.63, since=200_000) == []


def test_open_jump_ends_with_stretch(det):
    book(det, 0, 0.40, 0.42)
    book(det, 300, 0.44, 0.46)
    out = book(det, 400, 0.50, 0.52, since=400_000)  # 这一段断了：手上的跳动收尾，到不了的事后价格留空
    assert [e["status"] for e in out] == ["closed", "final"]
    assert out[1]["mid_after_5m"] is None and out[1]["mid_after_30m"] is None


def test_only_first_outcome_book_and_known_tokens(det):
    book(det, 0, 0.40, 0.42, asset="N")
    assert book(det, 300, 0.60, 0.62, asset="N") == []
    det.outcomes.pop("N")
    assert trades(det, 0, 10.0, asset="N") == []


def test_volume_flow_and_gamma_baseline(det):
    det.volume24h = {"0xM": 2880.0}                   # 平时 5 分钟 10 美元
    book(det, 0, 0.40, 0.42)
    trades(det, 60, 30.0, buy=50.0)                   # 主动买 Yes：看涨
    trades(det, 180, 20.0, buy=10.0, asset="N")      # 主动买 No：看跌
    trades(det, -600, 999.0, buy=1.0)                 # 窗口以外
    [ev] = book(det, 300, 0.44, 0.46)
    assert ev["trades"] == 2 and ev["notional"] == pytest.approx(50.0)
    assert ev["flow"] == pytest.approx((50 - 10) / 60)
    assert ev["baseline_source"] == "gamma" and ev["volume_ratio"] == pytest.approx(5.0)


def test_own_baseline_once_stretch_is_a_day_old(det):
    det.volume24h = {"0xM": 1e9}
    for h in range(24):
        trades(det, h * 3600 + 600, 12.0)             # 前 24 小时一共 288 美元：平时 5 分钟 1 美元
    book(det, DAY, 0.40, 0.42)
    trades(det, DAY + 120, 7.0)
    [ev] = book(det, DAY + 300, 0.44, 0.46)
    assert ev["baseline_source"] == "own"
    assert ev["baseline_notional_24h"] == pytest.approx(288.0)
    assert ev["volume_ratio"] == pytest.approx(7.0)


def test_short_breaks_keep_own_baseline_long_ones_reset(det):
    det.volume24h = {"0xM": 2880.0}
    book(det, 0, 0.40, 0.42, since=0)
    book(det, 2000, 0.40, 0.42, since=2_000_000)              # 断线 30 多分钟后新的一段：成交记录还算连续
    book(det, DAY, 0.40, 0.42, since=2_000_000)
    [ev] = book(det, DAY + 300, 0.44, 0.46, since=2_000_000)
    assert ev["baseline_source"] == "own"
    book(det, DAY + 9000, 0.40, 0.42, since=(DAY + 9000) * 1000)   # 停了 2 个多小时：重新攒
    book(det, DAY + 10000, 0.40, 0.42, since=(DAY + 9000) * 1000)
    [ev] = book(det, DAY + 10300, 0.44, 0.46, since=(DAY + 9000) * 1000)
    assert ev["baseline_source"] == "gamma"
