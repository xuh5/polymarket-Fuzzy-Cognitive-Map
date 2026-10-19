from pmdata.ingest.fetch_markets import find_gone


def test_markets_missing_from_this_round_get_tombstones():
    gone, suspicious = find_gone({"1", "2", "3", "4", "5"}, {"1", "2", "3", "4", "6"})
    assert gone == {"5"}
    assert not suspicious


def test_too_many_disappearing_at_once_is_treated_as_api_error():
    gone, suspicious = find_gone({"1", "2", "3", "4", "5"}, {"1", "2"}, max_ratio=0.2)
    assert gone == set()
    assert suspicious


def test_first_round_with_empty_catalog_deletes_nothing():
    assert find_gone(set(), {"1", "2"}) == (set(), False)
