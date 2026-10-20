import json
from datetime import UTC, datetime

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pmdata.archive import archiver
from pmdata.common.config import TOPIC_CLOB, TOPIC_COMMENTS, TOPIC_MARKETS


class FakeMsg:
    """代替 confluent_kafka 的 Message，只实现归档程序用到的方法。"""

    def __init__(self, topic, partition, offset, key, value, ts):
        self._topic, self._partition, self._offset = topic, partition, offset
        self._key = key.encode() if key is not None else None
        self._value = value.encode() if value is not None else None
        self._ts = int(datetime.fromisoformat(ts).timestamp() * 1000)

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def key(self):
        return self._key

    def value(self):
        return self._value

    def timestamp(self):
        return 1, self._ts


class FakeConsumer:
    """代替真正的 consumer：记下提交了哪些 offset。"""

    def __init__(self):
        self.commits = []

    def commit(self, offsets, asynchronous):
        self.commits.append({(tp.topic, tp.partition): tp.offset for tp in offsets})


def clob_msg(offset, ts="2026-10-19T12:00:00+00:00", partition=0, event_type="book"):
    value = json.dumps({"source": "clob_ws", "received_at": ts, "conn_id": "c1", "seq": 7, "frame_index": 0,
                        "payload": {"event_type": event_type, "market": "0xA", "price": "0.50"}})
    return FakeMsg(TOPIC_CLOB, partition, offset, "0xA", value, ts)


class TestToRow:
    def test_clob_columns_extracted_and_value_kept_verbatim(self):
        msg = clob_msg(5)
        row = archiver.to_row(msg)
        assert row["kafka_offset"] == 5 and row["kafka_key"] == "0xA"
        assert row["event_type"] == "book" and row["market"] == "0xA"
        assert row["conn_id"] == "c1" and row["seq"] == 7 and row["frame_index"] == 0
        assert row["value"] == msg.value().decode()   # 原文一个字不改

    def test_market_tombstone(self):
        row = archiver.to_row(FakeMsg(TOPIC_MARKETS, 1, 3, "123", None, "2026-10-19T12:00:00+00:00"))
        assert row["is_tombstone"] is True and row["value"] is None and row["source"] is None

    def test_comment_event_id_as_string(self):
        value = json.dumps({"source": "gamma_comments", "received_at": "t", "event_id": 42, "payload": {}})
        row = archiver.to_row(FakeMsg(TOPIC_COMMENTS, 0, 0, "c9", value, "2026-10-19T12:00:00+00:00"))
        assert row["event_id"] == "42"

    def test_invalid_json_still_archived(self):
        row = archiver.to_row(FakeMsg(TOPIC_CLOB, 0, 0, None, "not json", "2026-10-19T12:00:00+00:00"))
        assert row["value"] == "not json" and row["event_type"] is None


def test_write_files_splits_by_kafka_date(tmp_path):
    rows = [archiver.to_row(clob_msg(1, "2026-10-19T23:59:59+00:00")),
            archiver.to_row(clob_msg(2, "2026-10-20T00:00:01+00:00"))]
    paths = archiver.write_files(TOPIC_CLOB, rows, tmp_path)
    assert [p.parent.name for p in paths] == ["date=2026-10-19", "date=2026-10-20"]
    assert not list(tmp_path.rglob("*.tmp"))
    table = pq.read_table(paths[0])
    assert table.num_rows == 1 and table.schema == archiver.schema_for(TOPIC_CLOB)
    # 比较毫秒数（Windows 上把带时区的时间戳转回 datetime 需要额外装 tzdata）
    expected_ms = int(datetime(2026, 10, 19, 23, 59, 59, tzinfo=UTC).timestamp() * 1000)
    assert table.column("kafka_timestamp").cast(pa.int64())[0].as_py() == expected_ms


class TestFlush:
    def test_commits_last_offset_plus_one_per_partition_after_writing(self, tmp_path):
        consumer = FakeConsumer()
        a = archiver.Archiver(consumer, tmp_path)
        for msg in [clob_msg(10, partition=0), clob_msg(11, partition=0), clob_msg(4, partition=2)]:
            a.handle(msg)
        a.flush(TOPIC_CLOB)
        assert consumer.commits == [{(TOPIC_CLOB, 0): 12, (TOPIC_CLOB, 2): 5}]
        assert pq.read_table(next(tmp_path.rglob("*.parquet"))).num_rows == 3
        assert a.batches[TOPIC_CLOB].rows == []

    def test_no_commit_when_write_fails(self, tmp_path, monkeypatch):
        consumer = FakeConsumer()
        a = archiver.Archiver(consumer, tmp_path)
        a.handle(clob_msg(1))

        def broken(*args):
            raise OSError("磁盘满了")

        monkeypatch.setattr(archiver, "write_files", broken)
        with pytest.raises(OSError):
            a.flush(TOPIC_CLOB)
        assert consumer.commits == []

    def test_empty_batch_writes_nothing(self, tmp_path):
        consumer = FakeConsumer()
        archiver.Archiver(consumer, tmp_path).flush_all()
        assert consumer.commits == [] and not list(tmp_path.rglob("*"))

    def test_due_by_row_count(self, tmp_path):
        consumer = FakeConsumer()
        a = archiver.Archiver(consumer, tmp_path, flush_seconds=3600, max_rows=2)
        a.handle(clob_msg(1))
        a.flush_due()
        assert consumer.commits == []
        a.handle(clob_msg(2))
        a.flush_due()
        assert consumer.commits == [{(TOPIC_CLOB, 0): 3}]
