"""时间工具：项目里所有时间都用 UTC、ISO 格式、精确到毫秒。"""
from datetime import UTC, datetime


def now_iso():
    return datetime.now(UTC).isoformat(timespec="milliseconds")


def parse_time(s):
    return datetime.fromisoformat(s) if s else None
