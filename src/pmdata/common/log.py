"""日志：所有程序统一用 logging 输出，格式为「UTC 时间 级别 模块: 内容」。"""
import logging
import time


def setup_logging(level=logging.INFO):
    logging.Formatter.converter = time.gmtime   # 日志时间也用 UTC，和数据里的时间对得上
    logging.basicConfig(
        level=level,
        format="%(asctime)sZ %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
