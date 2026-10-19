"""调用 Gamma API（市场、事件、评论都从这里拿）。"""
import time

import requests

from pmdata.common.config import GAMMA_URL

session = requests.Session()


def get_json(path, params=None):
    """调用接口；遇到限流(429)或服务器错误(5xx)就等一会儿重试。"""
    for attempt in range(5):
        resp = session.get(f"{GAMMA_URL}{path}", params=params, timeout=15)
        if resp.status_code == 429 or resp.status_code >= 500:
            wait = 2 ** attempt
            print(f"  收到 {resp.status_code}，{wait} 秒后重试")
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp.json()
    resp.raise_for_status()
