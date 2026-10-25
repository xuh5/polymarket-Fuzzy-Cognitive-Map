"""任务失败时发 Discord 消息。webhook 地址读环境变量 DISCORD_WEBHOOK_URL（在 .env 里，compose 传进容器）。

用法：DAG 的 default_args 里加 "on_failure_callback": discord_on_failure。
重试用完仍失败才会调用（中间的重试不发），所以收到消息就说明要人看一下了。
"""

import logging
import os

import requests

log = logging.getLogger(__name__)


def discord_on_failure(context):
    url = os.environ.get("DISCORD_WEBHOOK_URL")
    if not url:
        log.warning("没有设置 DISCORD_WEBHOOK_URL，不发失败通知")
        return
    ti = context["ti"]
    text = (
        f"🔴 **Airflow 任务失败：{ti.dag_id} / {ti.task_id}**\n"
        f"运行 {context['run_id']}，第 {ti.try_number} 次尝试（重试后仍失败）\n"
        f"看日志：Airflow 网页 localhost:8081 → {ti.dag_id} → 点红色的格子"
    )
    # 发通知失败不能让回调本身报错，记一条日志就行
    try:
        requests.post(url, json={"content": text}, timeout=10).raise_for_status()
    except requests.RequestException as e:
        log.warning(f"发送 Discord 通知失败：{e}")
