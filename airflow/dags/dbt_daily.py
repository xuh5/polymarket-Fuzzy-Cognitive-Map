"""每天 UTC 01:00 跑一次 dbt build：把 data/bronze 的 Parquet 重新加工成 silver/gold（data/warehouse.duckdb）。

01:00 时前一天的 bronze 已经写完整（archiver 每 5 分钟落一批）。dbt build = 建模型 + 跑测试，
任何一个测试不过整个任务就失败（标红），所以这个 DAG 同时也是每天一次的数据质量检查。
"""

from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG
from notify import discord_on_failure

DBT_DIR = "/opt/project/dbt"   # compose 里把 ./dbt 挂到这里，./data 挂到 /opt/project/data

with DAG(
    dag_id="dbt_daily",
    schedule="0 1 * * *",
    start_date=pendulum.datetime(2026, 10, 23, tz="UTC"),
    catchup=False,             # 不补跑 start_date 以来错过的日子：每次都是全量重建，补跑没有意义
    max_active_runs=1,         # warehouse.duckdb 同一时间只能一个程序写
    dagrun_timeout=timedelta(hours=1),
    default_args={
        "retries": 1,          # 失败（比如数据库正被别的程序占着）10 分钟后再试一次
        "retry_delay": timedelta(minutes=10),
        "on_failure_callback": discord_on_failure,   # 重试后仍失败就发 Discord
    },
    tags=["dbt"],
):
    BashOperator(
        task_id="dbt_build",
        bash_command="/opt/dbt-venv/bin/dbt build",
        cwd=DBT_DIR,
        env={
            # 编译结果和 dbt 自己的日志放容器里，不写进挂载的 dbt/ 目录，免得和宿主机手动跑的混在一起
            "DBT_TARGET_PATH": "/tmp/dbt-target",
            "DBT_LOG_PATH": "/tmp/dbt-logs",
            "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
            "DBT_USE_COLORS": "false",   # Airflow 任务日志里不要颜色控制符
            # DuckDB 内存上限和线程数（profiles.yml 读它们）：容器和 Kafka、采集器共用 Docker 的 15.5GB 内存。
            # 线程多了每个线程解析 JSON 的内存加起来会超过上限，详见 profiles.yml
            "DUCKDB_MEMORY_LIMIT": "4GB",
            "DUCKDB_THREADS": "2",
        },
        append_env=True,       # 在容器原有环境变量的基础上加这几个（不加的话 PATH 等都没了）
        execution_timeout=timedelta(minutes=45),
    )
