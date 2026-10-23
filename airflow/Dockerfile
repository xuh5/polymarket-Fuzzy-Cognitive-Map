# Airflow 镜像：官方镜像 + dbt（给每天定时跑 dbt build 用）
# 基础镜像要用户在台式机本地 docker pull（SSH 下拉不了），之后在 SSH 下用 DOCKER_BUILDKIT=0 docker build 构建
FROM apache/airflow:3.3.2-python3.12

# dbt 装进单独的虚拟环境，不装进 Airflow 自己的 Python 环境：
# 两者依赖很多（如 protobuf、jinja2），混在一起容易版本冲突，升级其中一个可能把另一个弄坏。
# 版本和宿主机一致（pyproject 的 [dbt] 组 + requirements.txt 的 duckdb），改版本时两边一起改：
# duckdb 版本不同可能读写不了同一个 warehouse.duckdb 文件。
USER root
RUN python -m venv /opt/dbt-venv \
 && PIP_USER=false /opt/dbt-venv/bin/pip install --no-cache-dir \
      dbt-core==1.12.6 dbt-duckdb==1.11.0 duckdb==1.5.6
USER airflow
