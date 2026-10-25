# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

# Polymarket 实时数据与模拟盘系统

## 目标
实时采集 Polymarket（科技/AI 类市场）的市场列表、订单簿/成交、评论 → Kafka → 归档 + 实时指标 → 模拟盘 → 可回放回测。
三个目标：实战 Kafka、可回放的模拟盘、能在面试中讲清楚的简历项目。真钱交易不在本期范围（美国用户只能通过 Polymarket US 交易）。

第一期全部用 Python 3.12；模拟盘只模拟主动吃单。完整的阶段计划（阶段 0–6）和验收标准见根目录的 `Polymarket 实时数据与模拟盘系统：目标与交付物.docx`（含个人信息，只留在本机：.gitignore 忽略 *.docx，2026-10-22 起不再跟踪，但早期提交的历史里还有）。每个阶段满足验收标准后才进入下一阶段，阶段 6 的内容不提前做。

## 与用户协作的方式
- 用户是数据工程师，刚学完 Kafka 入门课。请用中文大白话解释，少堆术语。
- 每完成一步，都要解释做了什么、为什么这么做（用户要在面试中讲清楚）。
- 关键设计决策先提方案，由用户决定。每次只做一小步，做完让用户确认再继续。
- 写调用 Polymarket API 的代码之前，先查官方文档（https://docs.polymarket.com/llms.txt），不要凭记忆写。
  注意：CLOB 已在 2026 年 4 月升级到 V2，网上很多教程是旧版；V2 市场的 clobTokenIds 可能为空，ID 在 positionIds 里；RTDS 的价格订阅迁到了 PolyBolt，RTDS 的实时评论推送正在下线且没有替代（所以评论用 REST 轮询）。
- 代码注释和输出信息使用中文，与现有脚本保持一致。

## 环境
- 台式机 DESKTOP-FAQJAN1（Windows，24 小时开机）：代码在 D:\polymarket，Docker Desktop（WSL2）。所有程序都跑在台式机上。
- 用户在笔记本上通过 VS Code Remote-SSH 连接台式机（ssh desktop）。
- Python：项目虚拟环境 .venv。运行依赖只写在 requirements.txt（固定版本，Docker 也用它）；pyproject.toml 通过 dynamic dependencies 引用它，dev 依赖（pytest、ruff）写在 pyproject 的 `[dev]` 里。
- 坑：通过 SSH 远程执行 docker pull 会报凭据错误（Windows 凭据管理器在 SSH 会话中不可用）。
  镜像必须由用户在台式机本地执行 docker compose pull；其他 docker 命令可以远程执行。
- `data/`、`*.parquet`、`.env`、`*.docx` 不进 Git。
- GitHub：**公开仓库** https://github.com/xuh5/polymarket-Fuzzy-Cognitive-Map（2026-10-22 创建，remote `origin`，分支 master；名字以后可能改）。公开的，提交前确认没有密码等敏感内容。README.md 是英文、面向外部读者，项目内容变了要同步更新。gh 登录令牌用 `--insecure-storage` 存在 %APPDATA%\GitHub CLI\hosts.yml（SSH 会话读不到 Windows 凭据管理器）；git push 也走 gh 的令牌（已执行 `gh auth setup-git`）。

## 常用命令

```bash
.venv\Scripts\activate
pip install -e ".[dev,dbt]"              # 以可编辑模式安装本项目 + pytest/ruff + dbt（新环境装一次即可）
pytest                                   # 跑测试（纯逻辑，不连网、不连 Kafka，零点几秒）
ruff check .                             # lint；ruff check . --fix 自动修可修的

python scripts/create_topics.py                       # 创建 topic / 同步已存在 topic 的配置
python -m pmdata.ingest.fetch_markets                 # 拉一轮 Gamma 开放市场写 pm.raw.markets；--every 3600 一直跑
python -m pmdata.ingest.select_markets                # 从 pm.raw.markets 筛出订阅名单并打印（只读）
python -m pmdata.ingest.collect_clob --duration 300   # WebSocket 采集写 pm.raw.clob；不加 --duration 一直跑；--refresh-every 默认 3600
python -m pmdata.ingest.collect_comments              # 轮询评论一轮写 pm.raw.comments；--every 600 一直跑
python -m pmdata.quality.check_clob --hours 24        # 数据质量检查（只读）：断线/缺口、每小时消息量、重复、缺 received_at、吞吐量
python -m pmdata.query.market_24h --list               # 列出过去 24 小时有数据的市场（不知道查哪个时先看这个）
python -m pmdata.query.market_24h --market google      # 阶段 2 验收查询：某市场过去 24 小时的汇总/缺口/成交/盘口（读 gold 表，先 dbt build）；--hours/--end/--limit
python -m pmdata.archive.archiver --duration 90 --group-id pm-archiver-test   # 试跑归档（用测试 group，别占用正式的 pm-archiver；配合 PM_DATA_DIR 写到别处）
python -m pmdata.metrics.run --group-id pm-metrics-test --warmup-hours 2 --duration 120   # 试跑实时指标（注意：会写进正式的 pm.metrics.* topic）
python scripts/verify_book_replay.py [日期]             # 回放 bronze 核对本地订单簿（全量约 2.5 分钟、CPU 占满，尽量指定日期）
```

dbt（在 `dbt/` 目录下运行；Windows 上必须设 `PYTHONUTF8=1`，否则 dbt 用 GBK 读含中文的 yml 会报 UnicodeDecodeError）：

```bash
cd dbt
PYTHONUTF8=1 ../.venv/Scripts/dbt build      # 建所有模型 + 跑所有测试（PowerShell：$env:PYTHONUTF8=1; dbt build）
```

在宿主机手动跑采集程序时注意：容器里的采集器也在写同一个 topic（会多出 conn_id 和缺口标记，影响 check_clob 的统计）。

## 代码结构

- `src/pmdata/common/`：所有程序共用的东西，改约定只改这里。`config.py`（Kafka 地址、topic 名、接口地址）、`kafka.py`（`make_producer()`、`envelope()` 统一信封、`read_to_end()` 把 topic 读到「调用时刻」的末尾、`latest_by_key()` 读 compact topic 并按 key 去重/处理 tombstone）、`gamma.py`（带 429/5xx 重试的 `get_json`）、`timeutil.py`、`log.py`（`setup_logging()`，UTC 时间）。`config.DATA_DIR` 读环境变量 `PM_DATA_DIR`（默认 `data`，容器里是 `/app/data`）。
  `read_to_end` 从每个分区的 low watermark 开始读（不是写死 offset 0：clob 的旧数据被保留策略删掉后 offset 0 不存在，consumer 会按 auto.offset.reset 跳到末尾而读不到东西）；传 `since` 时用 `offsets_for_times` 按时间定位。
- `src/pmdata/ingest/`：阶段 1 采集（fetch_markets、select_markets、collect_clob、collect_comments）。
- `src/pmdata/quality/`：数据质量检查。
- `src/pmdata/archive/`：阶段 2 归档（archiver）。
- `src/pmdata/query/`：查询工具（market_24h：按 slug/conditionId/Gamma ID 精确匹配，否则按问题文本模糊搜索；只读打开 warehouse.duckdb，按 exchange_ts 筛选）。测试用内存 DuckDB 建迷你 gold/silver 表。后续阶段按同样方式加 `metrics/`、`sim/`、`replay/`。
- `dbt/`：dbt-duckdb 项目。数据库是 `data/warehouse.duckdb`（单文件，同一时间只能一个程序写）；`profiles.yml` 放在项目里；source `bronze` 用 `external_location` 直接读 `../data/bronze/pm.raw.{name}/*/*.parquet`（hive_partitioning 生成 date 列）。`models/staging` → schema `silver`，`models/marts` → schema `gold`（`macros/generate_schema_name.sql` 去掉了 dbt 默认的 `main_` 前缀）；都是 table，每次全量重建。staging 命名 `stg_<来源>__<实体>`，每个模型先按 kafka_partition + kafka_offset 去重（qualify row_number）。
- `scripts/`：一次性脚本——create_topics.py；阶段 2 验收用的核对脚本 verify_gold_vs_bronze.py（Python 独立重算 vs gold）、verify_trades_vs_official.py（vs 官方 Data API v2，`--start/--end`），用户决定不做成定期任务，只保留以便复现。`tests/`：pytest，采集器的 Kafka 写入用 FakeProducer 替换（monkeypatch 模块里的 `make_producer`）。

## Docker

采集和归档程序正式运行在 Docker 里（Dockerfile 打包成 `polymarket-app` 镜像，`PYTHONPATH=/app/src`，compose 里的 `fetch-markets`、`collect-clob`、`collect-comments`、`archiver`、`metrics`、`pg-sink` 六个服务共用它，用 `python -m pmdata.xxx` 启动，`restart: unless-stopped`）。archiver 把 `./data` 挂到 `/app/data`，`stop_grace_period: 60s` 让它停止前写完手里的数据：

```bash
docker compose ps                          # 看各容器状态
docker logs -f collect-clob                # 看采集器日志（每分钟一行统计）
DOCKER_BUILDKIT=0 docker build -t polymarket-app .   # 改了代码后重新构建（SSH 下必须这样）
docker compose up -d --no-deps <服务名>     # 只用新镜像重建指定服务
```

- 容器里跑的是构建镜像时 COPY 进去的代码副本，改 D 盘上的代码不影响正在跑的容器；只有重建镜像 + 重建容器才会换代码。
- SSH 下 `docker compose build` 会报凭据错误：BuildKit 即使本地有基础镜像也会去 Docker Hub 查元数据。用旧版构建器 `DOCKER_BUILDKIT=0 docker build` 直接用本地基础镜像就行；换基础镜像版本时才需要用户在台式机本地构建。
- 重建镜像后**不要**直接 `docker compose up -d`：镜像 ID 变了，compose 会重建所有用这个镜像的容器，正在跑的采集器会被重启（产生缺口）。只想动某个服务时用 `up -d --no-deps <服务名>`。
- 想验证新镜像又不影响正在跑的容器：用别的标签构建（`-t polymarket-app:test`），再 `docker run --rm --network polymarket_default -e KAFKA_BOOTSTRAP=kafka:19092 polymarket-app:test python -m ...` 跑只读命令。

另有 `postgres`（postgres:17.6，卷 pg-data，5432 端口）和 `grafana`（grafana/grafana:12.1.1，3000 端口，VS Code 端口转发访问，用户名 admin）。密码在根目录 `.env`（POSTGRES_PASSWORD、GRAFANA_ADMIN_PASSWORD，随机生成、不进 Git）：compose 自动读它做变量替换，宿主机程序由 config.py 的 load_dotenv 读。Grafana 的数据源、看板和告警用 provisioning 放在 `grafana/`（看板 JSON 是 `grafana/dashboards/overview.json`，网页上改了要导出覆盖这个文件才会保留；告警是 `grafana/provisioning/alerting/alerts.yml`，发到 Discord）。

Airflow 3.3.2（2026-10-23 加入，镜像 `polymarket-airflow` = 官方 `apache/airflow:3.3.2-python3.12` + `/opt/dbt-venv` 里单独装的 dbt，见 `airflow/Dockerfile`）：精简部署 LocalExecutor，`airflow-init`（一次性：建元数据表 + admin 用户）、`airflow-apiserver`（网页，宿主机 **8081**，只绑 127.0.0.1，用户 admin / `.env` 的 AIRFLOW_ADMIN_PASSWORD）、`airflow-scheduler`（任务就在这里跑，挂了 `./dbt` 和 `./data` 到 `/opt/project/`）、`airflow-dag-processor`；不装 triggerer/redis/worker。元数据库是现有 postgres 里单独的 `airflow` 库和用户（手动建的，新环境要先建，命令见 README）。DAG 在 `airflow/dags/`（只读挂载，改了不用重建镜像）。基础镜像同样要用户在台式机本地 pull，之后 SSH 下 `DOCKER_BUILDKIT=0 docker build -t polymarket-airflow airflow/`，再 `docker compose up -d --no-deps airflow-init airflow-apiserver airflow-scheduler airflow-dag-processor`。
- 坑：Git Bash 里 `docker exec` 带 `/opt/...` 路径要加 `MSYS_NO_PATHCONV=1`，否则路径被改成 Windows 路径。
- 手动跑一次 DAG（不用先打开）：`MSYS_NO_PATHCONV=1 docker exec airflow-scheduler airflow dags test dbt_daily`。

Kafka 地址读环境变量 `KAFKA_BOOTSTRAP`（在 `pmdata.common.config`），默认 `localhost:9092`（宿主机直接跑）；容器里设为 `kafka:19092`。

## Kafka（docker-compose.yml）
- confluentinc/cp-kafka:7.9.0，单节点 KRaft，已关闭自动建 topic（`KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`）：写入不存在的 topic 会超时报错，新 topic 必须先在 create_topics.py 里建。
- 宿主机程序连 localhost:9092；Docker 内部服务连 kafka:19092。
- Kafka UI（kafbat）：8080 端口，用户通过 VS Code 端口转发访问 localhost:8080。
- 危险命令（docker compose down -v 等会删数据的操作）执行前必须先问用户。

## Topics（scripts/create_topics.py 管理，幂等，可重复运行）
新增 topic 也加在这个脚本里。

| topic | key | 分区 | 策略 |
|---|---|---|---|
| pm.raw.markets | 市场 id | 3 | compact |
| pm.raw.clob | 市场 id（conditionId，即 WS 消息的 `market` 字段） | 3 | 保留 7 天；订单簿快照、变化、成交放在同一个 topic，以保证顺序 |
| pm.raw.comments | 评论 id | 3 | compact，永久保留每条评论的最新版本 |
| pm.metrics.book | 市场 id | 3 | 保留 7 天；盘口类指标，按 token 每秒最多一条 |
| pm.metrics.trades | 市场 id | 3 | 保留 7 天；成交类指标，按 token 每分钟一条 |

create_topics.py 对已存在的 topic 也会按代码同步配置（incremental_alter_configs，值为 None 表示删除该项恢复默认）。注意 compact 只清理已封存的 segment（默认 1GB 或 7 天才封存），所以小 topic 在头 7 天里会保留所有历史版本、offset 远大于 key 数；读 compact topic 一律用 `latest_by_key()`（后读到的覆盖先读到的）。

pm.raw.markets 的 tombstone：fetch_markets 每轮对比上一轮，从开放列表里消失的市场（已关闭）写一条 value 为空的消息，compact 后这个 key 就被删掉；`load_latest_markets()` 读到空 value 会把该市场从结果里去掉。保护：本轮拉取有任何报错就整轮作废、不写；一轮消失超过 20%（MAX_GONE_RATIO）视为 API 异常，不写 tombstone。实测 3 天后首次刷新写了 66 条，逐个查 Gamma 全部 closed=true，其中有 endDate 在 11 月但提前关闭的——光靠 endDate 过滤挡不住这种。

pm.raw.clob 的 key 用市场 ID 而不是 token ID：实测一条 `price_change` 几乎总是同时包含同一市场 Yes/No 两个 token 的变化。按 token 做 key 要么得拆消息（违反原样入库），要么另一个 token 的顺序得不到保证；按市场做 key，同一市场的所有事件落在同一分区、严格有序。

消息格式：信封 `{source, received_at(UTC ISO 毫秒), ..., payload: 原始数据，一个字都不改}`。原样入库是回放回测和数据追溯的前提，转换和清洗放在下游消费者里做。

pm.raw.clob 的信封额外字段：`conn_id`（每次 WebSocket 连接一个随机 ID）、`seq`（该连接内第几帧，从 1 开始；REST 和 collector 消息为 null）、`frame_index`（一帧是事件数组时拆开后的下标）。`source` 有三种：
- `clob_ws`：WebSocket 原始事件（book / price_change / last_trade_price / best_bid_ask / tick_size_change / market_resolved）。
  `new_market` 不写 Kafka、只计数：它推的是全平台所有新市场（实测 5 分钟 560 条，几乎全是体育盘口，占消息量约 10%），而新市场信息随时能从 Gamma API 拉回来，丢掉不会造成不可挽回的损失。
  key 一律是市场 ID：优先用事件里的 `market`，没有就用订阅名单把 `asset_id` 换算成市场 ID；都拿不到时计数（`无法确定市场`）并打印警告，key 为 null，消息照样写入。
- `clob_rest`：重连后 10 秒内没收到 book 快照时，用 `POST https://clob.polymarket.com/books` 补拉的快照（结构同 book，但没有 event_type）
- `collector`：采集器自己写的缺口标记 `{event_type: "gap", market, disconnected_at, reconnected_at, reason}`，每次连接成功后给每个订阅市场各写一条；`disconnected_at` 为 null 表示采集器刚启动或该市场刚加入订阅名单（reason 区分）。下游读到它就应作废该市场的订单簿状态，等下一个快照。
  名单刷新时被移出的市场写 `{event_type: "unsubscribed", market, at, reason}`，表示之后没数据是正常停止。实测退订后约 0.2 秒内还会到几条在途消息，下游要容忍。

pm.raw.comments：评论接口 `GET https://gamma-api.polymarket.com/comments?parent_entity_type=Event&parent_entity_id=...`（只能按 Event/Series 查，不能按 market）。`limit`/`offset` 都按**主楼**计数，回复跟在主楼后一起返回；排序按主楼时间，旧主楼下的新回复不会排到前面，所以每轮拉每个事件的全部评论、按评论 ID + 指纹（body/updatedAt/reactionCount/reportCount）比对，只写新增或变化的。key 改成评论 ID + compact 的原因：采集器启动时读一遍 topic 就知道哪些存过；若只保留 7 天，重启后 7 天前的评论会被当新评论重复写入。信封 `{source: "gamma_comments", received_at, event_id, payload}`。评论很少：首次回填 405 个事件共 951 条（约 1KB/条），新评论一天几条到几十条。

Producer 约定：`enable.idempotence=True` + `lz4` 压缩；HTTP 请求遇到 429/5xx 时按指数退避重试。

## 进度
- 阶段 0 完成：SSH、Docker、Kafka、Kafka UI、Git。
- 阶段 1 完成（2026-10-19 验收通过）：
  - 已完成：建好 3 个 topic；pmdata.ingest.fetch_markets 拉取 tech(1401)/ai(439) 标签的开放市场，去重后 2922 个写入 pm.raw.markets（同一市场可能同时带两个标签，按市场 id 去重并合并 tag_slugs）。
  - 市场数据中有用的字段：volume24hr、liquidityNum、acceptingOrders、enableOrderBook、clobTokenIds、events[0].id、feeSchedule（taker 费率，模拟盘要用）、orderPriceMinTickSize、orderMinSize、version。
  - 已完成：pmdata.ingest.select_markets 筛选订阅名单——acceptingOrders=true 且 enableOrderBook=true、endDate 距今至少 MIN_DAYS_LEFT(2) 天（已过期和无 endDate 的排除），按 volume24hr 降序取前 30 个；按 version 取两个 token（v1 解析 clobTokenIds 的 JSON 字符串，v2 用 positionIds），下标 0/1 对应 outcomes[0]/[1]。非 Yes/No 的二选一市场（如 Anthropic/OpenAI）和系列题都保留。目前数据全是 v1，v2 分支未经真实数据验证。
    已过期的市场 acceptingOrders 仍可能是 true（等待结算），所以必须单独按 endDate 过滤。
  - 已完成（第一版）：pmdata.ingest.collect_clob。启动时筛一次名单，运行期间不变；每 10 秒发文本 `PING`，30 秒无消息即重连，重连等待 1→2→4…最多 60 秒。
    WebSocket 实测：订阅后服务器立即推送所有 token 的 book 快照（一帧 JSON 数组），所以重连后重新订阅就能拿到新快照；文档没写这一点。
  - 已完成：订阅名单自动刷新，两个程序通过 Kafka 解耦——fetch_markets.py `--every 3600` 定时拉 Gamma 写 pm.raw.markets（含 tombstone）；collect_clob.py 每 `--refresh-every` 秒重读 pm.raw.markets 重新筛名单，新旧对比后在同一连接上发 `{"assets_ids": [...], "operation": "subscribe"|"unsubscribe"}`。读 Kafka 失败或新名单为空时沿用旧名单。拉市场的程序挂了不影响采集，只是名单不再更新。
    实测动态 subscribe 后服务器同样立即推送新 token 的 book 快照。
  - 已完成：两个程序放进 Docker。24 小时验收从 2026-10-18 18:27 UTC 开始（collect-clob 首个 conn_id=8f673cdfa78a）。
  - 已完成：pmdata.quality.check_clob。用 `offsets_for_times` 按时间定位到起点再读，不从头扫；断线次数按 conn_id 对缺口标记去重统计。
  - 已完成：pmdata.ingest.collect_comments（REST 轮询，每 10 分钟一轮，一轮约 170 秒），已放进 Docker（collect-comments）。
  - 已完成：项目重构为 `src/pmdata` 包 + pyproject + pytest + ruff，2026-10-19 21:26 UTC 部署到三个容器。
  - **验收结果**（collect-clob 2026-10-18 18:27 ~ 10-19 21:24 UTC，26.9 小时，无人工干预）：196.6 万条消息，缺 received_at 0 条，重复 0 条；平均 20.3 条/秒，峰值 83 条/秒；断线 1 次——服务端主动关闭（1001 going away），1.6 秒后自动重连并拿到新快照，缺口占比 0.0017%（完整率 99.998%）；名单自动刷新移出/补入 32 个市场。collect-comments 运行 22 小时、103 轮，1 次单个事件请求超时（下一轮自动补上），用户确认视为通过。
- 阶段 2（归档与查询）完成（2026-10-20 用户验收通过）。交付物：archiver（bronze）、dbt silver 7 + gold 4 个模型（80 个测试）、查询工具 market_24h、正确性核对、存储/完整率指标。遗留可选：采集器静默超时 30 秒可考虑缩短（先确认服务端回 PONG，观察几天再定）。设计与过程：
  - 三个 topic 都归档成 Parquet；目录 `data/bronze/<topic>/date=YYYY-MM-DD/part-xxx.parquet`（Hive 风格分区，DuckDB 可按日期裁剪）。
  - 信封字段拆成列（received_at、source、event_type、market、conn_id、seq 等）+ Kafka 的 partition/offset/key/timestamp，payload 原样存成 JSON 字符串；partition+offset 用于去重。
  - 每 5 分钟（或攒够一定条数）写一个文件，**文件写成功后才提交 offset**（at-least-once：崩溃最多重复、不会丢）。
  - **已完成：pmdata.archive.archiver，2026-10-20 05:04 UTC 起在 Docker（archiver 容器）里运行**。一个 consumer（group `pm-archiver`）订阅三个 topic，每个 topic 各攒一批，满 5 分钟或 10 万条写一个文件；按 Kafka 时间戳的 UTC 日期分目录（跨午夜的批次拆成两个文件）；先写 .tmp 再改名；SIGTERM 时写完剩余数据、提交后退出。
    列：kafka_partition/offset/key/timestamp、source、received_at、各 topic 抽取列（clob：event_type/market/conn_id/seq/frame_index；comments：event_id；markets：is_tombstone）+ `value` = **Kafka 消息原文**（整条信封，不经 json 重新序列化，避免 0.50→0.5、中文转义之类的改动；tombstone 为 null）。抽取列只为过滤方便，以 value 为准。
    首次启动从 offset 0 补读 4 天积压（240 万条）十几秒追上；Kafka 里 lz4 的 1.35GB → Parquet zstd 约 200MB。核对：每个分区 offset 从 0 连续、无重复、已提交 = 最后一条 + 1；docker restart 后从提交位置继续、无重复。
    分区倾斜（clob p0/p1/p2 ≈ 49%/34%/17%）：按市场 key 分区 + 单个热门市场占 11%，属 key skew；当前吞吐下无影响，保持按市场分区以保证同市场有序。
    小文件合并先不做。duckdb 等做 dbt 时再加。
  - **用 dbt（用户 2026-10-19 决定）**：dbt 不能读 Kafka，所以分两段——Python 归档程序负责 Kafka → Parquet（练 consumer group / offset 提交），dbt-duckdb 负责 Parquet 之后的 SQL 加工（练 dbt）。
    - 仓库根目录建 `dbt/` 项目，引擎用 DuckDB（dbt-duckdb，本机即可跑，不需要数据仓库）；归档出的 Parquet 作为 dbt 的 source。
    - 分层对应 medallion：Parquet 原始归档 = bronze（dbt sources）；`models/staging`（silver）把 payload JSON 拆成列——如 book 快照、price_change 逐条展开、成交、缺口标记、市场名录每小时快照、评论；`models/marts`（gold）放面向分析的表。
    - 阶段 2 验收「查出任一市场过去 24 小时的成交和盘口变化」做成 dbt 模型或 analysis。
    - 数据质量检查（重复、缺口、非空、取值范围）尽量写成 dbt tests；`pmdata.quality.check_clob` 继续保留，用于检查 Kafka 里的实时数据。
    - 适用范围：dbt 只管批量的历史数据加工；阶段 3 的实时指标仍然是 Python consumer 从 Kafka 读着算，dbt 是批处理工具用不上。
    - dbt 先在宿主机手动跑 `dbt run`（用户 2026-10-19 决定），模型稳定后再考虑定时。版本：dbt-core 1.12.6 + dbt-duckdb 1.11.0 + duckdb 1.5.6，放在 pyproject 的 `[dbt]` 组（Docker 不需要，不进 requirements.txt）。
    - 数据模型（用户 2026-10-20 同意）：silver 一种事件一张表（trades、price_changes 展开、book_levels 展开、best_bid_ask、markers、markets snapshots、comments），只拆列/去重；gold 用星型模型，阶段 2 只建验收需要的 dim_markets、dim_tokens（asset_id → 市场 + Yes/No）、fct_trades、fct_top_of_book。时间两个都留：exchange_ts（payload.timestamp 毫秒）和 received_at。完整订单簿重建不放 dbt，阶段 3/5 用 Python 做。
    - 已完成 silver 7 个模型 + 47 个测试（`dbt build` 约 22 秒，warehouse.duckdb 约 270MB）：stg_clob__trades / price_changes（unnest + generate_subscripts 展开，行数 = 消息数 × 2）/ book_levels（bids、asks 各自展开；level_index 是原数组下标，原数组不是按最优价排序）/ best_bid_ask / markers / stg_markets__snapshots（同时有 gamma_market_id 和 market_id=conditionId；outcomes、clobTokenIds 是 JSON 字符串要先 json() 再转数组）/ stg_comments（每条评论只留最新版本）。公共片段在 `macros/bronze_helpers.sql`（dedup_kafka、ms_to_ts），通用测试 `tests/generic/between.sql`。核对：成交里出现过的 token 100% 能在市场名录里找到。
    - 已完成 gold 4 个模型 + 25 个测试（含 relationships 外键测试）：dim_markets（每个市场最近一次有内容的快照；已关闭的保留并标 is_delisted，历史成交还要查问题文本）、dim_tokens（unnest token_ids/outcomes，asset_id → 市场 + 选项）、fct_trades（加 notional = price × size）、fct_top_of_book（price_change 的变化后最优价 + 订单簿快照算出的最优价，按 asset_id 内 kafka_offset 顺序只留买一/卖一真的变了的行；加 spread、mid_price）。
    - 已完成：验收查询工具 pmdata.query.market_24h（duckdb 依赖移到 requirements.txt；容器镜像下次重建时才会装上，当前容器不需要）。
      报告的盘口汇总按 as-of 取值（mid_at_start = 窗口开始前最后一次变化后的值，*_at_end 同理，book_as_of 标明盘口来自何时）：用户验收时发现冷门市场（Indiana 2027）窗口内 1 万多条 price_change 但买一卖一一次没变，fct_top_of_book 0 行，旧版报告就看不到盘口；数据本身是对的。
    - **正确性核对（验收第 ② 步，2026-10-20 通过）**：
      1. 独立实现对账：用 Python 直接解析 bronze 原始 JSON（不经 SQL），自己去重、按 offset 顺序推算买一卖一变化，和 gold 逐条比对；两次随机共 12 个市场（含 4 个正在采集的活跃市场，最多 307 笔成交 / 2 万次变化）全部逐条一致。注意比对时要截到仓库建表时的 offset（之后 archiver 又写了新数据）。
      2. 和官方 Data API v2 `GET https://data-api.polymarket.com/v2/trades?condition=<conditionId>&filter_amount=0&taker_only=true`（游标分页 next_cursor；start/end 只对 user 查询生效，按市场只能从新往旧翻；timestamp 是区块时间秒；默认 filter_amount 会滤掉 0.01 份以下）按 (transaction_hash, token_id) 配对：10-20 13:00~19:30 一直在采集的 5 个市场共 253 笔，全部配上、数量全部一致，期间 5 次断线没有漏成交。
      3. 永久对账测试 `dbt/tests/assert_clob_silver_counts_match_bronze.sql`：silver 各表行数 = bronze 去重后的消息数（price_change 按数组长度、快照按快照数），只比 silver 建表时读到的 offset 范围。
    - **指标（2026-10-20 统计）**：
      - 存储：10-19 全天 clob 178.6 万条，原始 JSON 1388MB → Parquet(zstd) 156.5MB，**压缩 8.9 倍**（约 88 字节/条）；markets 559MB → 37.7MB（14.8 倍，每小时整份名录，重复度高）；comments 可忽略。对比 Kafka 里 lz4 只压到约 1.3 倍（按条压缩、批次小）。按现在的量一年 clob 约 57GB。
      - 小文件：每 5 分钟一个文件，clob 一天约 290 个（10-20 已 197 个），暂不影响查询，量大了再做合并。
      - 完整率（缺口 = 重连时刻 − 重连前最后一条 WS 消息的时间，所有市场合计）：10-19 全天重连 3 次、缺口 15.5 秒，**99.982%**（含 21:26 部署重启 10.7 秒）；10-20 0:00~20:40 重连 14 次、缺口 67.9 秒，**99.909%**——其中 12:37 服务端 42 秒不发消息也不断开，靠 30 秒无消息超时才发现，单次占 41.6 秒。同期 collect-comments、fetch-markets 的 REST 请求全部正常，只有 WebSocket 长连接被断，原因未定（可能是服务端/中间网络回收长连接）。
    - **发现：断线变频繁**。阶段 1 验收期 27 小时断 1 次；10-19 23:46 起到 10-20 19:40 断了 15 次（几乎都是 `ConnectionClosedError: no close frame received or sent`，1 次 30 秒无消息），每次约 1.6 秒恢复，合计缺口约 23 秒（完整率仍约 99.97%）。原因未查，算完整率指标时一起看。
      盘口来源对比（10-19）：price_change 里的最优价变化 28.9 万次 / 51 个市场，best_bid_ask 事件只有 9260 次 / 29 个市场（86% 能在前者里找到），所以不用 best_bid_ask 事件。bronze 实测：一天 price_change 177 万（98%）、best_bid_ask 9260、book 3286、成交仅 1524 笔。
  - **发现：台式机时钟漂移**。received_at − exchange_ts 的中位数呈锯齿：每小时约 −0.07 秒、一天累计约 −1.6 秒后突然跳回（被校准一次），所有事件类型偏移一致；Windows Time 服务（w32time）是 STOPPED。阶段 2 不受影响（行情用 exchange_ts）；**阶段 3 测延迟之前必须由用户在台式机用管理员 PowerShell 开启 w32time 并配置 NTP**（sc config w32time start= auto; net start w32time; w32tm /config /manualpeerlist:"time.cloudflare.com,0x9" /syncfromflags:manual /update; w32tm /resync）。
  - 已完成（2026-10-20）：长期运行的服务（fetch_markets、collect_clob、collect_comments、archiver）统一用 logging（模块里 `log = logging.getLogger(...)`，入口调 `setup_logging()`）；命令行报告（select_markets、check_clob、market_24h、scripts）保留 print，因为输出就是报告本身。**三个采集容器还在跑旧代码**，下次重建镜像时用 `up -d --no-deps <服务>` 逐个换。CI 暂不做（用户 2026-10-19 决定）。
  - 按 medallion 架构命名（用户 2026-10-19 确认）：bronze = Parquet 原始归档（data/bronze/），silver/gold = dbt 模型（存在 DuckDB 文件里）。

- 阶段 3（实时处理）完成（2026-10-22 用户验收通过）。用户说之后目标要变（2026-10-22，尚未细说；仓库名里的 Fuzzy Cognitive Map 可能与此有关），阶段 4 开工前先问清楚。
- **之后的安排（用户 2026-10-22 决定）**：阶段 4、5（模拟交易、回测）暂缓——用户认可简单盘口指标 + 主动吃单不可能盈利，项目方向要重新想。两条线并行：
  - 稳定（Claude 做，每步让用户确认）：① 告警推送（Grafana 告警：容器挂了 / 数据断了通知到手机，待用户定渠道：邮件 / Telegram / Discord）② Airflow（每天 dbt build、数据质量检查，出问题通知）③ Schema Registry 等新目标需要新 topic 时再加。
  - 研究方向（用户主导，Claude 用已有 bronze/gold 数据做快速探索）：讨论过的方向——预测市场的收益来源是判断比市场准、相关市场之间的不一致（如不同截止日期、negRisk 各选项之和）、消息在相关市场间传导的延迟；Fuzzy Cognitive Map 可用来建模市场/事件之间的影响关系。可做的探索：市场间 log-odds 变化的相关性和领先滞后、逻辑相关市场的价格矛盾、新闻冲击下的反应时差。开工前用户须先在台式机开启 w32time 时间同步（见上面「台式机时钟漂移」）。
  - 和阶段 2 的区别：阶段 2 的 fct_top_of_book 只有买一卖一（直接取 price_change 自带的 best_bid/best_ask），且是批量事后算；阶段 3 在内存里实时维护完整多档订单簿，阶段 4 模拟吃单算滑点要用。
  - 实时指标（用户 2026-10-21 选定）：盘口类按 token——中间价/价差、加权中间价（microprice）、盘口不平衡（前 1 档 / 前 5 档）、深度（中间价 ±1 分 / ±5 分内的美元挂单量）；成交类按市场 1 分钟窗口——成交量/成交额/笔数、主动买 vs 主动卖；数据新鲜度（距上一条消息几秒，给告警用）；多时间尺度价格变化（1/5/15/60 分钟，用对数几率 log-odds 相减，不直接用价格相减）。不做：VWAP、Yes+No 套利检查、评论热度。
    输出频率：盘口类每个 token 最多每秒一次（取这一秒最后的值，顺便避开交易所拆消息的「半步」状态）；成交类每分钟一次。
  - 已向用户提出的拆分（2026-10-20，待下个会话拍板）：① 订单簿重建器（新 consumer group 读 pm.raw.clob；快照整份替换、price_change 改一档、gap 作废等快照；写成不依赖数据来源的纯逻辑模块，阶段 4/5 复用；验收 = 定时拉官方 REST 快照逐档对比）→ ② 实时指标（中间价、价差、盘口不平衡；成交量/热度按 1 分钟窗口）写新 topic pm.metrics.* → ③ consumer 写 Postgres + Grafana（Docker）→ ④ 延迟 p50/p99（每条指标带交易所时间/收到时间/算完时间）。
  - 用户 2026-10-21 选 A：重建器和指标放一个程序（订单簿是内存状态，按状态边界拆服务；指标与落库之间已有 pm.metrics 解耦）。
  - 2026-10-21 用户已开启 w32time（自动启动，源 time.cloudflare.com），但默认配置校正很慢（MinPollInterval 10 = 17 分钟、UpdateInterval 360000），开启后仍慢约 0.98 秒；测延迟前还要调快（待处理）。
  - 已完成：订单簿模块 `src/pmdata/book/orderbook.py`（纯逻辑：`BookStore.apply(信封)` → 有变化的 asset_id；价格/数量用 Decimal；快照整份替换、price_change 的 size 是该价位新总量、0 删除；gap 作废、unsubscribed/market_resolved 删除；不可用时的 price_change 忽略并计数）+ tests/test_orderbook.py。
    回放核对 `scripts/verify_book_replay.py`（bronze 全量 5 天、1020 万次）：本地买一卖一 vs price_change 自带的 best_bid/best_ask 不一致 189 次（0.002%），全部在 10-21 06:52 唯一一次 REST 补拉之后 4 分钟内（REST 快照时间戳比当时 WS 流还旧，之后自愈）。
    发现：交易所会把同一时刻的一次更新拆成多条消息（交易所时间戳相同），每条带的最优价都是全部改完后的；同一条消息里同一 token 有多处变化时也一样。所以按（token, 时间戳）分组比对；阶段 3 算指标时也要注意这种「半步」状态。交易所用 best_bid=0 / best_ask=1 表示该侧为空。
    按单日回放时开头没有快照，会忽略大量变化（正常）。
  - 用户 2026-10-21 同意：指标写两个 topic `pm.metrics.book`（按 token 每秒最多一条，含价格变化）和 `pm.metrics.trades`（按 token 每分钟一条），key 都是市场 ID；时间一律用交易所时间（event time，回放结果才和实时一致），只有数据新鲜度用电脑时间；价格变化用中间价。
  - 已完成：指标模块 `src/pmdata/metrics/engine.py`（纯逻辑，`MetricsEngine.apply(信封)` → [(topic, key, 指标)]，内部用 BookStore）+ tests/test_metrics_engine.py。
    「这一秒/分钟结束」：同市场消息同分区有序，收到该市场更晚的消息即结束；冷门市场靠运行程序每秒 `flush(现在 − 宽限)` 兜底，回放结束调 `flush()`。别的市场的时间推进不能用来结束本市场（在别的分区，回放时分区顺序和实时不同）。
    成交按 token 而不是按市场汇总（主动买 Yes 和主动买 No 意思相反）。WS 成交的 side = 吃单方方向：和官方 Data API taker_only 逐笔比对 869 笔全部一致。
    log-odds 价格夹到 [0.001, 0.999]；价格变化 = logit(现在中间价) − logit(N 秒前那一刻的中间价)，没有那么早的数据时为 null。数据新鲜度（指标 9）放在运行程序里，不在 engine。
    运行程序 `pmdata.metrics.run`（group `pm-metrics`）：启动时 offsets_for_times 往回 24 小时预热（只更新状态不输出），offset >= 上次提交位置才输出；第一次启动从当前末尾开始输出（`plan_start`）。at-least-once：producer.flush 确认后才提交；写 Kafka 失败直接退出让 Docker 重启。所有分区追上后每秒 `flush(现在 − 3 秒)` 兜底。分区重新分配时清空 engine 重新预热（只跑一个实例）。SIGTERM 时不 flush engine，只提交。
    2026-10-22 宿主机试跑（预热 2 小时、19.5 万条 10 秒追上）：60 个 token 全部有输出，约 13 条/秒。延迟（算完 − 收到）稳定后 p50 1.9 秒、p99 5 秒——是设计决定的：等这一秒结束 + 等同市场下一条消息或 3 秒宽限 + 1 秒检查间隔；预热完成那一刻会补输出一批攒着的秒（延迟几十秒，正常）。收到 − 交易所时间中位数 −113ms，说明本机时钟仍偏慢，w32time 待调。
    **2026-10-22 09:15 UTC 起在 Docker（metrics 容器）里正式运行**：首次启动预热 211 万条 71 秒；预热期间新到的消息之后一口气补处理（那一批延迟几十秒，正常）；追上后 60 个 token 全有输出，算完 − 收到 p50 1.7 秒 / p90 4.3 秒 / p99 5.0 秒；追上后「忽略的变化」不再增长（所有盘口都可用）。
    回放 10-20 全天：173 万条消息 126 秒（约 1.4 万条/秒，实时只有约 20 条/秒），输出盘口指标 88.9 万条（116 个 token）、成交指标 894 条；最大 1 小时 log-odds 变化 1.67，和 gold.fct_top_of_book 同时段中间价 0.045→0.2265 一致。

  - 已完成（2026-10-22，用户同意的设计）：Postgres + Grafana。`pmdata.sink.pg_writer`（group `pm-pg-sink`，容器 pg-sink，09:48 UTC 起运行）读 pm.metrics.book / pm.metrics.trades / pm.raw.markets，每批（≤5000 条）先在批内按主键去重，再 upsert（ON CONFLICT DO UPDATE）进 metrics_book（主键 asset_id+second）、metrics_trades（asset_id+minute）、tokens（asset_id → 市场问题、Yes/No，tombstone 不删）；数据库事务提交后才提交 offset；启动时自动建表；每小时删 7 天前的数据；`inserted_at` 由数据库填，测端到端延迟。首次启动补写 2.3 万行指标 + 31.7 万条市场消息（去重成 6272 个 token），一分钟内追上。
    看板 9 个面板：数据新鲜度（>30 秒变红；真正的告警推送要等通知渠道定了再做）、异动榜（Yes 按 60 分钟 log-odds 变化绝对值排序）、选市场后看盘口/价格变化/价差/盘口不平衡/深度、成交流向（两个 token 合并成 Yes 视角：看涨 = 买 Yes + 卖 No）、延迟 p50/p99。所有面板 SQL 都在真实数据上跑过。
  - 阶段 3 验收（2026-10-22 通过）：
    - 看板实时刷新：用户 2026-10-22 打开看板确认。Grafana 保留密码登录（用户先要免登录又改回要密码；admin + .env 里的 GRAFANA_ADMIN_PASSWORD）。
    - 本地订单簿 vs 官方 REST 快照：`scripts/verify_book_vs_rest.py`（同一份 BookStore，预热 24 小时追上实时，每轮 POST /books 要全部 token，拿本地「处理到快照时间戳 T 为止」的状态逐档比：该市场来了时间 > T 的消息就在处理它之前比）。2026-10-22 跑 10 轮（每分钟一轮）：能对齐的 584 个盘口 582 个完全一致（99.66%），42047 个价位 2 个不一致（99.9952%）；那 2 个是同一市场 Yes/No 镜像的同一处变化，本地最后更新时间 = T（同一毫秒），即官方快照取在这一毫秒的更新之前，是同毫秒边界问题，不是重建错误。另有 10 个盘口「官方快照比本地旧」（拿到快照时本地最后更新已 > T，盘口不能倒退，无法对齐，单独统计）。
    - 延迟（用户决定不校准时钟，注明误差即可）：2026-10-22 09:50~10:23 UTC，60 个 token、2.4 万条指标。交易所→收到 p50 −149ms / p99 −64ms（负数是本机时钟慢约 0.23 秒，实际约 80ms）；收到→算完 p50 1.70s / p99 4.97s（「每秒输出一次 + 3 秒宽限」的设计等待）；算完→入库 p50 0.51s / p99 1.00s（sink 每秒一批）；**端到端 交易所→入库 p50 2.1s / p99 5.6s（时钟误差约 ±0.3 秒）**。
      时钟：w32time 已开，但默认最长 9 小时才校准一次、慢慢拨，期间每小时慢约 0.07 秒，误差在 0～0.5 秒间摆动，不会无限扩大。要准确测「交易所→收到」再调快（MinPollInterval/MaxPollInterval 6、UpdateInterval 100）。

- 稳定线 ② Airflow（2026-10-23）：
  - 用户 2026-10-22 同意的方案：精简部署（LocalExecutor）、元数据放现有 postgres、第一个 DAG 每天 UTC 01:00 dbt build；一次一个 DAG，跑稳再加。
  - 已完成：`airflow/dags/dbt_daily.py`（BashOperator 跑 `/opt/dbt-venv/bin/dbt build`；catchup=False、max_active_runs=1、失败 10 分钟后重试 1 次；dbt 的 target/日志放容器 /tmp，不写进挂载的 dbt/）。2026-10-23 08:43 用 `dags test` 跑通：97 秒，80/80 通过。用户 2026-10-23 已在网页上打开，之后每天 01:00 自动跑。
  - **DuckDB 内存（2026-10-23 踩坑）**：容器里第一次跑 dbt 爆内存（DuckDB 用到 12.6GB；还连带 9p 挂载读文件报 Cannot allocate memory），采集器等容器没受影响。二分定位：原因是 stg_markets__snapshots 每条原始 JSON 约 8KB，**每个线程解析 JSON 都占一大块内存**，DuckDB 默认 24 线程；不去重、只取 6 个字段也会爆，和去重写法无关（一度以为是「带着 value 去重」，改了模型后实测推翻，已改回）。4GB 上限下：4 线程 0.4 秒爆，2 线程 14 秒跑完；clob 模型 24 线程也没问题。解决：profiles.yml 的 memory_limit / threads 读环境变量 `DUCKDB_MEMORY_LIMIT` / `DUCKDB_THREADS`，DAG 里设 4GB / 2；宿主机默认 16GB / 24（原默认 80% = 25GB，加上 Docker 的 15.5GB 超过整机 32GB）。容器里全量 dbt build 约 2 分钟。
    另：archiver 首次启动补读积压时把 10-19 的 markets 写成了单个 7 万行、一个 row group 的文件（value 解压后 559MB），实测不是爆内存的原因；以后如做小文件合并可顺便重写。
- 稳定线 ① 告警推送（2026-10-25，用户选 Discord）：
  - Discord webhook 地址在 `.env` 的 `DISCORD_WEBHOOK_URL`（用户自己填的，谁拿到都能往频道发消息，不进 Git），compose 传给 grafana 和 Airflow 容器。
  - Grafana 告警用 provisioning：`grafana/provisioning/alerting/alerts.yml`（联系方式 discord + 通知策略：按 alertname 分组、没恢复每 4 小时再提醒、恢复时发 ✅ + 规则组「数据管道」每分钟查 Postgres）。规则：① 实时数据断了：metrics_book 最新 inserted_at 超过 120 秒且持续 1 分钟（10-23 统计 23 小时最长间隔 4 秒）；② 市场名录没更新：tokens 最新 updated_at 超过 2.5 小时、持续 5 分钟。两条都是查不到数据 / Postgres 连不上也告警。文件里 `$XXX` 会被替换成环境变量（annotations 除外），通知模板里别写 `$`。
  - Airflow：`airflow/dags/notify.py` 的 `discord_on_failure`，dbt_daily 的 default_args 挂 on_failure_callback（重试用完仍失败才发）。
  - 2026-10-25 验证：两条规则都在跑、状态正常；Grafana 测试接口（POST /api/alertmanager/grafana/config/api/v1/receivers/test）和 Airflow 回调各发一条测试消息到 Discord，都成功。
    真实演练（用户同意）：07:47:10 停 pg-sink → 07:50:45 触发（3 分 35 秒）→ 07:51:15 🔴 发到 Discord → 07:58:14 启动（演练脚本在 Git Bash 里用 python -c 匹配中文规则名失败，多停了 7 分钟；pg-sink 从提交的 offset 续上，Postgres 数据完整）→ 07:59:40 恢复正常 → 08:01:15 ✅ 发出。查发送结果：GET /api/alertmanager/grafana/config/api/v1/receivers 的 lastNotifyAttempt / lastNotifyAttemptError。
  - **2026-10-23 09:11 UTC Windows 自动更新重启了台式机**（MoUsoCoreWorker 发起）：42 秒后开机，约 1 分钟后所有容器自动起来（collect-clob 重启 1 次后连上 Kafka）。采集缺口 2 分 8 秒（09:11:07 → 09:13:14），看板指标断 3 分 44 秒（metrics 预热 24 小时），bronze 无重复；Airflow 10-24、10-25 的 dbt_daily 都成功。用户问能不能关机：能，所有消费者从提交的 offset 续上，但关机期间的 WebSocket 行情（订单簿变化、成交）永久缺失（评论下一轮补齐；成交可用 Data API 补）。建议用户设置 Windows 更新的「使用时段」。
  - 待做（第二批）：外部心跳（整机/Docker/网络挂了时 Grafana 自己也挂了，要靠 healthchecks.io 之类，需用户注册；用户 2026-10-25 说「再说吧」）；每小时健康检查 DAG（归档在写文件、评论采集、断线次数、磁盘）。之后再按下面「Airflow」里的清单加 DAG。
  - 用户 2026-10-25 说想学 Airflow：已讲过概念（DAG/Task/Operator/DAG Run/Task Instance、4 个容器分工、catchup、回调、DAG 文件顶层代码会被反复执行）和 dbt tests 的三种写法；建议拿「每小时健康检查 DAG」当练习（Claude 搭骨架、用户写关键部分，练 `a >> b` 依赖和 PythonOperator）。
  - 用户问过关机能不能补数据：Kafka 只能让下游补「已进 Kafka 的数据」；关机期间官方能补的只有成交（Data API v2 /trades）和价格曲线（`GET https://data-api.polymarket.com/v2/prices-history`，bucket_seconds 60~86400，1 分钟粒度至少保留 7 天、5 分钟 60 天、30 分钟 90 天、3/12 小时永久，单次范围最多 15 天）；**历史订单簿官方没有任何接口**（文档明说要自己存）。补数程序还没写。

- **下一次会话从这里开始（2026-10-25）**：稳定线主体已完成，剩下的都是可选小活（外部心跳、健康检查 DAG、Kafka 消费积压监控、三个采集容器换新代码）。已向用户提议研究线先做一个只读的小探索：逻辑相关市场的价格矛盾（同一事件不同阈值如估值 2T ≥ 3T、不同截止日期 10 月 ≤ 12 月、negRisk 各选项之和 ≈ 1），看违反频率、幅度、扣手续费后还剩多少。数据：订单簿只有成交量前 30 的市场，但 bronze 的 pm.raw.markets 每小时有全部约 2900 个市场的快照（payload 里带价格字段，用前先确认字段名），可做小时级分析。用户还没选「研究探索」还是「健康检查 DAG 练 Airflow」，开工前先问。

## 以后的想法（暂不做）
- Schema Registry（用户 2026-10-20 问起）：建议在阶段 3 引入，用于我们自己定义格式的 pm.metrics.* topic（Avro 或 JSON Schema，兼容性规则 BACKWARD），可顺带用 Kafka Connect JDBC Sink 写 Postgres 代替手写 consumer；raw topic 的 payload 是外部原样数据、格式不归我们管，不适合强约束，最多给信封注册一个宽松 schema。需新增 confluentinc/cp-schema-registry:7.9.0 容器（镜像要用户在台式机本地 pull）。
- Airflow（用户 2026-10-20 决定：先做阶段 3，Airflow 以后再做，计划不大改；2026-10-23 已部署，见上面进度）。想做的 DAG：① 每天定时 dbt build（**已完成 dbt_daily**）② 数据质量检查（check_clob + dbt test，不达标通知用户）③ 上游新鲜度检查（可用 dbt `source freshness`；分钟级的「采集器挂了」更适合阶段 3 的 Grafana 告警）④ bronze 小文件合并 ⑤ 每天抽几个市场和官方 Data API 对账（用户同意把核对脚本改成定期任务）。之后还可放阶段 5 的每晚回测、阶段 6 的历史回填。通知渠道：Discord（2026-10-25 定）。Airflow 只调度批处理，常驻服务仍由 Docker 管。
- 流处理框架（用户 2026-10-22 决定：阶段 3 先全用 Python，以后可能换）：讨论过 Flink（要加 JobManager/TaskManager 集群、PyFlink 调试痛苦，订单簿只能用 DataStream API 写）和 Kafka Streams（Java 库、无集群、状态存 Kafka changelog、自带 exactly-once，计划文档里本就列为阶段 3 可选加分项）；也提过 Python 的 Quix Streams。以后替换的做法：新程序用新 consumer group 读 pm.raw.clob、先写测试 topic（如 pm.metrics.book.v2）和 Python 版并行对比，一致后切换正式 topic，下游不用改——前提是 pm.metrics.* 的消息格式写成文档固定下来。Python engine 保留作阶段 4/5 复用和参考实现。
- Spark：用户同意当前数据量（clob 约 150MB/天）不需要，DuckDB 更快；放到阶段 5 以后（如阶段 6 大量历史回填 + S3）再考虑。
- 结算结果（resolution state 接口）：阶段 4/5 算盈亏必须知道哪边赢，作为阶段 4 的一部分做。WS 的 market_resolved 只覆盖订阅的市场。
- 阶段 6：历史价格（prices history，可回填几个月，用于更长回测；一次性批量数据应直接写 Parquet，不走 Kafka）；带钱包地址的成交明细、持仓大户、排行榜、按钱包的交易历史/已平仓持仓/盈亏曲线（胜率需自己从已平仓持仓算），用于大户/聪明钱分析。
- negRisk 套利候选：订阅整个 negRisk 事件的全部选项（如「谁有最好的 AI 模型」各家公司），检查各选项 Yes 价格之和是否偏离 1。偏离（扣除手续费后）可作为套利策略的候选，留到阶段 4 模拟交易时考虑。
