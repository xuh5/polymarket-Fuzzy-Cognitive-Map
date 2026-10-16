# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

# Polymarket 实时数据与模拟盘系统

## 目标
实时采集 Polymarket（科技/AI 类市场）的市场列表、订单簿/成交、评论 → Kafka → 归档 + 实时指标 → 模拟盘 → 可回放回测。
三个目标：实战 Kafka、可回放的模拟盘、能在面试中讲清楚的简历项目。真钱交易不在本期范围（美国用户只能通过 Polymarket US 交易）。

第一期全部用 Python 3.12；模拟盘只模拟主动吃单。完整的阶段计划（阶段 0–6）和验收标准见根目录的 `Polymarket 实时数据与模拟盘系统：目标与交付物.docx`。每个阶段满足验收标准后才进入下一阶段，阶段 6 的内容不提前做。

## 与用户协作的方式
- 用户是数据工程师，刚学完 Kafka 入门课。请用中文大白话解释，少堆术语。
- 每完成一步，都要解释做了什么、为什么这么做（用户要在面试中讲清楚）。
- 关键设计决策先提方案，由用户决定。每次只做一小步，做完让用户确认再继续。
- 写调用 Polymarket API 的代码之前，先查官方文档（https://docs.polymarket.com/llms.txt），不要凭记忆写。
  注意：CLOB 已在 2026 年 4 月升级到 V2，网上很多教程是旧版；V2 市场的 clobTokenIds 可能为空，ID 在 positionIds 里；RTDS 正在迁移到 PolyBolt。
- 代码注释和输出信息使用中文，与现有脚本保持一致。

## 环境
- 台式机 DESKTOP-FAQJAN1（Windows，24 小时开机）：代码在 D:\polymarket，Docker Desktop（WSL2）。所有程序都跑在台式机上。
- 用户在笔记本上通过 VS Code Remote-SSH 连接台式机（ssh desktop）。
- Python：项目虚拟环境 .venv，依赖写在 requirements.txt。
- 坑：通过 SSH 远程执行 docker pull 会报凭据错误（Windows 凭据管理器在 SSH 会话中不可用）。
  镜像必须由用户在台式机本地执行 docker compose pull；其他 docker 命令可以远程执行。
- `data/`、`*.parquet`、`.env` 不进 Git。

## 常用命令

```bash
docker compose up -d                     # 启动 Kafka + Kafka UI
.venv\Scripts\activate                   # 激活虚拟环境
pip install -r requirements.txt
python scripts/create_topics.py          # 创建 topic
python scripts/fetch_markets.py          # 从 Gamma API 拉开放市场写入 pm.raw.markets
```

目前没有测试、lint 或构建配置。

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
| pm.raw.clob | 资产/token id | 3 | 保留 7 天；订单簿快照、变化、成交放在同一个 topic，以保证顺序 |
| pm.raw.comments | 事件 id | 3 | 保留 7 天 |

消息格式：信封 `{source, received_at(UTC ISO 毫秒), ..., payload: 原始数据，一个字都不改}`。原样入库是回放回测和数据追溯的前提，转换和清洗放在下游消费者里做。

Producer 约定：`enable.idempotence=True` + `lz4` 压缩；HTTP 请求遇到 429/5xx 时按指数退避重试。

## 进度
- 阶段 0 完成：SSH、Docker、Kafka、Kafka UI、Git。
- 阶段 1 进行中：
  - 已完成：建好 3 个 topic；scripts/fetch_markets.py 拉取 tech(1401)/ai(439) 标签的开放市场，去重后 2922 个写入 pm.raw.markets（同一市场可能同时带两个标签，按市场 id 去重并合并 tag_slugs）。
  - 市场数据中有用的字段：volume24hr、liquidityNum、acceptingOrders、enableOrderBook、clobTokenIds、events[0].id、feeSchedule（taker 费率，模拟盘要用）、orderPriceMinTickSize、orderMinSize、version。
  - 下一步：筛选订阅名单（acceptingOrders=true 且 enableOrderBook=true，按 volume24hr 降序取前 30 个，每个市场取 Yes/No 两个 token）→ WebSocket 采集订单簿和成交写入 pm.raw.clob（带断线重连和心跳，重连后重新拿快照并记录缺口）→ 采集评论。
  - 验收标准：连续运行 24 小时无人工干预；每条消息带本地接收时间。
