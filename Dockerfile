# 采集程序的镜像：fetch-markets 和 collect-clob 共用，只是启动命令不同
FROM python:3.12-slim

WORKDIR /app
# 日志立刻输出（不缓冲），docker logs 才能实时看到；中文输出用 UTF-8
ENV PYTHONUNBUFFERED=1 \
    PYTHONIOENCODING=utf-8

# 先装依赖再拷代码：只改代码时，装依赖这一层可以直接用缓存，构建更快
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY scripts/ scripts/
