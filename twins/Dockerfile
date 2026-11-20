# 孪生市场判断（pmdata.twins.run）的镜像：项目代码 + Claude Code（用 `claude -p` 调用 AI，扣订阅额度）。
# 单独一个镜像，共用的 polymarket-app 不用多装 Claude Code。构建（在仓库根目录，SSH 下用旧版构建器）：
#   DOCKER_BUILDKIT=0 docker build -f twins/Dockerfile -t polymarket-twins .
# 登录用 `claude setup-token` 生成的长期令牌，放在 .env 的 CLAUDE_CODE_OAUTH_TOKEN（compose 传进来）。
FROM python:3.12-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1 \
    PYTHONIOENCODING=utf-8 \
    PYTHONPATH=/app/src \
    PATH=/root/.local/bin:$PATH

# Claude Code 官方安装脚本，固定版本（和台式机上测试用的一样）
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && curl -fsSL https://claude.ai/install.sh | bash -s 2.1.312 \
    && claude --version

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ src/
