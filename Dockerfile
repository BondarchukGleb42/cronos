# syntax=docker/dockerfile:1
FROM python:3.14-slim-bookworm AS builder

COPY --from=ghcr.io/astral-sh/uv:0.8.22 /uv /uvx /usr/local/bin/

ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=never
WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --no-install-project

COPY src ./src
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --no-editable \
    && .venv/bin/python -c "from pathlib import Path; import cronos; assert Path(cronos.__file__).with_name('schema.sql').is_file()"

FROM python:3.14-slim-bookworm AS runtime

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates fonts-dejavu-core libstdc++6 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 cronos \
    && useradd --uid 10001 --gid 10001 --no-create-home --home-dir /tmp --shell /usr/sbin/nologin cronos \
    && mkdir -p /data/artifacts \
    && chown 10001:10001 /data/artifacts

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    XDG_CACHE_HOME=/tmp/.cache \
    ARTIFACTS_DIR=/data/artifacts
WORKDIR /app
COPY --from=builder /app/.venv /app/.venv

USER 10001:10001
EXPOSE 8000
CMD ["python", "-m", "cronos.gateway"]
