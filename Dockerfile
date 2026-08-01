# syntax=docker/dockerfile:1
#
# Job Agent -- single-process image.
#
# Serves the "clone and run it" goal: one container, no separately started
# Prefect server, no SQL Server. Defaults to SQLite and the HTTP fetch backend.
#
#   docker build -t job-agent .
#   docker run --rm -p 5001:5001 \
#     -e JOBAGENT_JWT_SECRET="$(openssl rand -hex 32)" \
#     -v job-agent-data:/data \
#     job-agent
#
# Playwright browsers are NOT installed by default -- they add roughly 1 GB and
# the default fetch backend is HTTP. For the Playwright-capable variant:
#   docker build --build-arg WITH_PLAYWRIGHT=1 -t job-agent:playwright .

FROM python:3.12-slim AS builder

ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /build
COPY requirements.lock.txt ./

# --only-binary=:all: is load-bearing, not an optimisation. All 152 locked
# distributions publish manylinux or pure-Python wheels (verified against the
# dev venv: no dist-info carries a locally-built linux_x86_64 tag), so the image
# needs no compiler and no -dev headers.
#
# The flag turns that into an enforced invariant: if a future dependency bump
# pulls in something that would need building, the image fails loudly here
# instead of silently growing a ~400 MB build-essential layer.
RUN python -m venv /opt/venv \
    && /opt/venv/bin/pip install --upgrade pip \
    && /opt/venv/bin/pip install --only-binary=:all: -r requirements.lock.txt


FROM python:3.12-slim AS runtime

ARG WITH_PLAYWRIGHT=0

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/opt/venv/bin:$PATH" \
    # Local-first defaults. Every one is overridable at run time.
    JOBAGENT_ENV=prod \
    JOBAGENT_DATABASE_URL="sqlite:////data/job-agent.db" \
    JOBAGENT_OUTPUT_DIR=/data/output \
    JOBAGENT_USE_PLAYWRIGHT=false \
    JOBAGENT_USE_LLM_ENRICH=false \
    JOBAGENT_USE_LLM_SCORING=false

# unixodbc is the runtime shared library the pyodbc wheel links against. It is
# needed even on the SQLite path, because `import pyodbc` happens at import time
# via app.db. curl backs the healthcheck. No -dev packages, no compiler.
RUN apt-get update && apt-get install -y --no-install-recommends \
        unixodbc \
        curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /opt/venv /opt/venv

RUN if [ "$WITH_PLAYWRIGHT" = "1" ]; then \
        playwright install --with-deps chromium; \
    fi

WORKDIR /app

# Source only. .dockerignore keeps .env, output/ and docs out of the layer.
COPY app ./app
COPY alembic ./alembic
COPY alembic.ini ./
COPY config ./config
COPY templates ./templates
COPY README.md ./

# Non-root. /data is the only writable path the app needs.
RUN useradd --create-home --uid 10001 jobagent \
    && mkdir -p /data/output \
    && chown -R jobagent:jobagent /data /app
USER jobagent

VOLUME ["/data"]
EXPOSE 5001

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD curl -fsS http://127.0.0.1:5001/health || exit 1

CMD ["uvicorn", "app.fastapi_run:app", "--host", "0.0.0.0", "--port", "5001"]
