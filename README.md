# Job Agent (Local-First Job Search + Scoring Pipeline)

Job Agent is a local-first automation pipeline that crawls job listings (e.g., StepStone), enriches and scores them, and stores structured artifacts per user/profile/run. It combines a FastAPI backend, a SQL database, and Prefect for orchestration.

This project can be run:
- **Locally** with a local database (e.g., SQL Server in Podman/Docker, or another local DB backend if you adapt the connection string).
- **In Azure** (e.g., Azure SQL Database) by switching the database connection and running the same API + Prefect services.
- **In the browser** for interaction (FastAPI serves a web UI or API docs; you can drive the app via browser + curl).

---

## Tech Stack

- **API:** FastAPI
- **Orchestration:** Prefect
- **DB:** SQL Server (ODBC / `pyodbc`) — supports Azure SQL as well
- **Crawler/Fetcher:** Playwright + “polite fetch” (robots/headers/backoff)
- **Migrations:** Alembic (if enabled in your repo)
- **Auth:** HttpOnly cookie session (CLI-tested via `curl` cookie jar)

---

## How You Run It (Two-Terminal Requirement)

You will typically keep **two terminals running**:

1) **Terminal A — Prefect server** (durable orchestration + dashboard)
2) **Terminal B — FastAPI (uvicorn)** (browser + API interaction)

You can then use:
- **Browser**: for interactive access / dashboards / Swagger UI
- **CLI**: for runs (crawl/process) and status/log endpoints

---

## Prerequisites

### System
- Python **3.11+** (3.12 recommended)
- Container runtime (**Podman** or **Docker**) if using a local SQL Server container
- **ODBC Driver 18 for SQL Server** installed on your host (needed for `mssql+pyodbc`)
- Optional: `jq` for CLI formatting

### Python environment
```bash
python -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -r requirements.txt
```

---

## Configuration

Create a local env file by copying an example:

```bash
cp .env.example .env
cp .env.dev.example .env.dev
```

Fill in the placeholders in the copied files:
- Replace values like `<DB_HOST>`, `<DB_USER>`, `<DB_PASSWORD>`, `<LOCAL_DB_PASSWORD>`.
- Keep example URLs/flags as-is unless your setup differs.
- Never commit `.env` or `.env.dev`.

Create `.env.dev` (local dev):

### Option A — Local DB (SQL Server container)

```ini
JOBAGENT_ENV=dev
JOBAGENT_OUTPUT_DIR=output

# Local SQL Server container on port 14330
JOBAGENT_DATABASE_URL=mssql+pyodbc:///?odbc_connect=DRIVER%3D%7BODBC+Driver+18+for+SQL+Server%7D%3BSERVER%3D127.0.0.1%2C14330%3BDATABASE%3Djobagent_dev%3BUID%3Dsa%3BPWD%3D<LOCAL_DB_PASSWORD>%3BEncrypt%3Dno%3BTrustServerCertificate%3Dyes%3BLoginTimeout%3D30%3BConnection+Timeout%3D30%3B

JOBAGENT_DATABASE_MIGRATOR_URL=mssql+pyodbc:///?odbc_connect=DRIVER%3D%7BODBC+Driver+18+for+SQL+Server%7D%3BSERVER%3D127.0.0.1%2C14330%3BDATABASE%3Djobagent_dev%3BUID%3Dsa%3BPWD%3D<LOCAL_DB_PASSWORD>%3BEncrypt%3Dno%3BTrustServerCertificate%3Dyes%3BLoginTimeout%3D30%3BConnection+Timeout%3D30%3B

# Prefect server
PREFECT_API_URL=http://127.0.0.1:8373/api
```

### Option B — Azure SQL Database

Set your DB URL to your Azure SQL instance. The exact connection string varies by driver and your auth method, but conceptually:

```ini
JOBAGENT_ENV=prod
JOBAGENT_OUTPUT_DIR=output

# Azure SQL (example pattern; update placeholders)
JOBAGENT_DATABASE_URL=mssql+pyodbc:///?odbc_connect=DRIVER%3D%7BODBC+Driver+18+for+SQL+Server%7D%3BSERVER%3Dtcp%3A<AZURE_SQL_SERVER>.database.windows.net%2C1433%3BDATABASE%3D<DB_NAME>%3BUID%3D<USER>%3BPWD%3D<PASSWORD>%3BEncrypt%3Dyes%3BTrustServerCertificate%3Dno%3BConnection+Timeout%3D30%3B

JOBAGENT_DATABASE_MIGRATOR_URL=<same-as-above-or-admin-credential>

# Prefect server URL (choose local or hosted)
PREFECT_API_URL=http://127.0.0.1:8373/api
```

Notes for Azure:

* Ensure **Firewall rules** allow your client (or deployment host) to reach Azure SQL.
* Prefer **Encrypt=yes** and **TrustServerCertificate=no** for production.
* Use Azure Key Vault / secrets manager rather than committing credentials.

Load env vars:

```bash
set -a; source .env.dev; set +a
```

---

## Local DB: Running SQL Server in a Container

If your SQL Server is containerized:

### Podman

```bash
podman ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | grep jobagent-mssql-dev \
  || podman start jobagent-mssql-dev
```

Verify:

```bash
podman ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | grep jobagent-mssql-dev
# jobagent-mssql-dev  Up ...  0.0.0.0:14330->1433/tcp
```

---

## Terminal A: Start Prefect Server (Required)

Run Prefect server in its own terminal:

```bash
source .venv/bin/activate
set -a; source .env.dev; set +a

prefect server start --host 127.0.0.1 --port 8373
```

Health:

```bash
curl -sS http://127.0.0.1:8373/api/health
# true
```

Prefect dashboard (browser):

* [http://127.0.0.1:8373](http://127.0.0.1:8373)

---

## Terminal B: Start FastAPI (Uvicorn) (Required)

Run the API in a second terminal:

```bash
source .venv/bin/activate
set -a; source .env.dev; set +a

uvicorn app.fastapi_run:app --host 127.0.0.1 --port 5001 --reload
```

FastAPI in browser:

* Base: [http://127.0.0.1:5001](http://127.0.0.1:5001)
* Swagger UI: [http://127.0.0.1:5001/docs](http://127.0.0.1:5001/docs)
* (If enabled) ReDoc: [http://127.0.0.1:5001/redoc](http://127.0.0.1:5001/redoc)

---

## Authentication (CLI)

Cookie-based auth (HttpOnly). Use a cookie jar file.

### Signup

```bash
curl -sS -c jar.txt -X POST http://127.0.0.1:5001/auth/signup \
  -H "Content-Type: application/json" \
  -d '{"email":"you@example.com","password":"YourStrongPassword!"}' | jq
```

### Login

```bash
curl -sS -c jar.txt -X POST http://127.0.0.1:5001/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email":"you@example.com","password":"YourStrongPassword!"}' >/dev/null
```

Verify:

```bash
curl -sS -b jar.txt http://127.0.0.1:5001/api/my/profiles | jq
```

---

## Running a Pipeline (Crawl → Process)

### 1) Crawl

```bash
RUN_ID="$(date -u +%Y%m%dT%H%M%S)-$(python - <<'PY'
import uuid; print(str(uuid.uuid4())[:8])
PY
)"
echo "RUN_ID=$RUN_ID"

python -m app.prefect_run crawl --list-max-age-days=20 --run-id="$RUN_ID"
```

### 2) Process + Score

```bash
python -m app.prefect_run process \
  --cutoff-iso=2025-12-28T06:55:37.053218Z \
  --backend=auto \
  --profile-key=dev_profile \
  --use-llm-scoring \
  --apply-blocker-cap \
  --run-id="$RUN_ID"
```

Outputs go under something like:

```
output/<user_id>/<profile_key>/<run_id>/
```

---

## Uploading a Resume (Optional)

```bash
curl -sS -b jar.txt -X POST http://127.0.0.1:5001/api/my/resume \
  -F "file=@/absolute/path/to/CV.pdf" | jq
```

If you get `curl: (26) Failed to open/read local data...`, the file path is wrong.

---

## Azure Deployment Notes (High Level)

There are multiple valid ways to run this in Azure. The simplest is:

* **Azure SQL** for database
* **One VM** (or container host) running:

  * Prefect server
  * FastAPI (uvicorn)
  * Workers that execute Playwright / pipeline tasks

Key points:

* Ensure outbound access for crawling (StepStone etc.)
* Ensure Azure SQL firewall and credentials are configured
* Store secrets securely (Key Vault / env injection)
* If running Playwright headless in Linux containers/VMs, ensure required system deps are installed

A minimal operational model in Azure:

1. Set `JOBAGENT_DATABASE_URL` to Azure SQL
2. Run Prefect server and FastAPI as system services (or containers)
3. Monitor via Prefect UI and FastAPI `/docs`

---

## Common Issues

### 401 `Missing token`

You forgot `-b jar.txt` or you’re not logged in.

### Prefect `Connection refused`

Prefect server is not running or `PREFECT_API_URL` is wrong.

### DB container “Up” but app DB health fails

Connectivity may be fine but your `/health/db` endpoint can still fail if the check function is broken. Validate DB independently with a simple SQLAlchemy query.


