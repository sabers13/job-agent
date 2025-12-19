# Job Fetching Agent

FastAPI service and Prefect workflows to crawl StepStone listings, fetch job details, score junior fit, and emit report bundles.

## Folder map
- `app/`
  - `fastapi_run.py` — FastAPI entrypoint (`uvicorn app.fastapi_run:app`).
  - `prefect_run.py` — Prefect CLI/flows (`python -m app.prefect_run crawl|process`).
  - `__init__.py` — package marker.
  - `common/`
    - `utils.py`, `__init__.py`
  - `fetching/`
    - `http_client.py`, `polite_fetch.py`, `__init__.py`
- `pipeline/`
  - `pipeline.py`, `templating.py`, `output.py`, `state.py`, `llm_enrich.py`, `scoring.py`, `parsers.py`, `models.py`, `__init__.py`
  - `stepstone/`
    - `search_http.py`, `search_playwright.py`, `smoke.py`, `dates.py`, `__init__.py`
- `config/`
  - `stepstone_seeds.json.example`, `stepstone_seeds.json` (optional)
  - `settings.py`, `focus.py` — centralized runtime/settings (imported via `app.config.*`)
- `templates/`
  - `report_md.j2`
- `tests/`
  - `test_score.py`, `test_scoring.py`, `test_smoke.py`
- `scripts/`
  - `filter_analysis_summary.py`
- `output/` — generated bundles, run artifacts, cache/state (created at runtime)
- `requirements.txt` — Python deps
- `.venv/` — optional local virtual environment (if created)
- Misc: `cv_complete.html`, `n8n workflows/job_agent_l7.json`

## Setup
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
# optional: playwright install --with-deps chromium
```

## Run the API (FastAPI)
```bash
uvicorn app.fastapi_run:app --reload --port 5001
```
Key endpoints:
- `POST /search_stepstone_list` — crawl listings (HTTP default, Playwright if `use_playwright=true`).
- `POST /job_details` — fetch/parse/enrich/score a job.
- `POST /bundle` — write a report bundle for a provided job payload.
- `POST /aggregate_report` — combine multiple reports.
- `GET /run_state` / `POST /run_state` — persist last run metadata.
- Legacy smoke: `GET /search_stepstone`.

## Run batch flows (Prefect)
```bash
# Crawl seeds (uses config/stepstone_seeds.json or env STEPSTONE_SEEDS_FILE/JSON)
python -m app.prefect_run crawl --list-max-age-days 4
# Process the latest run (dedupe, fetch details, score, write bundles)
python -m app.prefect_run process --cutoff-iso 2024-10-01T00:00:00Z
```

## Outputs
- Bundles under `output/` contain `REPORT.md` and `metadata.json` (optional score buckets in subfolders).
- Run summaries and cached state live under `output/runs/` and `output/_state/`.

## Notes
- Network/JS-heavy pages may require Playwright; toggle per request (`use_playwright`) or per seed.
- Env flags: `USE_PLAYWRIGHT`, `HEADLESS`, `REQUEST_DELAY_MS`, and Playwright/HTTP tuning in `fetching/polite_fetch.py`.
