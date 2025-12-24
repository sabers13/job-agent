from __future__ import annotations

import asyncio
import json
import os
import argparse
from dataclasses import dataclass, asdict
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

from dotenv import load_dotenv
from prefect import flow, get_run_logger, task

from app.config.settings import settings
from app.config.focus import DEFAULT_FOCUS, get_focus_config
from .pipeline.pipeline import fetch_job_details, write_job_bundle
from .pipeline.state import load_state, save_state
from .stepstone.search_http import search_stepstone
from .stepstone.search_playwright import search_stepstone_pw
from .pipeline.output import write_summary
from .common.utils import ensure_dir
from .stepstone.dates import parse_iso8601_utc

load_dotenv()
RUNS_BASE_DIR = Path("output") / "runs"


@dataclass
class SeedConfig:
    slug: str
    seed_url: str
    use_playwright: bool = False
    include_titles_any: Optional[List[str]] = None
    exclude_titles_any: Optional[List[str]] = None
    delay_sec: float = 1.2
    max_jobs: Optional[int] = None
    max_pages: Optional[int] = 80


def _iso_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _resolve_seed_configs(
    seeds: Optional[Sequence[SeedConfig]] = None,
) -> List[SeedConfig]:
    if seeds:
        return list(seeds)

    # Prefer explicit JSON override (e.g. injected per profile by GUI/FastAPI)
    env_json = getattr(settings, "seeds_json", None)
    if env_json:
        data = json.loads(env_json)
        return [SeedConfig(**item) for item in data]

    env_path = getattr(settings, "seeds_json_path", None)
    if env_path:
        path = Path(env_path).expanduser()
        if path.exists():
            data = json.loads(path.read_text(encoding="utf-8"))
            return [SeedConfig(**item) for item in data]

    env_path = str(settings.seeds_file) if getattr(settings, "seeds_file", None) else None
    if env_path:
        path = Path(env_path).expanduser()
        if not path.exists():
            raise FileNotFoundError(f"Seeds file not found: {path}")
        data = json.loads(path.read_text(encoding="utf-8"))
        return [SeedConfig(**item) for item in data]

    default_path = settings.seeds_file
    if default_path.exists():
        data = json.loads(default_path.read_text(encoding="utf-8"))
        return [SeedConfig(**item) for item in data]

    raise ValueError("No StepStone seed configuration supplied.")


@task(name="Load state")
def _load_state_task() -> Dict[str, Any]:
    return load_state()


@task(name="Persist state")
def _save_state_task(state: Dict[str, Any]) -> Dict[str, Any]:
    return save_state(state)


@task(name="Search seed")
def _search_seed_task(
    seed: SeedConfig,
) -> Dict[str, Any]:
    if seed.use_playwright:
        return asyncio.run(
            search_stepstone_pw(
                seed.seed_url,
                pages=None,
                delay_sec=seed.delay_sec,
                include_titles_any=seed.include_titles_any,
                exclude_titles_any=seed.exclude_titles_any,
                max_jobs=seed.max_jobs,
                max_pages_guard=seed.max_pages or 80,
                stop_urls=None,
            )
        )
    return search_stepstone(
        seed.seed_url,
        pages=None,
        delay_sec=seed.delay_sec,
        include_titles_any=seed.include_titles_any,
        exclude_titles_any=seed.exclude_titles_any,
        max_jobs=seed.max_jobs,
        max_pages_guard=seed.max_pages or 80,
        stop_urls=None,
    )


@task(name="Persist seed urls")
def _write_seed_urls(
    run_dir: Path,
    seed: SeedConfig,
    crawl_result: Dict[str, Any],
    list_cutoff_iso: Optional[str],
) -> Path:
    ensure_dir(run_dir)
    cutoff_dt = parse_iso8601_utc(list_cutoff_iso)
    jobs = crawl_result.get("jobs") or []
    filtered_jobs: List[Dict[str, Any]] = []
    if jobs:
        for job in jobs:
            if not cutoff_dt:
                filtered_jobs.append(job)
                continue
            posted_iso = job.get("posted_iso")
            posted_dt = parse_iso8601_utc(posted_iso)
            if posted_dt and posted_dt < cutoff_dt:
                continue
            filtered_jobs.append(job)

    if filtered_jobs:
        urls = [job["url"] for job in filtered_jobs if job.get("url")]
    else:
        # fallback to legacy behaviour when we don't have job metadata
        urls = crawl_result.get("urls", [])
        filtered_jobs = []

    crawl_result["jobs"] = filtered_jobs
    crawl_result["urls"] = urls
    payload = {
        "seed": asdict(seed),
        "metadata": {
            "count": len(urls),
            "pages_scanned": crawl_result.get("pages_scanned"),
            "estimated_total_pages": crawl_result.get("estimated_total_pages"),
        },
        "urls": urls,
        "jobs": filtered_jobs,
        "list_cutoff_iso": list_cutoff_iso,
    }
    path = run_dir / f"urls-{seed.slug}.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    crawl_result["filtered_urls"] = urls
    return path


@task(name="Process job")
def _process_job_task(
    url: str,
    seed_slug: str,
    cutoff_iso: Optional[str],
    *,
    backend: str = "auto",
    profile_key: Optional[str],
    use_llm_scoring: bool,
    apply_blocker_cap: bool,
) -> Dict[str, Any]:
    logger = get_run_logger()

    # Resolve focus/profile consistently with FastAPI
    try:
        focus = get_focus_config(profile_key) if profile_key else DEFAULT_FOCUS
    except Exception as exc:
        logger.warning("Unknown profile_key=%s; falling back to DEFAULT_FOCUS (%s)", profile_key, exc)
        focus = DEFAULT_FOCUS

    try:
        details = asyncio.run(
            fetch_job_details(
                url,
                backend=backend,  # IMPORTANT: no more None drift
                enrich=True,
                score=True,
                cutoff_iso=cutoff_iso,
                focus=focus,
                use_llm_scoring=use_llm_scoring,
                apply_blocker_cap=apply_blocker_cap,
            )
        )
    except TypeError:
        # If the pipeline signature doesn't yet include these kwargs, prefer
        # running without them over crashing the batch run.
        details = asyncio.run(
            fetch_job_details(
                url,
                backend=backend,
                enrich=True,
                score=True,
                cutoff_iso=cutoff_iso,
                focus=focus,
            )
        )
    except Exception as exc:
        return {
            "url": url,
            "seed_slug": seed_slug,
            "status": "error",
            "error": str(exc),
            "profile_key": profile_key,
            "backend": backend,
        }

    if details.get("stale"):
        return {
            "url": url,
            "seed_slug": seed_slug,
            "status": "stale",
            "details": details,
            "profile_key": profile_key,
            "backend": backend,
        }

    job = details.get("job") or {}
    scoring = details.get("scoring")
    if scoring:
        logger.debug(
            f"Score components for {url} ({seed_slug}): {scoring.get('components')}"
        )

    score_val = scoring.get("score") if isinstance(scoring, dict) else None
    if score_val is not None and score_val < settings.score_keep_threshold:
        return {
            "url": url,
            "seed_slug": seed_slug,
            "status": "rejected_low_score",
            "details": details,
            "threshold": settings.score_keep_threshold,
            "profile_key": profile_key,
            "backend": backend,
        }

    try:
        bundle_dir = write_job_bundle(job, scoring, seed_slug=seed_slug)
    except Exception as exc:
        return {
            "url": url,
            "seed_slug": seed_slug,
            "status": "bundle_failed",
            "details": details,
            "error": str(exc),
            "profile_key": profile_key,
            "backend": backend,
        }

    return {
        "url": url,
        "seed_slug": seed_slug,
        "status": "processed",
        "details": details,
        "bundle_dir": bundle_dir,
        "profile_key": profile_key,
        "backend": backend,
    }


@flow(name="Crawl StepStone Seeds")
def crawl_and_save_flow(
    seeds: Optional[Sequence[SeedConfig]] = None,
    *,
    list_cutoff_iso: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Crawl StepStone listing pages for the configured seeds and persist one JSON
    file per seed under output/runs/<timestamp>.
    """
    logger = get_run_logger()
    seed_configs = _resolve_seed_configs(seeds)
    state = _load_state_task()
    timestamp = _iso_timestamp()
    run_dir = RUNS_BASE_DIR / timestamp
    ensure_dir(run_dir)

    logger.info(f"Starting crawl run: {timestamp}")
    for seed in seed_configs:
        logger.info(f"Crawling seed {seed.slug} ...")
        result = _search_seed_task(seed)
        _write_seed_urls(run_dir, seed, result, list_cutoff_iso)
        filtered_urls = result.get("filtered_urls") or result.get("urls", [])
        logger.info(
            f"Seed {seed.slug} collected {len(filtered_urls)} urls."
        )

    new_state = {
        **state,
        "last_run": timestamp,
        "run_dir": f"runs/{timestamp}",
    }
    _save_state_task(new_state)
    logger.info("Crawl run complete. State updated.")
    return {"run_dir": str(run_dir), "timestamp": timestamp}


@flow(name="Process StepStone Run")
def process_run_flow(
    cutoff_iso: Optional[str] = None,
    *,
    profile_key: Optional[str] = None,
    backend: str = "auto",
    use_llm_scoring: Optional[bool] = None,
    apply_blocker_cap: Optional[bool] = None,
) -> Dict[str, Any]:
    """
    Load the latest crawl run, deduplicate job URLs, then fetch job details and
    generate bundles.
    """
    logger = get_run_logger()
    backend = backend or "auto"

    # Normalize flags (None => settings default)
    if use_llm_scoring is None:
        use_llm_scoring = bool(getattr(settings, "use_llm_scoring", False))
    if apply_blocker_cap is None:
        apply_blocker_cap = bool(getattr(settings, "apply_blocker_cap", False))

    logger.info(
        "process_run_flow: profile_key=%s backend=%s use_llm_scoring=%s apply_blocker_cap=%s cutoff_iso=%s",
        profile_key,
        backend,
        use_llm_scoring,
        apply_blocker_cap,
        cutoff_iso,
    )

    state = _load_state_task()
    run_dir_value = state.get("run_dir")
    if not run_dir_value:
        raise RuntimeError("run_state has no run_dir. Run crawl_and_save_flow first.")

    run_path = Path(run_dir_value)
    if not run_path.is_absolute():
        run_path = RUNS_BASE_DIR.parent / run_dir_value
    if not run_path.exists():
        raise FileNotFoundError(f"Run directory not found: {run_path}")

    logger.info(f"Processing run directory {run_path}")
    files = sorted(run_path.glob("urls-*.json"))
    if not files:
        logger.warning("No urls-*.json files found in {}", run_path)
        return {"processed": [], "skipped": []}

    queue: List[Dict[str, str]] = []
    seen_global: set[str] = set()
    for path in files:
        data = json.loads(path.read_text(encoding="utf-8"))
        seed_info = data.get("seed", {})
        slug = seed_info.get("slug") or path.stem.replace("urls-", "")
        job_entries = data.get("jobs")
        if job_entries:
            for job in job_entries:
                url = job.get("url")
                if not url or url in seen_global:
                    continue
                seen_global.add(url)
                queue.append(
                    {
                        "url": url,
                        "seed_slug": slug,
                        "posted_iso": job.get("posted_iso"),
                    }
                )
        else:
            for url in data.get("urls", []):
                if url in seen_global:
                    continue
                seen_global.add(url)
                queue.append({"url": url, "seed_slug": slug})

    logger.info(f"Collected {len(queue)} unique URLs to process")
    processed: List[Dict[str, Any]] = []
    for item in queue:
        result = _process_job_task(
            item["url"],
            item["seed_slug"],
            cutoff_iso,
            profile_key=profile_key,
            backend=backend,
            use_llm_scoring=use_llm_scoring,
            apply_blocker_cap=apply_blocker_cap,
        )
        status = result.get("status")
        if status == "processed":
            logger.info(
                f"Processed {item['url']} ({item['seed_slug']}) -> {result.get('bundle_dir')}"
            )
        elif status == "stale":
            logger.info(f"Skipping stale job {item['url']} ({item['seed_slug']})")
        else:
            detail = result.get("error")
            if not detail and status == "rejected_low_score":
                score_val = ((result.get("details") or {}).get("scoring") or {}).get("score")
                detail = f"score={score_val} threshold={result.get('threshold')}"
            logger.warning(
                "Job %s (%s) finished with status %s: %s",
                item["url"],
                item["seed_slug"],
                status,
                detail,
            )
        processed.append(result)

    reports: List[Dict[str, Any]] = []
    for result in processed:
        if result.get("status") != "processed":
            continue
        details = result.get("details") or {}
        reports.append(
            {
                "job": details.get("job"),
                "scoring": details.get("scoring"),
                "output_dir": result.get("bundle_dir"),
            }
        )

    summary_path = None
    if reports:
        summary_path = write_summary(reports, str(run_path))

    analysis_entries: List[Dict[str, Any]] = []
    for res in processed:
        details = res.get("details") or {}
        job = details.get("job") or {}
        scoring = details.get("scoring") or {}
        components = scoring.get("components") or {}
        pros = [
            f"{key}: +{value}"
            for key, value in components.items()
            if isinstance(value, (int, float)) and value > 0
        ]
        cons = [
            f"{key}: {value}"
            for key, value in components.items()
            if isinstance(value, (int, float)) and value < 0
        ]
        analysis_entries.append(
            {
                "seed_slug": res.get("seed_slug"),
                "status": res.get("status"),
                "title": job.get("title"),
                "company": job.get("company"),
                "url": res.get("url") or job.get("url"),
                "score": scoring.get("score"),
                "pros": pros,
                "cons": cons,
            }
        )

    analysis_path = run_path / "analysis_summary.json"
    try:
        analysis_path.write_text(json.dumps(analysis_entries, ensure_ascii=False, indent=2), encoding="utf-8")
        logger.info(f"Wrote analysis summary to {analysis_path}")
    except Exception as exc:
        logger.warning("Failed to write analysis summary: %s", exc)

    flow_output = {
        "results": processed,
        "summary_path": summary_path,
        "analysis_summary_path": str(analysis_path),
    }

    try:
        stamp = datetime.now(timezone.utc).isoformat().replace(":", "-")
        output_path = run_path / f"process_result_{stamp}.json"
        output_path.write_text(json.dumps(flow_output, ensure_ascii=False, indent=2), encoding="utf-8")
        logger.info(f"Saved flow result snapshot to {output_path}")
    except Exception as exc:
        logger.warning("Failed to persist flow result snapshot: %s", exc)

    return flow_output


def _load_seeds_from_path(path: Path) -> List[SeedConfig]:
    data = json.loads(path.read_text(encoding="utf-8"))
    return [SeedConfig(**item) for item in data]


def _parse_cli_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prefect-based StepStone job agent flows.")
    sub = parser.add_subparsers(dest="command", required=True)

    crawl_parser = sub.add_parser("crawl", help="Run the crawl_and_save_flow.")
    crawl_parser.add_argument("--seeds-file", type=Path, help="Path to a JSON file with seed definitions.")
    crawl_parser.add_argument(
        "--list-cutoff-iso",
        type=str,
        default=None,
        help="Persist only listings with posted_iso on/after this ISO timestamp.",
    )
    crawl_parser.add_argument(
        "--list-max-age-days",
        type=float,
        default=None,
        help="Alternative to --list-cutoff-iso: persist listings newer than this many days.",
    )

    process_parser = sub.add_parser("process", help="Run the process_run_flow.")
    process_parser.add_argument("--cutoff-iso", type=str, default=None, help="ISO8601 cutoff date for stale detection.")
    process_parser.add_argument(
        "--profile-key",
        type=str,
        default=os.getenv("JOBAGENT_PROFILE_KEY", "junior_data_bi"),
        help="Focus profile key (e.g., junior_data_bi).",
    )
    process_parser.add_argument(
        "--backend",
        type=str,
        default="auto",
        choices=["auto", "http", "pw"],
        help="Fetch backend to use.",
    )

    llm_group = process_parser.add_mutually_exclusive_group()
    llm_group.add_argument("--use-llm-scoring", dest="use_llm_scoring", action="store_true")
    llm_group.add_argument("--no-use-llm-scoring", dest="use_llm_scoring", action="store_false")
    process_parser.set_defaults(use_llm_scoring=None)

    cap_group = process_parser.add_mutually_exclusive_group()
    cap_group.add_argument("--apply-blocker-cap", dest="apply_blocker_cap", action="store_true")
    cap_group.add_argument("--no-apply-blocker-cap", dest="apply_blocker_cap", action="store_false")
    process_parser.set_defaults(apply_blocker_cap=None)

    return parser.parse_args()


def _cli_entry() -> None:
    args = _parse_cli_args()
    seeds: Optional[List[SeedConfig]] = None
    if getattr(args, "seeds_file", None):
        seeds = _load_seeds_from_path(args.seeds_file)

    if args.command == "crawl":
        list_cutoff_iso = getattr(args, "list_cutoff_iso", None)
        max_age = getattr(args, "list_max_age_days", None)
        if max_age is not None:
            try:
                max_age_float = float(max_age)
                cutoff_dt = datetime.now(timezone.utc) - timedelta(days=max_age_float)
                list_cutoff_iso = cutoff_dt.isoformat(timespec="seconds").replace("+00:00", "Z")
            except Exception:
                pass
        crawl_and_save_flow(seeds=seeds, list_cutoff_iso=list_cutoff_iso)
    elif args.command == "process":
        process_run_flow(
            cutoff_iso=args.cutoff_iso,
            profile_key=args.profile_key,
            backend=args.backend,
            use_llm_scoring=args.use_llm_scoring,
            apply_blocker_cap=args.apply_blocker_cap,
        )
    else:
        raise ValueError(f"Unsupported command {args.command}")


if __name__ == "__main__":
    _cli_entry()
