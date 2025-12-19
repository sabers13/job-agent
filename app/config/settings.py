from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    pass


def _env(*names: str, default: str | None = None) -> str | None:
    for name in names:
        value = os.getenv(name)
        if value is not None and value != "":
            return value
    return default


def _env_bool(*names: str, default: bool) -> bool:
    value = _env(*names, default=None)
    if value is None:
        return default
    return value.strip().lower() in ("1", "true", "yes", "y", "on")


def _env_int(*names: str, default: int) -> int:
    value = _env(*names, default=None)
    return int(value) if value is not None else default


def _env_float(*names: str, default: float) -> float:
    value = _env(*names, default=None)
    return float(value) if value is not None else default


def _env_csv(*names: str, default: str) -> tuple[str, ...]:
    value = _env(*names, default=default) or default
    return tuple(part.strip() for part in value.split(",") if part.strip())


@dataclass(frozen=True)
class Settings:
    """
    Central runtime settings. Values can be overridden via env vars.
    """

    # LLM / scoring
    openai_model: str = os.getenv(
        "JOBAGENT_OPENAI_MODEL_ENRICH",
        os.getenv("JOBAGENT_OPENAI_MODEL", os.getenv("OPENAI_MODEL", "gpt-5-nano")),
    )
    openai_model_scoring: str = os.getenv(
        "JOBAGENT_OPENAI_MODEL_SCORING",
        os.getenv("OPENAI_MODEL_SCORING", "gpt-5-nano"),
    )
    score_keep_threshold: int = int(os.getenv("JOBAGENT_SCORE_KEEP_THRESHOLD", "70"))
    use_llm_scoring: bool = os.getenv("JOBAGENT_USE_LLM_SCORING", "false").lower() == "true"
    apply_blocker_cap: bool = os.getenv("JOBAGENT_APPLY_BLOCKER_CAP", "true").lower() == "true"
    llm_language_override: bool = os.getenv("JOBAGENT_LLM_LANGUAGE_OVERRIDE", "1").lower() in ("1", "true")
    llm_language_override_conf: float = float(os.getenv("JOBAGENT_LLM_LANGUAGE_OVERRIDE_CONF", "0.9"))

    # Fetching / crawling (canonical: JOBAGENT_*, with fallbacks)
    use_playwright_default: bool = _env_bool("JOBAGENT_USE" "_PLAYWRIGHT", "USE" "_PLAYWRIGHT", default=True)
    headless: bool = _env_bool("JOBAGENT_HEAD" "LESS", "HEAD" "LESS", default=True)

    # Polite fetch timing (seconds)
    fetch_delay_min_sec: float = _env_float("JOBAGENT_FETCH_DELAY_MIN_SEC", "JOB" "_FETCH_DELAY_MIN_SEC", default=6.0)
    fetch_delay_max_sec: float = _env_float("JOBAGENT_FETCH_DELAY_MAX_SEC", "JOB" "_FETCH_DELAY_MAX_SEC", default=12.0)
    fetch_failure_backoff_sec: float = _env_float(
        "JOBAGENT_FETCH_FAILURE_BACKOFF_SEC",
        "JOB" "_FETCH_FAILURE_BACKOFF_SEC",
        default=5.0,
    )

    # HTTP fetch behavior
    fetch_http_timeout_sec: float = _env_float("JOBAGENT_FETCH_HTTP_TIMEOUT", "JOB" "_FETCH_HTTP_TIMEOUT", default=35.0)
    fetch_http_retries: int = _env_int("JOBAGENT_FETCH_HTTP_RETRIES", "JOB" "_FETCH_HTTP_RETRIES", default=2)
    fetch_http_backoff_base: float = _env_float(
        "JOBAGENT_FETCH_HTTP_BACKOFF_BASE",
        "JOB" "_FETCH_HTTP_BACKOFF_BASE",
        default=3.0,
    )

    # Playwright tuning
    playwright_wait_until: str = _env(
        "JOBAGENT_FETCH_PW_WAIT_UNTIL",
        "JOB" "_FETCH_PW_WAIT_UNTIL",
        default="domcontentloaded",
    ) or "domcontentloaded"
    playwright_timeout_ms: int = _env_int(
        "JOBAGENT_FETCH_PW_TIMEOUT_MS",
        "JOB" "_FETCH_PW_TIMEOUT_MS",
        default=45000,
    )

    # Robots / access denied heuristics
    fetch_robots_ttl_sec: float = _env_float("JOBAGENT_FETCH_ROBOTS_TTL_SEC", "JOB" "_FETCH_ROBOTS_TTL_SEC", default=86400.0)
    fetch_access_denied_markers: tuple[str, ...] = tuple(
        marker.lower()
        for marker in _env_csv(
            "JOBAGENT_FETCH_ACCESS_DENIED_MARKERS",
            "JOB" "_FETCH_ACCESS_DENIED_MARKERS",
            default=(
                "access denied,request blocked,captcha required,forbidden,"
                "request unsuccessful,incapsula,imperva,verify you are human,"
                "temporarily blocked,security check"
            ),
        )
    )

    # Request headers
    fetch_user_agent: str = _env(
        "JOBAGENT_FETCH_USER_AGENT",
        "JOB" "_FETCH_USER_AGENT",
        default=(
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        ),
    ) or ""
    fetch_accept_language: str = _env(
        "JOBAGENT_FETCH_ACCEPT_LANGUAGE",
        "JOB" "_FETCH_ACCEPT_LANGUAGE",
        default="de-DE,de;q=0.9,en-US;q=0.7,en;q=0.6",
    ) or ""

    # Legacy delay used by some older utilities (ms)
    request_delay_ms: int = _env_int("JOBAGENT_REQUEST" "_DELAY_MS", "REQUEST" "_DELAY_MS", default=800)

    # Cache
    cache_enabled: bool = _env_bool("JOBAGENT_CACHE_ENABLED", default=True)
    cache_ttl_days: int = _env_int("JOBAGENT_CACHE_TTL_DAYS", default=7)
    cache_version: str = _env("JOBAGENT_CACHE_VERSION", default="v2") or "v2"
    cache_per_profile: bool = _env_bool("JOBAGENT_CACHE_PER_PROFILE", default=True)

    # Database (Azure SQL)
    database_url: str | None = _env("JOBAGENT_DATABASE_URL", default=None)
    database_migrator_url: str | None = _env("JOBAGENT_DATABASE_MIGRATOR_URL", default=None)
    db_echo: bool = _env_bool("JOBAGENT_DB_ECHO", default=False)
    db_pool_size: int = _env_int("JOBAGENT_DB_POOL_SIZE", default=5)
    db_max_overflow: int = _env_int("JOBAGENT_DB_MAX_OVERFLOW", default=10)

    # Auth / JWT
    jwt_secret: str | None = _env("JOBAGENT_JWT_SECRET", default=None)
    jwt_alg: str = _env("JOBAGENT_JWT_ALG", default="HS256") or "HS256"
    jwt_expires_min: int = _env_int("JOBAGENT_JWT_EXPIRES_MIN", default=120)

    # Paths
    output_dir: Path = Path(os.getenv("JOBAGENT_OUTPUT_DIR", "output"))
    seeds_file: Path = Path(
        _env(
            "JOBAGENT_STEPSTONE" "_SEEDS_FILE",
            "STEPSTONE" "_SEEDS_FILE",
            default="config/stepstone_seeds.json",
        )
        or "config/stepstone_seeds.json"
    )
    seeds_json: str | None = _env("JOBAGENT_STEPSTONE" "_SEEDS_JSON", "STEPSTONE" "_SEEDS_JSON", default=None)


settings = Settings()
