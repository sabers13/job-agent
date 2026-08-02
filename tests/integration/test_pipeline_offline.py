"""L3: HTML fixture → normalized record → score → artifacts, fully offline.

The last file in TEST-STRATEGY §4's layout, and the last gap in the oracle. Before this,
`pipeline/pipeline.py` sat at 22% and `pipeline/parsers.py` at **zero** — CP-1 §S6 found
that `job_html` and `tests/fixtures/jobs/job_stepstone_1.html` were staged in `conftest.py`
and read by nothing, so the parse → score → artifact path was covered at one of its three
stages. Dead scaffolding in a conftest reads as coverage that does not exist.

**Why this file blocks Slice 3.** Slice 3 moves `stepstone/` into `sources/` behind an
adapter interface. Nothing else in the suite would notice if the parse → score handoff
broke during that move: the scoring invariants build their jobs with `job_factory`, which
constructs a `UnifiedJobPosting` directly and never parses anything. The handoff is the
one seam the move actually touches, and §2 below is the test of it.

**Offline, at the polite-fetch boundary.** `stub_fetch` replaces
`app.pipeline.pipeline.fetch_job_html` — the seam AGENTS.md names ("Fetching goes through
the polite-fetch layer"), and the outermost point that is still this package's own. No
network, no Playwright, no LLM, no DB.

**`asyncio_mode = "strict"`**, so every async test carries `@pytest.mark.asyncio`. An
unmarked one does not skip — measured: pytest reports "async def functions are not
natively supported" and fails. TEST-STRATEGY flaw **F4** is dead rather than dormant, and
that is worth knowing before adding an async test here.

Two conventions this file inherits from CP-1 and does not relitigate:

* **`copy.deepcopy` before every `score_job`.** It writes into the caller's dict and edits
  nested structures in place on any call after the first (backlog **A11**, characterized
  in `tests/unit/test_scoring_invariants.py`). Reusing a parsed job across two scorings
  without copying compares `f(x)` against `f(mutate(x))`.
* **Relational assertions, and `ACCEPT_THRESHOLD` imported rather than restated.** The one
  absolute allowed is the accept threshold, and it is the app's constant (CP1-6).

§5 characterizes three defects. Per TEST-STRATEGY §2.4 they are pinned as-is and filed,
not fixed: a test that changes behaviour in the same commit that discovers it makes the
diff unreadable.

**Every claim here was mutation-checked**, because CP-1's finding was not "tests were
missing" but "tests asserted properties they could not fail to detect the absence of", and
a new file gets no exemption from that. 29 mutations, each applied to `app/`, run against
the *whole* suite, reverted: the targeted test had to go red and nothing else could. No
mutation ever reddened a test outside its expected set, and a deliberate no-op control
turned nothing red — so the harness is neither missing effects nor breaking everything.

It caught two assertions **in this file** that could not fail:

1. `test_a_run_summary_lists_every_bundled_job` asserted `str(score) in body`. Deleting
   the score from `write_summary`'s heading left the suite green, because the summary
   embeds the bundle path and that path already begins with the zero-padded score. Now
   asserted against the heading line.
2. `test_the_directory_name_is_ordered_by_score` asserted `startswith(f"{score:02d}_")`
   while its docstring claimed zero-padding. The fixture scores **27**, and `f"{27:02d}"`
   is indistinguishable from `f"{27}"` — two digits cannot demonstrate two-digit padding.
   Split out into a single-digit case.

Both are the CP-1 failure mode exactly, found in the file written to answer CP-1. That is
the argument for mutation-checking rather than review: neither survived contact with a
mutation, and both had already survived being written carefully and read twice.
"""

from __future__ import annotations

import copy
import dataclasses
import json
from pathlib import Path
from typing import Any

import pytest

from app.pipeline import ACCEPT_THRESHOLD
from app.pipeline.parsers import extract_jobposting_from_html
from app.pipeline.scoring import score_job

FIXTURE_URL = "https://example.com/job/abc-123"


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #


@pytest.fixture
def dortmund_profile(profile_factory):
    """A profile that actually matches the fixture posting.

    `profile_factory` defaults to Berlin and the fixture job is in Dortmund, so a test
    using the bare factory would assert against a location *miss* without saying so — the
    same vacuous-setup trap `test_the_cap_binds_regardless_of_how_good_the_rest_is`
    documents in the scoring invariants.
    """
    return profile_factory(
        titles_any={"Data Analyst"},
        locations_any={"Dortmund"},
        include_skills_any={"SQL", "Python"},
        nice_to_have={"Power BI"},
    )


@pytest.fixture
def stub_fetch(job_html, monkeypatch: pytest.MonkeyPatch):
    """Replace `fetch_job_html` at the polite-fetch boundary. Returns a call log.

    Patches the name **as imported into `app.pipeline.pipeline`**, not on
    `app.fetching.polite_fetch`: `pipeline.py` does `from ..fetching.polite_fetch import
    fetch_job_html` at module scope, so patching the source module would rebind a name
    nothing reads and the test would silently hit the real network.
    """
    import app.pipeline.pipeline as pipeline_module

    calls: list[str] = []
    html = job_html()

    async def _fake_fetch(url: str, *, preferred_backend: str | None = None):
        calls.append(url)
        return html, {
            "url": url,
            "backend": preferred_backend or "http",
            "ok": True,
            "attempts": [{"ok": True}],
        }

    monkeypatch.setattr(pipeline_module, "fetch_job_html", _fake_fetch)
    return calls


@pytest.fixture
def pipeline_output_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Redirect `write_job_bundle`'s output under `tmp_path`.

    `run_output_root` in `conftest.py` does not reach this path. It rebinds
    `fastapi_run.settings` and `pipeline.state`'s constants; `write_job_bundle` resolves
    its root from `app.pipeline.pipeline`'s own `settings` global, and `write_summary`
    from `app.pipeline.output`'s. Both have to move or a test writes into the developer's
    real `output/` tree — TEST-STRATEGY §6, and the exact failure `run_output_root`'s own
    docstring records from the first time that fixture was written.

    `JOBAGENT_OUTPUT_ROOT` is deleted rather than set, so the `settings` path is the one
    under test here. That the env var wins over settings at all is pinned separately in
    §5.3, because it is a convention violation rather than a design.
    """
    import app.pipeline.output as output_module
    import app.pipeline.pipeline as pipeline_module

    monkeypatch.delenv("JOBAGENT_OUTPUT_ROOT", raising=False)
    base = tmp_path / "pipeline-output"
    base.mkdir(parents=True, exist_ok=True)

    for module in (pipeline_module, output_module):
        monkeypatch.setattr(
            module, "settings", dataclasses.replace(module.settings, output_dir=base)
        )
    return base


def _core_record(base: dict[str, Any]) -> dict[str, Any]:
    """The parse → model step, mirroring `fetch_job_details` field for field.

    Deliberately spelled out rather than `**base`: `pipeline.py:101-115` applies an
    `or "Unknown"` to exactly three fields, and a helper that quietly passed `None`
    through instead would be testing a construction the pipeline never performs. If those
    lines change, this stops matching and the §2 handoff tests are the ones that notice.
    """
    from app.pipeline.models import UnifiedJobPosting

    return UnifiedJobPosting(
        title=base.get("title") or "Unknown",
        company=base.get("company") or "Unknown",
        location=base.get("location") or "Unknown",
        employment_type=base.get("employment_type"),
        date_posted=base.get("date_posted"),
        valid_through=base.get("valid_through"),
        url=base.get("url"),
        job_id=base.get("job_id"),
        salary=base.get("salary"),
        description_html=base.get("description_html"),
        description_text=base.get("description_text"),
    ).model_dump(mode="json")


# --------------------------------------------------------------------------- #
# 1. Parse — `parsers.extract_jobposting_from_html`
# --------------------------------------------------------------------------- #


def test_the_staged_html_fixture_is_actually_parsed(job_html) -> None:
    """Wires `job_html` + `job_stepstone_1.html`, which CP-1 §S6 found read by nothing.

    Every field asserted here is one the scorer or the artifact writer later consumes, so
    this is the fixture's contract rather than a restatement of the file.
    """
    parsed = extract_jobposting_from_html(job_html())

    assert parsed["raw_present"] is True, "the ld+json block was not found"
    assert parsed["title"] == "Junior Data Analyst (m/w/d)"
    assert parsed["company"] == "ExampleCorp"
    assert parsed["location"] == "Dortmund, NRW, DE"
    assert parsed["employment_type"] == "FULL_TIME"
    assert parsed["date_posted"] == "2024-10-01"
    assert parsed["job_id"] == "ABC-123"
    assert parsed["url"] == FIXTURE_URL
    assert "Python" in parsed["description_text"]
    assert "SQL" in parsed["description_text"]


def test_ld_json_is_preferred_over_the_h1(job_html) -> None:
    """Both carry a title here, so "it read the h1" and "it read the ld+json" would
    otherwise be indistinguishable — the fixture's `<h1>` and its ld+json `title` agree.

    Pinned because the h1 is the *fallback*, and a parser that silently stopped reading
    ld+json would still pass a bare title assertion against this fixture.
    """
    html = job_html().replace("<h1>Junior Data Analyst (m/w/d)</h1>", "<h1>WRONG — the h1</h1>")

    parsed = extract_jobposting_from_html(html)

    assert parsed["title"] == "Junior Data Analyst (m/w/d)"


def test_the_h1_is_the_fallback_when_there_is_no_ld_json() -> None:
    parsed = extract_jobposting_from_html("<html><body><h1>Data Analyst</h1></body></html>")

    assert parsed["raw_present"] is False
    assert parsed["title"] == "Data Analyst"
    assert parsed["company"] == "Unknown"


@pytest.mark.parametrize(
    ("name", "html"),
    [
        ("no ld+json and no h1", "<html><body><p>nothing</p></body></html>"),
        ("empty document", ""),
        (
            "malformed json in the block",
            '<html><head><script type="application/ld+json">{not json}</script></head></html>',
        ),
        (
            "ld+json that is not a JobPosting",
            '<html><head><script type="application/ld+json">{"@type":"Organization"}</script>'
            "</head></html>",
        ),
    ],
)
def test_unparseable_input_degrades_to_unknown_rather_than_raising(name: str, html: str) -> None:
    """The parser is the first thing to see a live page, so it must not be the thing that
    kills a batch run. Every degenerate shape yields a record, never an exception."""
    parsed = extract_jobposting_from_html(html)

    assert parsed["raw_present"] is False, name
    assert parsed["title"] == "Unknown", name
    assert parsed["company"] == "Unknown", name


@pytest.mark.parametrize(
    ("shape", "html", "expected_title"),
    [
        (
            "@graph nesting",
            '<html><head><script type="application/ld+json">{"@graph":[{"@type":"WebPage"},'
            '{"@type":"JobPosting","title":"Graph Role"}]}</script></head></html>',
            "Graph Role",
        ),
        (
            "top-level list",
            '<html><head><script type="application/ld+json">[{"@type":"Organization"},'
            '{"@type":"JobPosting","title":"List Role"}]</script></head></html>',
            "List Role",
        ),
        (
            "HTML-escaped JSON",
            '<html><head><script type="application/ld+json">{&quot;@type&quot;:'
            "&quot;JobPosting&quot;,&quot;title&quot;:&quot;Escaped Role&quot;}"
            "</script></head></html>",
            "Escaped Role",
        ),
    ],
)
def test_jobposting_is_found_however_the_page_nests_it(
    shape: str, html: str, expected_title: str
) -> None:
    """Three real-world wrappings `_pick_jobposting` handles. Each is a separate branch,
    and a job board changing its wrapper is a normal event, not an exotic one."""
    parsed = extract_jobposting_from_html(html)

    assert parsed["raw_present"] is True, shape
    assert parsed["title"] == expected_title, shape


def test_a_job_location_list_takes_the_first_entry() -> None:
    html = (
        '<html><head><script type="application/ld+json">{"@type":"JobPosting","title":"T",'
        '"jobLocation":[{"address":{"addressLocality":"Koeln"}},'
        '{"address":{"addressLocality":"Bonn"}}]}</script></head></html>'
    )

    parsed = extract_jobposting_from_html(html)

    assert parsed["location"] == "Koeln"


def test_nested_schema_org_salary_is_flattened() -> None:
    """`baseSalary.value` is a nested object in schema.org; the record wants it flat."""
    html = (
        '<html><head><script type="application/ld+json">{"@type":"JobPosting","title":"T",'
        '"baseSalary":{"value":{"currency":"EUR","unitText":"YEAR","minValue":40000,'
        '"maxValue":50000}}}</script></head></html>'
    )

    parsed = extract_jobposting_from_html(html)

    assert parsed["salary"] == {
        "currency": "EUR",
        "unit": "YEAR",
        "min": 40000,
        "max": 50000,
        "value": None,
    }


def test_description_markup_is_reduced_to_text() -> None:
    """`description_text` is what the scorer greps for skills; markup would hide them."""
    html = (
        '<html><head><script type="application/ld+json">{"@type":"JobPosting","title":"T",'
        '"description":"<p>SQL   and <b>Python</b></p>"}</script></head></html>'
    )

    parsed = extract_jobposting_from_html(html)

    assert parsed["description_text"] == "SQL and Python"
    assert "<b>" in parsed["description_html"], "the raw markup must survive alongside the text"


# --------------------------------------------------------------------------- #
# 2. Parse → score — the handoff Slice 3 puts at risk
# --------------------------------------------------------------------------- #


def test_every_field_the_scorer_reads_survives_the_parse_to_model_step(job_html) -> None:
    """`UnifiedJobPosting` is the contract between the parser and the scorer.

    A field the parser emits under a name the model does not declare is dropped silently
    — no error, just a job that scores wrong for an invisible reason. Asserted on the
    post-model record, because that is what `score_job` actually receives.
    """
    core = _core_record(extract_jobposting_from_html(job_html()))

    for field in ("title", "company", "location", "employment_type", "description_text"):
        assert core.get(field), f"{field} did not survive parse -> UnifiedJobPosting"
    assert core["title"] == "Junior Data Analyst (m/w/d)"
    assert core["description_text"].startswith("Wir suchen einen Junior Data Analyst")


def test_the_parsed_job_scores_and_explains_itself(job_html, dortmund_profile) -> None:
    """The end-to-end shape: a real posting in, a scored result with reasons out."""
    core = _core_record(extract_jobposting_from_html(job_html()))

    result = score_job(copy.deepcopy(core), dortmund_profile)

    assert isinstance(result["score"], int)
    assert 0 <= result["score"] <= 100
    assert result["reasons"], "a score with no reasons is unexplainable to the user"
    assert result["llm_enabled"] is False, "the offline suite must not reach an LLM"
    assert result["heuristic_score"] == result["score"]


def test_skills_found_by_the_parser_reach_the_scorer(job_html, profile_factory) -> None:
    """Monotonic in the *parsed* description, not a hand-built one.

    This is the assertion that would fail if Slice 3 broke the handoff — the description
    the parser extracts is the only thing feeding the skill match here, so a parser that
    stopped populating `description_text` would take this red while every unit-level
    scoring test stayed green.
    """
    core = _core_record(extract_jobposting_from_html(job_html()))

    def skill_component(skills: set[str]) -> float:
        profile = profile_factory(locations_any={"Dortmund"}, include_skills_any=skills)
        return score_job(copy.deepcopy(core), profile)["components"]["include_skills"]

    unmatched = skill_component({"Kubernetes", "Rust"})
    one = skill_component({"SQL"})
    both = skill_component({"SQL", "Python"})

    assert unmatched < one < both


def test_the_parsed_location_drives_the_location_component(job_html, profile_factory) -> None:
    """Dortmund is only known to the test because the parser assembled it from three
    schema.org address components. A regression in `_normalize_location` lands here."""
    core = _core_record(extract_jobposting_from_html(job_html()))

    matching = score_job(copy.deepcopy(core), profile_factory(locations_any={"Dortmund"}))
    elsewhere = score_job(copy.deepcopy(core), profile_factory(locations_any={"Hamburg"}))

    assert matching["components"]["location"] > elsewhere["components"]["location"]
    assert matching["score"] > elsewhere["score"]


def test_the_parsed_title_drives_the_title_match(job_html, profile_factory) -> None:
    core = _core_record(extract_jobposting_from_html(job_html()))

    wanted = score_job(
        copy.deepcopy(core),
        profile_factory(locations_any={"Dortmund"}, titles_any={"Data Analyst"}),
    )
    excluded = score_job(
        copy.deepcopy(core),
        profile_factory(
            locations_any={"Dortmund"},
            titles_any={"Data Analyst"},
            exclude_titles_any={"Junior"},
        ),
    )

    assert excluded["score"] < wanted["score"]


# --------------------------------------------------------------------------- #
# 3. Fetch → parse → score, through `fetch_job_details`
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_fetch_job_details_runs_parse_and_score_end_to_end(
    stub_fetch, dortmund_profile
) -> None:
    """The whole L3 path in one call, with only the network replaced."""
    from app.pipeline.pipeline import fetch_job_details

    result = await fetch_job_details(
        FIXTURE_URL, score=True, focus=dortmund_profile, use_cache=False
    )

    assert stub_fetch == [FIXTURE_URL], "the polite-fetch boundary was not the only fetch"
    assert result["ok"] is True
    assert result["job"]["title"] == "Junior Data Analyst (m/w/d)"
    assert result["job"]["location"] == "Dortmund, NRW, DE"
    assert result["scoring"]["score"] == result["job"]["junior_fit_score"], (
        "the score copied onto the job must be the score that was computed"
    )
    assert result["scoring"]["llm_enabled"] is False


@pytest.mark.asyncio
async def test_scoring_is_opt_in_and_an_unscored_job_says_so(stub_fetch, dortmund_profile) -> None:
    """`score=False` is the default, and it must mean *unscored* rather than *zero*.

    `junior_fit_score` is a declared field on `UnifiedJobPosting`, so the key is always
    present and carries `None` — it is not absent. That distinction is the contract: a
    consumer doing `job.get("junior_fit_score", 0)` would read an unscored job as the
    worst possible match, and `or 0` would do the same to a genuine zero. Pinned as
    `is None` so a future default of `0` fails here loudly.
    """
    from app.pipeline.pipeline import fetch_job_details

    result = await fetch_job_details(FIXTURE_URL, focus=dortmund_profile, use_cache=False)

    assert result["scoring"] is None
    assert result["job"]["junior_fit_score"] is None

    scored = await fetch_job_details(
        FIXTURE_URL, score=True, focus=dortmund_profile, use_cache=False
    )
    assert isinstance(scored["job"]["junior_fit_score"], int), (
        "the same key must carry a number once scoring is switched on"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("error_name", ["RobotsDisallowedError", "AccessDeniedError", "FetchError"])
async def test_fetch_failures_propagate_rather_than_becoming_an_empty_job(
    job_html, monkeypatch: pytest.MonkeyPatch, error_name: str
) -> None:
    """AGENTS.md §Conventions: no silent failure.

    Each of these three is re-raised by name in `fetch_job_details`. If one were ever
    swallowed, the caller would get a record with `title="Unknown"` and score it — a
    fabricated job in the artifacts, indistinguishable from a real bad posting.
    """
    import app.fetching.polite_fetch as polite
    import app.pipeline.pipeline as pipeline_module

    error_type = getattr(polite, error_name)

    async def _boom(url: str, *, preferred_backend: str | None = None):
        raise error_type("stubbed failure")

    monkeypatch.setattr(pipeline_module, "fetch_job_html", _boom)

    with pytest.raises(error_type):
        await pipeline_module.fetch_job_details(FIXTURE_URL, use_cache=False)


@pytest.mark.asyncio
async def test_the_cache_spares_the_second_fetch(stub_fetch, run_output_root) -> None:
    """`run_output_root` is what puts `CACHE_DIR` on `tmp_path`; without it this test
    would write into the developer's real output tree."""
    from app.pipeline.pipeline import fetch_job_details

    first = await fetch_job_details(FIXTURE_URL, use_cache=True)
    second = await fetch_job_details(FIXTURE_URL, use_cache=True)

    assert len(stub_fetch) == 1, "the second call should have been served from cache"
    assert second["job"]["title"] == first["job"]["title"]


@pytest.mark.asyncio
async def test_use_cache_false_always_refetches(stub_fetch, run_output_root) -> None:
    from app.pipeline.pipeline import fetch_job_details

    await fetch_job_details(FIXTURE_URL, use_cache=True)
    await fetch_job_details(FIXTURE_URL, use_cache=False)

    assert len(stub_fetch) == 2


@pytest.mark.asyncio
async def test_enrichment_output_is_merged_into_the_job(
    stub_fetch, dortmund_profile, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The enricher is stubbed at its own boundary — the default suite reaches no LLM."""
    import app.pipeline.pipeline as pipeline_module

    def _fake_enrich(core, focus=None):
        return {**core, "skills_detected": ["SQL", "Python"]}, {"ok": True, "model": "stub"}

    monkeypatch.setattr(pipeline_module, "enrich_jobposting", _fake_enrich)

    result = await pipeline_module.fetch_job_details(
        FIXTURE_URL, enrich=True, focus=dortmund_profile, use_cache=False
    )

    assert result["enrichment_meta"]["ok"] is True
    assert result["job"]["skills_detected"] == ["SQL", "Python"]


@pytest.mark.asyncio
async def test_a_failing_enricher_does_not_lose_the_job(
    stub_fetch, dortmund_profile, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Enrichment is the optional stage; the posting is the deliverable.

    This is the *good* shape of AGENTS.md's broad-except rule — the handler logs at ERROR
    with the exception attached and records a typed `error_type`, rather than collapsing
    into a generic failure. Pinned so a later refactor cannot quietly downgrade it.
    """
    import app.pipeline.pipeline as pipeline_module

    def _boom(core, focus=None):
        raise RuntimeError("enricher exploded")

    monkeypatch.setattr(pipeline_module, "enrich_jobposting", _boom)

    result = await pipeline_module.fetch_job_details(
        FIXTURE_URL, enrich=True, score=True, focus=dortmund_profile, use_cache=False
    )

    assert result["ok"] is True
    assert result["job"]["title"] == "Junior Data Analyst (m/w/d)"
    assert result["enrichment_meta"]["ok"] is False
    assert result["enrichment_meta"]["error_type"] == "enrich_wrapper_failure"
    assert "enricher exploded" in result["enrichment_meta"]["error_message"]
    assert result["scoring"] is not None, "scoring must still run on the unenriched job"


# --------------------------------------------------------------------------- #
# 4. Score → artifact
# --------------------------------------------------------------------------- #


def _bundle(job: dict[str, Any], scoring: dict[str, Any] | None = None, **kwargs) -> Path:
    from app.pipeline.pipeline import write_job_bundle

    return Path(write_job_bundle(job, scoring, **kwargs))


def test_a_scored_job_becomes_a_bundle_on_disk(
    job_html, dortmund_profile, pipeline_output_root
) -> None:
    """TEST-STRATEGY §5.2 — the artifacts are the project's stated deliverable."""
    core = _core_record(extract_jobposting_from_html(job_html()))
    scoring = score_job(copy.deepcopy(core), dortmund_profile)

    out_dir = _bundle(core, scoring)

    assert out_dir.is_dir()
    assert out_dir.is_relative_to(pipeline_output_root), f"escaped tmp_path: {out_dir}"
    assert (out_dir / "REPORT.md").is_file()
    assert (out_dir / "metadata.json").is_file()


def test_the_bundle_metadata_round_trips_the_job_and_the_score(
    job_html, dortmund_profile, pipeline_output_root
) -> None:
    """`metadata.json` is what the GUI and every downstream reader parse."""
    core = _core_record(extract_jobposting_from_html(job_html()))
    scoring = score_job(copy.deepcopy(core), dortmund_profile)

    meta = json.loads((_bundle(core, scoring) / "metadata.json").read_text(encoding="utf-8"))

    assert meta["job"]["title"] == core["title"]
    assert meta["job"]["company"] == core["company"]
    assert meta["scoring"]["score"] == scoring["score"]
    assert meta["created_at"], "an artifact with no timestamp cannot be ordered"


def test_the_report_names_the_role_and_the_score(
    job_html, dortmund_profile, pipeline_output_root
) -> None:
    core = _core_record(extract_jobposting_from_html(job_html()))
    scoring = score_job(copy.deepcopy(core), dortmund_profile)

    report = (_bundle(core, scoring) / "REPORT.md").read_text(encoding="utf-8")

    assert core["company"] in report
    assert core["title"] in report
    assert str(scoring["score"]) in report


def test_the_directory_name_is_ordered_by_score(
    job_html, dortmund_profile, pipeline_output_root
) -> None:
    """The score prefix is what makes `ls` a ranking, and it identifies the posting."""
    core = _core_record(extract_jobposting_from_html(job_html()))
    scoring = score_job(copy.deepcopy(core), dortmund_profile)

    name = _bundle(core, scoring).name

    assert name.startswith(f"{scoring['score']:02d}_")
    assert "examplecorp" in name and "junior-data-analyst" in name


def test_a_single_digit_score_is_zero_padded_so_the_ranking_still_sorts(
    job_html, pipeline_output_root
) -> None:
    """`09_` sorts under `27_`; a bare `9_` sorts *above* it, silently mis-ranking `ls`.

    Split out and driven with an explicit single-digit score because the fixture posting
    scores 27, and `f"{27:02d}"` is indistinguishable from `f"{27}"`. The test above
    therefore asserted padding it could not detect the loss of — caught by mutation, and
    the same coincidence-satisfied shape as the summary assertion in §4. Two digits can
    never demonstrate two-digit padding.
    """
    core = _core_record(extract_jobposting_from_html(job_html()))

    single_digit = _bundle(core, {"score": 9}).name
    double_digit = _bundle(core, {"score": 27}).name

    assert single_digit.startswith("09_"), single_digit
    assert single_digit < double_digit, "zero-padding is what makes the prefixes sortable"


def test_an_accepted_score_is_filed_under_a_bucket_and_a_rejected_one_is_not(
    job_html, pipeline_output_root
) -> None:
    """The accept threshold, asserted relationally against the constant the app owns.

    Driven with synthesised scores rather than a second HTML fixture: the boundary under
    test is `_score_bucket`'s, and inventing a posting that happens to clear 70 would pin
    the fixture's tuning instead of the threshold. Both sides of the boundary are checked,
    one step apart, so neither an off-by-one nor a bucket that never fires can pass.
    """
    core = _core_record(extract_jobposting_from_html(job_html()))

    accepted = _bundle(core, {"score": ACCEPT_THRESHOLD})
    rejected = _bundle(core, {"score": ACCEPT_THRESHOLD - 1})

    assert accepted.parent.name != "bundles", "an accepted job was not filed under a bucket"
    assert accepted.parent.parent.name == "bundles"
    assert rejected.parent.name == "bundles", "a rejected job must not get an accept bucket"


def test_a_strong_llm_score_rescues_a_weak_heuristic_into_potential_applications(
    job_html, pipeline_output_root
) -> None:
    """`decide_potential`'s whole purpose: heuristic below the line, LLM above it.

    Stated as ±1 around `ACCEPT_THRESHOLD` so the test survives a threshold change, which
    is the property CP1-6 promoted the constant to make expressible.
    """
    core = _core_record(extract_jobposting_from_html(job_html()))

    out_dir = _bundle(
        core,
        {"score": ACCEPT_THRESHOLD - 1, "llm_score": ACCEPT_THRESHOLD + 1},
    )

    pot_root = pipeline_output_root / "potential_applications"
    assert pot_root.is_dir(), "a rescued job was not copied into potential_applications/"
    copied = pot_root / out_dir.name
    assert (copied / "potential_reason.json").is_file()
    assert (copied / "REPORT.md").is_file(), "the bundle itself must be copied, not just a note"

    reason = json.loads((copied / "potential_reason.json").read_text(encoding="utf-8"))
    assert reason["final_score"] < ACCEPT_THRESHOLD <= reason["llm_score"]


def test_a_job_strong_on_both_scores_is_not_a_potential_application(
    job_html, pipeline_output_root
) -> None:
    """The negative half. Without it, a `decide_potential` that returned True
    unconditionally would pass the test above."""
    core = _core_record(extract_jobposting_from_html(job_html()))

    _bundle(core, {"score": ACCEPT_THRESHOLD + 1, "llm_score": ACCEPT_THRESHOLD + 1})

    assert not (pipeline_output_root / "potential_applications").exists()


def test_a_run_summary_lists_every_bundled_job(job_html, dortmund_profile, pipeline_output_root):
    """`REPORT_SUMMARY.md` is one of the five artifacts TEST-STRATEGY §5.2 names.

    Asserted against the **heading line**, not the whole document. `str(score) in body`
    was the first draft and it could not fail: the summary embeds the bundle path, and
    that path already starts with the zero-padded score (`27_examplecorp-...`), so
    deleting the score from the heading left `"27" in body` true. Caught by mutation —
    removing `Score: {score}` from `write_summary` turned nothing red. Precisely the
    "assertion satisfied by coincidence" that CP-1 was convened over, reproduced in the
    file written to answer CP-1.
    """
    from app.pipeline.output import write_summary

    core = _core_record(extract_jobposting_from_html(job_html()))
    scoring = score_job(copy.deepcopy(core), dortmund_profile)
    out_dir = _bundle(core, scoring)

    summary_path = Path(
        write_summary(
            [{"job": core, "scoring": scoring, "output_dir": str(out_dir)}],
            out_dir=str(pipeline_output_root),
        )
    )

    assert summary_path.name == "REPORT_SUMMARY.md"
    body = summary_path.read_text(encoding="utf-8")
    headings = [line for line in body.splitlines() if line.startswith("## ")]
    entry = next(line for line in headings if core["title"] in line)

    assert core["company"] in entry
    assert str(scoring["score"]) in entry, f"the score is missing from its heading: {entry!r}"
    assert str(out_dir) in body, "the summary must point at the bundle it describes"


# --------------------------------------------------------------------------- #
# 5. Characterization of defects found while writing this file
# --------------------------------------------------------------------------- #
#
# Pinned as-is, filed in docs/backlog.md, NOT fixed here. TEST-STRATEGY §2.4: pinning
# first is what makes the later fix legible as a behaviour change rather than something
# indistinguishable from a refactor regression in the diff.


@pytest.mark.asyncio
async def test_staleness_raises_when_only_one_side_carries_a_timezone(stub_fetch) -> None:
    """**Backlog A14 — live bug.** `TypeError` escapes `fetch_job_details` uncaught.

    `_parse_iso8601` preserves whatever offset it is given, so a date-only `datePosted`
    yields a *naive* datetime and a `Z`-suffixed cutoff yields an *aware* one. Line 158
    compares them directly and Python refuses.

    This is the normal combination, not an exotic one. The fixture's `datePosted` is
    `"2024-10-01"` — date-only is what StepStone emits — and `prefect_run.py:680` builds
    its cutoff as `.isoformat().replace("+00:00", "Z")`, which is aware. So the batch path
    supplies exactly the pair that raises.

    Asserted as `pytest.raises` because that is what the code does today. When A14 is
    fixed this test fails, which is the point: the fix is then a visible, deliberate
    behaviour change and this test is rewritten in the same commit.
    """
    from app.pipeline.pipeline import fetch_job_details

    with pytest.raises(TypeError, match="offset-naive and offset-aware"):
        await fetch_job_details(FIXTURE_URL, cutoff_iso="2025-01-01T00:00:00Z", use_cache=False)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("cutoff", "expected_stale"),
    [("2025-01-01", True), ("2024-01-01", False), (None, False)],
)
async def test_staleness_works_when_both_sides_agree_about_timezones(
    stub_fetch, cutoff: str | None, expected_stale: bool
) -> None:
    """The other half of A14: the comparison is correct whenever it is reachable.

    Without this the bug above would look like "staleness is broken". It is not — it is
    broken *only* across the naive/aware boundary, which is what makes it a normalisation
    fix rather than a rewrite, and which is worth recording before anyone attempts it.
    """
    from app.pipeline.pipeline import fetch_job_details

    result = await fetch_job_details(FIXTURE_URL, cutoff_iso=cutoff, use_cache=False)

    assert result["stale"] is expected_stale
    assert result["job"].get("stale", False) is expected_stale


def test_the_candidate_german_level_does_not_move_the_heuristic_score(
    job_html, profile_factory
) -> None:
    """**Backlog A15.** A native speaker scores identically to an A1 beginner.

    The fixture posting requires German B2. `_penalize_language` applies a flat penalty
    keyed on the *posting's* detected level, scaled by confidence, and never consults
    `focus.candidate_german_level`. That field is read in exactly one place —
    `classify_blockers` — and only against `llm_part`, so with LLM scoring off (the
    default, and the gate's configuration) it is inert.

    Characterized, not fixed: making it live is a scoring behaviour change, which is
    bucket C, and it would move every German posting's score at once.
    """
    core = _core_record(extract_jobposting_from_html(job_html()))

    scores = {
        level: score_job(
            copy.deepcopy(core),
            profile_factory(locations_any={"Dortmund"}, candidate_german_level=level),
        )["score"]
        for level in ("A1", "B1", "B2", "C2", "Native", "Unknown")
    }

    assert len(set(scores.values())) == 1, f"A15 appears fixed — scores now differ: {scores}"
    assert core["description_text"].count("B2") == 1, (
        "this test is only meaningful while the fixture posting states a German requirement"
    )


def test_the_output_root_env_var_overrides_settings(
    job_html, pipeline_output_root, tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """**Backlog A16.** `write_job_bundle` reads `os.getenv` directly.

    AGENTS.md §Conventions: "Configuration is read from settings in `app/config/`, never
    `os.environ` directly in feature code." `pipeline.py:185` does exactly that, and it
    takes precedence over `settings.output_dir` — so a stray environment variable
    silently relocates every artifact the run produces, and no settings-based test would
    see it.

    Pinned because Slice 6's service layer will move this code, and an undocumented
    env-var override is the kind of thing a move drops silently.
    """
    override = tmp_path / "env-override"
    monkeypatch.setenv("JOBAGENT_OUTPUT_ROOT", str(override))
    core = _core_record(extract_jobposting_from_html(job_html()))

    out_dir = _bundle(core, {"score": ACCEPT_THRESHOLD - 1})

    assert out_dir.is_relative_to(override)
    assert not out_dir.is_relative_to(pipeline_output_root), (
        "settings.output_dir was expected to lose to the environment variable"
    )


def test_schema_source_is_a_constant_and_raw_present_is_the_real_signal() -> None:
    """`schema_source` reads like a discriminator and is not one.

    It is the literal `"ld+json"` on every path, including the ones where no ld+json was
    found. A consumer branching on it would treat an h1-fallback record as structured
    data. `raw_present` is the field that actually carries the answer. Low severity, and
    exactly the kind of thing a rename during Slice 3 would otherwise enshrine.
    """
    without_ld_json = extract_jobposting_from_html("<html><body><h1>T</h1></body></html>")

    assert without_ld_json["schema_source"] == "ld+json"
    assert without_ld_json["raw_present"] is False
