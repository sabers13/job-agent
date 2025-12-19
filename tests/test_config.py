from app.config.focus import DEFAULT_FOCUS
from app.config.settings import settings


def test_focus_defaults_present():
    assert "Python" in DEFAULT_FOCUS.include_skills_any
    assert DEFAULT_FOCUS.titles_any
    assert DEFAULT_FOCUS.exclude_titles_any
    assert DEFAULT_FOCUS.locations_any


def test_settings_defaults_present():
    assert isinstance(settings.openai_model, str)
    assert isinstance(settings.score_keep_threshold, int)
    assert isinstance(settings.use_playwright_default, bool)
    assert settings.output_dir
