import asyncio

import pytest

from app.stepstone import smoke


def test_smoke_invalid_backend():
    with pytest.raises(ValueError):
        asyncio.run(smoke.search_stepstone({}, backend_override="invalid"))
