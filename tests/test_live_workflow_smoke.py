from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from scripts import live_workflow_smoke as smoke


async def test_disabled_smoke_does_not_connect(monkeypatch):
    monkeypatch.delenv("CRONOS_PERSONAL_SMOKE", raising=False)
    settings = Mock(side_effect=AssertionError("must not load"))
    monkeypatch.setattr(smoke, "Settings", settings)
    result = await smoke.evaluate()
    assert result["failed_checks"] == ["opt_in_required"]
    settings.assert_not_called()


async def test_existing_owner_is_not_cleaned_or_used(monkeypatch):
    monkeypatch.setenv("CRONOS_PERSONAL_SMOKE", "1")
    monkeypatch.setenv("DATABASE_URL", "postgresql://cronos_app@127.0.0.1:63830/cronos_test")
    monkeypatch.setenv("ALLTOKENS_API_KEY", "private-test-key")
    conn = SimpleNamespace(fetchval=AsyncMock(side_effect=["cronos_test", True, None]))

    @asynccontextmanager
    async def connection(user_id):
        assert user_id == smoke.USER
        yield conn

    store = SimpleNamespace(connection=connection, open=AsyncMock(), close=AsyncMock())
    monkeypatch.setattr(smoke, "SmokeStore", Mock(return_value=store))
    provider, cleanup = Mock(), AsyncMock()
    monkeypatch.setattr(smoke, "BoundedProvider", provider)
    monkeypatch.setattr(smoke, "cleanup", cleanup)
    result = await smoke.evaluate()
    assert result["failed_checks"] == ["synthetic_user_already_exists"]
    provider.assert_not_called()
    cleanup.assert_not_awaited()
    store.close.assert_awaited_once()


@pytest.mark.parametrize(
    "rows,valid",
    [
        ([["Яблоко", "120"]], True),
        ([["Apple", "120"]], False),
        ([["Яблоко", "12"]], False),
        ([["Яблоко", "120"], ["Яблоко", "120"]], False),
        ([["Яблоко", "NaN"]], False),
    ],
)
def test_csv_readback_checks_exact_values_and_shape(rows, valid):
    assert smoke.csv_matches({"tables": [{"columns": ["product", "price"], "rows": rows}]}) is valid


async def test_unrequested_provider_capabilities_are_blocked():
    provider = object.__new__(smoke.BoundedProvider)
    with pytest.raises(smoke.SmokeRefused, match="search_disabled"):
        await provider.search("synthetic")
    with pytest.raises(smoke.SmokeRefused, match="image_generation_disabled"):
        await provider.generate_image("synthetic")
