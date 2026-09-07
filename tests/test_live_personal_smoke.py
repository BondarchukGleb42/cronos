import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from scripts import live_personal_smoke as smoke

ENVIRONMENT = {
    "CRONOS_PERSONAL_SMOKE": "1",
    "DATABASE_URL": "postgresql://cronos_app@127.0.0.1:63830/cronos_test",
}


@pytest.mark.parametrize("user_id", [0, 1, 42001, True])
def test_smoke_refuses_real_or_invalid_owners(user_id):
    with pytest.raises(smoke.SmokeRefused, match="negative_synthetic_user_required"):
        smoke.validate_environment(user_id, ENVIRONMENT)


@pytest.mark.parametrize(
    "url",
    [
        "postgresql://cronos_app@127.0.0.1/cronos",
        "postgresql://cronos_app@database.example/cronos_test",
        "postgresql://cronos_app@localhost/cronos_test?host=database.example",
        "postgresql://cronos_app@localhost/cronos_test#fragment",
        "postgresql://cronos_app@localhost/cronos_test/another",
        "not-a-url",
    ],
)
def test_smoke_refuses_nonlocal_or_ambiguous_test_database(url):
    with pytest.raises(smoke.SmokeRefused, match="local_test_database_required"):
        smoke.validate_environment(smoke.USER, {**ENVIRONMENT, "DATABASE_URL": url})


def test_smoke_requires_explicit_opt_in_and_accepts_local_test_database():
    with pytest.raises(smoke.SmokeRefused, match="opt_in_required"):
        smoke.validate_environment(smoke.USER, {**ENVIRONMENT, "CRONOS_PERSONAL_SMOKE": "0"})
    assert smoke.validate_environment(smoke.USER, ENVIRONMENT) == "cronos_test"


def test_fact_checks_accept_paraphrases_but_reject_different_facts():
    assert smoke.two_sessions("Не более 2 занятий в неделю")
    assert smoke.two_sessions("Можно заниматься два раза в неделю")
    assert not smoke.two_sessions("Три занятия в неделю")
    assert not smoke.two_sessions("Два занятия в день")
    assert smoke.practice_first("Практические задачи в приоритете")
    assert smoke.practice_first("Сначала решаем задачи, потом разбираем теорию")
    assert not smoke.practice_first("Читать теорию")
    assert smoke.next_step_present("Решить 5 квадратных уравнений")
    assert smoke.next_step_present("Пять задач по квадратным уравнениям")
    assert not smoke.next_step_present("Пять задач по линейным уравнениям")


async def test_disabled_smoke_never_initializes_settings_database_or_provider(monkeypatch):
    monkeypatch.delenv("CRONOS_PERSONAL_SMOKE", raising=False)
    settings = Mock(side_effect=AssertionError("Settings must not load"))
    monkeypatch.setattr(smoke, "Settings", settings)
    report = await smoke.evaluate(smoke.USER)
    assert report["success"] is False
    assert report["failed_checks"] == ["opt_in_required"]
    assert report["calls"] == 0
    settings.assert_not_called()


async def test_existing_synthetic_owner_is_neither_overwritten_nor_cleaned(monkeypatch):
    for key, value in {**ENVIRONMENT, "ALLTOKENS_API_KEY": "private-test-key"}.items():
        monkeypatch.setenv(key, value)
    connection = SimpleNamespace(fetchval=AsyncMock(side_effect=["cronos_test", True, None]))

    @asynccontextmanager
    async def connect(user_id):
        assert user_id == smoke.USER
        yield connection

    store = SimpleNamespace(open=AsyncMock(), close=AsyncMock(), connection=connect)
    monkeypatch.setattr(smoke, "SmokeStore", Mock(return_value=store))
    provider = Mock(side_effect=AssertionError("Provider must not initialize"))
    monkeypatch.setattr(smoke, "MeteredProvider", provider)
    cleanup = AsyncMock(side_effect=AssertionError("Existing owner must not be cleaned"))
    monkeypatch.setattr(smoke, "cleanup", cleanup)
    report = await smoke.evaluate(smoke.USER)
    assert report["failed_checks"] == ["synthetic_user_already_exists"]
    assert report["calls"] == 0
    provider.assert_not_called()
    cleanup.assert_not_awaited()
    store.close.assert_awaited_once()
    encoded = json.dumps(report)
    assert "private-test-key" not in encoded
    assert ENVIRONMENT["DATABASE_URL"] not in encoded
    assert str(smoke.USER) not in encoded


async def test_synthetic_delivery_is_collected_without_outbox_or_telegram():
    store = smoke.SmokeStore(None, smoke.USER)
    await store.enqueue_for_run(
        {"user_id": smoke.USER}, {}, {"text": "private synthetic text"}, "run-op"
    )
    await store.enqueue(smoke.USER, smoke.USER, 0, {"text": "other private text"}, "generic-op")
    assert len(store.deliveries) == 2
    assert "private synthetic text" not in json.dumps(store.deliveries)
    assert store.pool is None
    with pytest.raises(smoke.SmokeRefused):
        await store.enqueue_for_run({"user_id": 42}, {}, {}, "wrong-owner")


async def test_cleanup_refuses_positive_owner_before_any_database_call():
    with pytest.raises(smoke.SmokeRefused, match="positive_cleanup_refused"):
        await smoke.cleanup(None, 42, [])
