import asyncio
import json
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime

import httpx
import pytest

from cronos.providers import Provider, ProviderError, _retry_after_seconds
from cronos.settings import Settings


async def ignore_delta(_):
    pass


def provider(handler):
    return Provider(
        Settings(database_url="postgresql://test", alltokens_api_key="private-test-key"),
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )


def success(stream):
    message = {"role": "assistant", "content": "ok"}
    data = {
        "id": "request-ok",
        "model": "qwen/qwen3.7-flash",
        "usage": {"prompt_tokens": 1, "completion_tokens": 1, "cost": 0.01},
    }
    if stream:
        data["choices"] = [{"index": 0, "delta": message, "finish_reason": "stop"}]
        return httpx.Response(
            200,
            text="data: " + json.dumps(data) + "\n\ndata: [DONE]\n\n",
            headers={"content-type": "text/event-stream"},
        )
    data["choices"] = [{"message": message, "finish_reason": "stop"}]
    return httpx.Response(200, json=data)


@pytest.mark.parametrize("stream", [False, True])
async def test_429_waits_one_then_two_seconds_and_keeps_only_safe_metadata(monkeypatch, stream):
    delays, calls = [], []

    async def sleep(delay):
        delays.append(delay)

    def handler(request):
        calls.append(request)
        if len(calls) < 3:
            return httpx.Response(
                429,
                json={
                    "error": {
                        "code": "private-test-key",
                        "message": "private prompt and private-test-key",
                    }
                },
            )
        return success(stream)

    monkeypatch.setattr("cronos.providers.asyncio.sleep", sleep)
    client = provider(handler)
    try:
        result = await client.complete(
            [{"role": "user", "content": "test"}],
            model="qwen/qwen3.7-flash",
            on_delta=ignore_delta if stream else None,
        )
        assert len(calls) == 3 and delays == [1, 2]
        assert [attempt["retry_delay_seconds"] for attempt in result["attempts"][:2]] == [1, 2]
        assert result["attempts"][0]["error_code"] == "http_429"
        assert "private" not in json.dumps(result["attempts"])
    finally:
        await client.close()


@pytest.mark.parametrize("stream", [False, True])
async def test_retry_after_is_capped_and_final_attempt_never_sleeps(monkeypatch, stream):
    delays, calls = [], []

    async def sleep(delay):
        delays.append(delay)

    def handler(request):
        calls.append(request)
        return httpx.Response(
            429,
            headers={"retry-after": "120"},
            json={"error": {"code": "rate_limit_exceeded", "message": "overloaded"}},
        )

    monkeypatch.setattr("cronos.providers.asyncio.sleep", sleep)
    client = provider(handler)
    try:
        with pytest.raises(ProviderError) as caught:
            await client.complete(
                [{"role": "user", "content": "test"}],
                model="qwen/qwen3.7-flash",
                on_delta=ignore_delta if stream else None,
            )
        assert len(calls) == 3 and delays == [30, 30]
        assert [attempt["retry_delay_seconds"] for attempt in caught.value.attempts] == [30, 30, 0]
        assert caught.value.attempts[0]["error_code"] == "rate_limit_exceeded"
        assert not caught.value.cost_unknown
    finally:
        await client.close()


@pytest.mark.parametrize(
    ("header", "expected"),
    [
        (None, 2),
        ("bad-header", 2),
        ("NaN", 2),
        ("inf", 2),
        ("-1", 2),
        ("0", 0),
        ("3.5", 3.5),
        ("60", 30),
    ],
)
def test_retry_after_parsing_falls_back_or_bounds_seconds(header, expected):
    assert _retry_after_seconds(header, 1) == expected


def test_retry_after_accepts_http_date_and_clamps_past_date():
    now = datetime(2026, 9, 6, tzinfo=UTC)
    assert (
        _retry_after_seconds(format_datetime(now + timedelta(seconds=7), usegmt=True), 0, now=now)
        == 7
    )
    assert (
        _retry_after_seconds(format_datetime(now - timedelta(seconds=7), usegmt=True), 0, now=now)
        == 0
    )


async def test_cancellation_during_backoff_never_sends_next_attempt(monkeypatch):
    calls = []

    async def sleep(delay):
        raise asyncio.CancelledError

    def handler(request):
        calls.append(request)
        return httpx.Response(429, json={"error": {"code": 429}})

    monkeypatch.setattr("cronos.providers.asyncio.sleep", sleep)
    client = provider(handler)
    try:
        with pytest.raises(asyncio.CancelledError):
            await client.complete([{"role": "user", "content": "test"}], on_delta=ignore_delta)
        assert len(calls) == 1
    finally:
        await client.close()


async def test_schema_error_is_not_retried_or_delayed(monkeypatch):
    calls = []

    async def sleep(delay):
        raise AssertionError("Unexpected sleep for a schema error")

    def handler(request):
        calls.append(request)
        return httpx.Response(400, json={"error": {"message": "invalid schema"}})

    monkeypatch.setattr("cronos.providers.asyncio.sleep", sleep)
    client = provider(handler)
    try:
        with pytest.raises(ProviderError):
            await client.complete([{"role": "user", "content": "test"}], on_delta=ignore_delta)
        assert len(calls) == 1
    finally:
        await client.close()
