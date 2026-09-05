import asyncio
import json

import httpx
import pytest

from cronos.providers import Provider, ProviderError
from cronos.settings import Settings


class SSEStream(httpx.AsyncByteStream):
    def __init__(self, *parts, error=None):
        self.parts = parts
        self.error = error

    async def __aiter__(self):
        for part in self.parts:
            yield part
        if self.error:
            raise self.error


def event(delta=None, *, finish=None, **extra):
    chunk = {
        "id": "gen-stream",
        "model": "qwen/qwen3.7-flash",
        "choices": [{"index": 0, "delta": delta or {}, "finish_reason": finish}],
        **extra,
    }
    return ("data: " + json.dumps(chunk, ensure_ascii=False) + "\n\n").encode()


def response(*parts, error=None):
    return httpx.Response(
        200,
        headers={"content-type": "text/event-stream", "x-request-id": "http-request-id"},
        stream=SSEStream(*parts, error=error),
    )


def provider(handler):
    return Provider(
        Settings(database_url="postgresql://test", alltokens_api_key="private-test-key"),
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )


async def ignore_delta(_):
    pass


async def test_stream_updates_before_response_finishes_and_keeps_exact_usage():
    previews = []
    bodies = []

    class InteractiveStream(httpx.AsyncByteStream):
        async def __aiter__(self):
            first = event({"role": "assistant", "content": "При"})
            # Split a multibyte Cyrillic code point across network chunks.
            split = first.index("П".encode()) + 1
            yield b": keepalive\r\n\r\n" + first[:split]
            yield first[split:]
            assert previews == ["При"]
            yield event({"content": "вет", "reasoning_content": "private reasoning"})
            assert previews == ["При", "Привет"]
            yield event(finish="stop")
            yield b'data: {"id":"gen-stream","choices":[],"usage":{"prompt_tokens":24,"completion_tokens":134,"cost":0.002252988123456789,"completion_tokens_details":{"reasoning_tokens":128}}}\n\n'
            yield b"data: [DONE]\n\n"

    def handler(request):
        bodies.append(json.loads(request.content))
        return httpx.Response(200, stream=InteractiveStream())

    async def preview(text):
        previews.append(text)

    p = provider(handler)
    try:
        result = await p.complete([{"role": "user", "content": "hello"}], on_delta=preview)
        assert bodies[0]["stream"] is True
        assert bodies[0]["stream_options"] == {"include_usage": True}
        assert result["message"] == {"role": "assistant", "content": "Привет"}
        assert result["usage"]["cost_rub"] == "0.002252988123456789"
        assert result["usage"]["completion_tokens"] == 134
        assert result["usage"]["raw"]["completion_tokens_details"]["reasoning_tokens"] == 128
        assert result["usage"]["request_id"] == "gen-stream"
        assert result["finish_reason"] == "stop"
        assert len(result["attempts"]) == 1
        json.dumps(result)  # No Decimal leaks into the stored JSON receipt.
    finally:
        await p.close()


async def test_stream_merges_interleaved_tool_calls_and_annotations():
    citation = {"type": "url_citation", "url_citation": {"url": "https://example.org/a"}}
    parts = [
        event(
            {
                "tool_calls": [
                    {"index": 1, "id": "call-", "function": {"name": "mem", "arguments": '{"x":'}}
                ]
            }
        ),
        event(
            {
                "tool_calls": [
                    {
                        "index": 0,
                        "id": "first",
                        "type": "function",
                        "function": {"name": "search", "arguments": "{}"},
                    }
                ]
            }
        ),
        event(
            {
                "tool_calls": [
                    {"index": 1, "id": "second", "function": {"name": "ory", "arguments": "2}"}}
                ],
                "annotations": [{"index": 0, **citation}],
            }
        ),
        event(
            {"annotations": [{"index": 0, "url_citation": {"title": "Source"}}]},
            finish="tool_calls",
            citations=["https://example.org/a", "https://example.org/b"],
        ),
        b"data: [DONE]\n\n",
    ]
    p = provider(lambda request: response(*parts))
    try:
        result = await p.complete([{"role": "user", "content": "go"}], on_delta=ignore_delta)
        tools = result["message"]["tool_calls"]
        assert [tool["id"] for tool in tools] == ["first", "call-second"]
        assert tools[1]["function"] == {"name": "memory", "arguments": '{"x":2}'}
        assert json.loads(tools[1]["function"]["arguments"])["x"] == 2
        assert result["sources"] == [
            {"url": "https://example.org/a", "title": "Source"},
            {"url": "https://example.org/b", "title": "example.org"},
        ]
        assert result["usage"]["cost_rub"] is None
    finally:
        await p.close()


@pytest.mark.parametrize("failure", ["read_timeout", "truncated", "invalid_json", "provider_error"])
async def test_partial_stream_is_never_retried_and_keeps_receipt_id(failure):
    calls = []
    previews = []

    def handler(request):
        calls.append(request)
        parts = [event({"content": "Part"}, usage={"cost": 0.01})]
        if failure == "invalid_json":
            parts.append(b"data: invalid private-test-key\n\n")
        elif failure == "provider_error":
            parts.append(b'data: {"error":{"message":"private-test-key"}}\n\n')
        error = httpx.ReadTimeout("private-test-key") if failure == "read_timeout" else None
        return response(*parts, error=error)

    async def preview(text):
        previews.append(text)

    p = provider(handler)
    try:
        with pytest.raises(ProviderError) as caught:
            await p.complete([{"role": "user", "content": "go"}], on_delta=preview)
        assert len(calls) == 1
        assert previews == ["Part"]
        assert caught.value.cost_unknown is True
        assert caught.value.usage["cost_rub"] is None
        assert caught.value.usage["request_id"] == "gen-stream"
        assert "private-test-key" not in str(caught.value)
    finally:
        await p.close()


async def test_connection_failure_before_headers_can_use_tools_fallback():
    models = []

    def handler(request):
        body = json.loads(request.content)
        models.append(body["model"])
        if len(models) == 1:
            raise httpx.ConnectError("connection unavailable", request=request)
        assert body["tool_choice"] == "auto"
        assert body["reasoning"] == {"enabled": False}
        return response(event({"content": "ok"}, finish="stop"), b"data: [DONE]\n\n")

    p = provider(handler)
    try:
        result = await p.complete(
            [{"role": "user", "content": "go"}],
            tools=[
                {"type": "function", "function": {"name": "test", "parameters": {"type": "object"}}}
            ],
            model="custom/model",
            on_delta=ignore_delta,
        )
        assert models == ["custom/model", p.settings.model_tools]
        assert [attempt["status"] for attempt in result["attempts"]] == [
            "connection_error",
            "success",
        ]
    finally:
        await p.close()


async def test_three_attempt_limit_and_http_auth_does_not_retry():
    models = []

    def unavailable(request):
        models.append(json.loads(request.content)["model"])
        return httpx.Response(404, json={"error": {"message": "No endpoint"}})

    p = provider(unavailable)
    try:
        with pytest.raises(ProviderError) as caught:
            await p.complete(
                [{"role": "user", "content": "go"}],
                tools=[
                    {
                        "type": "function",
                        "function": {"name": "test", "parameters": {"type": "object"}},
                    }
                ],
                model="custom/model",
                on_delta=ignore_delta,
            )
        assert models == ["custom/model", p.settings.model_tools, p.settings.model_tools]
        assert len(caught.value.attempts) == 3
    finally:
        await p.close()
    calls = []

    def forbidden(request):
        calls.append(request)
        return httpx.Response(401, json={"error": {"message": "private-test-key"}})

    p = provider(forbidden)
    try:
        with pytest.raises(ProviderError) as caught:
            await p.complete([{"role": "user", "content": "go"}], on_delta=ignore_delta)
        assert len(calls) == 1
        assert "private-test-key" not in str(caught.value)
    finally:
        await p.close()


async def test_read_timeout_waiting_for_headers_does_not_retry():
    calls = []

    def handler(request):
        calls.append(request)
        raise httpx.ReadTimeout("response delayed", request=request)

    p = provider(handler)
    try:
        with pytest.raises(ProviderError) as caught:
            await p.complete([{"role": "user", "content": "go"}], on_delta=ignore_delta)
        assert len(calls) == 1
        assert caught.value.cost_unknown is True
    finally:
        await p.close()


async def test_preview_failure_still_consumes_final_usage_without_retry():
    calls = []
    previews = []

    def handler(request):
        calls.append(request)
        return response(
            event({"content": "one"}),
            event({"content": " two"}),
            event(
                finish="stop",
                usage={"prompt_tokens": 2, "completion_tokens": 3, "cost": "0.000123"},
            ),
            b"data: [DONE]\n\n",
        )

    async def failed_preview(text):
        previews.append(text)
        raise RuntimeError("Telegram draft temporarily failed")

    p = provider(handler)
    try:
        result = await p.complete([{"role": "user", "content": "go"}], on_delta=failed_preview)
        assert previews == ["one"]
        assert result["message"]["content"] == "one two"
        assert result["usage"]["cost_rub"] == "0.000123"
        assert result["attempts"][0]["preview_failed"] is True
        assert len(calls) == 1
    finally:
        await p.close()


async def test_cancellation_is_not_swallowed_or_retried():
    calls = []

    def handler(request):
        calls.append(request)
        return response(event({"content": "part"}), event(finish="stop"))

    async def cancel(_):
        raise asyncio.CancelledError

    p = provider(handler)
    try:
        with pytest.raises(asyncio.CancelledError):
            await p.complete([{"role": "user", "content": "go"}], on_delta=cancel)
        assert len(calls) == 1
    finally:
        await p.close()
