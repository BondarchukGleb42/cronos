import base64
import json

import httpx
import pytest

from cronos.providers import Provider, ProviderError, normalize_usage
from cronos.settings import Settings


def settings(**overrides):
    return Settings(
        database_url="postgresql://test", alltokens_api_key="private-test-key", **overrides
    )


def completion(message=None, usage=None, model="qwen/qwen3.7-flash"):
    return {
        "id": "gen-test",
        "model": model,
        "choices": [
            {"message": message or {"role": "assistant", "content": "ok"}, "finish_reason": "stop"}
        ],
        "usage": usage
        if usage is not None
        else {"prompt_tokens": 24, "completion_tokens": 134, "cost": 0.002252988},
    }


def provider(handler, **overrides):
    return Provider(
        settings(**overrides), http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler))
    )


def test_usage_exact_cost_and_reasoning_subset():
    raw = '{"id":"gen-1","model":"qwen","usage":{"prompt_tokens":24,"completion_tokens":134,"cost":0.002252988123456789,"completion_tokens_details":{"reasoning_tokens":128}}}'
    usage = normalize_usage(json.loads(raw), raw_text=raw)
    assert usage["cost_rub"] == "0.002252988123456789"
    assert usage["completion_tokens"] == 134
    assert usage["raw"]["completion_tokens_details"]["reasoning_tokens"] == 128


def test_unknown_charge_is_distinct_from_zero():
    assert normalize_usage(completion(usage={}))["cost_rub"] is None
    assert normalize_usage(completion(usage={"cost": 0}))["cost_rub"] == "0"
    assert normalize_usage(completion(usage={"cost": -1}))["cost_rub"] is None
    assert normalize_usage(completion(usage={"cost": "NaN"}))["cost_rub"] is None


def test_receipt_uses_native_image_tokens_not_generic_counts():
    usage = normalize_usage(
        {
            "data": {
                "id": "gen-1",
                "model": "qwen",
                "tokens_prompt": 1557,
                "tokens_completion": 1,
                "native_tokens_prompt": 93,
                "native_tokens_completion": 2,
                "total_cost": 0.00037881,
            }
        },
        receipt=True,
    )
    assert (usage["prompt_tokens"], usage["completion_tokens"]) == (93, 2)
    assert usage["cost_rub"] == "0.00037881"


async def test_free_fallback_chain_is_three_attempts_without_sdk_retries():
    models = []

    def handler(request):
        models.append(json.loads(request.content)["model"])
        return httpx.Response(503, json={"error": {"message": "unavailable"}})

    p = provider(handler, model_free="mistralai/mistral-nemo")
    try:
        with pytest.raises(ProviderError) as exc:
            await p.complete([{"role": "user", "content": "hello"}])
        assert models == [
            "mistralai/mistral-nemo",
            "meta-llama/llama-3.1-8b-instruct",
            "inclusionai/ling-3.0-flash",
        ]
        assert len(exc.value.attempts) == 3
    finally:
        await p.close()


async def test_tools_route_uses_auto_choice_and_has_one_usage_receipt():
    requests = []
    tool = {
        "type": "function",
        "function": {"name": "web_search", "parameters": {"type": "object"}},
    }

    def handler(request):
        body = json.loads(request.content)
        requests.append(body)
        return httpx.Response(
            200,
            json=completion(
                message={
                    "role": "assistant",
                    "content": None,
                    "tool_calls": [
                        {
                            "id": "call-1",
                            "type": "function",
                            "function": {"name": "web_search", "arguments": "{}"},
                        }
                    ],
                }
            ),
        )

    p = provider(handler)
    try:
        result = await p.complete([{"role": "user", "content": "search"}], tools=[tool])
        assert requests[0]["model"] == "deepseek/deepseek-v4-flash"
        assert requests[0]["tool_choice"] == "auto"
        assert requests[0]["reasoning"] == {"enabled": False}
        assert result["message"]["tool_calls"][0]["id"] == "call-1"
        assert result["usage"]["cost_rub"] == "0.002252988"
        assert len(requests) == 1
    finally:
        await p.close()


@pytest.mark.parametrize("selected", ["mistralai/mistral-nemo", "custom/text-model"])
async def test_explicit_tools_model_falls_back_to_verified_tools_model(selected):
    requests = []

    def handler(request):
        body = json.loads(request.content)
        requests.append(body)
        if len(requests) == 1:
            return httpx.Response(404, json={"error": {"message": "No endpoint supports tools"}})
        return httpx.Response(200, json=completion())

    p = provider(handler)
    try:
        result = await p.complete(
            [{"role": "user", "content": "remember"}],
            tools=[
                {
                    "type": "function",
                    "function": {"name": "memory_list", "parameters": {"type": "object"}},
                }
            ],
            model=selected,
        )
        assert [body["model"] for body in requests] == [selected, p.settings.model_tools]
        assert requests[1]["reasoning"] == {"enabled": False}
        assert requests[1]["tools"] == requests[0]["tools"]
        assert len(result["attempts"]) == 2
    finally:
        await p.close()


async def test_explicit_vision_model_falls_back_to_verified_vision_route():
    requests = []

    def handler(request):
        body = json.loads(request.content)
        requests.append(body)
        if len(requests) == 1:
            return httpx.Response(404, json={"error": {"message": "No vision endpoint"}})
        return httpx.Response(200, json=completion())

    p = provider(handler)
    try:
        await p.complete(
            [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image_url",
                            "image_url": {"url": "data:image/png;base64,dGVzdA=="},
                        }
                    ],
                }
            ],
            model="custom/vision-model",
        )
        assert [body["model"] for body in requests] == ["custom/vision-model", "qwen/qwen3.7-flash"]
        assert requests[1]["messages"] == requests[0]["messages"]
    finally:
        await p.close()


async def test_successful_fallback_has_only_success_cost_and_disables_unrequested_reasoning():
    requests = []

    def handler(request):
        body = json.loads(request.content)
        requests.append(body)
        if len(requests) < 3:
            return httpx.Response(503, json={"error": {"message": "unavailable"}})
        return httpx.Response(200, json=completion(model="inclusionai/ling-3.0-flash"))

    p = provider(handler)
    try:
        result = await p.complete([{"role": "user", "content": "hello"}])
        assert requests[-1]["reasoning"] == {"enabled": False}
        assert result["usage"]["cost_rub"] == "0.002252988"
        assert [a["status"] for a in result["attempts"]] == ["http_error", "http_error", "success"]
        assert result["attempts"][0]["cost_unknown"] is True
    finally:
        await p.close()


async def test_read_timeout_is_not_retried_and_cost_stays_unknown():
    calls = []

    def handler(request):
        calls.append(request)
        raise httpx.ReadTimeout("response interrupted", request=request)

    p = provider(handler)
    try:
        with pytest.raises(ProviderError) as exc:
            await p.complete([{"role": "user", "content": "hello"}])
        assert exc.value.cost_unknown is True
        assert len(calls) == 1
    finally:
        await p.close()


async def test_auth_error_has_no_fallback_or_provider_secret_in_message():
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(401, json={"error": {"message": "bad private-test-key"}})

    p = provider(handler)
    try:
        with pytest.raises(ProviderError) as exc:
            await p.complete([{"role": "user", "content": "hello"}])
        assert "private-test-key" not in str(exc.value)
        assert len(calls) == 1
    finally:
        await p.close()


async def test_native_search_extracts_verified_annotations_without_plugins():
    bodies = []

    def handler(request):
        bodies.append(json.loads(request.content))
        return httpx.Response(
            200,
            json=completion(
                model="perplexity/sonar",
                message={
                    "role": "assistant",
                    "content": "Python release [1].",
                    "annotations": [
                        {
                            "type": "url_citation",
                            "url_citation": {
                                "url": "https://python.org/downloads/",
                                "title": "Python",
                            },
                        },
                        {
                            "type": "url_citation",
                            "url_citation": {
                                "url": "https://python.org/downloads/",
                                "title": "Duplicate",
                            },
                        },
                        {
                            "type": "url_citation",
                            "url_citation": {"url": "javascript:alert(1)", "title": "Invalid"},
                        },
                    ],
                },
                usage={
                    "prompt_tokens": 23,
                    "completion_tokens": 40,
                    "cost": 0.605176,
                    "cost_details": {"inference_additional_cost": 0.5976412},
                },
            ),
        )

    p = provider(handler)
    try:
        result = await p.search("Latest Python release")
        assert bodies[0]["web_search_options"] == {"search_context_size": "low"}
        assert "plugins" not in bodies[0]
        assert result["sources"] == [{"url": "https://python.org/downloads/", "title": "Python"}]
        assert result["usage"]["cost_rub"] == "0.605176"
    finally:
        await p.close()


async def test_search_without_structured_sources_does_not_claim_search_succeeded():
    p = provider(
        lambda _: httpx.Response(
            200,
            json=completion(message={"role": "assistant", "content": "I found https://python.org"}),
        )
    )
    try:
        with pytest.raises(ProviderError, match="no usable sources") as exc:
            await p.search("Python")
        assert exc.value.usage is not None
        assert exc.value.usage["cost_rub"] == "0.002252988"
    finally:
        await p.close()


async def test_search_switches_incapable_configured_model_to_verified_native_search():
    selected = []

    def handler(request):
        if request.method == "GET":
            return httpx.Response(
                200,
                json={
                    "data": [{"id": "mistralai/mistral-nemo", "supported_parameters": ["tools"]}]
                },
            )
        selected.append(json.loads(request.content)["model"])
        return httpx.Response(
            200,
            json=completion(
                message={
                    "role": "assistant",
                    "content": "Found a source.",
                    "annotations": [
                        {
                            "type": "url_citation",
                            "url_citation": {"url": "https://python.org/", "title": "Python"},
                        }
                    ],
                }
            ),
        )

    p = provider(handler, model_search="mistralai/mistral-nemo")
    try:
        result = await p.search("Python")
        assert selected == ["perplexity/sonar"]
        assert result["sources"][0]["url"] == "https://python.org/"
    finally:
        await p.close()


async def test_vision_switches_free_text_model_to_vision_route():
    selected = []

    def handler(request):
        selected.append(json.loads(request.content)["model"])
        return httpx.Response(200, json=completion())

    p = provider(handler)
    try:
        await p.complete(
            [
                {
                    "role": "user",
                    "content": [
                        {"type": "image_url", "image_url": {"url": "data:image/png;base64,AA=="}}
                    ],
                }
            ],
            model=p.settings.model_free,
        )
        assert selected == ["qwen/qwen3.7-flash"]
    finally:
        await p.close()


async def test_image_inline_response_preserves_total_charge_and_chat_endpoint():
    data = base64.b64decode(
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII="
    )
    bodies = []

    def handler(request):
        assert request.url.path == "/api/v1/chat/completions"
        bodies.append(json.loads(request.content))
        return httpx.Response(
            200,
            json=completion(
                message={
                    "role": "assistant",
                    "content": None,
                    "images": [
                        {
                            "image_url": {
                                "url": "data:image/png;base64," + base64.b64encode(data).decode()
                            }
                        }
                    ],
                },
                usage={
                    "prompt_tokens": 12,
                    "completion_tokens": 1445,
                    "cost": 8.1544476,
                    "completion_tokens_details": {"image_tokens": 1120},
                },
            ),
        )

    p = provider(handler)
    try:
        result = await p.generate_image("blue square")
        assert result["data"] == data
        assert result["mime"] == "image/png"
        assert result["usage"]["cost_rub"] == "8.1544476"
        assert result["usage"]["completion_tokens"] == 1445
        assert bodies[0]["modalities"] == ["image", "text"]
        assert bodies[0]["image_config"]["image_size"] == "1K"
    finally:
        await p.close()


async def test_catalog_preserves_explicit_units_and_avoids_broken_all_filter():
    queries = []

    def handler(request):
        modality = request.url.params["output_modalities"]
        queries.append(modality)
        return httpx.Response(
            200,
            json={
                "data": [
                    {
                        "id": "model-" + modality,
                        "pricing": {"prompt": "4"},
                        "pricing_units": {"prompt": "rub_per_1m"},
                    }
                ]
            },
        )

    p = provider(handler)
    try:
        models = await p.catalog()
        assert queries == ["text", "image"]
        assert len(models) == 2
        assert models[0]["pricing_units"]["prompt"] == "rub_per_1m"
    finally:
        await p.close()


async def test_receipt_fetch_preserves_decimal_precision():
    raw = '{"data":{"id":"gen-1","native_tokens_prompt":93,"native_tokens_completion":2,"tokens_prompt":1557,"tokens_completion":1,"total_cost":0.000378811234567891}}'

    def handler(request):
        assert request.url.params["id"] == "gen-1"
        return httpx.Response(200, text=raw, headers={"content-type": "application/json"})

    p = provider(handler)
    try:
        result = await p.receipt("gen-1")
        assert result["cost_rub"] == "0.000378811234567891"
        assert result["prompt_tokens"] == 93
    finally:
        await p.close()
