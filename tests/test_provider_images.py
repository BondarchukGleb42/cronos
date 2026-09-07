import base64
import json

import httpx
import pytest

from cronos.providers import Provider, ProviderError
from cronos.settings import Settings

PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII="
)
REFERENCE = "data:image/png;base64," + base64.b64encode(PNG).decode()
SECOND_REFERENCE = "data:image/webp;base64," + base64.b64encode(b"test-reference-2").decode()


def provider(handler, **settings):
    return Provider(
        Settings(database_url="postgresql://test", alltokens_api_key="test-key", **settings),
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )


def response(images=None):
    return httpx.Response(
        200,
        json={
            "id": "gen-image-test",
            "model": "google/gemini-3.1-flash-image-preview",
            "choices": [
                {
                    "message": {
                        "role": "assistant",
                        "content": "Done",
                        "images": images
                        if images is not None
                        else [{"image_url": {"url": REFERENCE}}],
                    },
                    "finish_reason": "stop",
                }
            ],
            "usage": {"prompt_tokens": 140, "completion_tokens": 1120, "cost": 8.100025},
        },
    )


async def test_multi_image_edit_serializes_all_references_in_order_on_every_retry():
    requests = []
    references = [REFERENCE, SECOND_REFERENCE]

    def handler(request):
        assert request.url.path == "/api/v1/chat/completions"
        body = json.loads(request.content)
        requests.append(body)
        if len(requests) == 1:
            return httpx.Response(503, json={"error": {"message": "Unavailable"}})
        return response()

    p = provider(handler)
    try:
        result = await p.generate_image("Combine both references", image_urls=references)
        expected = [
            {"type": "text", "text": "Combine both references"},
            {"type": "image_url", "image_url": {"url": REFERENCE}},
            {"type": "image_url", "image_url": {"url": SECOND_REFERENCE}},
        ]
        assert len(requests) == 2
        assert all(body["messages"] == [{"role": "user", "content": expected}] for body in requests)
        assert all(body["model"] == p.settings.model_image for body in requests)
        assert all(body["modalities"] == ["image", "text"] for body in requests)
        assert all(body["image_config"] == {"image_size": "1K"} for body in requests)
        assert references == [REFERENCE, SECOND_REFERENCE]
        assert result["data"] == PNG
        assert result["usage"]["cost_rub"] == "8.100025"
        assert result["usage"]["completion_tokens"] == 1120
        assert len(result["attempts"]) == 2
    finally:
        await p.close()


async def test_single_image_positional_argument_remains_supported():
    requests = []

    def handler(request):
        requests.append(json.loads(request.content))
        return response()

    p = provider(handler)
    try:
        await p.generate_image("Keep the composition", REFERENCE)
        assert requests[0]["messages"][0]["content"][1] == {
            "type": "image_url",
            "image_url": {"url": REFERENCE},
        }
        assert "aspect_ratio" not in requests[0]["image_config"]
    finally:
        await p.close()


@pytest.mark.parametrize(
    "kwargs",
    [
        {"image_urls": ["https://example.com/image.png"]},
        {"image_urls": [REFERENCE, ""]},
        {"image_urls": [REFERENCE, None]},
        {"image_urls": ["data:image/png;base64,invalid!"]},
        {"image_urls": ["data:image/png;base64,"]},
        {"image_urls": ["data:image/png,unencoded"]},
        {"image_urls": REFERENCE},
        {"image_data_url": ""},
        {"image_data_url": REFERENCE, "image_urls": [SECOND_REFERENCE]},
        {"image_urls": [REFERENCE] * 15},
    ],
)
async def test_invalid_reference_never_becomes_a_generation_without_references(kwargs):
    requests = []

    def handler(request):
        requests.append(request)
        return response()

    p = provider(handler)
    try:
        with pytest.raises(ProviderError) as error:
            await p.generate_image("Edit these", **kwargs)
        assert requests == []
        assert error.value.usage is None
        assert error.value.cost_unknown is False
    finally:
        await p.close()


async def test_unsupported_multi_image_route_keeps_all_references_and_fails():
    requests = []

    def handler(request):
        requests.append(json.loads(request.content))
        return httpx.Response(404, json={"error": {"message": "No image edit endpoint"}})

    p = provider(handler)
    try:
        with pytest.raises(ProviderError) as error:
            await p.generate_image("Edit", image_urls=[REFERENCE, SECOND_REFERENCE])
        assert len(requests) == 3
        assert all(body == requests[0] for body in requests)
        assert len(requests[0]["messages"][0]["content"]) == 3
        assert len(error.value.attempts) == 3
    finally:
        await p.close()


@pytest.mark.parametrize(
    "images",
    [
        [],
        [None, "invalid", {"image_url": None}],
        [{"image_url": {"url": "data:image/png;base64,!"}}],
    ],
)
async def test_missing_or_malformed_generated_image_retains_usage_and_attempts(images):
    requests = []

    def handler(request):
        requests.append(request)
        return response(images)

    p = provider(handler)
    try:
        with pytest.raises(ProviderError) as error:
            await p.generate_image("Edit", image_urls=[REFERENCE])
        assert error.value.usage["cost_rub"] == "8.100025"
        assert len(error.value.attempts) == 1
        assert len(requests) == 1
    finally:
        await p.close()


@pytest.mark.parametrize("selected", [None, "custom/text-model", "deepseek/reasoning"])
@pytest.mark.parametrize("streaming", [False, True])
async def test_multimodal_chat_keeps_images_and_tools_on_vision_route(selected, streaming):
    requests = []
    messages = [
        {"role": "user", "content": "Previous text"},
        {
            "role": "user",
            "content": [
                {"type": "text", "text": "Compare these"},
                {"type": "image_url", "image_url": {"url": REFERENCE}},
                {"type": "image_url", "image_url": {"url": SECOND_REFERENCE}},
            ],
        },
    ]
    tools = [
        {"type": "function", "function": {"name": "file_create", "parameters": {"type": "object"}}}
    ]

    def handler(request):
        requests.append(json.loads(request.content))
        return httpx.Response(503, json={"error": {"message": "Unavailable"}})

    async def on_delta(text):
        pytest.fail("An unsuccessful attempt must not stream output")

    # The text role aliases and fallback configuration must not override vision,
    # even when a reasoning request would otherwise trigger model substitution.
    p = provider(
        handler, model_vision="custom/verified-vision", model_fallbacks="custom/verified-vision"
    )
    try:
        with pytest.raises(ProviderError):
            await p.complete(
                messages,
                tools=tools,
                model=selected,
                reasoning=True,
                on_delta=on_delta if streaming else None,
            )
        assert len(requests) == 3
        assert all(body["model"] == "custom/verified-vision" for body in requests)
        assert all(body["messages"] == messages for body in requests)
        assert all(body["tools"] == tools for body in requests)
    finally:
        await p.close()
