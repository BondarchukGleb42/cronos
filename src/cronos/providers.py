"""Model transport and verified AllTokens response normalization."""

import base64
import binascii
import json
import ssl
from collections.abc import AsyncIterator, Awaitable, Callable
from decimal import Decimal, InvalidOperation
from typing import Any
from urllib.parse import urlsplit

import certifi
import httpx
from openai import APIConnectionError, APIStatusError, APITimeoutError, AsyncOpenAI

from cronos.settings import Settings

VERIFIED_SEARCH_MODEL = "perplexity/sonar"
REASONING_MODELS = {
    "qwen/qwen3.7-flash",
    "deepseek/deepseek-v4-flash",
    "inclusionai/ling-3.0-flash",
}
RETRYABLE_STATUSES = {404, 408, 429, 500, 502, 503, 504}


class ProviderError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        usage: dict[str, Any] | None = None,
        attempts: list[dict[str, Any]] | None = None,
        cost_unknown: bool = False,
    ):
        super().__init__(message)
        self.usage = usage
        self.attempts = attempts or []
        self.cost_unknown = cost_unknown


def _count(value: Any) -> int:
    if isinstance(value, bool):
        return 0
    try:
        return max(0, int(value))
    except TypeError, ValueError, OverflowError:
        return 0


def _cost(value: Any) -> str | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        cost = Decimal(str(value))
    except InvalidOperation, TypeError, ValueError:
        return None
    return format(cost, "f") if cost.is_finite() and cost >= 0 else None


def normalize_usage(
    response: dict[str, Any], *, raw_text: str | None = None, receipt: bool = False
) -> dict[str, Any]:
    """Preserve the provider's total charge; never add token subsets to it.

    Live AllTokens pricing_units identify RUB, and native Sonar search billing
    matches its rub_per_unit rate. Catalog token prices are rounded; they must
    not replace the exact successful-response cost when settling a reservation.
    """
    data = response.get("data", {}) if receipt else response
    if not isinstance(data, dict):
        data = {}
    usage = data if receipt else data.get("usage") or {}
    if not isinstance(usage, dict):
        usage = {}
    exact = usage
    if raw_text is not None:
        parsed = json.loads(raw_text, parse_float=Decimal)
        exact = parsed.get("data", {}) if receipt else parsed.get("usage") or {}
        if not isinstance(exact, dict):
            exact = {}
    if receipt:
        prompt = usage.get("native_tokens_prompt", usage.get("tokens_prompt"))
        completion = usage.get("native_tokens_completion", usage.get("tokens_completion"))
        cost = exact.get("total_cost", exact.get("usage"))
        raw = {
            key: value
            for key, value in usage.items()
            if key.startswith(("tokens_", "native_tokens_"))
            or key in {"total_cost", "usage", "provider_name", "web_search_engine"}
        }
    else:
        prompt = usage.get("prompt_tokens")
        completion = usage.get("completion_tokens")
        cost = exact.get("cost")
        raw = usage
    return {
        "prompt_tokens": _count(prompt),
        "completion_tokens": _count(completion),
        "cost_rub": _cost(cost),
        "raw": raw,
        "model": data.get("model"),
        "provider": data.get("provider", data.get("provider_name")),
        "request_id": data.get("id"),
        "token_counts_known": prompt is not None and completion is not None,
    }


def _sources(response: dict[str, Any], message: dict[str, Any]) -> list[dict[str, str]]:
    candidates = []
    for annotation in message.get("annotations") or []:
        if isinstance(annotation, dict) and annotation.get("type") == "url_citation":
            citation = annotation.get("url_citation")
            if isinstance(citation, dict):
                candidates.append(citation)
    for citation in response.get("citations") or []:
        candidates.append({"url": citation} if isinstance(citation, str) else citation)
    candidates.extend(response.get("search_results") or [])
    sources = []
    seen = set()
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        url = candidate.get("url")
        if not isinstance(url, str) or url in seen:
            continue
        try:
            parsed = urlsplit(url)
        except ValueError:
            continue
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            continue
        if parsed.username or parsed.password:
            continue
        seen.add(url)
        sources.append({"url": url, "title": str(candidate.get("title") or parsed.hostname)})
    return sources


async def _sse_events(lines: AsyncIterator[str]) -> AsyncIterator[str]:
    """Decode SSE framing, including comments and events split across HTTP chunks."""
    data: list[str] = []
    async for line in lines:
        if not line:
            if data:
                yield "\n".join(data)
                data = []
        elif line.startswith("data:"):
            value = line[5:]
            data.append(value[1:] if value.startswith(" ") else value)
    if data:
        yield "\n".join(data)


class _StreamResult:
    def __init__(self, model: str):
        self.data: dict[str, Any] = {"model": model, "usage": {}}
        self.message: dict[str, Any] = {"role": "assistant", "content": None}
        self.tools: dict[int, dict[str, Any]] = {}
        self.annotations: dict[int, dict[str, Any]] = {}
        self.finish_reason: str | None = None
        self.cost: str | None = None
        self.saw_choice = False

    def add(self, raw: str) -> bool:
        chunk = json.loads(raw)
        if not isinstance(chunk, dict) or chunk.get("error"):
            raise ValueError("Invalid stream event")
        for key in ("id", "model", "provider", "provider_name"):
            if chunk.get(key) is not None:
                self.data[key] = chunk[key]
        usage = chunk.get("usage")
        if isinstance(usage, dict):
            self.data["usage"].update(usage)
            if "cost" in usage:
                exact = json.loads(raw, parse_float=Decimal)
                self.cost = _cost(exact["usage"]["cost"])
        for key in ("citations", "search_results"):
            if isinstance(chunk.get(key), list):
                self.data.setdefault(key, []).extend(chunk[key])
        changed = False
        for choice in chunk.get("choices") or []:
            if not isinstance(choice, dict) or choice.get("index", 0) != 0:
                continue
            self.saw_choice = True
            if choice.get("finish_reason") is not None:
                self.finish_reason = choice["finish_reason"]
                if self.finish_reason == "error":
                    raise ValueError("Failed stream")
            delta = choice.get("delta") or {}
            if not isinstance(delta, dict):
                raise ValueError("Invalid stream delta")
            # Reasoning tokens are provider metadata, never user-facing draft text.
            for key in ("content", "refusal"):
                fragment = delta.get(key)
                if isinstance(fragment, str):
                    self.message[key] = (self.message.get(key) or "") + fragment
                    changed |= key == "content" and bool(fragment)
            for fragment in delta.get("tool_calls") or []:
                index = fragment.get("index", 0)
                if not isinstance(index, int) or index < 0:
                    raise ValueError("Invalid tool index")
                tool = self.tools.setdefault(
                    index, {"id": "", "type": "function", "function": {"name": "", "arguments": ""}}
                )
                if isinstance(fragment.get("id"), str):
                    tool["id"] += fragment["id"]
                if fragment.get("type"):
                    tool["type"] = fragment["type"]
                function = fragment.get("function") or {}
                for key in ("name", "arguments"):
                    if isinstance(function.get(key), str):
                        tool["function"][key] += function[key]
            for annotation in delta.get("annotations") or []:
                if not isinstance(annotation, dict):
                    continue
                index = annotation.get("index")
                if isinstance(index, int) and index >= 0:
                    target = self.annotations.setdefault(index, {})
                    for key, value in annotation.items():
                        if isinstance(value, dict) and isinstance(target.get(key), dict):
                            target[key].update(value)
                        elif key != "index":
                            target[key] = value
                else:
                    annotations = self.message.setdefault("annotations", [])
                    if annotation not in annotations:
                        annotations.append(annotation)
        return changed

    def usage(self, *, interrupted: bool = False) -> dict[str, Any]:
        usage = normalize_usage(self.data)
        usage["cost_rub"] = None if interrupted else self.cost
        return usage

    def result(self, attempts: list[dict[str, Any]]) -> dict[str, Any]:
        if self.tools:
            self.message["tool_calls"] = [self.tools[index] for index in sorted(self.tools)]
        if self.annotations:
            self.message.setdefault("annotations", []).extend(
                self.annotations[index] for index in sorted(self.annotations)
            )
        return {
            "message": self.message,
            "usage": self.usage(),
            "finish_reason": self.finish_reason,
            "attempts": attempts,
            "sources": _sources(self.data, self.message),
        }


class Provider:
    def __init__(self, settings: Settings, *, http_client: httpx.AsyncClient | None = None):
        self.settings = settings
        if http_client is None:
            context = ssl.create_default_context(cafile=certifi.where())
            context.minimum_version = ssl.TLSVersion.TLSv1_2
            if settings.provider_tls12:
                context.maximum_version = ssl.TLSVersion.TLSv1_2
            http_client = httpx.AsyncClient(
                verify=context,
                http2=False,
                timeout=httpx.Timeout(settings.provider_timeout_seconds, connect=10),
            )
        self._client = AsyncOpenAI(
            api_key=settings.alltokens_api_key.get_secret_value(),
            base_url=settings.alltokens_base_url.rstrip("/") + "/",
            max_retries=0,
            http_client=http_client,
            timeout=httpx.Timeout(settings.provider_timeout_seconds, connect=10),
        )

    async def close(self) -> None:
        await self._client.close()

    async def _completion(self, payload: dict[str, Any], models: list[str]) -> dict[str, Any]:
        attempts: list[dict[str, Any]] = []
        for model in models[:3]:
            try:
                request_payload = dict(payload)
                if model in REASONING_MODELS:
                    extra = dict(request_payload.get("extra_body") or {})
                    extra.setdefault("reasoning", {"enabled": False})
                    request_payload["extra_body"] = extra
                raw = await self._client.chat.completions.with_raw_response.create(
                    model=model,
                    messages=request_payload["messages"],
                    stream=False,
                    **{
                        k: v
                        for k, v in request_payload.items()
                        if k not in {"model", "messages", "stream"}
                    },
                )
                data = json.loads(raw.text)
                if not isinstance(data, dict):
                    raise ValueError("Expected an object response")
            except APIStatusError as exc:
                attempts.append(
                    {
                        "model": model,
                        "status": "http_error",
                        "code": exc.status_code,
                        "request_id": exc.request_id,
                        "cost_unknown": exc.status_code >= 500 or exc.status_code == 408,
                    }
                )
                if exc.status_code in RETRYABLE_STATUSES:
                    continue
                raise ProviderError(
                    f"AI provider rejected the request (HTTP {exc.status_code}).",
                    attempts=attempts,
                ) from None
            except (APIConnectionError, APITimeoutError) as exc:
                cause = exc.__cause__
                before_send = isinstance(cause, (httpx.ConnectError, httpx.ConnectTimeout))
                attempts.append(
                    {"model": model, "status": "connection_error" if before_send else "unknown"}
                )
                if before_send:
                    continue
                raise ProviderError(
                    "The provider response was interrupted; its final cost is unknown.",
                    attempts=attempts,
                    cost_unknown=True,
                ) from None
            except ValueError, TypeError:
                raise ProviderError(
                    "The provider returned an invalid response.",
                    attempts=attempts,
                    cost_unknown=True,
                ) from None
            usage = normalize_usage(data, raw_text=raw.text)
            choices = data.get("choices") or []
            message = choices[0].get("message") if choices else None
            if not isinstance(message, dict):
                raise ProviderError("The provider returned no assistant message.", usage=usage)
            attempts.append(
                {"model": model, "status": "success", "request_id": usage["request_id"]}
            )
            return {
                "message": message,
                "usage": usage,
                "finish_reason": choices[0].get("finish_reason"),
                "attempts": attempts,
                "sources": _sources(data, message),
            }
        raise ProviderError(
            "No configured model could complete the request.",
            attempts=attempts,
            cost_unknown=any(attempt.get("cost_unknown", False) for attempt in attempts),
        )

    async def complete(
        self,
        messages: list[dict],
        tools: list[dict] | None = None,
        model: str | None = None,
        reasoning: bool = False,
        on_delta: Callable[[str], Awaitable[None]] | None = None,
    ) -> dict[str, Any]:
        has_image = False
        for message in messages:
            content = message.get("content")
            if isinstance(content, list):
                has_image |= any(
                    isinstance(part, dict) and part.get("type") == "image_url" for part in content
                )
        fallbacks = [m.strip() for m in self.settings.model_fallbacks.split(",") if m.strip()]
        if has_image and (not model or model in [self.settings.model_free, *fallbacks]):
            model = self.settings.model_vision
        selected = model or (
            self.settings.model_reasoning
            if reasoning
            else self.settings.model_tools
            if tools
            else self.settings.model_free
        )
        if (
            reasoning
            and selected in [self.settings.model_free, *fallbacks]
            and selected not in REASONING_MODELS
        ):
            selected = self.settings.model_reasoning
        payload: dict[str, Any] = {
            "messages": messages,
            "max_tokens": self.settings.max_output_tokens,
            "stream": False,
        }
        if tools:
            payload["tools"] = tools
            # Qwen's verified route accepts auto/none, not forced function choice.
            payload["tool_choice"] = "auto"
        if reasoning or selected in REASONING_MODELS:
            payload["extra_body"] = {"reasoning": {"enabled": reasoning}}
        if has_image:
            chain = [selected, self.settings.model_vision, self.settings.model_vision]
        elif tools:
            chain = [selected, self.settings.model_tools, self.settings.model_tools]
        elif selected == self.settings.model_free and not reasoning:
            chain = list(dict.fromkeys([selected, *fallbacks]))[:3]
        else:
            # Do not silently fall back from vision/tools to an incompatible text model.
            chain = [selected] * 3
        if on_delta is not None:
            return await self._stream_completion(payload, chain, on_delta)
        return await self._completion(payload, chain)

    async def _stream_completion(
        self,
        payload: dict[str, Any],
        models: list[str],
        on_delta: Callable[[str], Awaitable[None]],
    ) -> dict[str, Any]:
        attempts: list[dict[str, Any]] = []
        for model in models[:3]:
            stream = _StreamResult(model)
            opened = False
            preview_failed = False
            try:
                request = {
                    key: value
                    for key, value in payload.items()
                    if key not in {"model", "messages", "stream", "stream_options"}
                }
                if model in REASONING_MODELS:
                    extra = dict(request.get("extra_body") or {})
                    extra.setdefault("reasoning", {"enabled": False})
                    request["extra_body"] = extra
                async with self._client.chat.completions.with_streaming_response.create(
                    model=model,
                    messages=payload["messages"],
                    stream=True,
                    stream_options={"include_usage": True},
                    **request,
                ) as raw:
                    opened = True
                    stream.data["id"] = raw.headers.get("x-request-id")
                    done = False
                    async for event in _sse_events(raw.iter_lines()):
                        if event.strip() == "[DONE]":
                            done = True
                            break
                        if stream.add(event) and not preview_failed:
                            try:
                                await on_delta(stream.message["content"])
                            except Exception:
                                # A draft transport failure must not cancel a billable generation.
                                preview_failed = True
                    if not stream.saw_choice or not (done or stream.finish_reason):
                        raise ValueError("Truncated stream")
            except APIStatusError as exc:
                attempts.append(
                    {
                        "model": model,
                        "status": "http_error",
                        "code": exc.status_code,
                        "request_id": exc.request_id,
                        "cost_unknown": exc.status_code >= 500 or exc.status_code == 408,
                    }
                )
                if not opened and exc.status_code in RETRYABLE_STATUSES:
                    continue
                raise ProviderError(
                    f"AI provider rejected the request (HTTP {exc.status_code}).",
                    attempts=attempts,
                    cost_unknown=opened,
                ) from None
            except (APIConnectionError, APITimeoutError, httpx.HTTPError) as exc:
                cause = exc.__cause__ if isinstance(exc, APIConnectionError) else exc
                before_send = not opened and isinstance(
                    cause, (httpx.ConnectError, httpx.ConnectTimeout)
                )
                attempts.append(
                    {
                        "model": model,
                        "status": "connection_error" if before_send else "interrupted",
                        "request_id": stream.data.get("id"),
                        "cost_unknown": not before_send,
                    }
                )
                if before_send:
                    continue
                raise ProviderError(
                    "The provider stream was interrupted; its final cost is unknown.",
                    usage=stream.usage(interrupted=True),
                    attempts=attempts,
                    cost_unknown=True,
                ) from None
            except ValueError, TypeError, AttributeError:
                attempts.append(
                    {
                        "model": model,
                        "status": "invalid_stream",
                        "request_id": stream.data.get("id"),
                        "cost_unknown": True,
                    }
                )
                raise ProviderError(
                    "The provider returned an incomplete or invalid stream; its final cost is unknown.",
                    usage=stream.usage(interrupted=True),
                    attempts=attempts,
                    cost_unknown=True,
                ) from None
            attempts.append(
                {
                    "model": model,
                    "status": "success",
                    "request_id": stream.data.get("id"),
                    "preview_failed": preview_failed,
                }
            )
            return stream.result(attempts)
        raise ProviderError(
            "No configured model could complete the request.",
            attempts=attempts,
            cost_unknown=any(attempt.get("cost_unknown", False) for attempt in attempts),
        )

    async def search(self, query: str) -> dict[str, Any]:
        # This is an explicit native-search tool call, not a request that a text
        # model invent sources. Native web_search_options was verified live.
        model = self.settings.model_search
        if model != VERIFIED_SEARCH_MODEL:
            catalog = await self.catalog()
            configured = next((m for m in catalog if m.get("id") == model), {})
            if "web_search_options" not in configured.get("supported_parameters", []):
                model = VERIFIED_SEARCH_MODEL
        result = await self._completion(
            {
                "messages": [
                    {
                        "role": "system",
                        "content": "Search the web. Answer using the retrieved sources and cite them. Never invent sources.",
                    },
                    {"role": "user", "content": query},
                ],
                "max_tokens": min(self.settings.max_output_tokens, 2048),
                "stream": False,
                "web_search_options": {"search_context_size": "low"},
            },
            [model] * 3,
        )
        text = result["message"].get("content")
        if not result["sources"] or not isinstance(text, str) or not text.strip():
            raise ProviderError(
                "Web search returned no usable sources.",
                usage=result["usage"],
                attempts=result["attempts"],
            )
        return {
            "text": text,
            "sources": result["sources"],
            "usage": result["usage"],
            "attempts": result["attempts"],
        }

    async def generate_image(
        self, prompt: str, image_data_url: str | None = None
    ) -> dict[str, Any]:
        content: Any = prompt
        if image_data_url:
            if not image_data_url.startswith("data:image/"):
                raise ProviderError("Image input must be an inline image data URL.")
            content = [
                {"type": "text", "text": prompt},
                {"type": "image_url", "image_url": {"url": image_data_url}},
            ]
        result = await self._completion(
            {
                "messages": [{"role": "user", "content": content}],
                "max_tokens": self.settings.max_output_tokens,
                "stream": False,
                "extra_body": {
                    "modalities": ["image", "text"],
                    "image_config": {"image_size": "1K", "aspect_ratio": "1:1"},
                },
            },
            [self.settings.model_image] * 3,
        )
        for image in result["message"].get("images") or []:
            url = image.get("image_url", {}).get("url", "")
            if not isinstance(url, str) or not url.startswith("data:image/"):
                continue
            try:
                header, encoded = url.split(",", 1)
                mime = header[5:].split(";", 1)[0]
                if not header.endswith(";base64") or mime not in {
                    "image/png",
                    "image/jpeg",
                    "image/webp",
                }:
                    continue
                if len(encoded) > 28_000_000:
                    raise ProviderError(
                        "Generated image exceeds the size limit.", usage=result["usage"]
                    )
                data = base64.b64decode(encoded, validate=True)
                if data:
                    return {
                        "data": data,
                        "mime": mime,
                        "usage": result["usage"],
                        "attempts": result["attempts"],
                    }
            except ValueError, binascii.Error:
                continue
        raise ProviderError(
            "The provider returned no supported inline image.", usage=result["usage"]
        )

    async def catalog(self) -> list[dict[str, Any]]:
        models: dict[str, dict[str, Any]] = {}
        # The documented output_modalities=all returned an empty list live.
        for modality in ("text", "image"):
            try:
                data = await self._client.get(
                    "/models",
                    cast_to=dict[str, Any],
                    options={"params": {"output_modalities": modality}},
                )
            except APIConnectionError, APIStatusError:
                raise ProviderError("The provider model catalog is unavailable.") from None
            for model in data.get("data", []):
                if isinstance(model, dict) and isinstance(model.get("id"), str):
                    models[model["id"]] = model
        return list(models.values())

    async def receipt(self, request_id: str) -> dict[str, Any]:
        try:
            raw = await self._client.get(
                "/generation", cast_to=httpx.Response, options={"params": {"id": request_id}}
            )
        except APIConnectionError, APIStatusError:
            raise ProviderError(
                "The provider usage receipt is unavailable.", cost_unknown=True
            ) from None
        return normalize_usage(raw.json(), raw_text=raw.text, receipt=True)
