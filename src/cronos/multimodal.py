"""Small durable image references; pixels are hydrated only for provider requests."""

import re

_LEGACY_IMAGE = re.compile(
    r"\[Пользователь приложил файл: [^\n]*?artifact_id=([0-9a-fA-F-]{36}), mime=image/[^\]]+\]"
)


def image_references(content) -> list[str]:
    if isinstance(content, str):
        return list(dict.fromkeys(_LEGACY_IMAGE.findall(content)))
    if not isinstance(content, list):
        return []
    return list(
        dict.fromkeys(
            part["artifact_id"]
            for part in content
            if isinstance(part, dict)
            and part.get("type") == "image_ref"
            and isinstance(part.get("artifact_id"), str)
        )
    )


def latest_image_references(messages: list[dict]) -> list[str]:
    for message in reversed(messages):
        refs = image_references(message.get("content"))
        if refs:
            return refs
    return []


def generated_image_ids(messages: list[dict]) -> list[str]:
    """Read only successful image tool receipts, never model-provided paths."""
    import json

    calls, result = set(), []
    for message in messages:
        if message.get("role") == "assistant":
            calls.update(
                call.get("id")
                for call in message.get("tool_calls", [])
                if call.get("function", {}).get("name") == "image_generate"
            )
        elif message.get("role") == "tool" and message.get("tool_call_id") in calls:
            try:
                receipt = json.loads(message.get("content", ""))
            except TypeError, ValueError:
                continue
            if isinstance(receipt, dict) and receipt.get("delivery") == "prepared":
                artifact_id = receipt.get("artifact_id")
                if isinstance(artifact_id, str) and artifact_id not in result:
                    result.append(artifact_id)
    return result


def clarification_before_image(message: dict) -> bool:
    """A question must finish the turn before a billable image action can start."""
    text = message.get("content")
    return (
        isinstance(text, str)
        and "?" in text
        and any(
            call.get("function", {}).get("name") == "image_generate"
            for call in message.get("tool_calls", [])
        )
    )
