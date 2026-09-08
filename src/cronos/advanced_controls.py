"""Hidden dialogue controls opened from Telegram's collapsible reply keyboard."""

from cronos.model_preferences import (
    DIALOGUE_MODELS,
    MODEL_LABELS,
    reasoning_enabled,
    resolve_dialogue_model,
)
from cronos.settings import Settings

MODEL_BUTTON = "🤖 Модель"
REASONING_BUTTON = "🧠 Режим рассуждения"
ADVANCED_ENTRYPOINTS = {MODEL_BUTTON: "model", REASONING_BUTTON: "reasoning"}


def advanced_callback(data: str) -> tuple[str, dict] | None:
    """Accept only the short identifiers emitted by these selectors."""
    parts = data.split(":")
    if len(parts) != 3 or parts[0] != "advanced":
        return None
    page, value = parts[1:]
    if page == "model" and value in DIALOGUE_MODELS:
        return page, {"model": DIALOGUE_MODELS[value]}
    if page == "reasoning" and value in {"off", "deep"}:
        return page, {"reasoning": value == "deep"}
    return None


def advanced_panel(page: str, preferences: dict, settings: Settings) -> dict:
    if page == "model":
        selected = resolve_dialogue_model(preferences, settings)
        text = f"{MODEL_BUTTON}\n\nСейчас: {MODEL_LABELS[selected]}"
        rows = [
            [
                {
                    "text": ("✓ " if model == selected else "") + MODEL_LABELS[model],
                    "callback_data": f"advanced:model:{alias}",
                }
            ]
            for alias, model in DIALOGUE_MODELS.items()
        ]
    elif page == "reasoning":
        enabled = reasoning_enabled(preferences)
        labels = {False: "Без рассуждения", True: "Глубокое рассуждение"}
        text = f"{REASONING_BUTTON}\n\nСейчас: {labels[enabled]}"
        rows = [
            [
                {
                    "text": ("✓ " if value == enabled else "") + labels[value],
                    "callback_data": f"advanced:reasoning:{'deep' if value else 'off'}",
                }
            ]
            for value in (False, True)
        ]
    else:
        raise ValueError("Неизвестная панель настроек")
    return {
        "text": text + "\n\nВыбор применяется к новым запросам во всех чатах.",
        "reply_markup": {"inline_keyboard": rows},
    }
