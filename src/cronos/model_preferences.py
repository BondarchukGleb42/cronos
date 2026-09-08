"""The curated dialogue models shared by chat controls and agent routing."""

from collections.abc import Mapping
from typing import Any

DIALOGUE_MODELS = {
    "luna": "openai/gpt-5.6-luna",
    "terra": "openai/gpt-5.6-terra",
    "sol": "openai/gpt-5.6-sol",
}
MODEL_LABELS = {
    model: f"GPT-5.6 {name.title()}" for name, model in DIALOGUE_MODELS.items()
}
DEFAULT_DIALOGUE_MODEL = DIALOGUE_MODELS["luna"]


def normalize_model_choice(value: Any) -> str:
    if not isinstance(value, str):
        raise ValueError("Choose GPT-5.6 Luna, Terra or Sol")
    name = value.strip().casefold().removeprefix("openai/")
    name = name.removeprefix("gpt-5.6-").removeprefix("gpt-5.6 ")
    if name == "solar":
        name = "sol"
    if name not in DIALOGUE_MODELS:
        raise ValueError("Choose GPT-5.6 Luna, Terra or Sol")
    return DIALOGUE_MODELS[name]


def resolve_dialogue_model(preferences: Mapping[str, Any], settings: Any = None) -> str:
    """Old catalogue selections adopt the new default without rewriting user data."""
    try:
        return normalize_model_choice(preferences.get("model"))
    except ValueError:
        return DEFAULT_DIALOGUE_MODEL


def reasoning_enabled(preferences: Mapping[str, Any]) -> bool:
    # A historical string such as "false" must not enable paid reasoning.
    return preferences.get("reasoning") is True
