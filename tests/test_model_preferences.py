import pytest

from cronos.model_preferences import (
    DEFAULT_DIALOGUE_MODEL,
    DIALOGUE_MODELS,
    normalize_model_choice,
    reasoning_enabled,
    resolve_dialogue_model,
)


@pytest.mark.parametrize(
    ("value", "alias"),
    [
        ("luna", "luna"),
        (" GPT-5.6 Terra ", "terra"),
        ("openai/gpt-5.6-sol", "sol"),
        ("solar", "sol"),
    ],
)
def test_explicit_choices_accept_names_and_canonical_ids(value, alias):
    assert normalize_model_choice(value) == DIALOGUE_MODELS[alias]


@pytest.mark.parametrize("value", [None, "", "qwen/qwen3.7-flash", "unknown", 5, {}])
def test_old_or_missing_selection_adopts_luna_but_cannot_be_selected_again(value):
    assert resolve_dialogue_model({"model": value}) == DEFAULT_DIALOGUE_MODEL
    with pytest.raises(ValueError):
        normalize_model_choice(value)


@pytest.mark.parametrize("value", [None, False, "false", "true", "deep", 1, {}])
def test_reasoning_requires_an_explicit_boolean(value):
    assert not reasoning_enabled({"reasoning": value})
    assert reasoning_enabled({"reasoning": True})
