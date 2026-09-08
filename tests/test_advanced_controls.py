"""Dialogue selectors are deterministic controls in the originating private topic."""

from contextlib import asynccontextmanager
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from cronos.advanced_controls import (
    ADVANCED_ENTRYPOINTS,
    MODEL_BUTTON,
    REASONING_BUTTON,
    advanced_callback,
    advanced_panel,
)
from cronos.home_views import main_panel, settings_panel
from cronos.model_preferences import DIALOGUE_MODELS, MODEL_LABELS
from cronos.settings import Settings
from cronos.storage import Store
from cronos.worker import Worker


def button_rows(panel):
    return panel["reply_markup"]["inline_keyboard"]


@pytest.mark.parametrize("alias", [None, *DIALOGUE_MODELS])
def test_model_selector_has_only_curated_models_and_marks_current_choice(alias):
    preferences = {"model": DIALOGUE_MODELS[alias]} if alias else {}
    panel = advanced_panel("model", preferences, Settings(database_url="postgresql://unused"))
    buttons = [row[0] for row in button_rows(panel)]
    selected = alias or "luna"
    assert [button["callback_data"] for button in buttons] == [
        f"advanced:model:{value}" for value in DIALOGUE_MODELS
    ]
    assert [button["text"] for button in buttons if button["text"].startswith("✓ ")] == [
        "✓ " + MODEL_LABELS[DIALOGUE_MODELS[selected]]
    ]
    assert "во всех чатах" in panel["text"]
    assert "openai/" not in str(panel)
    assert all(len(button["callback_data"].encode()) <= 64 for button in buttons)


@pytest.mark.parametrize(
    "value,enabled", [(None, False), (False, False), (True, True), ("false", False)]
)
def test_reasoning_selector_uses_explicit_boolean_and_two_honest_labels(value, enabled):
    panel = advanced_panel(
        "reasoning", {"reasoning": value}, Settings(database_url="postgresql://unused")
    )
    buttons = [row[0] for row in button_rows(panel)]
    assert [button["callback_data"] for button in buttons] == [
        "advanced:reasoning:off",
        "advanced:reasoning:deep",
    ]
    assert [button["text"] for button in buttons if button["text"].startswith("✓ ")] == [
        "✓ " + ("Глубокое рассуждение" if enabled else "Без рассуждения")
    ]


def test_advanced_selectors_are_not_advertised_in_main_or_settings_panels():
    for panel in (main_panel(), settings_panel({})):
        assert not any(
            button.get("callback_data", "").startswith("advanced:")
            for row in button_rows(panel)
            for button in row
        )
        assert MODEL_BUTTON not in panel["text"]
        assert REASONING_BUTTON not in panel["text"]


@pytest.mark.parametrize(
    "data",
    [
        "advanced:model:unknown",
        "advanced:reasoning:true",
        "advanced:reasoning:deep:extra",
        "advanced:model",
        "home:model:luna",
    ],
)
def test_callback_parser_rejects_unknown_or_ambiguous_values(data):
    assert advanced_callback(data) is None


@pytest.fixture
def case():
    event = {
        "id": uuid4(),
        "update_id": 777,
        "kind": "telegram",
        "payload": {
            "update_id": 777,
            "message": {
                "message_id": 501,
                "message_thread_id": 88,
                "chat": {"id": 42, "type": "private"},
                "from": {"id": 42, "is_bot": False},
                "text": MODEL_BUTTON,
            },
        },
    }
    state = {"locked": False, "preferences": {"tone": "Коротко"}}

    @asynccontextmanager
    async def lock(owner):
        assert owner == 42
        state["locked"] = True
        try:
            yield
        finally:
            state["locked"] = False

    async def current(event_id):
        assert state["locked"] and event_id == event["id"]
        return deepcopy(event)

    async def preferences(owner):
        assert state["locked"] and owner == 42
        return dict(state["preferences"])

    async def change(owner, values, source_key):
        assert state["locked"] and owner == 42
        state["preferences"].update(values)
        return values

    worker = Worker.__new__(Worker)
    worker.settings = Settings(database_url="postgresql://unused")
    worker.store = SimpleNamespace(
        user_lock=lock,
        event_current=AsyncMock(side_effect=current),
        preferences=AsyncMock(side_effect=preferences),
        set_model_preferences=AsyncMock(side_effect=change),
        enqueue_home_panel=AsyncMock(),
        conversation=AsyncMock(),
        queue_home=AsyncMock(),
    )
    worker.transport = SimpleNamespace(bot=SimpleNamespace(answer_callback_query=AsyncMock()))
    worker.home_control = AsyncMock(return_value=False)
    worker.answer = AsyncMock()
    return SimpleNamespace(worker=worker, store=worker.store, event=event, state=state)


def callback(case, data, *, user=42, message_id=501):
    message = case.event["payload"].pop("message")
    message["message_id"] = message_id
    message["from"] = {"id": 999, "is_bot": True}
    case.event["payload"]["callback_query"] = {
        "id": "unique-callback",
        "from": {"id": user, "is_bot": False},
        "message": message,
        "data": data,
    }


@pytest.mark.parametrize("label,page", ADVANCED_ENTRYPOINTS.items())
async def test_bottom_button_opens_selector_in_current_topic_without_home_or_model(
    case, label, page
):
    case.event["payload"]["message"]["text"] = label
    await case.worker.telegram(case.event)
    case.store.enqueue_home_panel.assert_awaited_once_with(
        42,
        42,
        88,
        advanced_panel(page, case.state["preferences"], case.worker.settings),
        f"home-panel:advanced:{case.event['id']}",
        777,
    )
    case.store.set_model_preferences.assert_not_awaited()
    case.store.conversation.assert_not_awaited()
    case.store.queue_home.assert_not_awaited()
    case.worker.home_control.assert_not_awaited()
    case.worker.answer.assert_not_awaited()


@pytest.mark.parametrize(
    "data,values",
    [(f"advanced:model:{alias}", {"model": model}) for alias, model in DIALOGUE_MODELS.items()]
    + [
        ("advanced:reasoning:off", {"reasoning": False}),
        ("advanced:reasoning:deep", {"reasoning": True}),
    ],
)
async def test_callback_changes_only_selected_preference_and_edits_current_selector(
    case, data, values
):
    case.state["preferences"].update({"model": DIALOGUE_MODELS["terra"], "reasoning": True})
    callback(case, data)
    await case.worker.telegram(case.event)
    case.store.set_model_preferences.assert_awaited_once_with(
        42, values, "callback:unique-callback:model-preference"
    )
    case.store.preferences.assert_awaited_once_with(42)
    panel = case.store.enqueue_home_panel.call_args.args[3]
    assert panel["edit_message_id"] == 501
    assert case.store.enqueue_home_panel.call_args.args[:3] == (42, 42, 88)
    if "model" in values:
        assert case.state["preferences"]["reasoning"] is True
    else:
        assert case.state["preferences"]["model"] == DIALOGUE_MODELS["terra"]
    assert case.state["preferences"]["tone"] == "Коротко"
    case.worker.home_control.assert_not_awaited()
    case.worker.answer.assert_not_awaited()


async def test_replayed_callback_renders_current_preferences_not_old_receipt(case):
    callback(case, "advanced:model:luna")
    case.state["preferences"]["model"] = DIALOGUE_MODELS["sol"]
    case.store.set_model_preferences.side_effect = None
    case.store.set_model_preferences.return_value = {"model": DIALOGUE_MODELS["luna"]}
    await case.worker.telegram(case.event)
    panel = case.store.enqueue_home_panel.call_args.args[3]
    assert "Сейчас: GPT-5.6 Sol" in panel["text"]


@pytest.mark.parametrize(
    "data", ["advanced:model:unknown", "advanced:reasoning:yes", "advanced:unknown"]
)
async def test_invalid_selector_callback_is_acknowledged_without_writes_or_model(case, data):
    callback(case, data)
    await case.worker.telegram(case.event)
    case.worker.transport.bot.answer_callback_query.assert_awaited_once()
    case.store.set_model_preferences.assert_not_awaited()
    case.store.preferences.assert_not_awaited()
    case.store.enqueue_home_panel.assert_not_awaited()
    case.worker.home_control.assert_not_awaited()
    case.worker.answer.assert_not_awaited()


async def test_callback_from_another_user_is_rejected_before_preferences(case):
    callback(case, "advanced:reasoning:deep", user=43)
    await case.worker.telegram(case.event)
    case.store.event_current.assert_not_awaited()
    case.store.set_model_preferences.assert_not_awaited()
    case.store.preferences.assert_not_awaited()


async def test_erased_event_cannot_reapply_setting(case):
    callback(case, "advanced:model:luna")
    case.store.event_current.side_effect = None
    case.store.event_current.return_value = None
    await case.worker.telegram(case.event)
    case.store.set_model_preferences.assert_not_awaited()
    case.store.enqueue_home_panel.assert_not_awaited()


@pytest.mark.parametrize(
    "text", ["Какая модель лучше?", "Расскажи про режим рассуждения", "Напиши 🤖 Модель"]
)
async def test_normal_text_is_not_intercepted_as_selector(case, text):
    assert await case.worker.advanced_control(case.event, 42, 42, 88, text, None) is False
    case.store.preferences.assert_not_awaited()


@pytest.mark.parametrize(
    "values",
    [
        {},
        {"tone": "short"},
        {"reasoning": "false"},
        {"reasoning": 1},
        {"model": "other/model"},
        {"model": None},
    ],
)
async def test_invalid_preferences_fail_before_database_access(values):
    store = Store.__new__(Store)
    store.ensure_user = AsyncMock()
    with pytest.raises(ValueError):
        await store.set_model_preferences(42, values, "invalid-control")
    store.ensure_user.assert_not_awaited()
