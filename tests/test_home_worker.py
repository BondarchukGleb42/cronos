from contextlib import asynccontextmanager
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from cronos.home import HomeUnavailable
from cronos.home_views import main_panel
from cronos.privacy import confirmation_keyboard
from cronos.worker import Worker


@pytest.fixture
def case(monkeypatch):
    home = {"id": uuid4(), "user_id": 42, "chat_id": 42, "thread_id": 77, "is_home": True}
    event = {
        "id": uuid4(),
        "update_id": 777,
        "kind": "telegram",
        "payload": {
            "update_id": 777,
            "message": {
                "message_id": 501,
                "message_thread_id": 77,
                "chat": {"id": 42, "type": "private"},
                "from": {"id": 42, "is_bot": False},
                "text": "/menu",
            },
        },
    }
    state = {"locked": False, "created": False}
    prefs = {"timezone": "Europe/Moscow", "tone": "Коротко", "proactivity": False}
    balance = {"plan": "START", "tokens_remaining": 1234, "soft_limits": True}
    request = {"id": uuid4(), "scope": "all", "thread_id": 77}

    @asynccontextmanager
    async def user_lock(user_id):
        assert user_id == 42
        state["locked"] = True
        try:
            yield
        finally:
            state["locked"] = False

    async def current(event_id):
        assert state["locked"] and event_id == event["id"]
        return deepcopy(event)

    async def preferences(user_id, changes=None):
        assert user_id == 42
        if changes:
            prefs.update(changes)
        return dict(prefs)

    async def set_proactivity(user_id, value, source_key):
        assert user_id == 42 and source_key == "callback:unique-callback:proactivity"
        prefs["proactivity"] = value
        return dict(prefs)

    async def ensure(*args, **kwargs):
        assert state["locked"]
        assert args[2:4] == (42, 42)
        return home, state["created"]

    worker = Worker.__new__(Worker)
    store = SimpleNamespace(
        user_lock=user_lock,
        event_current=AsyncMock(side_effect=current),
        home_creation_key=AsyncMock(return_value="home-create:42:generation"),
        get_home=AsyncMock(return_value=home),
        reset_home=AsyncMock(),
        enqueue=AsyncMock(),
        enqueue_home_panel=AsyncMock(),
        list_conversations=AsyncMock(return_value=[home]),
        list_schedules=AsyncMock(return_value=[]),
        memories=AsyncMock(return_value=[]),
        preferences=AsyncMock(side_effect=preferences),
        set_proactivity=AsyncMock(side_effect=set_proactivity),
        balance=AsyncMock(return_value=balance),
        change_plan=AsyncMock(return_value=balance),
        requests_prepare=AsyncMock(return_value=request),
        confirm_privacy_request=AsyncMock(),
        begin_privacy_erasure=AsyncMock(),
        finish_privacy_erasure=AsyncMock(),
    )
    transport = SimpleNamespace(bot=SimpleNamespace(answer_callback_query=AsyncMock()))
    for name, value in (("store", store), ("transport", transport), ("answer", AsyncMock())):
        monkeypatch.setattr(worker, name, value, raising=False)
    ensure_mock = AsyncMock(side_effect=ensure)
    monkeypatch.setattr("cronos.worker.ensure_home", ensure_mock)
    monkeypatch.setattr("cronos.worker.personal_main_panel", AsyncMock(return_value=main_panel()))
    erasure = AsyncMock()
    monkeypatch.setattr("cronos.worker.perform_erasure", erasure)
    return SimpleNamespace(
        worker=worker,
        store=store,
        home=home,
        request=request,
        event=event,
        state=state,
        ensure=ensure_mock,
        erasure=erasure,
    )


def callback(case, data, *, thread=77):
    message = case.event["payload"].pop("message")
    message["message_thread_id"] = thread
    message["from"] = {"id": 999, "is_bot": True}
    case.event["payload"]["callback_query"] = {
        "id": "unique-callback",
        "from": {"id": 42, "is_bot": False},
        "message": message,
        "data": data,
    }


@pytest.mark.parametrize("text", ["/start", "/menu", "меню", "Главное меню", "🪐 Главное меню"])
async def test_menu_entrypoints_are_deterministic_and_never_call_agent(case, text):
    case.event["payload"]["message"]["text"] = text
    await case.worker.telegram(case.event)
    case.worker.answer.assert_not_awaited()
    case.store.enqueue_home_panel.assert_awaited_once_with(
        42, 42, 77, main_panel(), f"home-panel:{case.event['id']}", 777
    )
    case.store.enqueue.assert_not_awaited()
    assert case.ensure.call_args.kwargs == {}


async def test_new_home_menu_is_sent_by_welcome_once_without_duplicate_panel(case):
    case.state["created"] = True
    await case.worker.telegram(case.event)
    case.store.enqueue.assert_not_awaited()
    case.store.enqueue_home_panel.assert_not_awaited()
    case.worker.answer.assert_not_awaited()


@pytest.mark.parametrize("thread", [77, 88])
async def test_inline_panels_edit_only_same_home_and_other_chat_gets_location(case, thread):
    callback(case, "home:guide", thread=thread)
    await case.worker.telegram(case.event)
    case.store.enqueue_home_panel.assert_awaited_once()
    first = case.store.enqueue_home_panel.call_args.args
    assert first[:3] == (42, 42, 77)
    assert first[5] == 777
    assert ("edit_message_id" in first[3]) is (thread == 77)
    if thread == 77:
        assert first[3]["edit_message_id"] == 501
        case.store.enqueue.assert_not_awaited()
    else:
        case.store.enqueue.assert_awaited_once()
        second = case.store.enqueue.call_args.args
        assert second[:3] == (42, 42, 88)
        assert "Меню открыто" in second[3]["text"]
        assert second[4] == f"home-location:{case.event['id']}"
    case.worker.answer.assert_not_awaited()


@pytest.mark.parametrize(
    ("route", "method"),
    [
        ("home:chats:0", "list_conversations"),
        ("home:tasks:0", "list_schedules"),
        ("home:memory:0", "memories"),
        ("home:settings", "preferences"),
        ("home:plans", "balance"),
    ],
)
async def test_personal_panels_load_only_actual_sender_data(case, route, method):
    callback(case, route)
    await case.worker.telegram(case.event)
    getattr(case.store, method).assert_awaited_once_with(42)
    case.worker.answer.assert_not_awaited()


async def test_mock_tariff_action_uses_callback_idempotency_key_and_returns_panel(case):
    callback(case, "plan:START")
    await case.worker.telegram(case.event)
    await case.worker.telegram(case.event)
    assert case.store.change_plan.await_count == 2
    assert all(
        call.args == (42, "START", "callback:unique-callback")
        for call in case.store.change_plan.call_args_list
    )
    case.store.balance.assert_not_awaited()
    assert "Тариф обновлён — без оплаты" in case.store.enqueue_home_panel.call_args.args[3]["text"]
    assert len({call.args[4] for call in case.store.enqueue_home_panel.call_args_list}) == 1
    assert all(call.args[5] == 777 for call in case.store.enqueue_home_panel.call_args_list)
    case.store.enqueue.assert_not_awaited()
    case.worker.answer.assert_not_awaited()


async def test_free_downgrade_displays_next_period_instead_of_claiming_immediate_change(case):
    callback(case, "plan:FREE")
    case.store.change_plan.return_value = {
        "plan": "PRO",
        "pending_plan": "FREE",
        "tokens_remaining": 1000,
    }
    await case.worker.telegram(case.event)
    case.store.change_plan.assert_awaited_once_with(42, "FREE", "callback:unique-callback")
    text = case.store.enqueue_home_panel.call_args.args[3]["text"]
    assert "Переход на FREE запланирован на следующий период" in text
    assert "Сейчас: PRO" in text


async def test_first_plain_message_queues_home_and_still_answers_the_user(case):
    case.event["payload"]["message"]["text"] = "Помоги составить меню на неделю"
    case.store.conversation = AsyncMock(return_value=case.home)
    case.store.queue_home = AsyncMock()
    await case.worker.telegram(case.event)
    case.store.queue_home.assert_awaited_once_with(42)
    case.worker.answer.assert_awaited_once()


@pytest.mark.parametrize(("value", "expected"), [("on", True), ("off", False)])
async def test_proactivity_toggle_writes_owner_preference_and_renders_fresh_value(
    case, value, expected
):
    callback(case, f"home:proactivity:{value}")
    await case.worker.telegram(case.event)
    case.store.set_proactivity.assert_awaited_once_with(
        42, expected, "callback:unique-callback:proactivity"
    )
    case.store.preferences.assert_awaited_once_with(42)
    panel = case.store.enqueue_home_panel.call_args.args[3]
    assert ("Писать первым: включено" if expected else "Писать первым: выключено") in panel["text"]
    case.worker.answer.assert_not_awaited()


async def test_clearall_button_only_prepares_same_confirmation_flow(case):
    callback(case, "home:clearall")
    await case.worker.telegram(case.event)
    case.store.requests_prepare.assert_awaited_once_with(
        42, case.home, "all", source_key=f"privacy-request:{case.event['id']}"
    )
    panel = case.store.enqueue.call_args.args[3]
    assert panel["reply_markup"] == confirmation_keyboard(case.request)
    assert "Полная очистка удалит" in panel["text"]
    case.store.confirm_privacy_request.assert_not_awaited()
    case.store.begin_privacy_erasure.assert_not_awaited()
    case.store.finish_privacy_erasure.assert_not_awaited()
    case.store.enqueue_home_panel.assert_not_awaited()
    case.erasure.assert_not_awaited()
    case.worker.answer.assert_not_awaited()


@pytest.mark.parametrize(
    "route", ["home:tasks:-1", "home:memory:1234567", "home:settings:extra", "home:unknown"]
)
async def test_invalid_panel_callback_does_not_load_data_or_create_home(case, route):
    callback(case, route)
    await case.worker.telegram(case.event)
    case.ensure.assert_not_awaited()
    case.store.enqueue.assert_not_awaited()
    case.store.enqueue_home_panel.assert_not_awaited()
    case.worker.answer.assert_not_awaited()


async def test_explicit_retry_rotates_once_by_stable_event_marker_when_home_absent(case):
    callback(case, "home:retry")
    case.store.get_home.return_value = None
    await case.worker.telegram(case.event)
    await case.worker.telegram(case.event)
    assert all(
        call.args == (42,) and call.kwargs == {"source_key": f"home-retry:{case.event['id']}"}
        for call in case.store.reset_home.call_args_list
    )
    assert case.store.reset_home.await_count == 2  # Store deduplicates this marker.


async def test_existing_home_retry_button_never_rotates_generation(case):
    callback(case, "home:retry")
    await case.worker.telegram(case.event)
    case.store.reset_home.assert_not_awaited()


@pytest.mark.parametrize("state", ["missing", "old-generation", "current"])
async def test_bootstrap_rereads_event_and_checks_generation_under_work_lock(case, state):
    stale = {
        "id": case.event["id"],
        "kind": "home_init",
        "payload": {"user_id": 42, "generation": "original"},
    }
    case.event.update(
        kind="home_init",
        payload={"user_id": 42, "generation": "generation" if state == "current" else "old"},
    )
    if state == "missing":
        case.store.event_current.side_effect = None
        case.store.event_current.return_value = None
    await case.worker.home_init(stale)
    if state == "current":
        case.ensure.assert_awaited_once()
    else:
        case.ensure.assert_not_awaited()
    if state == "missing":
        case.store.home_creation_key.assert_not_awaited()
    case.worker.answer.assert_not_awaited()


async def test_unknown_home_result_shows_explicit_retry_without_llm(case):
    case.ensure.side_effect = HomeUnavailable("Telegram не подтвердил создание")
    await case.worker.telegram(case.event)
    panel = case.store.enqueue.call_args.args[3]
    assert "Проверь список тем" in panel["text"]
    assert panel["reply_markup"]["inline_keyboard"][0][0]["callback_data"] == "home:retry"
    case.store.reset_home.assert_not_awaited()
    case.store.enqueue_home_panel.assert_not_awaited()
    case.worker.answer.assert_not_awaited()


@pytest.mark.parametrize(("event_update_id", "expected"), [(777, 777), (None, 888)])
async def test_panel_order_uses_event_update_id_then_payload_fallback(
    case, event_update_id, expected
):
    case.event["update_id"] = event_update_id
    case.event["payload"]["update_id"] = 888
    await case.worker.telegram(case.event)
    assert case.store.enqueue_home_panel.call_args.args[5] == expected


@pytest.mark.parametrize("source", ["control", "bootstrap"])
async def test_general_fallback_panel_is_pinned(case, source):
    case.home.update(thread_id=0, is_home=False)
    if source == "control":
        case.event["payload"]["message"]["message_thread_id"] = 0
        await case.worker.telegram(case.event)
        case.store.enqueue_home_panel.assert_awaited_once()
        payload = case.store.enqueue_home_panel.call_args.args[3]
        case.store.enqueue.assert_not_awaited()
    else:
        case.event.update(kind="home_init", payload={"user_id": 42, "generation": "generation"})
        await case.worker.home_init(case.event)
        case.store.enqueue.assert_awaited_once()
        payload = case.store.enqueue.call_args.args[3]
        case.store.enqueue_home_panel.assert_not_awaited()
    assert payload == {**main_panel(), "pin": True}
