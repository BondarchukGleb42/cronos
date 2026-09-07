from contextlib import asynccontextmanager
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest

from cronos.privacy import confirmation_keyboard, confirmation_text
from cronos.worker import Worker


@pytest.fixture
def case(monkeypatch):
    conversation = {"id": uuid4(), "user_id": 42, "chat_id": 42, "thread_id": 77}
    request = {"id": uuid4(), "user_id": 42, "scope": "chat", "thread_id": 77}
    run = {"id": uuid4(), "user_id": 42, "fence": 1, "memory_revision": 0}
    event = {
        "id": uuid4(),
        "payload": {
            "message": {
                "message_id": 11,
                "message_thread_id": 77,
                "chat": {"id": 42, "type": "private"},
                "from": {"id": 42, "is_bot": False},
                "text": "/delete",
            }
        },
    }
    worker = Worker.__new__(Worker)
    lock_state = []

    @asynccontextmanager
    async def user_lock(user_id):
        lock_state.append(user_id)
        try:
            yield
        finally:
            lock_state.pop()

    async def event_current(event_id):
        assert lock_state == [42] and event_id == event["id"]
        return deepcopy(event)

    store = SimpleNamespace(
        user_lock=user_lock,
        event_current=AsyncMock(side_effect=event_current),
        conversation=AsyncMock(return_value=conversation),
        requests_prepare=AsyncMock(return_value=request),
        enqueue=AsyncMock(),
        pending_privacy_request=AsyncMock(return_value=None),
        confirm_privacy_request=AsyncMock(return_value=None),
        cancel_privacy_request=AsyncMock(return_value=False),
        begin_privacy_erasure=AsyncMock(),
        finish_privacy_erasure=AsyncMock(),
        start_run=AsyncMock(return_value=run),
        privacy_request_for_run=AsyncMock(return_value=request),
        enqueue_for_run=AsyncMock(return_value=1),
        ensure_user=AsyncMock(),
        add_message=AsyncMock(),
        queue_topic_title=AsyncMock(),
        finish_run=AsyncMock(),
        run_metrics=AsyncMock(),
    )
    transport = SimpleNamespace(
        bot=SimpleNamespace(answer_callback_query=AsyncMock()),
        draft=AsyncMock(),
        delete_topic=AsyncMock(),
        delete_messages=AsyncMock(),
    )
    agent = SimpleNamespace(
        run=AsyncMock(return_value=confirmation_text(request)),
        artifacts=SimpleNamespace(erase_user=Mock()),
    )
    for name, value in (("store", store), ("transport", transport), ("agent", agent)):
        monkeypatch.setattr(worker, name, value, raising=False)
    erasure = AsyncMock()
    monkeypatch.setattr("cronos.worker.perform_erasure", erasure)
    return SimpleNamespace(
        worker=worker,
        event=event,
        conversation=conversation,
        request=request,
        run=run,
        erasure=erasure,
    )


def callback(case, data):
    message = case.event["payload"].pop("message")
    message["from"] = {"id": 999, "is_bot": True}  # The button's message belongs to our bot.
    case.event["payload"]["callback_query"] = {
        "id": "callback-id",
        "from": {"id": 42, "is_bot": False},
        "message": message,
        "data": data,
    }


def assert_no_erasure(case):
    case.erasure.assert_not_awaited()
    case.worker.store.begin_privacy_erasure.assert_not_awaited()
    case.worker.store.finish_privacy_erasure.assert_not_awaited()
    case.worker.agent.artifacts.erase_user.assert_not_called()
    case.worker.transport.delete_topic.assert_not_awaited()
    case.worker.transport.delete_messages.assert_not_awaited()


@pytest.mark.parametrize(
    ("trigger", "scope"), [("chat:delete", "chat"), ("/delete", "chat"), ("/clearall", "all")]
)
async def test_delete_entry_points_only_prepare_and_enqueue_confirmation(case, trigger, scope):
    case.request["scope"] = scope
    if trigger == "chat:delete":
        callback(case, trigger)
    else:
        case.event["payload"]["message"]["text"] = trigger
    await case.worker.telegram(case.event)
    case.worker.store.requests_prepare.assert_awaited_once_with(
        42, case.conversation, scope, source_key=f"privacy-request:{case.event['id']}"
    )
    case.worker.store.enqueue.assert_awaited_once_with(
        42,
        42,
        77,
        {
            "text": confirmation_text(case.request),
            "reply_markup": confirmation_keyboard(case.request),
        },
        f"privacy-confirm:{case.request['id']}",
    )
    case.worker.store.confirm_privacy_request.assert_not_awaited()
    case.worker.agent.run.assert_not_awaited()
    assert_no_erasure(case)


@pytest.mark.parametrize(
    ("phrase", "scope"),
    [("Подтверждаю полную очистку", "all"), ("Подтверждаю удаление чата", "chat")],
)
@pytest.mark.parametrize("pending", [False, True])
async def test_plain_confirmation_resolves_matching_pending_scope_and_origin(
    case, phrase, scope, pending
):
    case.request["scope"] = scope
    case.event["payload"]["message"]["text"] = phrase
    if pending:
        case.worker.store.pending_privacy_request.return_value = case.request
        case.worker.store.confirm_privacy_request.return_value = case.request
    await case.worker.telegram(case.event)
    case.worker.store.pending_privacy_request.assert_awaited_once_with(42, 77, scope)
    if pending:
        case.worker.store.confirm_privacy_request.assert_awaited_once_with(
            42, case.request["id"], 77
        )
        case.worker.store.enqueue.assert_not_awaited()
    else:
        case.worker.store.confirm_privacy_request.assert_not_awaited()
        assert (
            "Нет действующего подтверждения" in case.worker.store.enqueue.call_args.args[3]["text"]
        )
    case.worker.store.requests_prepare.assert_not_awaited()
    case.worker.agent.run.assert_not_awaited()
    assert_no_erasure(case)


@pytest.mark.parametrize("accepted", [False, True])
async def test_callback_confirmation_uses_actual_sender_and_origin_without_direct_deletion(
    case, accepted
):
    token = case.request["id"].hex
    callback(case, f"privacy:confirm:{token}")
    if accepted:
        case.worker.store.confirm_privacy_request.return_value = case.request
    await case.worker.telegram(case.event)
    case.worker.store.confirm_privacy_request.assert_awaited_once_with(42, token, 77)
    response = case.worker.transport.bot.answer_callback_query.call_args.kwargs["text"]
    assert response == ("Очистка началась" if accepted else "Подтверждение устарело или недоступно")
    case.worker.store.requests_prepare.assert_not_awaited()
    case.worker.agent.run.assert_not_awaited()
    assert_no_erasure(case)


async def test_unknown_malformed_nonce_is_rejected_without_crashing(case):
    callback(case, "privacy:confirm:invalid-nonce")
    case.worker.store.confirm_privacy_request.side_effect = ValueError("Invalid UUID")
    await case.worker.telegram(case.event)
    case.worker.transport.bot.answer_callback_query.assert_awaited_once_with(
        "callback-id", text="Подтверждение недоступно"
    )
    assert_no_erasure(case)


async def test_confirmed_callback_replay_does_not_prepare_or_erase_again(case):
    token = case.request["id"].hex
    callback(case, f"privacy:confirm:{token}")
    case.worker.store.confirm_privacy_request.side_effect = [case.request, None]
    await case.worker.telegram(case.event)
    await case.worker.telegram(case.event)
    assert case.worker.store.confirm_privacy_request.await_count == 2
    assert (
        case.worker.transport.bot.answer_callback_query.call_args.kwargs["text"]
        == "Подтверждение устарело или недоступно"
    )
    case.worker.store.requests_prepare.assert_not_awaited()
    assert_no_erasure(case)


async def test_stale_telegram_event_cannot_prepare_deletion(case):
    case.worker.store.event_current.side_effect = None
    case.worker.store.event_current.return_value = None
    await case.worker.telegram(case.event)
    case.worker.store.conversation.assert_not_awaited()
    case.worker.store.requests_prepare.assert_not_awaited()
    case.worker.store.enqueue.assert_not_awaited()
    assert_no_erasure(case)


async def test_telegram_uses_fresh_payload_under_lock_instead_of_stale_request_text(case):
    stale = deepcopy(case.event)
    case.event["payload"]["message"]["text"] = "/stop"
    await case.worker.telegram(stale)
    case.worker.store.event_current.assert_awaited_once_with(case.event["id"])
    case.worker.store.requests_prepare.assert_not_awaited()
    assert "Остановил" in case.worker.store.enqueue.call_args.args[3]["text"]
    assert_no_erasure(case)


@pytest.mark.parametrize("target_thread", [77, 88])
async def test_pending_intent_answer_attaches_confirmation_and_skips_history_and_title(
    case, target_thread
):
    case.request["thread_id"] = target_thread
    case.request["conversation_id"] = uuid4()
    case.request["origin_thread_id"] = 77
    receipt = confirmation_text(case.request)
    case.worker.agent.run.return_value = receipt
    assert await case.worker.answer(case.event, case.conversation, "Удали тот чат") == "done"
    case.worker.store.privacy_request_for_run.assert_awaited_once_with(case.run["id"])
    payload = case.worker.store.enqueue_for_run.call_args.args[2]
    assert payload == {
        "text": receipt,
        "format": "rich",
        "reply_markup": confirmation_keyboard(case.request),
    }
    case.worker.store.ensure_user.assert_not_awaited()
    case.worker.store.add_message.assert_not_awaited()
    case.worker.store.queue_topic_title.assert_not_awaited()
    case.worker.store.finish_run.assert_awaited_once_with(case.run["id"], "done", fence=1)
    assert_no_erasure(case)
