from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from cronos.topics import create_chat
from cronos.worker import Worker


@asynccontextmanager
async def unlocked(*args):
    yield


def creation_case(*, enabled=True, inserted=True, result=None):
    conn = SimpleNamespace(fetchval=AsyncMock(return_value="intent" if inserted else None))

    @asynccontextmanager
    async def connection(*args):
        yield conn

    store = SimpleNamespace(
        operation=AsyncMock(return_value=result),
        connection=connection,
        save_operation=AsyncMock(),
        conversation=AsyncMock(return_value={"id": uuid4()}),
        enqueue=AsyncMock(),
    )
    transport = SimpleNamespace(
        topic_capabilities=AsyncMock(return_value=SimpleNamespace(has_topics_enabled=enabled)),
        create_topic=AsyncMock(return_value={"message_thread_id": 42, "name": "Новый чат"}),
    )
    return store, transport


async def test_create_chat_is_persisted_before_welcome_and_can_resume_delivery():
    store, transport = creation_case()
    result = await create_chat(store, transport, 7, 7, "event:1")
    assert result["thread_id"] == 42
    assert store.enqueue.call_args.args[2] == 42
    store.save_operation.assert_awaited_once()
    store.operation.return_value = {"thread_id": 42, "name": "Новый чат"}
    await create_chat(store, transport, 7, 7, "event:1")
    transport.create_topic.assert_awaited_once()
    assert store.enqueue.call_args.args[-1] == "topic-welcome:7:42"


async def test_ambiguous_creation_is_not_repeated():
    store, transport = creation_case(inserted=False)
    result = await create_chat(store, transport, 7, 7, "event:1")
    assert "мог уже появиться" in result["error"]
    transport.create_topic.assert_not_awaited()
    store.enqueue.assert_not_awaited()


async def test_disabled_topics_do_not_create_a_fake_conversation():
    store, transport = creation_case(enabled=False)
    result = await create_chat(store, transport, 7, 7, "event:1")
    assert "не включены" in result["error"]
    transport.create_topic.assert_not_awaited()
    store.conversation.assert_not_awaited()


def title_case():
    worker = Worker.__new__(Worker)
    conv = {
        "id": uuid4(),
        "user_id": 7,
        "chat_id": 7,
        "thread_id": 42,
        "title": "Новый чат",
        "revision": 0,
    }
    run = {"id": uuid4(), "user_id": 7, "fence": 1}
    result = {"message": {"content": "План тренировок"}, "usage": {"cost_rub": "0.001"}}
    context = {
        "conversation": conv,
        "messages": [
            {"role": "user", "content": "Хочу бегать"},
            {"role": "assistant", "content": "Начнём с трёх тренировок"},
        ],
        "message_count": 2,
    }
    worker.store = SimpleNamespace(
        user_lock=unlocked,
        topic_title_context=AsyncMock(return_value=context),
        start_run=AsyncMock(return_value=run),
        operation=AsyncMock(return_value=None),
        save_operation=AsyncMock(),
        set_topic_title=AsyncMock(return_value=True),
        finish_run=AsyncMock(),
        add_message=AsyncMock(),
        enqueue=AsyncMock(),
    )
    worker.provider = SimpleNamespace(topic_title=AsyncMock(return_value=result))

    async def paid(run, op, call, **kwargs):
        return await call()

    worker.agent = SimpleNamespace(paid=paid, active=AsyncMock())
    worker.transport = SimpleNamespace(edit_topic=AsyncMock(return_value=True))
    event = {
        "id": uuid4(),
        "kind": "topic_title",
        "payload": {
            "user_id": 7,
            "conversation_id": str(conv["id"]),
            "revision": 0,
        },
    }
    return worker, event, context, result


async def test_title_job_renames_native_topic_without_polluting_conversation():
    worker, event, context, _ = title_case()
    await worker.topic_title(event)
    worker.transport.edit_topic.assert_awaited_once_with(7, 42, "План тренировок")
    worker.store.set_topic_title.assert_awaited_once_with(
        7,
        context["conversation"]["id"],
        "План тренировок",
        2,
        0,
    )
    worker.store.add_message.assert_not_awaited()
    worker.store.enqueue.assert_not_awaited()


async def test_title_job_retry_uses_saved_model_result_after_telegram_failure():
    worker, event, context, result = title_case()
    worker.transport.edit_topic.side_effect = RuntimeError("network")
    with pytest.raises(RuntimeError):
        await worker.topic_title(event)
    worker.store.set_topic_title.assert_not_awaited()
    worker.store.operation.return_value = result
    worker.transport.edit_topic.side_effect = None
    worker.transport.edit_topic.return_value = True
    await worker.topic_title(event)
    worker.provider.topic_title.assert_awaited_once()


@pytest.mark.parametrize("changed_revision", [True, False])
async def test_stale_or_already_titled_job_does_not_call_model(changed_revision):
    worker, event, context, _ = title_case()
    if changed_revision:
        context["conversation"]["revision"] = 1
    else:
        worker.store.topic_title_context.return_value = None
    await worker.topic_title(event)
    worker.provider.topic_title.assert_not_awaited()
    worker.transport.edit_topic.assert_not_awaited()


async def test_manual_topic_rename_updates_metadata_without_model_reply():
    worker = Worker.__new__(Worker)
    worker.store = SimpleNamespace(
        user_lock=unlocked,
        sync_topic_title=AsyncMock(return_value={"title": "Моё название"}),
    )
    worker.transport = SimpleNamespace(edit_topic=AsyncMock(return_value=True))
    worker.answer = AsyncMock()
    await worker.telegram(
        {
            "payload": {
                "message": {
                    "chat": {"id": 7, "type": "private"},
                    "from": {"id": 7},
                    "message_thread_id": 42,
                    "message_id": 100,
                    "forum_topic_edited": {"name": "Моё название"},
                }
            }
        }
    )
    worker.store.sync_topic_title.assert_awaited_once_with(
        7, 7, 42, "Моё название", manual=True, update_id=100
    )
    worker.transport.edit_topic.assert_awaited_once_with(7, 42, "Моё название")
    worker.answer.assert_not_awaited()


@pytest.mark.parametrize(
    "actor_is_bot,implicit,manual",
    [(True, False, False), (False, True, False), (False, False, True)],
)
async def test_created_topics_respect_implicit_or_explicit_user_names(
    actor_is_bot, implicit, manual
):
    worker = Worker.__new__(Worker)
    worker.store = SimpleNamespace(
        user_lock=unlocked,
        sync_topic_title=AsyncMock(return_value={"title_auto": not manual}),
        enqueue=AsyncMock(),
    )
    worker.answer = AsyncMock()
    await worker.telegram(
        {
            "payload": {
                "message": {
                    "chat": {"id": 7, "type": "private"},
                    "from": {"id": 123 if actor_is_bot else 7, "is_bot": actor_is_bot},
                    "message_thread_id": 42,
                    "forum_topic_created": {"name": "Новый чат", "is_name_implicit": implicit},
                }
            }
        }
    )
    worker.store.sync_topic_title.assert_awaited_once_with(
        7, 7, 42, "Новый чат", manual=manual, created=True
    )
    welcome = worker.store.enqueue.call_args.args
    assert welcome[:3] == (7, 7, 42)
    assert ("Название появится" in welcome[3]["text"]) is not manual
    worker.answer.assert_not_awaited()


async def test_stale_manual_rename_does_not_overwrite_a_newer_name_in_telegram():
    worker = Worker.__new__(Worker)
    worker.store = SimpleNamespace(
        user_lock=unlocked, sync_topic_title=AsyncMock(return_value=None)
    )
    worker.transport = SimpleNamespace(edit_topic=AsyncMock())
    await worker.telegram(
        {
            "payload": {
                "message": {
                    "chat": {"id": 7, "type": "private"},
                    "from": {"id": 7},
                    "message_id": 100,
                    "message_thread_id": 42,
                    "forum_topic_edited": {"name": "Старое имя"},
                }
            }
        }
    )
    worker.transport.edit_topic.assert_not_awaited()
