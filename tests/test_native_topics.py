from contextlib import asynccontextmanager
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from cronos.worker import Worker

OWNER_ID = 920051
BOT_ID = 920099
THREAD_ID = 77


def topic_event(kind="created", *, implicit=False, bot_actor=False):
    service = {"name": "Планы поездки"}
    if kind == "created":
        service["is_name_implicit"] = implicit
    return {
        "id": uuid4(),
        "kind": "telegram",
        "user_id": OWNER_ID,
        "payload": {
            "update_id": 150,
            "message": {
                "message_id": 78,
                "message_thread_id": THREAD_ID,
                "chat": {"id": OWNER_ID, "type": "private"},
                "from": {"id": BOT_ID if bot_actor else OWNER_ID, "is_bot": bot_actor},
                f"forum_topic_{kind}": service,
            },
        },
    }


class NativeTopicStore:
    """Expose the durable event only while the owning user's lock is held."""

    def __init__(self, current):
        self.current = deepcopy(current)
        self.lock_owner = None
        self.locked_users = []
        self.current_reads = []
        self.sync_topic_title = AsyncMock(side_effect=self.sync)
        self.enqueue = AsyncMock(side_effect=self.enqueue_locked)
        self.conversation = AsyncMock()

    @asynccontextmanager
    async def user_lock(self, user_id):
        assert self.lock_owner is None
        self.lock_owner = user_id
        self.locked_users.append(user_id)
        try:
            yield
        finally:
            self.lock_owner = None

    async def event_current(self, event_id):
        assert self.lock_owner == OWNER_ID, "Event must be refreshed inside its user's lock"
        self.current_reads.append(event_id)
        return self.current

    async def sync(self, user_id, chat_id, thread_id, title, *, manual=False, **kwargs):
        assert self.lock_owner == OWNER_ID
        return {
            "id": uuid4(),
            "user_id": user_id,
            "chat_id": chat_id,
            "thread_id": thread_id,
            "title": title,
            "title_auto": not manual,
        }

    async def enqueue_locked(self, *args, **kwargs):
        assert self.lock_owner == OWNER_ID


def worker_with_event(event):
    worker = Worker.__new__(Worker)
    worker.store = NativeTopicStore(event)
    worker.transport = SimpleNamespace(edit_topic=AsyncMock(return_value=True))
    worker.owner = "worker:native-topics-test"
    return worker


@pytest.mark.parametrize(
    ("implicit", "bot_actor", "manual"),
    [(False, False, True), (True, False, False), (False, True, False)],
    ids=["native-explicit-name-pinned", "native-implicit-name-auto", "bot-actor-owner-from-chat"],
)
async def test_native_topic_creation_preserves_owner_and_title_policy(implicit, bot_actor, manual):
    event = topic_event(implicit=implicit, bot_actor=bot_actor)
    worker = worker_with_event(event)

    await worker.telegram(event)

    assert worker.store.current_reads == [event["id"]]
    assert worker.store.locked_users == [OWNER_ID]
    worker.store.sync_topic_title.assert_awaited_once_with(
        OWNER_ID,
        OWNER_ID,
        THREAD_ID,
        "Планы поездки",
        manual=manual,
        created=True,
    )
    worker.store.conversation.assert_not_called()
    worker.transport.edit_topic.assert_not_called()
    worker.store.enqueue.assert_awaited_once()
    delivery = worker.store.enqueue.await_args.args
    assert delivery[:3] == (OWNER_ID, OWNER_ID, THREAD_ID)
    assert delivery[4] == f"topic-welcome:{OWNER_ID}:{THREAD_ID}"
    assert ("Название появится после моего ответа" in delivery[3]["text"]) is not manual
    callbacks = [
        button["callback_data"]
        for row in delivery[3]["reply_markup"]["inline_keyboard"]
        for button in row
    ]
    assert "chat:new" in callbacks
    assert "chat:delete" in callbacks


@pytest.mark.parametrize("kind", ["created", "edited"])
async def test_erased_service_event_cannot_recreate_topic_or_send_welcome(kind):
    stale_event = topic_event(kind)
    worker = worker_with_event(stale_event)
    worker.store.current = None  # The privacy transaction erased it before the lock was acquired.

    await worker.telegram(stale_event)

    assert worker.store.current_reads == [stale_event["id"]]
    assert worker.store.locked_users == [OWNER_ID]
    worker.store.sync_topic_title.assert_not_called()
    worker.store.conversation.assert_not_called()
    worker.store.enqueue.assert_not_called()
    worker.transport.edit_topic.assert_not_called()


async def test_current_native_title_edit_pins_and_restores_users_name():
    event = topic_event("edited")
    worker = worker_with_event(event)

    await worker.telegram(event)

    assert worker.store.current_reads == [event["id"]]
    worker.store.sync_topic_title.assert_awaited_once_with(
        OWNER_ID, OWNER_ID, THREAD_ID, "Планы поездки", manual=True, update_id=78
    )
    worker.transport.edit_topic.assert_awaited_once_with(OWNER_ID, THREAD_ID, "Планы поездки")
    worker.store.enqueue.assert_not_called()


async def test_non_private_service_update_does_not_create_a_user_chat():
    event = topic_event()
    event["payload"]["message"]["chat"]["type"] = "supergroup"
    worker = worker_with_event(event)

    await worker.telegram(event)

    assert worker.store.current_reads == []
    assert worker.store.locked_users == []
    worker.store.sync_topic_title.assert_not_called()
    worker.store.enqueue.assert_not_called()
