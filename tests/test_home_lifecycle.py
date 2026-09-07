from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from aiogram.exceptions import TelegramBadRequest
from aiogram.methods import EditForumTopic

from cronos.home import HOME_TITLE, HomeUnavailable, ensure_home
from cronos.home_views import main_panel


class MemoryStore:
    """A narrow fake of the durable intent/result and outbox dedupe contracts."""

    def __init__(self):
        self.home = None
        self.generation = str(uuid4())
        self.results, self.intents, self.chats, self.outbox, self.resets = {}, set(), {}, {}, {}
        self.enqueue_error = None
        self.enqueue_keys = []
        self.set_home_calls = []

    async def get_home(self, user_id):
        assert user_id == 42
        return self.home

    async def home_creation_key(self, user_id):
        assert user_id == 42
        return f"home-create:{user_id}:{self.generation}"

    async def operation(self, operation_id):
        return self.results.get(operation_id)

    @asynccontextmanager
    async def connection(self):
        yield self

    async def fetchval(self, query, operation_id, user_id):
        assert user_id == 42
        if operation_id in self.intents:
            return None
        self.intents.add(operation_id)
        return operation_id

    async def save_operation(self, operation_id, user_id, run_id, kind, result):
        assert user_id == 42 and run_id is None
        self.results.setdefault(operation_id, result)

    async def conversation(self, user_id, chat_id, thread_id, title=""):
        assert user_id == chat_id == 42
        return self.chats.setdefault(
            thread_id,
            {
                "id": str(uuid4()),
                "user_id": user_id,
                "chat_id": chat_id,
                "thread_id": thread_id,
                "title": title,
                "is_home": False,
            },
        )

    async def set_home(self, user_id, conversation_id):
        assert user_id == 42
        self.set_home_calls.append(conversation_id)
        self.home = next(row for row in self.chats.values() if row["id"] == conversation_id)
        self.home.update(is_home=True, title=HOME_TITLE, title_auto=False)
        return self.home

    async def enqueue(self, user_id, chat_id, thread_id, payload, key):
        assert user_id == chat_id == 42
        self.enqueue_keys.append(key)
        if self.enqueue_error:
            raise self.enqueue_error
        self.outbox.setdefault(key, {"thread_id": thread_id, "payload": payload})

    async def reset_home(self, user_id, conversation_id=None, *, source_key=None):
        assert user_id == 42
        if source_key in self.resets:
            return self.resets[source_key]
        if conversation_id is not None:
            assert self.home is not None
            assert self.home["id"] == conversation_id
            self.home["is_home"] = False
            self.home = None
        self.generation = str(uuid4())
        key = await self.home_creation_key(user_id)
        self.resets[source_key] = key
        return key


@pytest.fixture
def case(monkeypatch):
    monkeypatch.setattr("cronos.home.personal_main_panel", AsyncMock(return_value=main_panel()))
    return SimpleNamespace(
        store=MemoryStore(),
        transport=SimpleNamespace(
            topic_capabilities=AsyncMock(return_value=SimpleNamespace(has_topics_enabled=True)),
            create_topic=AsyncMock(return_value={"message_thread_id": 77, "name": HOME_TITLE}),
            edit_topic=AsyncMock(return_value=True),
        ),
    )


async def test_creation_uses_durable_key_promotes_home_and_pins_one_main_welcome(case):
    key = await case.store.home_creation_key(42)
    home, created = await ensure_home(case.store, case.transport, 42, 42, "source-1")
    assert created and home["is_home"] and not home["title_auto"]
    case.transport.create_topic.assert_awaited_once_with(42, HOME_TITLE)
    assert key + ":intent" in case.store.intents
    assert key in case.store.results
    assert case.store.set_home_calls == [home["id"]]
    assert case.store.outbox == {
        f"home-welcome:{home['id']}": {
            "thread_id": 77,
            "payload": {**main_panel(), "pin": True},
        }
    }
    assert not any(key.startswith("topic-welcome:") for key in case.store.outbox)


async def test_existing_home_requeues_same_welcome_key_without_second_create_or_pin(case):
    first = await ensure_home(case.store, case.transport, 42, 42, "source-1")
    second = await ensure_home(case.store, case.transport, 42, 42, "source-2")
    assert second == (first[0], False)
    case.transport.create_topic.assert_awaited_once()
    assert len(case.store.outbox) == 1 and len(set(case.store.enqueue_keys)) == 1


async def test_enqueue_failure_after_home_promotion_recovers_without_new_topic(case):
    case.store.enqueue_error = RuntimeError("database unavailable")
    with pytest.raises(RuntimeError):
        await ensure_home(case.store, case.transport, 42, 42, "source-1")
    assert case.store.home is not None
    case.store.enqueue_error = None
    home, created = await ensure_home(case.store, case.transport, 42, 42, "source-1")
    assert not created and home["is_home"]
    case.transport.create_topic.assert_awaited_once()
    assert len(case.store.outbox) == 1


async def test_unknown_remote_create_is_not_retried_under_new_source_without_explicit_reset(case):
    case.transport.create_topic.side_effect = TimeoutError()
    for source in ("source-1", "source-2"):
        with pytest.raises(HomeUnavailable):
            await ensure_home(case.store, case.transport, 42, 42, source)
    case.transport.create_topic.assert_awaited_once()
    assert not case.store.outbox and case.store.home is None
    assert case.store.resets == {}
    previous = case.store.generation
    key = await case.store.reset_home(42, source_key="explicit-button-event")
    assert await case.store.reset_home(42, source_key="explicit-button-event") == key
    assert case.store.generation != previous and len(case.store.resets) == 1
    case.transport.create_topic.side_effect = None
    home, created = await ensure_home(case.store, case.transport, 42, 42, "source-3")
    assert created and home["is_home"]
    assert case.transport.create_topic.await_count == 2


async def test_missing_verified_home_rotates_with_stable_marker_and_retains_old_history(case):
    old, _ = await ensure_home(case.store, case.transport, 42, 42, "source-1")
    old_generation = case.store.generation
    case.transport.edit_topic.side_effect = TelegramBadRequest(
        method=EditForumTopic(chat_id=42, message_thread_id=77, name=HOME_TITLE),
        message="Bad Request: TOPIC_NOT_FOUND",
    )
    case.transport.create_topic.return_value = {"message_thread_id": 88, "name": HOME_TITLE}
    home, created = await ensure_home(
        case.store, case.transport, 42, 42, "verify-event", verify=True
    )
    assert created and home["thread_id"] == 88
    assert old["is_home"] is False and 77 in case.store.chats
    assert case.store.generation != old_generation
    assert list(case.store.resets) == ["home-missing:verify-event"]


async def test_unrelated_verification_error_never_rotates_or_creates_home(case):
    await ensure_home(case.store, case.transport, 42, 42, "source-1")
    case.transport.edit_topic.side_effect = TelegramBadRequest(
        method=EditForumTopic(chat_id=42, message_thread_id=77, name=HOME_TITLE),
        message="Bad Request: CHAT_ADMIN_REQUIRED",
    )
    with pytest.raises(TelegramBadRequest):
        await ensure_home(case.store, case.transport, 42, 42, "source-2", verify=True)
    assert case.store.resets == {}
    case.transport.create_topic.assert_awaited_once()


async def test_disabled_topics_return_general_without_fake_home_or_welcome_pin(case):
    case.transport.topic_capabilities.return_value = SimpleNamespace(has_topics_enabled=False)
    home, created = await ensure_home(case.store, case.transport, 42, 42, "source-1")
    assert not created and home["thread_id"] == 0 and not home["is_home"]
    assert case.store.home is None and not case.store.outbox
    case.transport.create_topic.assert_not_awaited()
