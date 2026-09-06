from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from aiogram.exceptions import TelegramBadRequest, TelegramNetworkError
from aiogram.methods import CreateForumTopic

from cronos.agent import Agent
from cronos.topics import create_chat


class MemoryStore:
    def __init__(self):
        self.results = {}
        self.intents = set()
        self.conversation = AsyncMock(return_value={"id": "conversation-77"})
        self.enqueue = AsyncMock()

    async def operation(self, operation_id):
        return self.results.get(operation_id)

    @asynccontextmanager
    async def connection(self):
        yield self

    async def fetchval(self, query, operation_id, user_id):
        if operation_id in self.intents:
            return None
        self.intents.add(operation_id)
        return operation_id

    async def save_operation(self, operation_id, user_id, run_id, kind, result):
        self.results.setdefault(operation_id, result)


@pytest.fixture
def case():
    return SimpleNamespace(
        store=MemoryStore(),
        transport=SimpleNamespace(
            topic_capabilities=AsyncMock(return_value=SimpleNamespace(has_topics_enabled=True)),
            create_topic=AsyncMock(return_value={"message_thread_id": 77, "name": "Работа"}),
        ),
    )


@pytest.mark.parametrize(
    "error",
    [
        TimeoutError(),
        TelegramNetworkError(
            method=CreateForumTopic(chat_id=1, name="Работа"), message="Response lost"
        ),
    ],
)
@pytest.mark.parametrize("names", [("  Работа  ", "работа"), (" \n ", "Новый чат")])
async def test_ambiguous_create_cannot_repeat_under_a_new_tool_call_id(case, error, names):
    case.transport.create_topic.side_effect = error
    first = await create_chat(
        case.store, case.transport, 1, 1, "tool:1", names[0], creation_scope="run:1"
    )
    case.transport.create_topic.side_effect = None
    second = await create_chat(
        case.store, case.transport, 1, 1, "tool:2", names[1], creation_scope="run:1"
    )
    assert first == second
    assert second["status"] == "unknown"
    assert second["may_have_completed"] is True
    assert second["automatic_retry_allowed"] is False
    case.transport.create_topic.assert_awaited_once()
    case.store.conversation.assert_not_awaited()
    case.store.enqueue.assert_not_awaited()


async def test_success_replays_existing_topic_but_other_names_and_user_requests_work(case):
    first = await create_chat(
        case.store, case.transport, 1, 1, "tool:1", "Работа", creation_scope="run:1"
    )
    assert (
        await create_chat(
            case.store, case.transport, 1, 1, "tool:2", "работа", creation_scope="run:1"
        )
        == first
    )
    case.transport.create_topic.assert_awaited_once()
    await create_chat(case.store, case.transport, 1, 1, "tool:3", "Учёба", creation_scope="run:1")
    await create_chat(case.store, case.transport, 1, 1, "tool:4", "Работа", creation_scope="run:2")
    assert case.transport.create_topic.await_count == 3


async def test_new_button_event_can_retry_after_unknown_result(case):
    case.transport.create_topic.side_effect = TimeoutError()
    first = await create_chat(case.store, case.transport, 1, 1, "button:1")
    assert first["status"] == "unknown"
    case.transport.create_topic.side_effect = None
    second = await create_chat(case.store, case.transport, 1, 1, "button:2")
    assert second["thread_id"] == 77
    assert case.transport.create_topic.await_count == 2


async def test_database_failure_after_remote_success_does_not_duplicate_topic(case):
    save = case.store.save_operation
    case.store.save_operation = AsyncMock(side_effect=RuntimeError("database unavailable"))
    with pytest.raises(RuntimeError):
        await create_chat(
            case.store, case.transport, 1, 1, "tool:1", "Работа", creation_scope="run:1"
        )
    case.store.save_operation = save
    result = await create_chat(
        case.store, case.transport, 1, 1, "tool:2", "Работа", creation_scope="run:1"
    )
    assert result["status"] == "unknown"
    case.transport.create_topic.assert_awaited_once()


async def test_definitive_rejection_stays_distinct_from_unknown_on_replay(case):
    case.transport.create_topic.side_effect = TelegramBadRequest(
        method=CreateForumTopic(chat_id=1, name="Работа"), message="Topics disabled"
    )
    first = await create_chat(
        case.store, case.transport, 1, 1, "tool:1", "Работа", creation_scope="run:1"
    )
    second = await create_chat(
        case.store, case.transport, 1, 1, "tool:2", "Работа", creation_scope="run:1"
    )
    assert first == second
    assert second["status"] == "rejected"
    assert second["may_have_completed"] is False
    case.transport.create_topic.assert_awaited_once()


async def test_agent_topic_tool_supplies_run_scope(monkeypatch, case):
    create = AsyncMock(return_value={"thread_id": 77})
    monkeypatch.setattr("cronos.agent.create_chat", create)
    agent = Agent.__new__(Agent)
    agent.store, agent.transport = case.store, case.transport
    run = {"id": uuid4(), "user_id": 1}
    result = await agent.execute("topic_create", {"name": "Работа"}, "tool:1", run, {"chat_id": 1})
    assert result["thread_id"] == 77
    create.assert_awaited_once_with(
        case.store,
        case.transport,
        1,
        1,
        "tool:1:topic",
        "Работа",
        creation_scope=f"run:{run['id']}",
    )
