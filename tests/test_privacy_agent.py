import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from langgraph.checkpoint.memory import InMemorySaver

from cronos.agent import Agent
from cronos.privacy import confirmation_text
from cronos.settings import Settings


@pytest.fixture
def agent_case(monkeypatch):
    agent = Agent.__new__(Agent)
    conversation = {"id": uuid4(), "user_id": 42, "chat_id": 42, "thread_id": 77}
    run = {"id": uuid4(), "user_id": 42, "fence": 1}
    request = {"id": uuid4(), "user_id": 42, "scope": "all", "thread_id": 77}
    monkeypatch.setattr(
        agent,
        "store",
        SimpleNamespace(requests_prepare=AsyncMock(return_value=request)),
        raising=False,
    )
    return SimpleNamespace(agent=agent, conversation=conversation, run=run, request=request)


@pytest.mark.parametrize("scope", ["all", "chat"])
async def test_privacy_tool_only_prepares_request_and_returns_confirmation_receipt(
    agent_case, scope
):
    case = agent_case
    case.request["scope"] = scope
    target = str(uuid4()) if scope == "chat" else None
    args = {"scope": scope, "conversation_id": target}
    result = await case.agent.execute(
        "privacy_request", args, "test-op", case.run, case.conversation
    )
    case.agent.store.requests_prepare.assert_awaited_once_with(
        42, case.conversation, scope, target, source_key="test-op", run_id=case.run["id"]
    )
    assert result == {
        "confirmation_required": True,
        "request_id": str(case.request["id"]),
        "message": confirmation_text(case.request),
    }
    # No provider, transport, or artifact manager exists on this object: the tool
    # must stop at preparing a request rather than invoke any deletion path.


@pytest.mark.parametrize("mode", [{"scheduled": True}, {"proactive": True}])
@pytest.mark.parametrize("scope", ["all", "chat"])
async def test_background_execution_cannot_prepare_destructive_confirmation(mode, scope):
    agent = Agent.__new__(Agent)
    with pytest.raises(ValueError, match="недоступен"):
        await agent.execute("privacy_request", {"scope": scope}, "op", {}, {}, **mode)


async def test_graph_ends_at_privacy_receipt_without_another_model_or_later_tool(
    agent_case, monkeypatch
):
    case = agent_case
    agent = case.agent
    saver = InMemorySaver()

    @asynccontextmanager
    async def connection():
        yield SimpleNamespace(execute=AsyncMock())

    monkeypatch.setattr(
        "cronos.agent.psycopg.AsyncConnection.connect", AsyncMock(return_value=connection())
    )
    monkeypatch.setattr("cronos.agent.FencedSaver", lambda conn, run: saver)
    agent.settings = Settings(database_url="postgresql://unused")
    agent.transport = None
    agent.active = AsyncMock()
    for name, value in {
        "preferences": {},
        "ensure_user": {"plan": "FREE"},
        "memories": [],
        "history": [],
        "list_artifacts": [],
        "list_schedules": [],
        "operation": None,
    }.items():
        setattr(agent.store, name, AsyncMock(return_value=value))
    agent.store.save_operation = AsyncMock()
    response = {
        "message": {
            "role": "assistant",
            "content": "Все данные уже удалены!",
            "tool_calls": [
                {
                    "id": "privacy",
                    "type": "function",
                    "function": {
                        "name": "privacy_request",
                        "arguments": json.dumps({"scope": "all"}),
                    },
                },
                {
                    "id": "later",
                    "type": "function",
                    "function": {
                        "name": "memory_forget",
                        "arguments": json.dumps({"query": "all"}),
                    },
                },
            ],
        }
    }
    agent.provider = SimpleNamespace(
        complete=AsyncMock(side_effect=[response, AssertionError("extra model call")])
    )

    async def paid(run, op, function, **kwargs):
        return await function()

    agent.paid = paid
    agent.execute = AsyncMock(wraps=agent.execute)
    answer = await agent.run(case.run, case.conversation, "Удали всё")
    assert answer == confirmation_text(case.request)
    assert "Все данные уже удалены!" not in answer
    agent.provider.complete.assert_awaited_once()
    agent.execute.assert_awaited_once()
    assert agent.execute.call_args.args[0] == "privacy_request"
    agent.store.requests_prepare.assert_awaited_once()
    tool_receipts = [
        call.args
        for call in agent.store.save_operation.call_args_list
        if call.args[3] == "privacy_request"
    ]
    assert len(tool_receipts) == 1
    assert tool_receipts[0][4]["confirmation_required"] is True
