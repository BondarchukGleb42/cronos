import asyncio
import csv
import io
import json
import os
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
from urllib.parse import urlsplit
from uuid import UUID, uuid4

import asyncpg
import pytest

from cronos.action_confirmation import needs_schedule_repair
from cronos.agent import Agent, available_tools, schedule_tool_results
from cronos.initiative import InitiativeStoreMixin
from cronos.initiative_tools import INITIATIVE_DECISION_TOOLS
from cronos.settings import Settings
from cronos.storage import Store


class InitiativeStore(Store, InitiativeStoreMixin):
    pass


def reply(text=None, *, tool=None, args=None):
    message = {"role": "assistant", "content": text}
    if tool:
        message["tool_calls"] = [
            {
                "id": f"call-{tool}",
                "type": "function",
                "function": {"name": tool, "arguments": json.dumps(args)},
            }
        ]
    return {"message": message, "usage": {"cost_rub": "0.001", "model": "fake/initiative"}}


def test_prepared_policy_only_exposes_approved_tools_and_decisions():
    policy = {"available": True, "allowed_tools": ["web_search", "file_create", "upgrade"]}
    tools = available_tools(proactive=True, scheduled=True, initiative=policy)
    names = {tool["function"]["name"] for tool in tools}
    assert names == {"web_search", "file_create"} | INITIATIVE_DECISION_TOOLS
    assert available_tools(proactive=True) is None


def test_initiative_creation_claim_requires_actual_configuration_receipt():
    claim = "Настроил инициативу по проекту."
    assert needs_schedule_repair(claim, []) is True
    message = reply(tool="initiative_configure", args={})["message"]
    receipt = {
        "role": "tool",
        "tool_call_id": "call-initiative_configure",
        "content": json.dumps(
            {
                "id": str(uuid4()),
                "schedule_id": str(uuid4()),
                "available": True,
                "due_at": "2026-09-08T10:00:00+03:00",
                "dynamic": True,
                "proactive": True,
            }
        ),
    }
    assert needs_schedule_repair(claim, schedule_tool_results([message, receipt])) is False
    receipt["content"] = json.dumps({"error": "User agreement is required"})
    assert needs_schedule_repair(claim, schedule_tool_results([message, receipt])) is True


async def test_prepared_search_keeps_one_ruble_budget_for_the_whole_run():
    agent = Agent.__new__(Agent)
    agent.settings = Settings(max_run_cost_rub=25, proactive_max_cost_rub=1)
    policy = {
        "id": str(uuid4()),
        "schedule_id": str(uuid4()),
        "project_id": str(uuid4()),
        "revision": 1,
        "project_revision": 1,
        "available": True,
        "allowed_tools": ["web_search"],
    }

    @asynccontextmanager
    async def connection(owner):
        yield SimpleNamespace(fetchval=AsyncMock(side_effect=[250_000, 100_000]))

    agent.store = SimpleNamespace(
        connection=connection,
        run_active=AsyncMock(return_value=True),
        get_initiative_for_schedule=AsyncMock(return_value=policy),
        reserve=AsyncMock(),
        record_usage=AsyncMock(),
    )
    agent.provider = SimpleNamespace(
        search=AsyncMock(return_value={"text": "source", "usage": {"cost_rub": "0.001"}})
    )
    await agent.execute(
        "web_search",
        {"query": "prices"},
        "op",
        {"id": uuid4(), "user_id": 1, "fence": 1},
        {},
        proactive=True,
        scheduled=True,
        initiative=policy,
    )
    agent.store.reserve.assert_awaited_once_with(1, "op:usage", 650_000)


@pytest.fixture
async def graph(tmp_path):
    if not os.getenv("DATABASE_URL") or not os.getenv("ADMIN_DATABASE_URL"):
        pytest.skip("Real local PostgreSQL URLs required")
    owner = 987000000000071
    settings = Settings(artifacts_dir=str(tmp_path))
    for dsn in (settings.database_url, settings.admin_database_url):
        parsed = urlsplit(dsn.get_secret_value())
        assert parsed.hostname in {"localhost", "127.0.0.1"} and parsed.path == "/cronos_test"
    admin = await asyncpg.connect(settings.admin_database_url.get_secret_value())
    store = InitiativeStore(settings)
    await store.open()
    created = False
    try:
        assert not await admin.fetchval("SELECT 1 FROM users WHERE user_id=$1", owner), (
            "Synthetic initiative graph owner already exists"
        )
        await store.ensure_user(owner)
        created = True
        conversation = await store.conversation(owner, owner, 730)
        project = await store.create_project(
            owner,
            conversation["id"],
            {"name": "Проверка цен", "goal": "Следить за полезными скидками"},
            "initiative-graph-project",
        )
        await store.preferences(owner, {"proactivity": True})
        event_id = uuid4()
        await admin.execute(
            "INSERT INTO events(id,kind,payload) VALUES($1,'telegram',$2::jsonb)",
            event_id,
            json.dumps({"user_id": owner}),
        )
        configure_run = await store.start_run(event_id, owner, conversation["id"])
        policy = await store.configure_initiative(
            owner,
            project["id"],
            {
                "purpose": "Полезное сравнение цен",
                "instruction": "Найди скидку на огурцы, подготовь CSV со ссылкой, отправляй при изменениях",
                "allowed_tools": ["web_search", "file_create"],
                "due_at": datetime.now(UTC) - timedelta(minutes=1),
                "interval_seconds": 86400,
            },
            "initiative-graph-configure",
            configure_run,
        )
        assert await store.schedule_due(owner) == 1
        occurrence = await admin.fetchrow(
            "SELECT * FROM occurrences WHERE schedule_id=$1", UUID(policy["schedule_id"])
        )
        run = await store.start_run(occurrence["event_id"], owner, conversation["id"])
        provider = SimpleNamespace(
            complete=AsyncMock(),
            search=AsyncMock(
                return_value={
                    "text": "Огурцы 130 руб/кг; https://example.org/price",
                    "usage": {"cost_rub": "0.001"},
                }
            ),
        )
        agent = Agent(settings, store, provider, SimpleNamespace(draft=AsyncMock()))
        yield SimpleNamespace(
            agent=agent,
            store=store,
            admin=admin,
            owner=owner,
            conversation=conversation,
            run=run,
            provider=provider,
            policy=policy,
        )
    finally:
        if created:
            run_ids = [
                str(row["id"])
                for row in await admin.fetch("SELECT id FROM runs WHERE user_id=$1", owner)
            ]
            events = [
                row["event_id"]
                for row in await admin.fetch("SELECT event_id FROM runs WHERE user_id=$1", owner)
            ]
            for table in ("checkpoint_writes", "checkpoint_blobs", "checkpoints"):
                await admin.execute(
                    f"DELETE FROM langgraph.{table} WHERE thread_id=ANY($1::text[])", run_ids
                )
            await admin.execute(
                "DELETE FROM occurrences WHERE schedule_id IN (SELECT id FROM schedules WHERE user_id=$1)",
                owner,
            )
            for table in (
                "outbox",
                "artifacts",
                "operations",
                "usage",
                "reservations",
                "ledger",
                "run_metrics",
                "runs",
                "projects",
                "schedules",
                "messages",
                "memory",
                "conversations",
                "users",
            ):
                await admin.execute(f"DELETE FROM {table} WHERE user_id=$1", owner)
            await admin.execute(
                "DELETE FROM events WHERE id=ANY($1::uuid[]) OR payload->>'user_id'=$2",
                events,
                str(owner),
            )
        await store.close()
        await admin.close()


async def test_real_prepared_graph_searches_makes_csv_decides_and_replays_without_delivery(graph):
    case = graph
    case.provider.complete.side_effect = [
        reply(tool="web_search", args={"query": "Свежие цены огурцов"}),
        reply(
            tool="file_create",
            args={
                "format": "csv",
                "filename": "prices.csv",
                "content": "",
                "columns": ["Продукт", "Цена", "Источник"],
                "rows": [["Огурцы", 130, "https://example.org/price"]],
            },
        ),
        reply(
            tool="initiative_decide",
            args={
                "summary": "Подтверждена полезная скидка",
                "evidence_key": "Огурцы Москва 130 руб/кг https://example.org/price",
                "send": True,
            },
        ),
        reply("Нашёл полезную скидку. Подготовил сравнение в CSV."),
    ]
    result = await case.agent.run(
        case.run,
        case.conversation,
        "Подготовь согласованную инициативу",
        proactive=True,
        scheduled=True,
        initiative=case.policy,
    )
    assert result["initiative_decision"]["should_send"] is True
    assert len(result["prepared_artifact_ids"]) == 1
    artifact = await case.store.get_artifact(case.owner, result["prepared_artifact_ids"][0])
    text = await asyncio.to_thread(Path(artifact["path"]).read_text, encoding="utf-8-sig")
    rows = list(csv.reader(io.StringIO(text)))
    assert rows == [["Продукт", "Цена", "Источник"], ["Огурцы", "130", "https://example.org/price"]]
    assert (
        await case.admin.fetchval("SELECT count(*) FROM outbox WHERE user_id=$1", case.owner) == 0
    )
    assert await case.store.validate_initiative_delivery(
        case.owner, result["initiative_decision"]["delivery_guard"]
    )
    replay = await case.agent.run(
        case.run,
        case.conversation,
        "Подготовь согласованную инициативу",
        proactive=True,
        scheduled=True,
        initiative=case.policy,
    )
    assert replay == result
    assert case.provider.complete.await_count == 4 and case.provider.search.await_count == 1


async def test_real_prepared_graph_without_decision_cannot_authorize_a_message(graph):
    graph.provider.complete.side_effect = [reply("Полезная инициатива без решения.")]
    result = await graph.agent.run(
        graph.run,
        graph.conversation,
        "Проверь проект",
        proactive=True,
        scheduled=True,
        initiative=graph.policy,
    )
    assert result["initiative_decision"]["should_send"] is False
    assert (
        await graph.admin.fetchval("SELECT count(*) FROM outbox WHERE user_id=$1", graph.owner) == 0
    )
