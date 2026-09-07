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
from uuid import UUID

import asyncpg
import pytest

from cronos.action_confirmation import UNCONFIRMED_SCHEDULE_TEXT
from cronos.agent import SCHEDULED_TOOL_NAMES, Agent, available_tools
from cronos.capabilities import TOOLS
from cronos.providers import ProviderError
from cronos.settings import Settings
from cronos.storage import Store


def test_explicit_scheduled_tools_enable_report_work_but_not_account_mutations():
    names = {tool["function"]["name"] for tool in available_tools(scheduled=True)}
    assert names == SCHEDULED_TOOL_NAMES
    assert {"web_search", "file_create", "deep_reason", "image_analyze"} <= names
    assert not names & {"schedule_create", "preferences_set", "plan_change", "topic_create"}
    assert available_tools(scheduled=True, proactive=True) is None
    assert available_tools(proactive=True) is None
    assert available_tools() is TOOLS


@pytest.mark.parametrize(
    "name",
    [
        "schedule_create",
        "schedule_change",
        "preferences_set",
        "memory_write",
        "memory_forget",
        "topic_create",
        "plan_change",
        "top_up",
        "upgrade",
    ],
)
async def test_scheduled_execution_rejects_unadvertised_mutations_before_any_side_effect(name):
    agent = Agent.__new__(Agent)
    # No store/provider attributes: a denied call must fail before touching them.
    with pytest.raises(ValueError, match="недоступен"):
        await agent.execute(name, {}, "op", {}, {}, scheduled=True)


async def test_initiative_execution_rejects_even_read_tools():
    agent = Agent.__new__(Agent)
    with pytest.raises(ValueError, match="недоступен"):
        await agent.execute("web_search", {"query": "prices"}, "op", {}, {}, proactive=True)


async def test_scheduled_search_uses_normal_run_budget():
    agent = Agent.__new__(Agent)
    agent.settings = Settings(
        database_url="postgresql://unused", max_run_cost_rub=25, proactive_max_cost_rub=1
    )

    @asynccontextmanager
    async def connection(user_id):
        yield SimpleNamespace(fetchval=AsyncMock(return_value=0))

    agent.store = SimpleNamespace(
        connection=connection,
        run_active=AsyncMock(return_value=True),
        reserve=AsyncMock(),
        record_usage=AsyncMock(),
    )
    response = {"text": "Fresh prices", "usage": {"cost_rub": "0.25"}}
    agent.provider = SimpleNamespace(search=AsyncMock(return_value=response))
    result = await agent.execute(
        "web_search",
        {"query": "prices"},
        "op",
        {"id": "run", "user_id": 1, "fence": 1},
        {},
        scheduled=True,
    )
    assert result == response
    agent.store.reserve.assert_awaited_once_with(1, "op:usage", 25_000_000)


def response(content=None, *, tool=None, args=None):
    message = {"role": "assistant", "content": content}
    if tool:
        message["tool_calls"] = [
            {
                "id": f"call-{tool}",
                "type": "function",
                "function": {"name": tool, "arguments": json.dumps(args)},
            }
        ]
    return {"message": message, "usage": {"cost_rub": "0.001", "model": "fake/test"}}


@pytest.fixture
async def scheduled_graph_case(tmp_path):
    if not os.environ.get("DATABASE_URL") or not os.environ.get("ADMIN_DATABASE_URL"):
        pytest.skip("Real PostgreSQL URLs required")
    user_id = 922000000000021
    settings = Settings(artifacts_dir=str(tmp_path))
    for dsn in (settings.database_url, settings.admin_database_url):
        parsed = urlsplit(dsn.get_secret_value())
        if (
            parsed.hostname not in {"localhost", "127.0.0.1", "::1"}
            or parsed.path != "/cronos_test"
        ):
            pytest.skip("Positive synthetic artifact users are restricted to local cronos_test DB")
    admin = await asyncpg.connect(settings.admin_database_url.get_secret_value(), timeout=10)
    store = Store(settings)
    await store.open()
    created_user = False
    try:
        if await admin.fetchval("SELECT 1 FROM users WHERE user_id=$1", user_id):
            pytest.fail("Synthetic scheduled test user already exists; do not erase another run")
        await store.ensure_user(user_id)
        created_user = True
        conversation = await store.conversation(user_id, user_id, 77, "Prices test")
        scheduled = await store.schedule(
            user_id,
            conversation,
            datetime.now(UTC) - timedelta(minutes=1),
            "Не удалось подготовить свежий отчёт",
            instruction="Свежие цены на огурцы, CSV с источниками и датой проверки",
            dynamic=True,
            proactive=False,
            interval_seconds=86400,
            source_key="test:scheduled-agent-report",
        )
        assert await store.schedule_due(user_id) == 1
        occurrence = await admin.fetchrow(
            "SELECT * FROM occurrences WHERE schedule_id=$1", UUID(scheduled["id"])
        )
        run = await store.start_run(occurrence["event_id"], user_id, conversation["id"])
        provider = SimpleNamespace(complete=AsyncMock(), search=AsyncMock())
        transport = SimpleNamespace(draft=AsyncMock())
        agent = Agent(settings, store, provider, transport)
        yield SimpleNamespace(
            agent=agent,
            store=store,
            admin=admin,
            run=run,
            conversation=conversation,
            schedule=scheduled,
            occurrence=occurrence,
            provider=provider,
            transport=transport,
            user_id=user_id,
        )
    finally:
        try:
            if created_user:
                run_ids = await admin.fetch("SELECT id FROM runs WHERE user_id=$1", user_id)
                event_ids = await admin.fetch(
                    "SELECT o.event_id FROM occurrences o JOIN schedules s ON s.id=o.schedule_id WHERE s.user_id=$1",
                    user_id,
                )
                if run_ids:
                    threads = [str(row["id"]) for row in run_ids]
                    for table in ("checkpoint_writes", "checkpoint_blobs", "checkpoints"):
                        await admin.execute(
                            f"DELETE FROM langgraph.{table} WHERE thread_id=ANY($1::text[])",
                            threads,
                        )
                await admin.execute(
                    "DELETE FROM occurrences WHERE schedule_id IN (SELECT id FROM schedules WHERE user_id=$1)",
                    user_id,
                )
                for table in (
                    "outbox",
                    "schedules",
                    "messages",
                    "memory",
                    "operations",
                    "reservations",
                    "usage",
                    "ledger",
                    "artifacts",
                    "run_metrics",
                    "runs",
                    "conversations",
                    "users",
                ):
                    await admin.execute(f"DELETE FROM {table} WHERE user_id=$1", user_id)
                if event_ids:
                    await admin.execute(
                        "DELETE FROM events WHERE id=ANY($1::uuid[])",
                        [row["event_id"] for row in event_ids],
                    )
        finally:
            await store.close()
            await admin.close()


async def test_real_scheduled_graph_searches_creates_guarded_csv_and_replays(scheduled_graph_case):
    case = scheduled_graph_case
    source = "https://example.org/prices"
    case.provider.search.return_value = {
        "text": f"Тестовые свежие цены: огурцы 150 руб/кг. Источник: {source}",
        "usage": {"cost_rub": "0.001", "model": "fake/search"},
    }
    case.provider.complete.side_effect = [
        response(tool="web_search", args={"query": "Свежие цены огурцов сегодня руб/кг"}),
        response(
            tool="file_create",
            args={
                "format": "csv",
                "filename": "цены.csv",
                "content": "",
                "columns": ["Товар", "Цена руб/кг", "Источник"],
                "rows": [["Огурцы", 150, source]],
            },
        ),
        response("Свежая таблица подготовлена и отправляется."),
    ]
    answer = await case.agent.run(
        case.run, case.conversation, "Подготовь сегодняшний отчёт", scheduled=True
    )
    assert answer == "Свежая таблица подготовлена и отправляется."
    case.provider.search.assert_awaited_once()
    case.transport.draft.assert_not_awaited()
    for call in case.provider.complete.call_args_list:
        assert "on_delta" not in call.kwargs
        names = {tool["function"]["name"] for tool in call.kwargs["tools"]}
        assert "web_search" in names and "file_create" in names
        assert "schedule_create" not in names
    system = case.provider.complete.call_args_list[0].args[0][0]["content"]
    assert case.schedule["id"] in system
    receipt = await case.admin.fetchval(
        "SELECT result FROM operations WHERE user_id=$1 AND kind='file_create'", case.user_id
    )
    if isinstance(receipt, str):
        receipt = json.loads(receipt)
    assert isinstance(receipt, dict) and receipt.get("delivery") == "queued", receipt
    assert receipt.get("artifact_id"), receipt
    rows = await case.admin.fetch("SELECT * FROM outbox WHERE user_id=$1", case.user_id)
    assert len(rows) == 1
    payload = rows[0]["payload"]
    if isinstance(payload, str):
        payload = json.loads(payload)
    assert payload["schedule_id"] == case.schedule["id"]
    assert payload["schedule_revision"] == case.occurrence["revision"]
    assert rows[0]["thread_id"] == 77
    csv_text = await asyncio.to_thread(Path(payload["document_path"]).read_text, "utf-8-sig")
    csv_rows = list(csv.reader(io.StringIO(csv_text)))
    assert csv_rows == [["Товар", "Цена руб/кг", "Источник"], ["Огурцы", "150", source]]
    await case.store.finish_run(case.run["id"], "done", fence=case.run["fence"])
    replay_run = await case.store.start_run(
        case.run["event_id"], case.user_id, case.conversation["id"]
    )
    assert (
        await case.agent.run(
            replay_run, case.conversation, "Подготовь сегодняшний отчёт", scheduled=True
        )
        == answer
    )
    assert case.provider.complete.await_count == 3
    assert case.provider.search.await_count == 1
    assert (
        await case.admin.fetchval("SELECT count(*) FROM outbox WHERE user_id=$1", case.user_id) == 1
    )


async def test_real_scheduled_search_failure_retries_tool_instead_of_caching_failure(
    scheduled_graph_case,
):
    case = scheduled_graph_case
    case.provider.complete.side_effect = [
        response(tool="web_search", args={"query": "Свежие цены огурцов"}),
        response("Данные получены с источниками."),
    ]
    case.provider.search.side_effect = ProviderError(
        "Temporary search failure", usage={"cost_rub": "0"}
    )
    with pytest.raises(ProviderError):
        await case.agent.run(case.run, case.conversation, "Свежие цены", scheduled=True)
    assert (
        await case.admin.fetchval(
            "SELECT count(*) FROM operations WHERE user_id=$1 AND kind='web_search'", case.user_id
        )
        == 0
    )
    await case.store.finish_run(case.run["id"], "failed", fence=case.run["fence"])
    replay_run = await case.store.start_run(
        case.run["event_id"], case.user_id, case.conversation["id"]
    )
    case.provider.search.side_effect = None
    case.provider.search.return_value = {
        "text": "Проверенные тестовые данные",
        "usage": {"cost_rub": "0.001"},
    }
    assert (
        await case.agent.run(replay_run, case.conversation, "Свежие цены", scheduled=True)
        == "Данные получены с источниками."
    )
    assert case.provider.complete.await_count == 2
    assert case.provider.search.await_count == 2


async def test_real_graph_unbacked_schedule_claim_has_one_repair_then_safe_fallback(
    scheduled_graph_case,
):
    case = scheduled_graph_case
    claim = "Настроил ежедневное уведомление о ценах на огурцы. Буду присылать каждое утро."
    case.provider.complete.side_effect = [response(claim), response(claim)]
    answer = await case.agent.run(case.run, case.conversation, "Настрой ежедневный отчёт")
    assert answer == UNCONFIRMED_SCHEDULE_TEXT
    assert case.provider.complete.await_count == 2
    assert (
        await case.admin.fetchval(
            "SELECT count(*) FROM operations WHERE user_id=$1 AND kind='schedule_create'",
            case.user_id,
        )
        == 0
    )
    await case.store.finish_run(case.run["id"], "done", fence=case.run["fence"])
    replay_run = await case.store.start_run(
        case.run["event_id"], case.user_id, case.conversation["id"]
    )
    assert (
        await case.agent.run(replay_run, case.conversation, "Настрой ежедневный отчёт")
        == UNCONFIRMED_SCHEDULE_TEXT
    )
    assert case.provider.complete.await_count == 2


async def test_real_graph_repair_creates_schedule_before_accepting_confirmation(
    scheduled_graph_case,
):
    case = scheduled_graph_case
    instruction = (
        "Каждый день собери свежие цены огурцов в Москве из доступных агрегаторов; "
        "создай CSV с колонками товар, цена руб/кг, источник, дата проверки."
    )
    final = "Расписание создано: ежедневно буду присылать CSV со свежими ценами на огурцы."
    case.provider.complete.side_effect = [
        response("Настроил ежедневный отчёт о ценах на огурцы. Буду присылать CSV каждое утро."),
        response(
            tool="schedule_create",
            args={
                "due_at": (datetime.now(UTC) + timedelta(days=1)).isoformat(),
                "text": "Не удалось подготовить свежий отчёт о ценах.",
                "instruction": instruction,
                "dynamic": True,
                "proactive": False,
                "interval_seconds": 86400,
            },
        ),
        response(final),
    ]
    answer = await case.agent.run(case.run, case.conversation, instruction)
    assert answer == final
    assert case.provider.complete.await_count == 3
    created = await case.admin.fetchrow(
        "SELECT * FROM schedules WHERE user_id=$1 AND source_key<>'test:scheduled-agent-report'",
        case.user_id,
    )
    assert created is not None
    assert created["dynamic"] is True
    assert created["proactive"] is False
    assert created["interval_seconds"] == 86400
    assert created["instruction"] == instruction
    assert (
        await case.admin.fetchval(
            "SELECT count(*) FROM operations WHERE user_id=$1 AND kind='schedule_create'",
            case.user_id,
        )
        == 1
    )
