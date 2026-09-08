import json
import os
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import asyncpg
import pytest

from cronos.agent import Agent, CancelledRun, analyze_table
from cronos.capabilities import SKILLS, TOOLS, catalog_context
from cronos.model_preferences import DIALOGUE_MODELS
from cronos.settings import Settings
from cronos.storage import Store


def test_table_arithmetic_uses_decimal_and_reports_invalid_rows():
    extracted = {
        "tables": [
            {
                "columns": ["Сумма"],
                "rows": [
                    ["1 200,50"],
                    ["3.25"],
                    [None],
                    ["не число"],
                    ["NaN"],
                    ["Infinity"],
                ],
            }
        ]
    }
    answer = analyze_table(extracted, {"column": "Сумма", "operation": "sum"})
    assert answer["value"] == "1203.75"
    assert answer["numeric_rows"] == 2
    assert answer["skipped_rows"] == 4


@pytest.mark.parametrize(
    ("operation", "expected"), [("mean", "3"), ("min", "1"), ("max", "5"), ("count", "2")]
)
def test_table_dictionary_rows(operation, expected):
    extracted = {
        "tables": [
            {
                "columns": ["amount"],
                "rows": [
                    {"amount": "1"},
                    {"amount": "5"},
                    {"amount": "missing"},
                ],
            }
        ]
    }
    assert (
        analyze_table(extracted, {"column": "amount", "operation": operation})["value"] == expected
    )


def test_table_without_extracted_tables_reports_user_error():
    with pytest.raises(ValueError):
        analyze_table({"tables": []}, {"column": "0", "operation": "sum"})


async def test_advertised_skill_names_are_discoverable_and_unknown_tools_are_denied(tmp_path):
    agent = Agent(
        Settings(database_url="postgresql://unused", artifacts_dir=str(tmp_path)),
        SimpleNamespace(),
        SimpleNamespace(),
        SimpleNamespace(),
    )
    discovery = next(tool for tool in TOOLS if tool["function"]["name"] == "skill_info")
    advertised = discovery["function"]["parameters"]["properties"]["skill"]["enum"]
    assert set(advertised) == set(SKILLS)
    assert len({tool["function"]["name"] for tool in TOOLS}) == len(TOOLS)
    for skill in advertised:
        result = await agent.execute(
            "skill_info", {"skill": skill}, "unused", {"user_id": -920001}, {}
        )
        assert result["summary"] in catalog_context()
        assert result["instructions"]
    with pytest.raises(ValueError):
        await agent.execute(
            "run_arbitrary_shell", {"command": "true"}, "unused", {"user_id": -920001}, {}
        )


def model_response(content=None, *, tool=None, args=None):
    message = {"role": "assistant", "content": content}
    if tool:
        message["tool_calls"] = [
            {
                "id": "call-test",
                "type": "function",
                "function": {
                    "name": tool,
                    "arguments": json.dumps(args),
                },
            }
        ]
    return {
        "message": message,
        "usage": {
            "cost_rub": "0.001",
            "model": "fake/test",
            "prompt_tokens": 10,
            "completion_tokens": 5,
        },
    }


@pytest.fixture
async def graph_case(request, tmp_path):
    """Opt-in real database test; all writes are confined to named synthetic users."""
    if not os.environ.get("DATABASE_URL") or not os.environ.get("ADMIN_DATABASE_URL"):
        pytest.skip("Real PostgreSQL URLs required; use scripts/run_local.py")
    user_id = request.param
    assert -920005 <= user_id <= -920001
    settings = Settings(artifacts_dir=str(tmp_path))
    admin = await asyncpg.connect(settings.admin_database_url.get_secret_value(), timeout=10)
    store = Store(settings)
    await store.open()
    event_id = uuid4()
    created_user = False
    try:
        # Do not delete any pre-existing rows: a parallel test must have its own ID.
        if await admin.fetchval("SELECT 1 FROM users WHERE user_id=$1", user_id):
            pytest.fail(
                "Synthetic test user already exists; inspect its unfinished test before rerun"
            )
        await store.ensure_user(user_id)
        created_user = True
        conversation = await store.conversation(user_id, user_id, 0, "Integration test")
        await admin.execute("INSERT INTO events(id,payload) VALUES($1,'{}')", event_id)
        run = await store.start_run(event_id, user_id, conversation["id"])
        transport = SimpleNamespace(
            draft=AsyncMock(), bot=SimpleNamespace(answer_callback_query=AsyncMock())
        )
        provider = SimpleNamespace(complete=AsyncMock(), search=AsyncMock())
        agent = Agent(settings, store, provider, transport)
        yield SimpleNamespace(
            agent=agent,
            store=store,
            admin=admin,
            run=run,
            conversation=conversation,
            provider=provider,
            user_id=user_id,
        )
    finally:
        try:
            if created_user:
                run_ids = await admin.fetch("SELECT id FROM runs WHERE user_id=$1", user_id)
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
                await admin.execute("DELETE FROM events WHERE id=$1", event_id)
        finally:
            await store.close()
            await admin.close()


@pytest.mark.parametrize("graph_case", [-920005], indirect=True)
@pytest.mark.parametrize("plan", ["FREE", "START", "PREMIUM", "PRO"])
async def test_every_plan_uses_luna_without_reasoning_by_default(graph_case, plan):
    case = graph_case
    await case.admin.execute("UPDATE users SET plan=$2 WHERE user_id=$1", case.user_id, plan)
    # Existing users may have selected an old catalogue model before the migration.
    await case.store.preferences(case.user_id, {"model": "qwen/qwen3.7-flash"})
    case.provider.complete.return_value = model_response("Готово")
    assert await case.agent.run(case.run, case.conversation, "Привет") == "Готово"
    args = case.provider.complete.await_args.kwargs
    assert args["model"] == DIALOGUE_MODELS["luna"]
    assert args["reasoning"] is False
    assert "deep_reason" not in {tool["function"]["name"] for tool in args["tools"]}


@pytest.mark.parametrize("graph_case", [-920005], indirect=True)
@pytest.mark.parametrize("model", DIALOGUE_MODELS.values())
@pytest.mark.parametrize("reasoning", [False, True])
async def test_explicit_model_and_mode_reach_the_real_graph(graph_case, model, reasoning):
    case = graph_case
    await case.store.set_model_preferences(
        case.user_id, {"model": model, "reasoning": reasoning}, str(uuid4())
    )
    case.provider.complete.return_value = model_response("Готово")
    await case.agent.run(case.run, case.conversation, "Привет")
    args = case.provider.complete.await_args.kwargs
    assert args["model"] == model
    assert args["reasoning"] is reasoning
    assert ("deep_reason" in {tool["function"]["name"] for tool in args["tools"]}) is reasoning


async def test_deep_reason_cannot_bypass_disabled_mode(tmp_path):
    store = SimpleNamespace(preferences=AsyncMock(return_value={"reasoning": False}))
    provider = SimpleNamespace(complete=AsyncMock())
    agent = Agent(Settings(artifacts_dir=str(tmp_path)), store, provider, None)
    result = await agent.execute(
        "deep_reason", {"problem": "Сложная задача"}, "test", {"user_id": -920005}, {}
    )
    assert "выключен" in result["error"]
    provider.complete.assert_not_awaited()


@pytest.mark.parametrize("model", DIALOGUE_MODELS.values())
@pytest.mark.parametrize("reasoning", [False, True])
async def test_artifact_vision_tool_preserves_model_and_reasoning(
    tmp_path, monkeypatch, model, reasoning
):
    monkeypatch.setattr("cronos.agent.image_bytes", lambda artifact, page: (b"image", "image/png"))
    store = SimpleNamespace(
        preferences=AsyncMock(return_value={"model": model, "reasoning": reasoning}),
        get_artifact=AsyncMock(return_value={}),
    )
    provider = SimpleNamespace(complete=AsyncMock(return_value=model_response("Красный")))
    agent = Agent(Settings(artifacts_dir=str(tmp_path)), store, provider, None)

    async def paid(run, op, function):
        return await function()

    agent.paid = paid
    result = await agent.execute(
        "image_analyze",
        {"artifact_id": "test", "question": "Какой цвет?"},
        "test",
        {"user_id": -920005},
        {},
    )
    assert result["text"] == "Красный"
    args = provider.complete.await_args
    assert args.kwargs == {"model": model, "reasoning": reasoning}
    assert args.args[0][0]["content"][1]["image_url"]["url"].startswith("data:image/png;base64,")


@pytest.mark.parametrize("graph_case", [-920005], indirect=True)
async def test_freeform_model_patch_replay_preserves_later_button_choice(graph_case):
    case = graph_case
    op = str(uuid4())
    await case.agent.execute(
        "preferences_set",
        {"model": "terra", "reasoning": True, "timezone": "Europe/Moscow"},
        op,
        case.run,
        case.conversation,
    )
    await case.store.set_model_preferences(
        case.user_id, {"model": "sol", "reasoning": False}, str(uuid4())
    )
    result = await case.agent.execute(
        "preferences_set", {"model": "terra", "reasoning": True}, op, case.run, case.conversation
    )
    assert result["model"] == DIALOGUE_MODELS["sol"]
    assert result["reasoning"] is False
    assert result["timezone_confirmed"] is True
    assert await case.store.operation(op) is None


@pytest.mark.parametrize("graph_case", [-920001], indirect=True)
async def test_real_graph_remembers_and_replays_without_recalling_model(graph_case):
    case = graph_case
    case.provider.complete.side_effect = [
        model_response(
            tool="memory_write",
            args={"content": "Тестовое предпочтение: чай", "category": "preference"},
        ),
        model_response("Запомнил предпочтение."),
    ]
    answer = await case.agent.run(case.run, case.conversation, "Запомни, что люблю чай")
    assert answer == "Запомнил предпочтение."
    assert case.provider.complete.await_count == 2
    assert [row["content"] for row in await case.store.memories(case.user_id)] == [
        "Тестовое предпочтение: чай"
    ]
    assert (
        await case.admin.fetchval(
            "SELECT count(*) FROM langgraph.checkpoints WHERE thread_id=$1", str(case.run["id"])
        )
        > 0
    )
    next_run = await case.store.start_run(
        case.run["event_id"], case.user_id, case.conversation["id"]
    )
    assert next_run["fence"] > case.run["fence"]
    assert await case.agent.run(next_run, case.conversation, "Запомни, что люблю чай") == answer
    assert case.provider.complete.await_count == 2
    assert (
        await case.admin.fetchval(
            "SELECT count(*) FROM ledger WHERE user_id=$1 AND kind='usage'", case.user_id
        )
        == 2
    )


@pytest.mark.parametrize("graph_case", [-920005], indirect=True)
async def test_real_graph_accepts_empty_optional_memory_ids_from_luna(graph_case):
    case = graph_case
    case.provider.complete.side_effect = [
        model_response(
            tool="memory_write",
            args={
                "content": "Любимая птица пользователя — воробей.",
                "category": "предпочтения",
                "scope": "global",
                "project_id": "",
                "expires_at": None,
                "supersedes_id": "",
            },
        ),
        model_response("Запомнил."),
    ]
    assert await case.agent.run(case.run, case.conversation, "Запомни любимую птицу") == "Запомнил."
    memories = await case.store.query_memories(case.user_id)
    assert len(memories) == 1
    assert memories[0]["content"] == "Любимая птица пользователя — воробей."
    assert memories[0]["scope"] == "global"
    assert memories[0]["project_id"] is None
    assert memories[0]["supersedes_id"] is None
    followup = case.provider.complete.await_args.args[0]
    receipt = json.loads(next(message["content"] for message in followup if message["role"] == "tool"))
    assert "error" not in receipt


@pytest.mark.parametrize("graph_case", [-920002], indirect=True)
async def test_real_graph_rejects_stale_fence_before_model_or_checkpoint_write(graph_case):
    case = graph_case
    await case.store.start_run(case.run["event_id"], case.user_id, case.conversation["id"])
    with pytest.raises(CancelledRun):
        await case.agent.run(case.run, case.conversation, "Нельзя выполнять")
    case.provider.complete.assert_not_called()
    assert (
        await case.admin.fetchval(
            "SELECT count(*) FROM langgraph.checkpoints WHERE thread_id=$1", str(case.run["id"])
        )
        == 0
    )


@pytest.mark.parametrize("graph_case", [-920003], indirect=True)
async def test_real_graph_cancel_during_model_records_cost_but_does_not_execute_tool(graph_case):
    case = graph_case

    async def cancel_then_return(*args, **kwargs):
        await case.admin.execute(
            "UPDATE runs SET cancel_requested=true WHERE id=$1", case.run["id"]
        )
        return model_response(tool="memory_write", args={"content": "must not be saved"})

    case.provider.complete.side_effect = cancel_then_return
    with pytest.raises(CancelledRun):
        await case.agent.run(case.run, case.conversation, "Отмени во время модели")
    assert await case.store.memories(case.user_id) == []
    assert (
        await case.admin.fetchval("SELECT cost_micro FROM usage WHERE user_id=$1", case.user_id)
        == 1000
    )


@pytest.mark.parametrize("graph_case", [-920004], indirect=True)
async def test_real_graph_search_tool_result_returns_to_model_with_sources(graph_case):
    case = graph_case
    case.provider.complete.side_effect = [
        model_response(tool="web_search", args={"query": "проверочный запрос"}),
        model_response("Ответ с источником https://example.com/source"),
    ]
    case.provider.search.return_value = {
        "text": "Проверенный результат",
        "sources": [{"url": "https://example.com/source", "title": "Test"}],
        "usage": {"model": "fake/search", "cost_rub": "0.002"},
    }
    answer = await case.agent.run(case.run, case.conversation, "Найди сведения")
    assert "https://example.com/source" in answer
    case.provider.search.assert_awaited_once_with("проверочный запрос")
    followup = case.provider.complete.call_args_list[1].args[0]
    tool_message = next(message for message in followup if message["role"] == "tool")
    assert tool_message["tool_call_id"] == "call-test"
    assert json.loads(tool_message["content"])["sources"][0]["url"] == "https://example.com/source"
    assert (
        await case.admin.fetchval(
            "SELECT sum(cost_micro) FROM usage WHERE user_id=$1", case.user_id
        )
        == 4000
    )
