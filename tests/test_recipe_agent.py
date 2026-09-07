"""Recipe tool and graph regressions."""

import copy
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from langgraph.checkpoint.memory import InMemorySaver

from cronos.agent import Agent
from cronos.recipe_tools import (
    RECIPE_START_TOOLS,
    UNCONFIRMED_RECIPE_TEXT,
    UNSTARTED_RECIPE_TEXT,
    execute_recipe_tool,
    explicit_recipe_start,
    incomplete_recipe_applications,
    recipe_context,
    recipe_needs_completion,
    recipe_needs_start,
    recipe_tool_allowed,
    recipe_waiting_answer,
)
from cronos.settings import Settings


def receipt_messages(tool, result, identifier="call"):
    return [
        {
            "role": "assistant",
            "content": None,
            "tool_calls": [
                {
                    "id": identifier,
                    "type": "function",
                    "function": {"name": tool, "arguments": "{}"},
                }
            ],
        },
        {"role": "tool", "tool_call_id": identifier, "content": json.dumps(result)},
    ]


@pytest.fixture
def case(tmp_path):
    user = 44006
    conversation = {"id": uuid4(), "user_id": user, "chat_id": user, "thread_id": 26}
    run = {"id": uuid4(), "user_id": user, "conversation_id": conversation["id"], "fence": 3}
    recipe = {
        "id": str(uuid4()),
        "revision": 2,
        "version": 2,
        "name": "Отчёт",
        "description": "Описание",
        "status": "active",
        "context_excluded": False,
    }
    application = {
        "id": str(uuid4()),
        "recipe_id": recipe["id"],
        "version": 2,
        "name": "Отчёт",
        "status": "ready",
        "completed": False,
        "inputs": {},
        "missing_inputs": [],
        "input_schema": [],
        "steps": [{"tool": "deep_reason", "instruction": "Разбери данные"}],
        "requirements": ["Понятный вывод"],
    }
    store = SimpleNamespace(
        recipe_save=AsyncMock(return_value=recipe),
        recipe_list=AsyncMock(return_value=[recipe]),
        recipe_get=AsyncMock(return_value=recipe),
        recipe_apply=AsyncMock(return_value=application),
        recipe_complete=AsyncMock(
            return_value={**application, "status": "completed", "completed": True}
        ),
        list_pending_recipe=AsyncMock(return_value=[]),
        enqueue=AsyncMock(),
        enqueue_for_run=AsyncMock(),
        save_artifact=AsyncMock(),
    )
    provider = SimpleNamespace(complete=AsyncMock())
    agent = Agent(
        Settings(
            database_url="postgresql://unused", artifacts_dir=str(tmp_path), max_model_steps=5
        ),
        store,
        provider,
        None,
    )
    return SimpleNamespace(
        user=user,
        conversation=conversation,
        run=run,
        recipe=recipe,
        application=application,
        store=store,
        provider=provider,
        agent=agent,
    )


@pytest.mark.parametrize(
    "name", ["recipe_save", "recipe_list", "recipe_get", "recipe_apply", "recipe_complete"]
)
async def test_dispatch_uses_runtime_owner_conversation_fence_without_execution(case, name):
    args = {
        "recipe_id": case.recipe["id"],
        "application_id": case.application["id"],
        "inputs": {"source": "data"},
    }
    original = copy.deepcopy(args)
    await execute_recipe_tool(
        case.store, name, args, "stable-operation", case.run, case.conversation
    )
    call = getattr(case.store, name).call_args
    assert call.args[0] == case.user
    if name in {"recipe_save", "recipe_apply", "recipe_complete"}:
        assert "stable-operation" in call.args and case.run in call.args
    if name == "recipe_apply":
        assert call.args == (
            case.user,
            case.recipe["id"],
            args["inputs"],
            "stable-operation",
            case.run,
            case.conversation["id"],
        )
    assert args == original
    case.provider.complete.assert_not_awaited()
    case.store.enqueue.assert_not_awaited()
    case.store.enqueue_for_run.assert_not_awaited()
    case.store.save_artifact.assert_not_awaited()


async def test_dispatch_does_not_turn_failed_completion_into_success(case):
    case.store.recipe_complete.side_effect = ValueError("Missing file_create receipt")
    with pytest.raises(ValueError, match="receipt"):
        await execute_recipe_tool(
            case.store,
            "recipe_complete",
            {"application_id": case.application["id"]},
            "complete",
            case.run,
            case.conversation,
        )


async def test_context_excludes_forgotten_recipes_and_long_input_values(case):
    case.store.recipe_list.return_value.append({"id": str(uuid4()), "context_excluded": True})
    pending = {
        **case.application,
        "status": "awaiting_input",
        "inputs": {"source": "PRIVATE DOCUMENT CONTENT"},
        "missing_inputs": ["month"],
        "input_schema": [
            {"name": "source", "type": "string"},
            {"name": "month", "type": "string", "description": "Месяц отчёта"},
        ],
    }
    case.store.list_pending_recipe.return_value = [pending, {"context_excluded": True}]
    result = await recipe_context(case.store, case.user, case.conversation["id"])
    case.store.list_pending_recipe.assert_awaited_once_with(case.user, case.conversation["id"])
    assert len(result["available"]) == len(result["awaiting_input"]) == 1
    assert result["awaiting_input"][0]["provided_input_names"] == ["source"]
    assert result["awaiting_input"][0]["input_schema"] == [pending["input_schema"][1]]
    assert "PRIVATE DOCUMENT" not in json.dumps(result)


def test_completion_guard_requires_matching_successful_current_tool_receipt(case):
    messages = receipt_messages("recipe_apply", case.application)
    assert recipe_needs_completion(messages)
    assert incomplete_recipe_applications(messages)[0]["id"] == case.application["id"]
    messages += [{"role": "assistant", "content": "Готово, сценарий полностью выполнен!"}]
    messages += receipt_messages(
        "recipe_complete", {"error": "missing receipt", "completed": False}, "failed"
    )
    messages += receipt_messages(
        "file_create",
        {**case.application, "status": "completed", "completed": True},
        "different-tool",
    )
    messages += [
        {
            "role": "tool",
            "tool_call_id": "unmatched",
            "content": json.dumps({**case.application, "status": "completed", "completed": True}),
        }
    ]
    assert recipe_needs_completion(messages)
    messages += receipt_messages(
        "recipe_complete",
        {**case.application, "status": "completed", "completed": True},
        "verified",
    )
    assert not recipe_needs_completion(messages)


@pytest.mark.parametrize(
    "broken",
    [
        None,
        [],
        "bad",
        {"error": "failure"},
        {"id": "invalid", "status": "ready"},
        {"status": "completed", "completed": True},
    ],
)
def test_malformed_receipts_cannot_confirm_or_create_completion(case, broken):
    assert not recipe_needs_completion(receipt_messages("recipe_apply", broken))
    assert recipe_needs_completion(
        receipt_messages("recipe_apply", case.application)
        + receipt_messages("recipe_complete", broken, "broken")
    )


def test_waiting_answer_requests_required_data_and_makes_no_completion_claim(case):
    result = {
        **case.application,
        "status": "awaiting_input",
        "missing_inputs": ["statement"],
        "input_schema": [
            {"name": "statement", "description": "Выписка за месяц", "type": "artifact"}
        ],
    }
    answer = recipe_waiting_answer(result)
    assert "Выписка за месяц" in answer and "Прикрепи" in answer
    assert "завершён" not in answer and "выполнен" not in answer
    assert recipe_waiting_answer(case.application) is None
    assert recipe_waiting_answer({**result, "context_excluded": True}) is None


def test_active_recipe_rejects_account_schedule_and_recursive_effects(case):
    messages = receipt_messages("recipe_apply", case.application)
    for tool in (
        "schedule_create",
        "preferences_set",
        "privacy_request",
        "plan_change",
        "recipe_apply",
        "recipe_save",
    ):
        assert not recipe_tool_allowed(messages, tool)
    for tool in (
        "file_read",
        "image_generate",
        "deep_reason",
        "workflow_observe",
        "recipe_complete",
    ):
        assert recipe_tool_allowed(messages, tool)
    assert recipe_tool_allowed([], "schedule_create")


async def graph_case(case, monkeypatch, replies):
    """Actual Agent/LangGraph/checkpoint graph; only I/O dependencies are doubles."""
    saver, operations = InMemorySaver(), {}

    @asynccontextmanager
    async def graph_connection():
        yield SimpleNamespace(execute=AsyncMock())

    @asynccontextmanager
    async def billing_connection(owner):
        assert owner == case.user
        yield SimpleNamespace(fetchval=AsyncMock(return_value=0))

    async def save_operation(op, owner, run_id, kind, result):
        assert owner == case.user and run_id == case.run["id"]
        operations.setdefault(op, copy.deepcopy(result))

    for name, value in {
        "preferences": {},
        "ensure_user": {"plan": "FREE"},
        "query_memories": [],
        "history": [],
        "conversation_recall": [],
        "list_artifacts": [],
        "list_schedules": [],
        "run_active": True,
        "get_project": None,
        "list_projects": [],
    }.items():
        setattr(case.store, name, AsyncMock(return_value=value))
    case.store.operation = AsyncMock(side_effect=lambda op: copy.deepcopy(operations.get(op)))
    case.store.save_operation = AsyncMock(side_effect=save_operation)
    case.store.connection = billing_connection
    case.store.reserve = AsyncMock()
    case.store.record_usage = AsyncMock()
    monkeypatch.setattr(
        "cronos.agent.psycopg.AsyncConnection.connect",
        AsyncMock(side_effect=lambda *args, **kwargs: graph_connection()),
    )
    monkeypatch.setattr("cronos.agent.FencedSaver", lambda conn, run: saver)
    case.provider.complete.side_effect = [
        {
            "message": reply,
            "usage": {
                "model": "test/recipes",
                "cost_rub": "0.001",
                "prompt_tokens": 10,
                "completion_tokens": 5,
            },
        }
        for reply in replies
    ]
    return saver


def calls(*tools):
    return {
        "role": "assistant",
        "content": None,
        "tool_calls": [
            {
                "id": f"call-{index}-{name}",
                "type": "function",
                "function": {"name": name, "arguments": json.dumps(args)},
            }
            for index, (name, args) in enumerate(tools)
        ],
    }


async def test_graph_waiting_inputs_stops_later_effects_and_checkpoint_replay(case, monkeypatch):
    case.store.recipe_apply.return_value = {
        **case.application,
        "status": "awaiting_input",
        "missing_inputs": ["month"],
        "input_schema": [{"name": "month", "description": "Месяц отчёта", "type": "string"}],
        "steps": [],
    }
    saver = await graph_case(
        case,
        monkeypatch,
        [
            calls(
                ("recipe_apply", {"recipe_id": case.recipe["id"]}),
                (
                    "file_create",
                    {"format": "txt", "filename": "must-not-exist.txt", "content": "not ready"},
                ),
            )
        ],
    )
    case.agent.artifacts.generate = lambda *args, **kwargs: pytest.fail(
        "Missing inputs must stop later file writes"
    )
    answer = await case.agent.run(case.run, case.conversation, "Запусти мой отчёт")
    assert "Месяц отчёта" in answer
    assert case.provider.complete.await_count == 1
    case.store.save_artifact.assert_not_awaited()
    case.store.enqueue_for_run.assert_not_awaited()
    assert (
        await case.agent.run({**case.run, "fence": 4}, case.conversation, "Запусти мой отчёт")
        == answer
    )
    assert case.store.recipe_apply.await_count == 1
    checkpoint = await saver.aget_tuple(
        {"configurable": {"thread_id": str(case.run["id"]), "checkpoint_ns": ""}}
    )
    assert checkpoint.checkpoint["channel_values"]["answer"] == answer


async def test_graph_repairs_unverified_completion_once_then_fails_closed(case, monkeypatch):
    await graph_case(
        case,
        monkeypatch,
        [
            calls(("recipe_apply", {"recipe_id": case.recipe["id"]})),
            {"role": "assistant", "content": "Готово, все шаги сценария выполнены."},
            {"role": "assistant", "content": "Сценарий успешно завершён."},
        ],
    )
    answer = await case.agent.run(case.run, case.conversation, "Выполни мой сценарий")
    assert answer == UNCONFIRMED_RECIPE_TEXT
    assert case.provider.complete.await_count == 3
    case.store.recipe_complete.assert_not_awaited()
    case.store.enqueue_for_run.assert_not_awaited()


async def test_graph_accepts_actual_completed_receipt_after_repair(case, monkeypatch):
    await graph_case(
        case,
        monkeypatch,
        [
            calls(("recipe_apply", {"recipe_id": case.recipe["id"]})),
            {"role": "assistant", "content": "Готово."},
            calls(("recipe_complete", {"application_id": case.application["id"]})),
            {"role": "assistant", "content": "Сценарий завершён."},
        ],
    )
    answer = await case.agent.run(case.run, case.conversation, "Выполни мой сценарий")
    assert answer == "Сценарий завершён."
    case.store.recipe_complete.assert_awaited_once_with(
        case.user, case.application["id"], f"{case.run['id']}:tool:3:0", case.run
    )
    assert case.provider.complete.await_count == 4


@pytest.mark.parametrize(
    "prompt,matched",
    [
        ("Запусти сохранённый сценарий «Отчёт»: product = Яблоко. Цену не указал.", True),
        ("Пожалуйста, выполни «Отчёт»", True),
        ("Примени Отчёт", True),
        ("Повтори мой отчёт", True),
        ("Не запускай Отчёт", False),
        ("Пожалуйста, не применяй Отчёт", False),
        ("Объясни, как запустить Отчёт", False),
        ("Запусти ли Отчёт?", False),
        ("Расскажи про Отчёт", False),
        ("> Запусти Отчёт", False),
        ("«Запусти Отчёт» — цитата файла", False),
        ("В файле написано:\nЗапусти Отчёт", False),
        ("Запусти Отчётность", False),
        ("Запусти другой сценарий\n[Пользователь приложил файл: Отчёт, artifact_id=x]", False),
    ],
)
def test_named_start_is_conservative_and_only_uses_current_user_command(case, prompt, matched):
    assert bool(explicit_recipe_start(prompt, [case.recipe])) is matched
    assert explicit_recipe_start(prompt, [{**case.recipe, "status": "inactive"}]) is None


def test_start_requires_matching_current_successful_apply_receipt(case):
    request = explicit_recipe_start("Запусти Отчёт", [case.recipe])
    messages = calls(("recipe_apply", {"recipe_id": case.recipe["id"]}))
    receipt = {
        "role": "tool",
        "tool_call_id": messages["tool_calls"][0]["id"],
        "content": json.dumps(case.application),
    }
    valid = [messages, receipt]
    assert not recipe_needs_start(valid, request)
    assert recipe_needs_start(valid + [{"role": "user", "content": "Запусти Отчёт снова"}], request)
    for result in [
        {**case.application, "recipe_id": str(uuid4())},
        {**case.application, "error": "failure"},
        {**case.application, "context_excluded": True},
        {**case.application, "status": "completed", "completed": False},
        {"status": "ready", "recipe_id": case.recipe["id"]},
    ]:
        assert recipe_needs_start([messages, {**receipt, "content": json.dumps(result)}], request)
    wrong_call = calls(("recipe_apply", {"recipe_id": str(uuid4())}))
    assert recipe_needs_start([wrong_call, receipt], request)
    assert recipe_needs_start([messages, {**receipt, "tool_call_id": "unmatched"}], request)


async def test_named_start_repairs_missing_input_ack_and_suppresses_drafts(case, monkeypatch):
    waiting = {
        **case.application,
        "status": "awaiting_input",
        "missing_inputs": ["price"],
        "input_schema": [{"name": "price", "description": "Цена", "type": "number"}],
        "steps": [],
    }
    case.store.recipe_apply.return_value = waiting
    saver = await graph_case(
        case,
        monkeypatch,
        [
            {"role": "assistant", "content": "Какую цену использовать?"},
            calls(
                ("recipe_apply", {"recipe_id": case.recipe["id"], "inputs": {"product": "Яблоко"}}),
                ("file_create", {"format": "txt", "filename": "blocked.txt", "content": "blocked"}),
            ),
        ],
    )
    replies = list(case.provider.complete.side_effect)

    async def complete(*args, **kwargs):
        assert {tool["function"]["name"] for tool in kwargs["tools"]} == RECIPE_START_TOOLS
        await kwargs["on_delta"]("Какую цену использовать?")
        return replies.pop(0)

    case.provider.complete.side_effect = complete
    case.agent.transport = SimpleNamespace(draft=AsyncMock())
    case.agent.artifacts.generate = lambda *args, **kwargs: pytest.fail("must wait for price")
    answer = await case.agent.run(case.run, case.conversation, "Запусти Отчёт: product = Яблоко")
    assert "Цена" in answer
    case.agent.transport.draft.assert_not_awaited()
    assert case.store.recipe_apply.call_args.args[2] == {"product": "Яблоко"}
    assert case.provider.complete.await_count == 2
    state = await saver.aget_tuple(
        {"configurable": {"thread_id": str(case.run["id"]), "checkpoint_ns": ""}}
    )
    assert state.checkpoint["channel_values"]["recipe_start_repairs"] == 1
    assert (
        await case.agent.run(case.run, case.conversation, "Запусти Отчёт: product = Яблоко")
        == answer
    )
    assert case.store.recipe_apply.await_count == 1


async def test_named_start_has_one_repair_with_minimal_step_budget(case, monkeypatch):
    case.agent.settings = case.agent.settings.model_copy(update={"max_model_steps": 1})
    await graph_case(
        case,
        monkeypatch,
        [
            {"role": "assistant", "content": "Нужны данные."},
            {"role": "assistant", "content": "Сценарий запущен."},
        ],
    )
    assert (
        await case.agent.run(case.run, case.conversation, "Запусти Отчёт") == UNSTARTED_RECIPE_TEXT
    )
    assert case.provider.complete.await_count == 2
    case.store.recipe_apply.assert_not_awaited()


async def test_named_start_blocks_wrong_recipe_and_side_effects(case, monkeypatch):
    await graph_case(
        case,
        monkeypatch,
        [
            calls(
                ("recipe_apply", {"recipe_id": str(uuid4())}),
                ("file_create", {"format": "txt", "filename": "blocked.txt", "content": "blocked"}),
            ),
            {"role": "assistant", "content": "Запущено."},
            {"role": "assistant", "content": "Готово."},
        ],
    )
    case.agent.artifacts.generate = lambda *args, **kwargs: pytest.fail("apply must happen first")
    assert (
        await case.agent.run(case.run, case.conversation, "Запусти Отчёт") == UNSTARTED_RECIPE_TEXT
    )
    case.store.recipe_apply.assert_not_awaited()
    case.store.save_artifact.assert_not_awaited()
    assert case.provider.complete.await_count == 3


async def test_start_and_completion_have_independent_bounded_repairs(case, monkeypatch):
    case.agent.settings = case.agent.settings.model_copy(update={"max_model_steps": 3})
    await graph_case(
        case,
        monkeypatch,
        [
            {"role": "assistant", "content": "Начинаю."},
            calls(("recipe_apply", {"recipe_id": case.recipe["id"]})),
            {"role": "assistant", "content": "Готово."},
            calls(("recipe_complete", {"application_id": case.application["id"]})),
            {"role": "assistant", "content": "Завершено."},
        ],
    )
    assert await case.agent.run(case.run, case.conversation, "Запусти Отчёт") == "Завершено."
    assert case.provider.complete.await_count == 5
    case.store.recipe_complete.assert_awaited_once()


async def test_normal_recipe_discussion_does_not_force_apply(case, monkeypatch):
    await graph_case(case, monkeypatch, [{"role": "assistant", "content": "Нужны товар и цена."}])
    assert (
        await case.agent.run(case.run, case.conversation, "Объясни, как работает Отчёт")
        == "Нужны товар и цена."
    )
    case.store.recipe_apply.assert_not_awaited()
    assert "file_create" in {
        tool["function"]["name"] for tool in case.provider.complete.call_args.kwargs["tools"]
    }
