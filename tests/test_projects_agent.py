import copy
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from langgraph.checkpoint.memory import InMemorySaver

from cronos.agent import Agent
from cronos.settings import Settings


@pytest.fixture
def case(tmp_path):
    owner = 42001
    conversation = {"id": uuid4(), "user_id": owner, "chat_id": owner, "thread_id": 77}
    run = {"id": uuid4(), "user_id": owner, "fence": 1}
    project = {
        "id": str(uuid4()),
        "user_id": owner,
        "name": "Предложение клиенту",
        "goal": "Подготовить согласованное предложение",
        "status": "active",
        "revision": 1,
        "state": {
            "summary": "Собираем требования",
            "constraints": ["Без изменения действующего договора"],
            "decisions": [],
            "open_questions": ["Какой объём работ нужен клиенту?"],
            "next_step": "Уточнить объём работ",
        },
    }
    store = SimpleNamespace(
        create_project=AsyncMock(return_value=project),
        list_projects=AsyncMock(return_value=[project]),
        get_project=AsyncMock(return_value=project),
        workflow_get=AsyncMock(return_value=None),
        update_project=AsyncMock(return_value={**project, "revision": 2}),
        attach_project=AsyncMock(return_value=project),
        detach_project=AsyncMock(return_value={"detached": True}),
        attach_project_artifact=AsyncMock(return_value=project),
        enqueue=AsyncMock(),
        enqueue_for_run=AsyncMock(),
    )
    provider = SimpleNamespace(complete=AsyncMock())
    agent = Agent(
        Settings(database_url="postgresql://unused", artifacts_dir=str(tmp_path)),
        store,
        provider,
        None,
    )
    return SimpleNamespace(
        agent=agent,
        store=store,
        provider=provider,
        project=project,
        owner=owner,
        conversation=conversation,
        run=run,
    )


async def test_create_project_forwards_owner_current_conversation_state_and_operation(case):
    args = {key: copy.deepcopy(case.project[key]) for key in ("name", "goal", "state")}
    result = await case.agent.execute(
        "project_create", args, "project-create-op", case.run, case.conversation
    )
    case.store.create_project.assert_awaited_once_with(
        case.owner,
        case.conversation["id"],
        {**args, "run_id": str(case.run["id"]), "run_fence": case.run["fence"]},
        "project-create-op",
    )
    assert result["id"] == case.project["id"]
    case.provider.complete.assert_not_awaited()
    case.store.enqueue.assert_not_awaited()
    case.store.enqueue_for_run.assert_not_awaited()


@pytest.mark.parametrize("include_completed", [False, True])
async def test_project_list_explicitly_scopes_owner_and_completed_filter(case, include_completed):
    result = await case.agent.execute(
        "project_list",
        {"include_completed": include_completed},
        "list-op",
        case.run,
        case.conversation,
    )
    case.store.list_projects.assert_awaited_once_with(
        case.owner, include_archived=include_completed, limit=50, offset=0
    )
    assert result == [case.project]


@pytest.mark.parametrize("explicit", [False, True])
async def test_project_get_resolves_current_conversation_only_when_no_id_is_given(case, explicit):
    args = {"project_id": case.project["id"]} if explicit else {}
    result = await case.agent.execute("project_get", args, "get-op", case.run, case.conversation)
    call = case.store.get_project.call_args
    assert call.args == (case.owner,)
    assert call.kwargs.get("project_id") == (case.project["id"] if explicit else None)
    assert call.kwargs.get("conversation_id") == (None if explicit else case.conversation["id"])
    assert result["id"] == case.project["id"]


async def test_project_update_passes_revision_and_run_context_without_mutating_arguments(case):
    args = {
        "project_id": case.project["id"],
        "revision": 1,
        "goal": "Согласовать пилот",
        "state": {"decisions": ["Начинаем с пилота"], "next_step": "Выбрать критерии приёмки"},
    }
    original = copy.deepcopy(args)
    result = await case.agent.execute(
        "project_update", args, "update-op", case.run, case.conversation
    )
    case.store.update_project.assert_awaited_once_with(
        case.owner,
        case.project["id"],
        {
            **{key: value for key, value in original.items() if key != "project_id"},
            "conversation_id": str(case.conversation["id"]),
            "run_id": str(case.run["id"]),
            "run_fence": case.run["fence"],
        },
        "update-op",
    )
    assert args == original
    assert result["revision"] == 2


@pytest.mark.parametrize("action", ["attach", "detach"])
@pytest.mark.parametrize("explicit", [False, True])
async def test_project_link_and_detach_keep_owner_target_and_replay_context(case, action, explicit):
    target = str(uuid4()) if explicit else case.conversation["id"]
    args = {"project_id": case.project["id"], "action": action}
    if explicit:
        args["conversation_id"] = target
    await case.agent.execute("project_link", args, "link-op", case.run, case.conversation)
    method = case.store.attach_project if action == "attach" else case.store.detach_project
    call = method.call_args
    assert call.args == (
        (case.owner, case.project["id"], target) if action == "attach" else (case.owner, target)
    )
    assert call.kwargs["source_key"] == "link-op"
    assert str(call.kwargs["run_id"]) == str(case.run["id"])
    assert call.kwargs["run_fence"] == case.run["fence"]


async def test_project_attach_file_forwards_owned_context_and_idempotency_key(case):
    artifact_id = str(uuid4())
    await case.agent.execute(
        "project_attach_file",
        {"project_id": case.project["id"], "artifact_id": artifact_id},
        "attach-op",
        case.run,
        case.conversation,
    )
    call = case.store.attach_project_artifact.call_args
    assert call.args == (case.owner, case.project["id"], artifact_id)
    assert call.kwargs["source_key"] == "attach-op"
    assert str(call.kwargs["run_id"]) == str(case.run["id"])
    assert call.kwargs["run_fence"] == case.run["fence"]
    assert str(call.kwargs["conversation_id"]) == str(case.conversation["id"])


async def test_missing_current_project_returns_found_false_without_creation(case):
    case.store.get_project.return_value = None
    result = await case.agent.execute("project_get", {}, "missing-op", case.run, case.conversation)
    assert result["found"] is False
    assert "id" not in result
    case.store.create_project.assert_not_awaited()


@pytest.mark.parametrize(
    "tool", ["project_create", "project_update", "project_link", "project_attach_file"]
)
async def test_scheduled_run_cannot_mutate_project_state(case, tool):
    with pytest.raises(ValueError, match="недоступен"):
        await case.agent.execute(
            tool, {}, "scheduled-op", case.run, case.conversation, scheduled=True
        )
    for method in (
        case.store.create_project,
        case.store.update_project,
        case.store.attach_project,
        case.store.attach_project_artifact,
    ):
        method.assert_not_awaited()


async def test_unavailable_project_is_not_converted_into_success(case):
    case.store.get_project.side_effect = ValueError("Проект не найден")
    foreign_id = str(uuid4())
    with pytest.raises(ValueError, match="Проект не найден"):
        await case.agent.execute(
            "project_get", {"project_id": foreign_id}, "foreign-op", case.run, case.conversation
        )
    assert case.store.get_project.call_args.args == (case.owner,)
    case.store.create_project.assert_not_awaited()
    case.store.update_project.assert_not_awaited()
    case.provider.complete.assert_not_awaited()


async def test_project_graph_create_replay_and_next_run_use_durable_current_state(
    case, monkeypatch
):
    """Use the actual LangGraph model/tools/checkpoint path with local storage doubles."""
    saver = InMemorySaver()
    projects, bindings, operations = {}, {}, {}

    @asynccontextmanager
    async def graph_connection():
        yield SimpleNamespace(execute=AsyncMock())

    @asynccontextmanager
    async def billing_connection(user_id):
        assert user_id == case.owner
        yield SimpleNamespace(fetchval=AsyncMock(return_value=0))

    async def create_project(user_id, conversation_id, args, op):
        assert user_id == case.owner
        assert conversation_id == case.conversation["id"]
        assert not projects, "A checkpoint replay must not repeat project creation"
        project = {**copy.deepcopy(case.project), **copy.deepcopy(args)}
        projects[project["id"]] = project
        bindings[conversation_id] = project["id"]
        return copy.deepcopy(project)

    async def get_project(user_id, *, project_id=None, conversation_id=None):
        assert user_id == case.owner
        return copy.deepcopy(projects.get(project_id or bindings.get(conversation_id)))

    async def list_projects(user_id, **kwargs):
        assert user_id == case.owner
        return copy.deepcopy(list(projects.values()))

    async def operation(op):
        return copy.deepcopy(operations.get(op))

    async def save_operation(op, user_id, run_id, kind, result):
        assert user_id == case.owner
        operations[op] = copy.deepcopy(result)

    for name, value in {
        "preferences": {},
        "ensure_user": {"plan": "FREE"},
        "memories": [],
        "query_memories": [],
        "history": [],
        "conversation_recall": [],
        "list_artifacts": [],
        "list_schedules": [],
        "recipe_list": [],
        "list_pending_recipe": [],
        "run_active": True,
    }.items():
        setattr(case.store, name, AsyncMock(return_value=value))
    case.store.create_project.side_effect = create_project
    case.store.get_project.side_effect = get_project
    case.store.list_projects.side_effect = list_projects
    case.store.operation = AsyncMock(side_effect=operation)
    case.store.save_operation = AsyncMock(side_effect=save_operation)
    case.store.connection = billing_connection
    case.store.reserve = AsyncMock()
    case.store.record_usage = AsyncMock()
    monkeypatch.setattr(
        "cronos.agent.psycopg.AsyncConnection.connect",
        AsyncMock(side_effect=lambda *args, **kwargs: graph_connection()),
    )
    monkeypatch.setattr("cronos.agent.FencedSaver", lambda conn, run: saver)

    create_args = {key: copy.deepcopy(case.project[key]) for key in ("name", "goal", "state")}
    messages = [
        {
            "role": "assistant",
            "content": None,
            "tool_calls": [
                {
                    "id": "create-project-call",
                    "type": "function",
                    "function": {
                        "name": "project_create",
                        "arguments": json.dumps(create_args),
                    },
                }
            ],
        },
        {"role": "assistant", "content": "Проект создан. Следующий шаг — уточнить объём работ."},
        {"role": "assistant", "content": "Остановились на уточнении объёма работ."},
    ]
    case.provider.complete.side_effect = [
        {
            "message": message,
            "usage": {
                "model": "test/projects",
                "cost_rub": "0.001",
                "prompt_tokens": 10,
                "completion_tokens": 5,
            },
        }
        for message in messages
    ]
    answer = await case.agent.run(
        case.run, case.conversation, "Создай проект для предложения клиенту"
    )
    assert answer == messages[1]["content"]
    case.store.create_project.assert_awaited_once()
    assert case.provider.complete.await_count == 2
    tool_receipt = next(
        message
        for message in case.provider.complete.call_args_list[1].args[0]
        if message["role"] == "tool"
    )
    assert json.loads(tool_receipt["content"])["id"] == case.project["id"]

    replay = await case.agent.run(
        {**case.run, "fence": 2}, case.conversation, "Создай проект для предложения клиенту"
    )
    assert replay == answer
    assert case.provider.complete.await_count == 2
    case.store.create_project.assert_awaited_once()
    assert case.store.record_usage.await_count == 2

    other_id = str(uuid4())
    projects[other_id] = {
        **copy.deepcopy(case.project),
        "id": other_id,
        "name": "Другой проект",
        "goal": "Подробности другого проекта не нужны в текущем контексте",
    }
    next_run = {**case.run, "id": uuid4()}
    continued = await case.agent.run(next_run, case.conversation, "Где остановились?")
    assert continued == messages[2]["content"]
    system = next(
        message["content"]
        for message in case.provider.complete.call_args.args[0]
        if message["role"] == "system"
    )
    for fact in (case.project["id"], case.project["goal"], case.project["state"]["next_step"]):
        assert fact in system
    assert "Другой проект" in system
    assert projects[other_id]["goal"] not in system
    assert case.provider.complete.await_count == 3
    checkpoint = await saver.aget_tuple(
        {"configurable": {"thread_id": str(case.run["id"]), "checkpoint_ns": ""}}
    )
    assert checkpoint.checkpoint["channel_values"]["answer"] == answer
    case.store.enqueue.assert_not_awaited()
    case.store.enqueue_for_run.assert_not_awaited()
