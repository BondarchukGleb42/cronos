"""Workflow routing, execution guards and the real LangGraph conversation path."""

import copy
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from langgraph.checkpoint.memory import InMemorySaver

from cronos.agent import Agent, available_tools
from cronos.capabilities import SKILLS, TOOLS
from cronos.settings import Settings
from cronos.workflow_tools import WORKFLOW_TOOL_NAMES, workflow_context
from cronos.workflows import WORKFLOW_TEMPLATES, WorkflowRevisionConflict


@pytest.fixture
def case(tmp_path):
    owner = 42905
    project = {
        "id": str(uuid4()),
        "name": "Учёба",
        "goal": "Разобраться с дробями",
        "revision": 2,
        "status": "active",
        "state": {"next_step": "Решить пример"},
    }
    conversation = {"id": uuid4(), "user_id": owner, "chat_id": owner, "thread_id": 77}
    run = {"id": uuid4(), "user_id": owner, "fence": 1, "event_id": uuid4()}
    workflow = {
        "id": str(uuid4()),
        "project_id": project["id"],
        "kind": "learning",
        "revision": 1,
        "project_revision": 2,
        "plan": copy.deepcopy(WORKFLOW_TEMPLATES["learning"]["example"]["plan"]),
        "parameters": copy.deepcopy(WORKFLOW_TEMPLATES["learning"]["example"]["parameters"]),
        "progress": {"observations_count": 0},
        "entries": [],
        "entries_total": 0,
    }
    store = SimpleNamespace(
        get_project=AsyncMock(return_value=project),
        list_projects=AsyncMock(return_value=[project]),
        workflow_start=AsyncMock(return_value=workflow),
        workflow_get=AsyncMock(return_value=workflow),
        workflow_observe=AsyncMock(return_value=workflow),
        workflow_replan=AsyncMock(return_value=workflow),
        schedule=AsyncMock(),
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
        owner=owner,
        project=project,
        conversation=conversation,
        run=run,
        workflow=workflow,
        store=store,
        provider=provider,
        agent=agent,
    )


async def test_templates_disclose_precise_shape_only_for_requested_kind(case):
    catalog = await case.agent.execute(
        "workflow_templates", {}, "catalog", case.run, case.conversation
    )
    assert {row["kind"] for row in catalog["templates"]} == set(WORKFLOW_TEMPLATES)
    assert all("plan_schema" not in row for row in catalog["templates"])
    template = await case.agent.execute(
        "workflow_templates", {"kind": "learning"}, "template", case.run, case.conversation
    )
    assert template["plan_schema"]["additionalProperties"] is False
    assert template["example"]["observation"]["evidence"]
    case.store.get_project.assert_not_awaited()
    case.provider.complete.assert_not_awaited()


@pytest.mark.parametrize(
    "tool,fields",
    [
        (
            "workflow_start",
            {
                "kind": "learning",
                "parameters": {"topics": ["дроби"]},
                "plan": {"next_step": "Разобрать пример"},
                "project_revision": 2,
            },
        ),
        (
            "workflow_observe",
            {"revision": 1, "observation": {"topic": "дроби", "evidence": "3/4", "correct": True}},
        ),
        (
            "workflow_replan",
            {
                "revision": 1,
                "project_revision": 2,
                "plan": {"next_step": "Следующий пример"},
                "project_summary": "Разобран пример",
                "project_next_step": "Следующий пример",
            },
        ),
    ],
)
async def test_mutations_forward_owned_project_operation_and_current_run(case, tool, fields):
    args = {**copy.deepcopy(fields), "project_id": case.project["id"]}
    original = copy.deepcopy(args)
    result = await case.agent.execute(tool, args, "workflow-operation", case.run, case.conversation)
    getattr(case.store, tool).assert_awaited_once_with(
        case.owner, case.project["id"], fields, "workflow-operation", run=case.run
    )
    assert result == case.workflow and args == original
    case.store.get_project.assert_awaited_once_with(
        case.owner, project_id=case.project["id"], conversation_id=None
    )
    case.store.schedule.assert_not_awaited()
    case.store.enqueue_for_run.assert_not_awaited()


async def test_get_defaults_to_current_conversation_and_passes_history_page(case):
    result = await case.agent.execute(
        "workflow_get", {"limit": 3, "offset": 4}, "get", case.run, case.conversation
    )
    assert result == case.workflow
    case.store.get_project.assert_awaited_once_with(
        case.owner, project_id=None, conversation_id=case.conversation["id"]
    )
    case.store.workflow_get.assert_awaited_once_with(
        case.owner, case.project["id"], limit=3, offset=4
    )


@pytest.mark.parametrize("project", [None, {"id": "cleared", "needs_context": True}])
async def test_missing_or_cleared_project_cannot_be_mutated(case, project):
    case.store.get_project.return_value = project
    with pytest.raises(ValueError, match="восстанови проект"):
        await case.agent.execute(
            "workflow_observe",
            {"revision": 1, "observation": {}},
            "no",
            case.run,
            case.conversation,
        )
    result = await case.agent.execute("workflow_get", {}, "get", case.run, case.conversation)
    assert result["found"] is False
    case.store.workflow_observe.assert_not_awaited()


async def test_revision_conflict_is_not_reported_as_saved(case):
    case.store.workflow_observe.side_effect = WorkflowRevisionConflict("reload")
    with pytest.raises(WorkflowRevisionConflict):
        await case.agent.execute(
            "workflow_observe",
            {"revision": 1, "observation": {}},
            "stale",
            case.run,
            case.conversation,
        )
    case.provider.complete.assert_not_awaited()
    case.store.enqueue_for_run.assert_not_awaited()


@pytest.mark.parametrize("name", ["workflow_start", "workflow_observe", "workflow_replan"])
async def test_scheduled_and_proactive_runs_cannot_modify_personal_observations(case, name):
    for flag in ("scheduled", "proactive"):
        with pytest.raises(ValueError, match="недоступен"):
            await case.agent.execute(
                name, {}, "background", case.run, case.conversation, **{flag: True}
            )
    getattr(case.store, name).assert_not_awaited()
    scheduled = {tool["function"]["name"] for tool in available_tools(scheduled=True)}
    assert name not in scheduled
    assert {"workflow_get", "workflow_templates"} <= scheduled


async def test_workflow_context_excludes_other_or_forgotten_projects(case):
    assert await workflow_context(case.store, case.owner, None) is None
    assert await workflow_context(case.store, case.owner, {"needs_context": True}) is None
    case.store.workflow_get.assert_not_awaited()
    assert await workflow_context(case.store, case.owner, case.project) == case.workflow
    case.store.workflow_get.assert_awaited_once_with(case.owner, case.project["id"], limit=5)
    case.store.workflow_get.return_value = {"project_id": case.project["id"], "needs_context": True}
    assert await workflow_context(case.store, case.owner, case.project) is None


async def test_registry_covers_workflow_tools_and_requires_revisions():
    schemas = {tool["function"]["name"]: tool["function"]["parameters"] for tool in TOOLS}
    assert WORKFLOW_TOOL_NAMES <= schemas.keys() and "workflows" in SKILLS
    assert "project_revision" in schemas["workflow_start"]["required"]
    assert "revision" in schemas["workflow_observe"]["required"]
    assert {"revision", "project_revision"} <= set(schemas["workflow_replan"]["required"])


async def test_real_graph_start_observe_replan_replay_and_next_turn_context(case, monkeypatch):
    saver, operations = InMemorySaver(), {}
    current = None

    @asynccontextmanager
    async def graph_connection():
        yield SimpleNamespace(execute=AsyncMock())

    @asynccontextmanager
    async def billing_connection(user_id):
        assert user_id == case.owner
        yield SimpleNamespace(fetchval=AsyncMock(return_value=0))

    async def get_workflow(owner, project_id, **kwargs):
        assert owner == case.owner and project_id == case.project["id"]
        return copy.deepcopy(current)

    async def start_workflow(owner, project_id, args, op, run):
        nonlocal current
        assert current is None
        assert owner == case.owner and project_id == case.project["id"]
        assert run["id"] == case.run["id"] and args["project_revision"] == 2
        case.project["revision"] = 3
        current = {**copy.deepcopy(case.workflow), "project_revision": 3}
        return copy.deepcopy(current)

    async def observe_workflow(owner, project_id, args, op, run):
        nonlocal current
        assert owner == case.owner and project_id == case.project["id"]
        assert current["revision"] == args["revision"] == 1
        current = {
            **current,
            "revision": 2,
            "entries_total": 1,
            "entries": [{"observation": args["observation"]}],
            "progress": {"observations_count": 1},
        }
        return copy.deepcopy(current)

    async def replan_workflow(owner, project_id, args, op, run):
        nonlocal current
        assert owner == case.owner and project_id == case.project["id"]
        assert args["revision"] == current["revision"] == 2
        assert args["project_revision"] == case.project["revision"] == 3
        case.project["revision"] = 4
        current = {**current, "revision": 3, "project_revision": 4, "plan": args["plan"]}
        return copy.deepcopy(current)

    async def save_operation(op, user_id, run_id, kind, result):
        assert user_id == case.owner
        operations[op] = copy.deepcopy(result)

    for name, value in {
        "preferences": {},
        "ensure_user": {"plan": "FREE"},
        "query_memories": [],
        "history": [],
        "conversation_recall": [],
        "list_artifacts": [],
        "list_schedules": [],
        "run_active": True,
    }.items():
        setattr(case.store, name, AsyncMock(return_value=value))
    case.store.workflow_get.side_effect = get_workflow
    case.store.workflow_start.side_effect = start_workflow
    case.store.workflow_observe.side_effect = observe_workflow
    case.store.workflow_replan.side_effect = replan_workflow
    case.store.operation = AsyncMock(side_effect=lambda op: copy.deepcopy(operations.get(op)))
    case.store.save_operation = AsyncMock(side_effect=save_operation)
    case.store.connection = billing_connection
    case.store.reserve = AsyncMock()
    case.store.record_usage = AsyncMock()
    monkeypatch.setattr(
        "cronos.agent.psycopg.AsyncConnection.connect",
        AsyncMock(side_effect=lambda *a, **k: graph_connection()),
    )
    monkeypatch.setattr("cronos.agent.FencedSaver", lambda conn, run: saver)

    example = copy.deepcopy(WORKFLOW_TEMPLATES["learning"]["example"])
    new_plan = {
        "exercises": [{"id": "next", "topic": "дроби", "prompt": "Вычисли 3/4 - 1/2"}],
        "next_step": "Решить пример на вычитание",
    }

    def call(name, args, call_id):
        return {
            "role": "assistant",
            "content": None,
            "tool_calls": [
                {
                    "id": call_id,
                    "type": "function",
                    "function": {"name": name, "arguments": json.dumps(args)},
                }
            ],
        }

    replies = [
        call(
            "workflow_start",
            {
                "kind": "learning",
                "parameters": example["parameters"],
                "plan": example["plan"],
                "project_revision": 2,
            },
            "start",
        ),
        {"role": "assistant", "content": "План обучения сохранён."},
        call(
            "workflow_observe", {"revision": 1, "observation": example["observation"]}, "evidence"
        ),
        call(
            "workflow_replan",
            {
                "revision": 2,
                "project_revision": 3,
                "plan": new_plan,
                "project_summary": "Первый пример разобран",
                "project_next_step": new_plan["next_step"],
            },
            "next-plan",
        ),
        {"role": "assistant", "content": "Результат записан. Следующий пример — на вычитание."},
        {"role": "assistant", "content": "Остановились на примере 3/4 - 1/2."},
    ]
    case.provider.complete.side_effect = [
        {
            "message": row,
            "usage": {
                "model": "test/workflows",
                "cost_rub": "0.001",
                "prompt_tokens": 10,
                "completion_tokens": 5,
            },
        }
        for row in replies
    ]
    assert (
        await case.agent.run(case.run, case.conversation, "Сохрани этот план обучения")
        == replies[1]["content"]
    )
    second_run = {**case.run, "id": uuid4(), "event_id": uuid4()}
    answer = await case.agent.run(
        second_run, case.conversation, "Я решил 1/2 + 1/4 = 3/4. Запиши результат и обнови план"
    )
    assert answer == replies[4]["content"]
    before = case.provider.complete.await_count
    assert (
        await case.agent.run({**second_run, "fence": 2}, case.conversation, "Повтор события")
        == answer
    )
    assert case.provider.complete.await_count == before
    case.store.workflow_start.assert_awaited_once()
    case.store.workflow_observe.assert_awaited_once()
    case.store.workflow_replan.assert_awaited_once()
    await case.agent.run(
        {**case.run, "id": uuid4(), "event_id": uuid4()}, case.conversation, "Где остановились?"
    )
    system = next(
        message["content"]
        for message in case.provider.complete.call_args.args[0]
        if message["role"] == "system"
    )
    assert new_plan["next_step"] in system and example["observation"]["evidence"] in system
    assert current["id"] in system and current["revision"] == 3
    case.store.schedule.assert_not_awaited()
    case.store.enqueue_for_run.assert_not_awaited()
