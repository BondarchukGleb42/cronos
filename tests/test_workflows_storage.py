"""Real project cycles; dedicated synthetic owners, no provider or Telegram calls."""

import asyncio
import json
import os
from copy import deepcopy
from pathlib import Path
from uuid import UUID, uuid4

import asyncpg
import pytest
import pytest_asyncio

from cronos.settings import Settings
from cronos.storage import Store, privacy_event_id
from cronos.workflows import WORKFLOW_TEMPLATES, WorkflowRevisionConflict

A, B = -984201, -984202
pytestmark = pytest.mark.asyncio(loop_scope="module")


async def cleanup(store):
    for owner in (A, B):
        async with store.connection(owner) as conn:
            await conn.execute(
                "DELETE FROM occurrences WHERE schedule_id IN (SELECT id FROM schedules WHERE user_id=$1)",
                owner,
            )
            for table in (
                "privacy_requests",
                "deleted_topics",
                "outbox",
                "operations",
                "run_metrics",
                "runs",
                "schedules",
                "messages",
                "memory",
                "artifacts",
                "usage",
                "ledger",
                "reservations",
                "conversations",
                "users",
            ):
                await conn.execute(f"DELETE FROM {table} WHERE user_id=$1", owner)
            await conn.execute("DELETE FROM events WHERE payload->>'user_id'=$1", str(owner))


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def store():
    if not os.getenv("DATABASE_URL") or not os.getenv("ADMIN_DATABASE_URL"):
        pytest.skip("Real PostgreSQL URLs are required")
    value = Store(Settings())
    await value.open()
    try:
        for owner in (A, B):
            async with value.connection(owner) as conn:
                assert not await conn.fetchval("SELECT 1 FROM users WHERE user_id=$1", owner), (
                    "Synthetic workflow owner exists; inspect before rerunning"
                )
        yield value
    finally:
        await cleanup(value)
        await value.close()


@pytest_asyncio.fixture(autouse=True, loop_scope="module")
async def owners(store):
    for owner in (A, B):
        await store.ensure_user(owner)
    yield
    await cleanup(store)


async def project(store, owner=A, thread=10):
    conversation = await store.conversation(owner, owner, thread)
    result = await store.create_project(
        owner,
        conversation["id"],
        {
            "name": "Личный проект",
            "goal": "Пользовательская цель",
            "state": {"constraints": ["Оставить это ограничение"]},
        },
        f"project:{uuid4()}",
    )
    return result, conversation


async def start(
    store,
    kind="nutrition",
    *,
    owner=A,
    parameters=None,
    plan=None,
    source_key="start",
    run=None,
    existing=None,
):
    current, chat = existing or await project(store, owner)
    example = deepcopy(WORKFLOW_TEMPLATES[kind]["example"])
    args = {
        "kind": kind,
        "parameters": parameters if parameters is not None else example["parameters"],
        "plan": plan if plan is not None else example["plan"],
        "project_revision": current["revision"],
    }
    result = await store.workflow_start(owner, current["id"], args, source_key, run)
    return result, current, chat, args


async def observe(store, current, observation, source_key=None, run=None):
    return await store.workflow_observe(
        A,
        current["project_id"],
        {"revision": current["revision"], "observation": observation},
        source_key or f"observation:{uuid4()}",
        run,
    )


async def replan(store, current, plan, source_key="replan", **extra):
    return await store.workflow_replan(
        A,
        current["project_id"],
        {
            "revision": current["revision"],
            "project_revision": current["project_revision"],
            "plan": plan,
            "project_summary": "Обновлено по наблюдениям",
            "project_next_step": plan["next_step"],
            **extra,
        },
        source_key,
    )


async def source_run(store, chat):
    event_id = uuid4()
    async with store.connection() as conn:
        await conn.execute(
            "INSERT INTO events(id,payload) VALUES($1,$2)", event_id, {"user_id": chat["user_id"]}
        )
    return await store.start_run(event_id, chat["user_id"], chat["id"])


async def test_nutrition_plan_consumption_restock_and_replan_preserve_goal(store):
    current, original, _, args = await start(store)
    assert current["progress"]["pantry"][0]["quantity"] == 500
    assert await store.workflow_start(A, original["id"], args, "start") == current
    meal = deepcopy(WORKFLOW_TEMPLATES["nutrition"]["example"]["observation"])
    updated = await observe(store, current, meal, "dinner")
    assert updated["progress"]["pantry"][0]["quantity"] == 350
    assert updated["progress"]["meals_completed"] == 1
    assert await observe(store, current, meal, "dinner") == updated
    updated = await observe(
        store,
        updated,
        {"action": "restock", "items": [{"name": "рис", "quantity": 100, "unit": "г"}]},
    )
    assert updated["progress"]["pantry"][0]["quantity"] == 450
    plan = deepcopy(updated["plan"])
    plan["next_step"] = "Приготовить меньшую порцию завтра"
    final = await replan(store, updated, plan)
    assert final["revision"] == 4 and final["entries_total"] == 2
    assert final["progress"]["pantry"][0]["quantity"] == 450
    saved_project = await store.get_project(A, project_id=original["id"])
    assert saved_project["goal"] == original["goal"]
    assert saved_project["state"]["constraints"] == original["state"]["constraints"]
    assert saved_project["state"]["summary"] == "Обновлено по наблюдениям"
    assert saved_project["state"]["next_step"] == plan["next_step"]
    assert saved_project["history"][-1]["patch"]["state"]["next_step"] == plan["next_step"]
    async with store.connection(A) as conn:
        assert await conn.fetchval("SELECT count(*) FROM workflow_plans WHERE user_id=$1", A) == 2
        assert await conn.fetchval("SELECT count(*) FROM schedules WHERE user_id=$1", A) == 0


async def test_inventory_uses_decimal_arithmetic_and_rejects_unknown_units_or_negative_stock(store):
    params = {
        "household_count": 2,
        "cooking_minutes": 30,
        "pantry": [{"name": "рис", "quantity": 0.3, "unit": "кг"}],
    }
    current, _, _, _ = await start(store, parameters=params)
    current = await observe(
        store,
        current,
        {"action": "consume", "items": [{"name": "рис", "quantity": 0.1, "unit": "кг"}]},
    )
    assert current["progress"]["pantry"][0]["quantity"] == 0.2
    for quantity, unit in ((0.3, "кг"), (50, "г")):
        with pytest.raises(ValueError, match="known amount"):
            await observe(
                store,
                current,
                {
                    "action": "consume",
                    "items": [{"name": "рис", "quantity": quantity, "unit": unit}],
                },
            )
    assert (await store.workflow_get(A, current["project_id"]))["entries_total"] == 1


async def test_training_completed_feedback_updates_instead_of_double_counting(store):
    current, _, _, _ = await start(store, "training")
    observation = deepcopy(WORKFLOW_TEMPLATES["training"]["example"]["observation"])
    current = await observe(store, current, observation)
    assert current["progress"]["completed_sessions"] == 1
    assert current["progress"]["completed_minutes"] == 18
    current = await observe(
        store, current, {**observation, "minutes": 20, "feedback": "Уточнил время"}
    )
    assert current["progress"]["completed_sessions"] == 1
    assert current["progress"]["completed_minutes"] == 20
    plan = deepcopy(current["plan"])
    plan["sessions"].append(
        {"id": "week1-b", "name": "Следующая короткая сессия", "minutes": 15, "exercises": []}
    )
    plan["next_step"] = "Выполнить вторую сессию за 15 минут"
    current = await replan(store, current, plan)
    assert current["progress"]["completion_rate"] == 0.5
    assert current["progress"]["session_feedback"] == {"week1-a": "Уточнил время"}


async def test_learning_records_evidence_mistake_target_and_next_exercise(store):
    current, _, _, _ = await start(store, "learning")
    current = await observe(
        store,
        current,
        {
            "topic": "дроби",
            "correct": False,
            "evidence": "1/2 + 1/4 = 2/6",
            "mistake": "Сложены знаменатели",
        },
    )
    observation = deepcopy(WORKFLOW_TEMPLATES["learning"]["example"]["observation"])
    current = await observe(store, current, observation)
    assert not current["progress"]["topics"][0]["meets_target"]
    current = await observe(store, current, {**observation, "evidence": "2/3 + 1/3 = 1"})
    progress = current["progress"]["topics"][0]
    assert progress["evidence_count"] == 3 and progress["correct_count"] == 2
    assert progress["success_rate"] == pytest.approx(2 / 3)
    assert not progress["meets_target"] and progress["last_mistake"] == "Сложены знаменатели"
    plan = {
        "exercises": [
            {
                "id": "fractions-2",
                "topic": "дроби",
                "prompt": "Объясни, почему знаменатель нельзя складывать",
            }
        ],
        "next_step": "Разобрать ошибку со знаменателем",
    }
    current = await replan(store, current, plan)
    assert current["plan"]["exercises"][0]["id"] == "fractions-2"
    assert current["entries_total"] == 3
    page = await store.workflow_get(A, current["project_id"], limit=1, offset=1)
    assert page["entries"][0]["observation"]["evidence"] == observation["evidence"]
    assert page["entries_total"] == 3 and page["progress"] == current["progress"]


async def test_content_progress_uses_latest_published_metrics_and_reported_criteria(store):
    current, _, _, _ = await start(store, "content")
    observation = deepcopy(WORKFLOW_TEMPLATES["content"]["example"]["observation"])
    current = await observe(store, current, observation)
    assert (
        current["progress"]["published_items"] == 1
        and current["progress"]["interaction_rate"] == 0.08
    )
    current = await observe(store, current, {**observation, "views": 200, "interactions": 30})
    assert current["progress"]["views"] == 200 and current["progress"]["interactions"] == 30
    assert current["progress"]["interaction_rate"] == 0.15
    assert current["progress"]["criteria_evidence"] == {"post-1": ["Проверенный пример"]}


async def test_wellbeing_is_explicit_self_reported_diary_not_a_diagnosis(store):
    current, _, _, _ = await start(store, "wellbeing")
    current = await observe(store, current, {"energy": 4, "mood": 6, "note": "Устал после работы"})
    current = await observe(store, current, {"energy": 8, "mood": 8, "note": "Отдохнул"})
    assert current["progress"] == {
        "observations_count": 2,
        "self_reported": True,
        "latest_energy": 8,
        "latest_mood": 8,
        "mean_energy": 6,
        "mean_mood": 7,
    }
    assert "diagnosis" not in json.dumps(current)
    async with store.connection(A) as conn:
        assert await conn.fetchval("SELECT count(*) FROM schedules WHERE user_id=$1", A) == 0


async def test_observation_and_project_cas_fail_without_partial_writes(store):
    current, original, chat, _ = await start(store, "training")
    observation = deepcopy(WORKFLOW_TEMPLATES["training"]["example"]["observation"])
    results = await asyncio.gather(
        observe(store, current, observation, "a"),
        observe(store, current, observation, "b"),
        return_exceptions=True,
    )
    assert sum(isinstance(result, dict) for result in results) == 1
    assert sum(isinstance(result, WorkflowRevisionConflict) for result in results) == 1
    latest = await store.workflow_get(A, current["project_id"])
    await store.update_project(
        A,
        original["id"],
        {
            "revision": latest["project_revision"],
            "conversation_id": chat["id"],
            "state": {"summary": "Ручное изменение"},
        },
        "manual",
    )
    with pytest.raises(WorkflowRevisionConflict, match="Project changed"):
        await replan(store, latest, latest["plan"])
    result = await store.workflow_get(A, current["project_id"])
    assert result["revision"] == latest["revision"] and result["entries_total"] == 1
    assert (await store.get_project(A, project_id=original["id"]))["state"][
        "summary"
    ] == "Ручное изменение"


async def test_replan_replay_does_not_replace_a_newer_plan(store):
    current, _, _, _ = await start(store, "training")
    first_plan = {**current["plan"], "next_step": "Первое изменение"}
    first = await replan(store, current, first_plan, "first")
    second = await replan(
        store, first, {**first_plan, "next_step": "Последнее изменение"}, "second"
    )
    assert await replan(store, current, first_plan, "first") == second
    assert second["revision"] == 3


async def test_foreign_projects_rls_and_composite_links_are_rejected(store):
    own, _, _, _ = await start(store, "learning")
    foreign, foreign_project, _, _ = await start(store, "training", owner=B)
    with pytest.raises(ValueError):
        await store.workflow_get(A, foreign_project["id"])
    with pytest.raises(ValueError):
        await store.workflow_observe(
            A, foreign_project["id"], {"revision": 1, "observation": {}}, "foreign"
        )
    async with store.connection(B) as conn:
        for table in (
            "workflows",
            "workflow_observations",
            "workflow_operations",
            "workflow_plans",
        ):
            assert not await conn.fetchval(f"SELECT 1 FROM {table} WHERE user_id=$1", A)
    with pytest.raises(asyncpg.ForeignKeyViolationError):
        async with store.connection(A) as conn:
            await conn.execute(
                """INSERT INTO workflow_observations(id,user_id,project_id,workflow_id,revision,observation)
            VALUES($1,$2,$3,$4,2,'{}')""",
                uuid4(),
                A,
                UUID(own["project_id"]),
                UUID(foreign["id"]),
            )


async def test_cancelled_or_stale_fence_run_cannot_record_observation(store):
    original, chat = await project(store)
    run = await source_run(store, chat)
    current, _, _, _ = await start(store, "training", run=run, existing=(original, chat))
    observation = deepcopy(WORKFLOW_TEMPLATES["training"]["example"]["observation"])
    async with store.connection(A) as conn:
        await conn.execute("UPDATE runs SET cancel_requested=true WHERE id=$1", run["id"])
    with pytest.raises(ValueError, match="no longer active"):
        await observe(store, current, observation, run=run)
    async with store.connection(A) as conn:
        await conn.execute(
            "UPDATE runs SET cancel_requested=false,fence=fence+1 WHERE id=$1", run["id"]
        )
    with pytest.raises(ValueError, match="no longer active"):
        await observe(store, current, observation, run=run)
    assert (await store.workflow_get(A, original["id"]))["entries_total"] == 0


async def test_forget_hides_old_cycle_and_explicit_restart_rejects_old_start_replay(store):
    current, original, chat, args = await start(store, "wellbeing")
    current = await observe(
        store, current, {"energy": 4, "mood": 4, "note": "Старый личный дневник"}
    )
    await store.forget(A, "Забыть старый контекст")
    assert await store.workflow_get(A, original["id"]) == {
        "project_id": original["id"],
        "needs_context": True,
    }
    with pytest.raises(ValueError):
        await observe(store, current, {"energy": 5, "mood": 5, "note": "Must not revive"})
    hidden = await store.get_project(A, project_id=original["id"])
    restored = await store.update_project(
        A,
        original["id"],
        {
            "revision": hidden["revision"],
            "conversation_id": chat["id"],
            "name": "Новое начало",
            "goal": "Новая цель",
        },
        "restore",
    )
    assert (await store.workflow_get(A, original["id"]))["needs_context"]
    fresh, _, _, _ = await start(
        store, "wellbeing", source_key="fresh-start", existing=(restored, chat)
    )
    assert fresh["id"] != current["id"] and fresh["entries_total"] == 0
    assert "Старый личный дневник" not in json.dumps(fresh, ensure_ascii=False)
    with pytest.raises(WorkflowRevisionConflict, match="reset"):
        await store.workflow_start(A, original["id"], args, "start")
    async with store.connection(A) as conn:
        assert (
            await conn.fetchval("SELECT count(*) FROM workflow_observations WHERE user_id=$1", A)
            == 0
        )


async def test_chat_erasure_removes_its_observations_and_handles_incomplete_inventory(store):
    params = {"household_count": 2, "cooking_minutes": 30, "pantry": []}
    current, original, first_chat, _ = await start(store, parameters=params)
    second_chat = await store.conversation(A, A, 11)
    first_run, second_run = (
        await source_run(store, first_chat),
        await source_run(store, second_chat),
    )
    current = await observe(
        store,
        current,
        {"action": "restock", "items": [{"name": "рис", "quantity": 3, "unit": "кг"}]},
        run=first_run,
    )
    current = await observe(
        store,
        current,
        {"action": "consume", "items": [{"name": "рис", "quantity": 1, "unit": "кг"}]},
        run=second_run,
    )
    async with store.connection(A) as conn:
        await conn.execute("DELETE FROM runs WHERE user_id=$1 AND id=$2", A, first_run["id"])
        await conn.execute(
            "DELETE FROM conversations WHERE user_id=$1 AND id=$2", A, first_chat["id"]
        )
    current = await store.workflow_get(A, original["id"])
    assert current["entries_total"] == 1 and not current["progress"]["inventory_consistent"]
    assert current["progress"]["pantry"][0]["quantity"] is None
    repaired = await observe(
        store, current, {"action": "set", "items": [{"name": "рис", "quantity": 2, "unit": "кг"}]}
    )
    assert (
        repaired["progress"]["inventory_consistent"]
        and repaired["progress"]["pantry"][0]["quantity"] == 2
    )


async def test_full_wipe_cascades_workflows_without_changing_billing_or_other_owner(store):
    current, original, chat, _ = await start(store, "learning")
    await observe(store, current, WORKFLOW_TEMPLATES["learning"]["example"]["observation"])
    other, _, _, _ = await start(store, "training", owner=B)
    before = await store.ensure_user(A)
    request = await store.requests_prepare(A, chat, "all", source_key="workflow-wipe")
    assert await store.confirm_privacy_request(A, request["id"], request["origin_thread_id"])
    await store.begin_privacy_erasure(request["id"], privacy_event_id(request["id"]))
    after = await store.ensure_user(A)
    assert after["plan"] == before["plan"] and after["balance_micro"] == before["balance_micro"]
    assert await store.get_project(A, project_id=original["id"]) is None
    assert await store.workflow_get(B, other["project_id"])
    async with store.connection(A) as conn:
        for table in (
            "workflows",
            "workflow_observations",
            "workflow_plans",
            "workflow_operations",
        ):
            assert await conn.fetchval(f"SELECT count(*) FROM {table} WHERE user_id=$1", A) == 0


async def test_workflow_migration_can_repeat_with_existing_journal(store):
    current, _, _, _ = await start(store, "learning")
    current = await observe(
        store, current, WORKFLOW_TEMPLATES["learning"]["example"]["observation"]
    )
    conn = await asyncpg.connect(os.environ["ADMIN_DATABASE_URL"])
    try:
        sql = Path(__file__).parents[1].joinpath("src/cronos/workflows.sql").read_text()
        for _ in range(2):
            async with conn.transaction():
                await conn.execute(sql)
    finally:
        await conn.close()
    assert await store.workflow_get(A, current["project_id"]) == current


@pytest.mark.parametrize(
    "kind,field,value",
    [
        ("nutrition", "household_count", True),
        ("training", "available_minutes", -1),
        ("learning", "minimum_evidence", 0),
        ("content", "criteria", []),
        ("wellbeing", "focus", ""),
    ],
)
async def test_template_parameters_are_strict_and_invalid_start_leaves_project_unchanged(
    store, kind, field, value
):
    original, chat = await project(store)
    params = deepcopy(WORKFLOW_TEMPLATES[kind]["example"]["parameters"])
    params[field] = value
    with pytest.raises(ValueError):
        await start(store, kind, parameters=params, existing=(original, chat))
    assert await store.workflow_get(A, original["id"]) is None
    assert await store.get_project(A, project_id=original["id"]) == original


async def test_templates_export_schemas_examples_and_no_automatic_schedules(store):
    assert set(WORKFLOW_TEMPLATES) == {"nutrition", "training", "learning", "content", "wellbeing"}
    for template in WORKFLOW_TEMPLATES.values():
        for slot in ("parameters", "plan", "observation"):
            assert template[f"{slot}_schema"]["additionalProperties"] is False
            assert isinstance(template["example"][slot], dict)
        assert template["instructions"] and template["title"]
    json.dumps(WORKFLOW_TEMPLATES)
