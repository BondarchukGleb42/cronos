"""Real recipe persistence; only fresh, isolated synthetic owners are modified."""

import asyncio
import os
from uuid import UUID, uuid4

import asyncpg
import pytest
import pytest_asyncio

from cronos.recipes import RecipeRevisionConflict, RecipesStoreMixin
from cronos.settings import Settings
from cronos.storage import Store

A, B = -986601, -986602
pytestmark = pytest.mark.asyncio(loop_scope="module")


class RecipeStore(Store, RecipesStoreMixin):
    pass


MEAL = {
    "name": "Семейное меню",
    "description": "Меню и таблица покупок",
    "inputs": [
        {"name": "people", "type": "number", "description": "Число людей", "required": True},
        {"name": "vegetarian", "type": "boolean", "required": True},
        {"name": "notes", "type": "string", "required": False},
    ],
    "steps": [
        {"tool": "deep_reason", "instruction": "Составь меню с учётом входных данных"},
        {"tool": "file_create", "instruction": "Сохрани список покупок как CSV"},
    ],
    "output_requirements": ["Продукты и количества", "Учесть пожелания семьи"],
}
EXPENSES = {
    "name": "Расходы за месяц",
    "description": "Сводная таблица расходов из файла",
    "inputs": [{"name": "statement", "type": "artifact", "required": True}],
    "steps": [
        {"tool": "file_read", "instruction": "Прочитай выписку"},
        {"tool": "table_analyze", "instruction": "Подсчитай итог расходов"},
        {"tool": "file_create", "instruction": "Сохрани итоговую таблицу"},
    ],
    "output_requirements": ["Показать сумму без округления исходных данных"],
}


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def store():
    if not os.getenv("DATABASE_URL"):
        pytest.skip("Real PostgreSQL required; recipes.sql must already be applied")
    value = RecipeStore(Settings())
    await value.open()
    try:
        yield value
    finally:
        await value.close()


@pytest_asyncio.fixture(autouse=True, loop_scope="module")
async def owners(store):
    created = []
    try:
        for owner in (A, B):
            async with store.connection(owner) as conn:
                row = await conn.fetchval(
                    "INSERT INTO users(user_id) VALUES($1) ON CONFLICT DO NOTHING RETURNING user_id",
                    owner,
                )
                assert row is not None, "Refuse to overwrite a preexisting synthetic owner"
            created.append(owner)
        yield
    finally:
        for owner in created:
            async with store.connection(owner) as conn:
                for table in (
                    "recipes",
                    "privacy_requests",
                    "outbox",
                    "operations",
                    "run_metrics",
                    "runs",
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


async def run(store, owner=A, thread=10):
    conversation = await store.conversation(owner, owner, thread)
    event_id = uuid4()
    async with store.connection() as conn:
        await conn.execute(
            "INSERT INTO events(id,payload,state) VALUES($1,$2,'processing')",
            event_id,
            {"user_id": owner},
        )
    return await store.start_run(event_id, owner, conversation["id"])


async def artifact(store, owner=A):
    identifier = uuid4()
    async with store.connection(owner) as conn:
        await conn.execute(
            "INSERT INTO artifacts(id,user_id,filename,mime,path) VALUES($1,$2,'synthetic.csv','text/csv',$3)",
            identifier,
            owner,
            f"/synthetic/{identifier}.csv",
        )
    return str(identifier)


async def apply(store, recipe, source, values=None, key="apply"):
    return await store.recipe_apply(
        A, recipe["id"], values or {}, key, source, source["conversation_id"]
    )


async def operation(store, source, tool, result, key=None):
    identifier = key or str(uuid4())
    await store.save_operation(identifier, source["user_id"], source["id"], tool, result)
    return identifier


async def test_versioned_save_cas_replay_and_existing_application_are_pinned(store):
    source = await run(store)
    recipe = await store.recipe_save(A, MEAL, "save", source)
    waiting = await apply(store, recipe, source, {"people": 2})
    changed = await store.recipe_save(
        A,
        {
            "recipe_id": recipe["id"],
            "revision": 1,
            "name": "Новое меню",
            "steps": [{"tool": "web_search", "instruction": "Найди сезонные продукты"}],
        },
        "update",
        source,
    )
    assert changed["version"] == changed["revision"] == 2
    assert (await store.recipe_save(A, MEAL, "save", source))["version"] == 1
    assert (
        await store.recipe_save(A, {"recipe_id": recipe["id"], "revision": 1}, "update", source)
    )["version"] == 2
    with pytest.raises(RecipeRevisionConflict):
        await store.recipe_save(
            A, {"recipe_id": recipe["id"], "revision": 1, "name": "Stale"}, "stale", source
        )
    fresh_run = await run(store)
    ready = await apply(store, recipe, fresh_run, {"vegetarian": True}, key="resume")
    assert ready["version"] == 1 and ready["inputs"] == {"people": 2, "vegetarian": True}
    assert [step["tool"] for step in ready["steps"]] == ["deep_reason", "file_create"]
    assert (await apply(store, recipe, source, key="apply"))["status"] == "superseded"
    assert waiting["steps"] == [] and waiting["missing_inputs"] == ["vegetarian"]
    assert await store.list_pending_recipe(A, source["conversation_id"]) == []
    compact = await store.recipe_list(A, query="новое", limit=1)
    assert compact[0]["name"] == "Новое меню" and "steps" not in compact[0]
    async with store.connection(A) as conn:
        with pytest.raises(asyncpg.RaiseError, match="immutable"):
            async with conn.transaction():
                await conn.execute("UPDATE recipe_versions SET definition='{}' WHERE user_id=$1", A)


async def test_missing_inputs_do_not_execute_and_types_or_foreign_artifacts_are_rejected(store):
    source = await run(store)
    recipe = await store.recipe_save(A, MEAL, "meal", source)
    pending = await apply(store, recipe, source)
    assert pending["status"] == "awaiting_input" and pending["steps"] == []
    assert len(await store.list_pending_recipe(A, source["conversation_id"])) == 1
    with pytest.raises(ValueError, match="incomplete"):
        await store.recipe_complete(A, pending["id"], "complete", source)
    for invalid in (
        {"people": True},
        {"people": "2"},
        {"people": float("nan")},
        {"vegetarian": 1},
        {"notes": []},
        {"undeclared": "value"},
    ):
        with pytest.raises(ValueError):
            await apply(store, recipe, source, invalid, key=str(uuid4()))
    expense = await store.recipe_save(A, EXPENSES, "expense", source)
    foreign = await artifact(store, B)
    with pytest.raises(ValueError, match="unavailable"):
        await apply(store, expense, source, {"statement": foreign}, key="foreign")
    own = await artifact(store)
    ready = await apply(store, expense, source, {"statement": own}, key="owned")
    assert ready["status"] == "ready" and ready["inputs"]["statement"] == own
    async with store.connection(A) as conn:
        assert await conn.fetchval("SELECT count(*) FROM operations WHERE user_id=$1", A) == 0


async def test_completion_requires_each_real_current_run_receipt_and_owned_outputs(store):
    source = await run(store)
    recipe = await store.recipe_save(
        A,
        {
            "name": "Два файла",
            "steps": [
                {"tool": "file_create", "instruction": "Первый файл"},
                {"tool": "file_create", "instruction": "Второй файл"},
            ],
        },
        "save",
        source,
    )
    own = await artifact(store)
    await operation(
        store, source, "file_create", {"artifact_id": own, "delivery": "queued"}
    )  # Before apply cannot count.
    app = await apply(store, recipe, source)
    foreign = await artifact(store, B)
    for result in (
        {},
        [],
        {"artifact_id": own, "error": "failed"},
        {"artifact_id": own, "completed": False},
        {"artifact_id": "not-uuid"},
        {"artifact_id": foreign, "delivery": "queued"},
        {"artifact_id": own, "delivery": "queued", "status": []},
    ):
        await operation(store, source, "file_create", result)
    other_run = await run(store, A, thread=20)
    await operation(store, other_run, "file_create", {"artifact_id": own, "delivery": "queued"})
    with pytest.raises(ValueError, match="file_create x2"):
        await store.recipe_complete(A, app["id"], "complete", source)
    first = await operation(
        store, source, "file_create", {"artifact_id": own, "delivery": "queued"}
    )
    with pytest.raises(ValueError, match="file_create x1"):
        await store.recipe_complete(A, app["id"], "complete", source)
    second_artifact = await artifact(store)
    second = await operation(
        store, source, "file_create", {"artifact_id": second_artifact, "delivery": "queued"}
    )
    completed = await store.recipe_complete(A, app["id"], "complete", source)
    assert completed["status"] == "completed" and completed["completed"]
    assert not completed["semantic_requirements_verified"]
    assert set(completed["output_artifact_ids"]) == {own, second_artifact}
    async with store.connection(A) as conn:
        assert set(
            await conn.fetchval(
                "SELECT array_agg(operation_id) FROM recipe_application_receipts WHERE user_id=$1",
                A,
            )
        ) == {first, second}


async def test_completed_replay_never_reexecutes_or_reuses_receipts_for_another_application(store):
    source = await run(store)
    recipe = await store.recipe_save(
        A,
        {"name": "Разбор", "steps": [{"tool": "deep_reason", "instruction": "Разбери вопрос"}]},
        "save",
        source,
    )
    app = await apply(store, recipe, source)
    with pytest.raises(ValueError, match="current recipe"):
        await apply(store, recipe, source, key="overlap")
    await operation(store, source, "deep_reason", {"analysis": "Результат"})
    results = await asyncio.gather(
        *[store.recipe_complete(A, app["id"], "complete", source) for _ in range(2)]
    )
    assert all(result["completed"] for result in results)
    assert (await apply(store, recipe, source))["status"] == "completed"
    fresh = await apply(store, recipe, source, key="next")
    with pytest.raises(ValueError, match="deep_reason x1"):
        await store.recipe_complete(A, fresh["id"], "next-complete", source)
    with pytest.raises(ValueError, match="another operation"):
        await store.recipe_complete(A, fresh["id"], "complete", source)
    async with store.connection(A) as conn:
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM recipe_application_receipts WHERE user_id=$1", A
            )
            == 1
        )


async def test_allowed_declarations_and_owner_isolation_include_database_fks(store):
    source = await run(store)
    for forbidden in (
        "shell",
        "python",
        "privacy_request",
        "preferences_set",
        "schedule_create",
        "recipe_apply",
        "plan_change",
    ):
        with pytest.raises(ValueError):
            await store.recipe_save(
                A,
                {"name": "Unsafe", "steps": [{"tool": forbidden, "instruction": "Do it"}]},
                str(uuid4()),
                source,
            )
    recipe = await store.recipe_save(A, EXPENSES, "save", source)
    other_run = await run(store, B)
    assert await store.recipe_get(B, recipe["id"]) is None
    assert await store.recipe_list(B) == []
    assert await store.list_pending_recipe(B, source["conversation_id"]) == []
    with pytest.raises(ValueError):
        await store.recipe_apply(
            B, recipe["id"], {}, "foreign", other_run, other_run["conversation_id"]
        )
    with pytest.raises(ValueError, match="another conversation"):
        await store.recipe_apply(
            A, recipe["id"], {}, "foreign-conversation", source, other_run["conversation_id"]
        )
    async with store.connection(B) as conn:
        assert await conn.fetchval("SELECT count(*) FROM recipes") == 0
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            async with conn.transaction():
                await conn.execute(
                    "INSERT INTO recipe_versions(user_id,recipe_id,version,definition,memory_revision) VALUES($1,$2,2,'{}',0)",
                    B,
                    UUID(recipe["id"]),
                )


async def test_run_fence_cancellation_and_reset_block_mutation(store):
    source = await run(store)
    with pytest.raises(ValueError):
        await store.recipe_save(A, MEAL, "stale", {**source, "fence": source["fence"] + 1})
    async with store.connection(A) as conn:
        await conn.execute(
            "UPDATE runs SET cancel_requested=true WHERE user_id=$1 AND id=$2", A, source["id"]
        )
    with pytest.raises(ValueError):
        await store.recipe_save(A, MEAL, "cancel", source)
    async with store.connection(A) as conn:
        await conn.execute(
            "UPDATE runs SET cancel_requested=false WHERE user_id=$1 AND id=$2", A, source["id"]
        )
        await conn.execute(
            "UPDATE users SET memory_revision=memory_revision+1,content_reset_at=clock_timestamp() WHERE user_id=$1",
            A,
        )
    with pytest.raises(ValueError, match="context reset"):
        await store.recipe_save(A, MEAL, "reset", source)


async def test_forget_hides_definitions_inputs_and_requires_full_explicit_resave(store):
    source = await run(store)
    recipe = await store.recipe_save(A, MEAL, "save", source)
    pending = await apply(store, recipe, source, {"notes": "Чувствительная подробность"})
    async with store.connection(A) as conn:
        await conn.execute("UPDATE users SET memory_revision=memory_revision+1 WHERE user_id=$1", A)
    fresh = await run(store)
    assert await store.recipe_list(A) == []
    assert await store.list_pending_recipe(A, source["conversation_id"]) == []
    hidden = await store.recipe_get(A, recipe["id"])
    assert hidden["context_excluded"] and "name" not in hidden and "steps" not in hidden
    replay = await apply(store, recipe, fresh)
    assert (
        replay["id"] == pending["id"]
        and replay["context_excluded"]
        and replay["inputs"] == {}
        and replay["steps"] == []
    )
    with pytest.raises(ValueError, match="re-save"):
        await apply(store, recipe, fresh, key="new")
    with pytest.raises(ValueError, match="full re-save"):
        await store.recipe_save(
            A, {"recipe_id": recipe["id"], "revision": 1, "name": "Renamed"}, "partial", fresh
        )
    full = await store.recipe_save(
        A, {**MEAL, "recipe_id": recipe["id"], "revision": 1}, "full", fresh
    )
    assert full["revision"] == 2 and not full["context_excluded"]
    new = await apply(store, recipe, fresh, {"people": 1, "vegetarian": False}, key="fresh")
    assert new["status"] == "ready" and "notes" not in new["inputs"]


async def test_full_recipe_wipe_cascades_without_touching_billing_or_other_owner(store):
    source, other = await run(store), await run(store, B)
    own = await store.recipe_save(A, MEAL, "save", source)
    foreign = await store.recipe_save(B, MEAL, "save", other)
    await apply(store, own, source)
    async with store.connection(A) as conn:
        await conn.execute(
            "INSERT INTO ledger(operation_id,user_id,kind,amount_micro) VALUES('recipe-test-billing',$1,'usage',123)",
            A,
        )
        await conn.execute("DELETE FROM recipes WHERE user_id=$1", A)
        for table in (
            "recipes",
            "recipe_versions",
            "recipe_applications",
            "recipe_operations",
            "recipe_application_receipts",
        ):
            assert await conn.fetchval(f"SELECT count(*) FROM {table} WHERE user_id=$1", A) == 0
        assert await conn.fetchval("SELECT amount_micro FROM ledger WHERE user_id=$1", A) == 123
    assert (await store.recipe_get(B, foreign["id"]))["name"] == MEAL["name"]
