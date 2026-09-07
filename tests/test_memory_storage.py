"""Scoped memory and current-fact guarantees on real PostgreSQL."""

import asyncio
import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4

import asyncpg
import pytest
import pytest_asyncio

from cronos.memory import MemoryRevisionConflict
from cronos.settings import Settings
from cronos.storage import Store

A, B = -984101, -984102
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
        pytest.skip("Real PostgreSQL DATABASE_URL and ADMIN_DATABASE_URL are required")
    value = Store(Settings())
    await value.open()
    try:
        for owner in (A, B):
            async with value.connection(owner) as conn:
                assert not await conn.fetchval("SELECT 1 FROM users WHERE user_id=$1", owner), (
                    "Synthetic memory owner already exists; inspect before rerunning"
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


async def write(store, text="Работаю утром", owner=A, source_key=None, **kwargs):
    return await store.write_memory(
        owner, text, source_key=source_key or f"memory:{uuid4()}", **kwargs
    )


async def project(store, owner=A, thread=10):
    conversation = await store.conversation(owner, owner, thread, "Synthetic chat")
    record = await store.create_project(
        owner, conversation["id"], {"name": "Учёба", "goal": "Сдать экзамен"}, f"project:{uuid4()}"
    )
    return record, conversation


async def source_run(store, conversation):
    event_id = uuid4()
    async with store.connection() as conn:
        await conn.execute(
            "INSERT INTO events(id,payload) VALUES($1,$2)",
            event_id,
            {"user_id": conversation["user_id"]},
        )
    return await store.start_run(event_id, conversation["user_id"], conversation["id"])


async def test_scope_selection_isolates_chats_and_links_project_context(store):
    first, chat = await project(store)
    second, other = await project(store, thread=11)
    continuation = await store.conversation(A, A, 12)
    await store.attach_project(A, first["id"], continuation["id"])
    global_fact = await write(store, "Предпочитаю краткие ответы")
    local_fact = await write(
        store, "Задача этого чата", scope="conversation", conversation_id=chat["id"]
    )
    project_fact = await write(store, "Экзамен в июне", scope="project", project_id=first["id"])
    other_fact = await write(store, "Другой предмет", scope="project", project_id=second["id"])
    assert {row["id"] for row in await store.query_memories(A)} == {global_fact["id"]}
    assert {row["id"] for row in await store.query_memories(A, conversation_id=chat["id"])} == {
        global_fact["id"],
        local_fact["id"],
        project_fact["id"],
    }
    assert {
        row["id"] for row in await store.query_memories(A, conversation_id=continuation["id"])
    } == {
        global_fact["id"],
        project_fact["id"],
    }
    assert {row["id"] for row in await store.query_memories(A, conversation_id=other["id"])} == {
        global_fact["id"],
        other_fact["id"],
    }
    assert len(await store.query_memories(A, all_scopes=True)) == 4
    assert await store.query_memories(B, all_scopes=True) == []
    json.dumps(await store.query_memories(A, all_scopes=True))


async def test_same_content_independent_scopes_and_legacy_exact_dedupe(store):
    first, chat = await project(store)
    global_fact = await write(store)
    local_fact = await write(store, scope="conversation", conversation_id=chat["id"])
    project_fact = await write(store, scope="project", project_id=first["id"])
    assert len({row["id"] for row in (global_fact, local_fact, project_fact)}) == 3
    assert (await write(store))["id"] == global_fact["id"]
    assert len(await store.query_memories(A, all_scopes=True)) == 3
    remembered = await store.remember(A, "Совместимый факт")
    assert (await store.remember(A, "Совместимый факт"))["id"] == remembered["id"]
    assert len(await store.memories(A)) == 4


async def test_expiry_uses_database_time_and_expired_fact_can_be_recorded_again(store):
    async with store.connection(A) as conn:
        now = await conn.fetchval("SELECT now()")
    expired = await write(store, "Старая встреча", expires_at=now - timedelta(seconds=1))
    future = await write(store, "Будущая встреча", expires_at=(now + timedelta(days=1)).isoformat())
    assert [row["id"] for row in await store.query_memories(A)] == [future["id"]]
    assert len(await store.query_memories(A, include_inactive=True)) == 2
    renewed = await write(store, "Старая встреча")
    assert renewed["id"] != expired["id"]
    records = {row["id"]: row for row in await store.query_memories(A, include_inactive=True)}
    assert records[expired["id"]]["status"] == "inactive"
    assert {row["id"] for row in await store.query_memories(A)} == {renewed["id"], future["id"]}


async def test_superseding_keeps_only_new_fact_active_and_replay_is_stable(store):
    original = await write(store, "Занимаюсь по понедельникам")
    replacement = await write(
        store,
        "Перенёс занятия на вторник",
        supersedes_id=original["id"],
        source_key="replace",
        source="event-2",
    )
    assert replacement["supersedes_id"] == original["id"] and replacement["source"] == "event-2"
    assert [row["id"] for row in await store.query_memories(A)] == [replacement["id"]]
    all_facts = {row["id"]: row for row in await store.query_memories(A, include_inactive=True)}
    assert all_facts[original["id"]]["status"] == "superseded"
    replay = await write(
        store, "Перенёс занятия на вторник", supersedes_id=original["id"], source_key="replace"
    )
    assert replay == replacement
    with pytest.raises(MemoryRevisionConflict):
        await write(store, "Среда", supersedes_id=original["id"])
    with pytest.raises(MemoryRevisionConflict):
        await store.revise_memory(
            A, original["id"], {"revision": 2, "status": "active"}, "resurrect"
        )


async def test_supersede_cannot_cross_owner_or_scope(store):
    _, chat = await project(store)
    global_fact = await write(store)
    foreign = await write(store, owner=B)
    with pytest.raises(ValueError, match="same scope"):
        await write(
            store,
            "New",
            scope="conversation",
            conversation_id=chat["id"],
            supersedes_id=global_fact["id"],
        )
    with pytest.raises(ValueError, match="unavailable"):
        await write(store, "New", supersedes_id=foreign["id"])
    assert len(await store.query_memories(A, all_scopes=True)) == 1
    assert len(await store.query_memories(B, all_scopes=True)) == 1


async def test_revision_partial_update_replay_and_compare_and_swap(store):
    original = await write(
        store, expires_at=datetime.now(UTC) + timedelta(days=1), source="original-event"
    )
    first = await store.revise_memory(
        A, original["id"], {"revision": 1, "category": "goal", "source": "new-event"}, "first"
    )
    assert first["content"] == original["content"] and first["expires_at"] == original["expires_at"]
    latest = await store.revise_memory(
        A, original["id"], {"revision": 2, "status": "inactive", "expires_at": None}, "second"
    )
    assert latest["revision"] == 3 and latest["expires_at"] is None
    assert await store.query_memories(A) == []
    replay = await store.revise_memory(
        A, original["id"], {"revision": 1, "category": "goal"}, "first"
    )
    assert replay == latest
    with pytest.raises(MemoryRevisionConflict):
        await store.revise_memory(A, original["id"], {"revision": 1, "content": "Stale"}, "old")
    assert (await store.query_memories(A, include_inactive=True))[0] == latest


async def test_concurrent_memory_revision_has_one_winner(store):
    original = await write(store)
    results = await asyncio.gather(
        store.revise_memory(A, original["id"], {"revision": 1, "content": "A"}, "a"),
        store.revise_memory(A, original["id"], {"revision": 1, "content": "B"}, "b"),
        return_exceptions=True,
    )
    assert sum(isinstance(result, dict) for result in results) == 1
    assert sum(isinstance(result, MemoryRevisionConflict) for result in results) == 1
    assert (await store.query_memories(A))[0]["revision"] == 2


async def test_no_duplicate_write_after_later_revision(store):
    original = await write(store, source_key="original-write")
    latest = await store.revise_memory(
        A, original["id"], {"revision": 1, "content": "Работаю вечером"}, "evening"
    )
    assert await write(store, source_key="original-write") == latest
    assert len(await store.query_memories(A)) == 1
    with pytest.raises(ValueError, match="another operation"):
        await store.revise_memory(
            A, original["id"], {"revision": 2, "content": "No"}, "original-write"
        )


async def test_scoped_owner_validation_and_rls(store):
    foreign_project, foreign_chat = await project(store, B)
    foreign_fact = await write(store, owner=B)
    for args in (
        {"scope": "conversation", "conversation_id": foreign_chat["id"]},
        {"scope": "project", "project_id": foreign_project["id"]},
    ):
        with pytest.raises(ValueError):
            await write(store, **args)
    with pytest.raises(ValueError):
        await store.query_memories(A, conversation_id=foreign_chat["id"])
    with pytest.raises(ValueError):
        await store.query_memories(A, project_id=foreign_project["id"])
    with pytest.raises(ValueError):
        await store.revise_memory(
            A, foreign_fact["id"], {"revision": 1, "content": "No"}, "foreign"
        )
    own = await write(store)
    async with store.connection(B) as conn:
        assert not await conn.fetchval("SELECT 1 FROM memory WHERE user_id=$1", A)
        assert not await conn.fetchval("SELECT 1 FROM memory_operations WHERE user_id=$1", A)
    with pytest.raises(asyncpg.ForeignKeyViolationError):
        async with store.connection(A) as conn:
            await conn.execute(
                """INSERT INTO memory(id,user_id,content,scope,conversation_id)
                VALUES($1,$2,'No','conversation',$3)""",
                uuid4(),
                A,
                foreign_chat["id"],
            )
    with pytest.raises(asyncpg.ForeignKeyViolationError):
        async with store.connection(A) as conn:
            await conn.execute(
                "UPDATE memory SET supersedes_id=$2 WHERE id=$1",
                UUID(own["id"]),
                UUID(foreign_fact["id"]),
            )


async def test_active_run_required_for_write_and_revision(store):
    _, chat = await project(store)
    run = await source_run(store, chat)
    fact = await write(store, run=run, source=str(run["event_id"]))
    async with store.connection(A) as conn:
        receipt = await conn.fetchrow("SELECT * FROM memory_operations WHERE user_id=$1", A)
        assert receipt["run_id"] == run["id"] and receipt["conversation_id"] == chat["id"]
        assert "content" not in receipt and "source" not in receipt
        await conn.execute("UPDATE runs SET cancel_requested=true WHERE id=$1", run["id"])
    with pytest.raises(ValueError, match="no longer active"):
        await write(store, "Cancelled write", run=run)
    async with store.connection() as conn:
        await conn.execute(
            "UPDATE runs SET cancel_requested=false,fence=fence+1 WHERE id=$1", run["id"]
        )
    with pytest.raises(ValueError, match="no longer active"):
        await store.revise_memory(A, fact["id"], {"revision": 1, "content": "Stale"}, "stale", run)
    assert len(await store.query_memories(A)) == 1


async def test_source_chat_deletion_keeps_global_fact_but_removes_scoped_fact_and_receipts(store):
    _, chat = await project(store)
    run = await source_run(store, chat)
    global_fact = await write(store, run=run)
    await write(store, "Local", scope="conversation", conversation_id=chat["id"], run=run)
    async with store.connection(A) as conn:
        await conn.execute("DELETE FROM runs WHERE user_id=$1", A)
        await conn.execute("DELETE FROM conversations WHERE user_id=$1 AND id=$2", A, chat["id"])
        assert (
            await conn.fetchval("SELECT count(*) FROM memory_operations WHERE user_id=$1", A) == 0
        )
    assert [row["id"] for row in await store.query_memories(A, all_scopes=True)] == [
        global_fact["id"]
    ]


async def test_deleting_predecessor_clears_reference_without_removing_current_fact(store):
    old = await write(store, "Old")
    current = await write(store, "Current", supersedes_id=old["id"])
    async with store.connection(A) as conn:
        await conn.execute("DELETE FROM memory WHERE user_id=$1 AND id=$2", A, UUID(old["id"]))
        assert (
            await conn.fetchval("SELECT count(*) FROM memory_operations WHERE user_id=$1", A) == 1
        )
    row = (await store.query_memories(A))[0]
    assert row["id"] == current["id"] and row["supersedes_id"] is None


async def test_search_is_literal_case_insensitive_and_paginates(store):
    for text in ("Стоимость 100%", "Первая цель", "Вторая цель"):
        await write(store, text)
    assert len(await store.query_memories(A, query="ЦЕЛЬ")) == 2
    assert [row["content"] for row in await store.query_memories(A, query="%")] == [
        "Стоимость 100%"
    ]
    all_rows = await store.query_memories(A)
    assert [(await store.query_memories(A, limit=1, offset=i))[0]["id"] for i in range(3)] == [
        row["id"] for row in all_rows
    ]


async def test_forget_excludes_project_and_restoration_cannot_reuse_old_context(store):
    original, chat = await project(store)
    original = await store.update_project(
        A,
        original["id"],
        {
            "revision": 1,
            "conversation_id": chat["id"],
            "name": "Private old name",
            "goal": "Private old goal",
            "state": {"summary": "Private summary", "decisions": ["Private decision"]},
        },
        "private-project",
    )
    await write(store, "Private fact")
    await store.forget(A, "Private fact")
    hidden = await store.get_project(A, project_id=original["id"])
    assert set(hidden) == {"id", "revision", "status", "needs_context"} and hidden["needs_context"]
    assert "Private" not in json.dumps(await store.list_projects(A))
    with pytest.raises(ValueError, match="name and goal"):
        await store.update_project(
            A,
            original["id"],
            {
                "revision": hidden["revision"],
                "conversation_id": chat["id"],
                "state": {"next_step": "Continue"},
            },
            "incomplete-restore",
        )
    restored = await store.update_project(
        A,
        original["id"],
        {
            "revision": hidden["revision"],
            "conversation_id": chat["id"],
            "name": "Новый план",
            "goal": "Новая цель",
            "state": {"next_step": "Начать заново"},
        },
        "restore",
    )
    assert restored["state"] == {
        "summary": "",
        "constraints": [],
        "decisions": [],
        "open_questions": [],
        "next_step": "Начать заново",
    }
    assert "Private" not in json.dumps(restored)
    assert all(row["revision"] > hidden["revision"] for row in restored["history"])


async def test_memory_migration_is_idempotent_with_existing_scoped_facts(store):
    record, chat = await project(store)
    await write(store, scope="conversation", conversation_id=chat["id"])
    await write(store, scope="project", project_id=record["id"])
    original = await store.query_memories(A, all_scopes=True)
    conn = await asyncpg.connect(os.environ["ADMIN_DATABASE_URL"])
    try:
        sql = Path(__file__).parents[1].joinpath("src/cronos/memory.sql").read_text()
        for _ in range(2):
            async with conn.transaction():
                await conn.execute(sql)
    finally:
        await conn.close()
    assert await store.query_memories(A, all_scopes=True) == original


@pytest.mark.parametrize(
    "kwargs",
    [
        {"scope": "unknown"},
        {"scope": "conversation"},
        {"scope": "project"},
        {"expires_at": "2026-09-07T10:00:00"},
        {"expires_at": "not a timestamp"},
        {"scope": "global", "conversation_id": str(uuid4())},
    ],
)
async def test_invalid_memory_scope_or_expiry_does_not_write(store, kwargs):
    with pytest.raises(ValueError):
        await write(store, **kwargs)
    assert await store.query_memories(A, all_scopes=True) == []
