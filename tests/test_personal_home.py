"""Personal home queries and actions against isolated real PostgreSQL owners."""

import os
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest
import pytest_asyncio

from cronos.home_dashboard import (
    continue_home_project,
    hide_home_project,
    home_overview,
    home_project_panel,
    personal_main_panel,
)
from cronos.settings import Settings
from cronos.storage import Store

A, B = -985801, -985802
pytestmark = pytest.mark.asyncio(loop_scope="module")


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def store():
    if not os.getenv("DATABASE_URL"):
        pytest.skip("Real PostgreSQL required; no migration is run by these tests")
    value = Store(Settings())
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
                inserted = await conn.fetchval(
                    "INSERT INTO users(user_id) VALUES($1) ON CONFLICT DO NOTHING RETURNING user_id",
                    owner,
                )
                assert inserted is not None, "Never overwrite a preexisting test owner"
            created.append(owner)
        yield
    finally:
        for owner in created:
            async with store.connection(owner) as conn:
                await conn.execute(
                    "DELETE FROM occurrences WHERE schedule_id IN (SELECT id FROM schedules WHERE user_id=$1)",
                    owner,
                )
                for table in (
                    "privacy_requests",
                    "outbox",
                    "operations",
                    "run_metrics",
                    "runs",
                    "schedules",
                    "messages",
                    "memory",
                    "artifacts",
                    "projects",
                    "usage",
                    "ledger",
                    "reservations",
                    "conversations",
                    "users",
                ):
                    await conn.execute(f"DELETE FROM {table} WHERE user_id=$1", owner)
                await conn.execute("DELETE FROM events WHERE payload->>'user_id'=$1", str(owner))


async def project(store, owner=A, name="Синтетический проект", status="active"):
    conversation = await store.conversation(owner, owner, int(uuid4()) % 1_000_000 + 2)
    return await store.create_project(
        owner,
        conversation["id"],
        {
            "name": name,
            "goal": "Подготовить материал",
            "status": status,
            "state": {"summary": "Согласована тема", "next_step": "Подготовить первый раздел"},
        },
        f"project:{uuid4()}",
    )


async def run(store, owner=A, *, kind="telegram"):
    conversation = await store.conversation(owner, owner, 10)
    identifier = uuid4()
    async with store.connection() as conn:
        await conn.execute(
            "INSERT INTO events(id,kind,payload,state) VALUES($1,$2,$3,'processing')",
            identifier,
            kind,
            {"user_id": owner},
        )
    return await store.start_run(identifier, owner, conversation["id"])


async def version(
    store, *, owner=A, name="result.csv", parent=None, status="done", kind="telegram", finish=True
):
    source = await run(store, owner, kind=kind)
    artifact_id = uuid4()
    async with store.connection(owner) as conn:
        await conn.execute(
            "INSERT INTO artifacts(id,user_id,filename,mime,path) VALUES($1,$2,$3,'text/csv',$4)",
            artifact_id,
            owner,
            name,
            f"/synthetic/{artifact_id}.csv",
        )
    result = await store.register_artifact_version(
        owner,
        artifact_id,
        parent_artifact_id=parent,
        source_key=f"version:{artifact_id}",
        run=source,
    )
    if finish:
        await store.finish_run(source["id"], status, source["fence"])
    return result, source


async def test_first_use_has_no_invented_projects_or_mutations(store):
    async with store.connection(A) as conn:
        await conn.execute(
            "INSERT INTO ledger(operation_id,user_id,kind,amount_micro) VALUES($1,$2,'usage',123)",
            f"billing:{A}",
            A,
        )
    overview = await home_overview(store, A)
    assert overview == {
        "projects": [],
        "project_count": 0,
        "hidden_count": 0,
        "results": [],
        "tasks": 0,
        "suggestions": True,
        "proactivity": True,
    }
    text = (await personal_main_panel(store, A))["text"]
    assert "Начни со своей задачи" in text and "Вот где можно продолжить" not in text
    async with store.connection(A) as conn:
        for table in ("projects", "conversations", "operations", "outbox"):
            assert await conn.fetchval(f"SELECT count(*) FROM {table} WHERE user_id=$1", A) == 0
        assert await conn.fetchval("SELECT amount_micro FROM ledger WHERE user_id=$1", A) == 123


async def test_home_reads_explicit_off_and_does_not_change_it_on_render(store):
    await store.set_proactivity(A, False, f"home-off:{A}")
    assert (await home_overview(store, A))["proactivity"] is False
    text = (await personal_main_panel(store, A))["text"]
    assert "Инициатива выключена" in text
    assert "Инициатива включена" not in text
    assert (await store.preferences(A))["proactivity"] is False


async def test_active_owner_projects_only_and_changed_revision_reappears(store):
    active = await project(store, name="Нужное дело")
    await project(store, name="Завершённое", status="completed")
    await project(store, name="Пауза", status="paused")
    excluded = await project(store, name="Забытое")
    foreign = await project(store, B, name="Чужой проект")
    async with store.connection(A) as conn:
        await conn.execute(
            "UPDATE projects SET context_excluded=true WHERE user_id=$1 AND id=$2",
            A,
            UUID(excluded["id"]),
        )
    assert [p["id"] for p in (await home_overview(store, A))["projects"]] == [active["id"]]
    assert not await hide_home_project(store, A, foreign["id"])
    assert await hide_home_project(store, A, active["id"])
    hidden = await home_overview(store, A)
    assert hidden["projects"] == [] and hidden["hidden_count"] == hidden["project_count"] == 1
    updated = await store.update_project(
        A,
        active["id"],
        {
            "revision": active["revision"],
            "conversation_id": active["conversation_ids"][0],
            "state": {"next_step": "Проверить результат"},
        },
        "changed-step",
    )
    visible = await home_overview(store, A)
    assert (
        visible["hidden_count"] == 0 and visible["projects"][0]["revision"] == updated["revision"]
    )
    assert "Проверить результат" in (await personal_main_panel(store, A))["text"]
    await store.preferences(A, {"home_suggestions": False})
    assert "Нужное дело" not in (await personal_main_panel(store, A))["text"]


async def test_recent_results_only_current_owned_done_telegram_and_current_memory_epoch(store):
    await version(store, name="Old forgotten result.csv")
    async with store.connection(A) as conn:
        await conn.execute("UPDATE users SET memory_revision=memory_revision+1 WHERE user_id=$1", A)
    first, _ = await version(store, name="Original.csv")
    current, _ = await version(store, name="Current.csv", parent=first["artifact_id"])
    await version(store, name="Still running.csv", finish=False)
    await version(store, name="Failed.csv", status="failed")
    await version(store, name="Scheduled.csv", kind="timer")
    excluded, _ = await version(store, name="Excluded.csv")
    async with store.connection(A) as conn:
        await conn.execute(
            "UPDATE artifact_versions SET context_excluded=true WHERE user_id=$1 AND artifact_id=$2",
            A,
            UUID(excluded["artifact_id"]),
        )
    await version(store, owner=B, name="Foreign.csv")
    overview = await home_overview(store, A)
    assert overview["results"] == [
        {"id": current["artifact_id"], "filename": "Current.csv", "version": 2}
    ]


async def test_restored_current_pointer_changes_recent_result_without_new_artifact(store):
    original, _ = await version(store, name="Original.csv")
    newer, _ = await version(store, name="Revised.csv", parent=original["artifact_id"])
    assert (await home_overview(store, A))["results"][0]["id"] == newer["artifact_id"]
    source = await run(store)
    await store.restore_artifact_version(
        A, original["artifact_id"], source_key="restore", run=source
    )
    restored = await home_overview(store, A)
    assert restored["results"] == [
        {"id": original["artifact_id"], "filename": "Original.csv", "version": 1}
    ]
    async with store.connection(A) as conn:
        assert await conn.fetchval("SELECT count(*) FROM artifacts WHERE user_id=$1", A) == 2


async def test_recent_results_are_bounded_to_three_heads(store):
    identifiers = []
    for index in range(4):
        result, _ = await version(store, name=f"Report {index}.csv")
        identifiers.append(result["artifact_id"])
    assert [row["id"] for row in (await home_overview(store, A))["results"]] == list(
        reversed(identifiers[-3:])
    )


async def test_stale_foreign_and_forgotten_project_callbacks_do_not_create_or_attach(store):
    completed = await project(store, status="completed")
    forgotten = await project(store)
    foreign = await project(store, B)
    async with store.connection(A) as conn:
        await conn.execute(
            "UPDATE projects SET context_excluded=true WHERE user_id=$1 AND id=$2",
            A,
            UUID(forgotten["id"]),
        )
        before = await conn.fetchval(
            "SELECT count(*) FROM project_conversations WHERE user_id=$1", A
        )
    transport = SimpleNamespace(topic_capabilities=AsyncMock(), create_topic=AsyncMock())
    for identifier in (completed["id"], forgotten["id"], foreign["id"], str(uuid4())):
        for panel in (
            await home_project_panel(store, A, identifier),
            await continue_home_project(store, transport, A, A, identifier, uuid4()),
        ):
            assert "контекст очищен" in panel["text"]
            assert all(
                not button["callback_data"].startswith("home:continue:")
                for row in panel["reply_markup"]["inline_keyboard"]
                for button in row
            )
    transport.topic_capabilities.assert_not_awaited()
    transport.create_topic.assert_not_awaited()
    async with store.connection(A) as conn:
        assert (
            await conn.fetchval("SELECT count(*) FROM project_conversations WHERE user_id=$1", A)
            == before
        )
        assert await conn.fetchval("SELECT count(*) FROM operations WHERE user_id=$1", A) == 0


async def test_open_project_card_is_readonly_and_explicit_continue_is_idempotent(store):
    active = await project(store, name="Экзамен")
    transport = SimpleNamespace(
        topic_capabilities=AsyncMock(return_value=SimpleNamespace(has_topics_enabled=True)),
        create_topic=AsyncMock(return_value={"message_thread_id": 770, "name": "Экзамен"}),
    )
    card = await home_project_panel(store, A, active["id"])
    assert "home:continue:" in str(card["reply_markup"])
    transport.create_topic.assert_not_awaited()
    event_id = uuid4()
    async with store.user_lock(A):
        first = await continue_home_project(store, transport, A, A, active["id"], event_id)
        second = await continue_home_project(store, transport, A, A, active["id"], event_id)
    assert first == second and "Чат проекта готов" in first["text"]
    transport.create_topic.assert_awaited_once_with(A, "Экзамен")
    current = await store.get_project(A, project_id=active["id"])
    assert len(current["conversation_ids"]) == 2 and current["revision"] == active["revision"] + 1
    async with store.connection(A) as conn:
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM conversations WHERE user_id=$1 AND thread_id=770", A
            )
            == 1
        )
        rows = await conn.fetch("SELECT thread_id,payload FROM outbox WHERE user_id=$1", A)
        assert len(rows) == 1 and rows[0]["thread_id"] == 770
        assert "Подготовить первый раздел" in rows[0]["payload"]["text"]


async def test_ambiguous_create_is_not_repeated_or_attached(store):
    active = await project(store)
    transport = SimpleNamespace(
        topic_capabilities=AsyncMock(return_value=SimpleNamespace(has_topics_enabled=True)),
        create_topic=AsyncMock(side_effect=TimeoutError()),
    )
    event_id = uuid4()
    for _ in range(2):
        result = await continue_home_project(store, transport, A, A, active["id"], event_id)
        assert "не подтвердил" in result["text"]
    transport.create_topic.assert_awaited_once()
    current = await store.get_project(A, project_id=active["id"])
    assert len(current["conversation_ids"]) == 1
    async with store.connection(A) as conn:
        assert await conn.fetchval("SELECT count(*) FROM outbox WHERE user_id=$1", A) == 0
