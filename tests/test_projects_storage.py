"""Project persistence against isolated PostgreSQL and dedicated synthetic owners."""

import asyncio
import os
from pathlib import Path
from uuid import UUID, uuid4

import asyncpg
import pytest
import pytest_asyncio

from cronos.projects import ProjectRevisionConflict
from cronos.settings import Settings
from cronos.storage import Store, privacy_event_id

A, B = -984001, -984002
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
                # Deleting users must cascade projects, including detached ones.
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
                    "Synthetic project owner already exists; inspect before rerunning"
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


async def create(store, owner=A, thread=10, **args):
    conversation = await store.conversation(owner, owner, thread, "Project conversation")
    project = await store.create_project(
        owner,
        conversation["id"],
        {"name": "Подготовка к экзамену", "goal": "Сдать в июне", **args},
        f"create:{uuid4()}",
    )
    return project, conversation


async def source_run(store, conversation):
    event_id = uuid4()
    async with store.connection() as conn:
        await conn.execute(
            "INSERT INTO events(id,payload) VALUES($1,$2)",
            event_id,
            {"user_id": conversation["user_id"]},
        )
    return await store.start_run(event_id, conversation["user_id"], conversation["id"])


async def artifact(store, owner=A):
    artifact_id = uuid4()
    async with store.connection(owner) as conn:
        await conn.execute(
            """INSERT INTO artifacts(id,user_id,filename,mime,path)
            VALUES($1,$2,'plan.txt','text/plain','/synthetic/project-plan.txt')""",
            artifact_id,
            owner,
        )
    return artifact_id


async def change(store, project, conversation, source="update", **patch):
    return await store.update_project(
        conversation["user_id"],
        project["id"],
        {"revision": project["revision"], "conversation_id": str(conversation["id"]), **patch},
        source,
    )


async def test_create_replay_and_current_project_lookup(store):
    conversation = await store.conversation(A, A, 10)
    args = {"name": "Здоровый сон", "goal": "Спать восемь часов", "state": {"summary": "Начинаем"}}
    project = await store.create_project(A, conversation["id"], args, "same-create")
    replay = await store.create_project(A, conversation["id"], args, "same-create")
    assert project == replay
    assert project["state"] == {
        "summary": "Начинаем",
        "next_step": "",
        "constraints": [],
        "decisions": [],
        "open_questions": [],
    }
    assert project["revision"] == 1 and len(project["history"]) == 1
    assert project["conversation_ids"] == [str(conversation["id"])]
    assert await store.get_project(A, conversation_id=conversation["id"]) == project
    assert await store.get_project(A, project_id=project["id"]) == project
    assert await store.get_project(B, project_id=project["id"]) is None
    assert await store.get_project(B, conversation_id=conversation["id"]) is None


async def test_partial_state_update_cas_and_late_replay_preserve_latest_data(store):
    original, conversation = await create(
        store,
        state={
            "summary": "План",
            "constraints": ["20 минут в день"],
            "decisions": ["Учиться утром"],
        },
    )
    first = await change(
        store, original, conversation, "first", state={"next_step": "Прочитать главу"}
    )
    latest = await change(
        store,
        first,
        conversation,
        "second",
        status="paused",
        state={"open_questions": ["Дата экзамена?"]},
    )
    assert latest["goal"] == original["goal"] and latest["name"] == original["name"]
    assert latest["state"] == {
        "summary": "План",
        "constraints": ["20 минут в день"],
        "decisions": ["Учиться утром"],
        "next_step": "Прочитать главу",
        "open_questions": ["Дата экзамена?"],
    }
    replay = await change(
        store, original, conversation, "first", state={"next_step": "Прочитать главу"}
    )
    assert replay == latest and replay["revision"] == 3
    with pytest.raises(ProjectRevisionConflict):
        await change(store, original, conversation, "stale", name="Старое имя")
    assert await store.get_project(A, project_id=original["id"]) == latest
    assert latest["history"][-1]["patch"] == {
        "status": "paused",
        "state": {"open_questions": ["Дата экзамена?"]},
    }


async def test_concurrent_same_revision_has_exactly_one_winner(store):
    project, conversation = await create(store)
    result = await asyncio.gather(
        change(store, project, conversation, "racer-a", name="A"),
        change(store, project, conversation, "racer-b", name="B"),
        return_exceptions=True,
    )
    assert sum(isinstance(value, dict) for value in result) == 1
    assert sum(isinstance(value, ProjectRevisionConflict) for value in result) == 1
    current = await store.get_project(A, project_id=project["id"])
    assert current["revision"] == 2 and len(current["history"]) == 2


async def test_list_includes_paused_and_paginates_completed_explicitly(store):
    for index, status in enumerate(("active", "paused", "completed")):
        await create(store, thread=index + 10, name=status, status=status)
    assert {row["status"] for row in await store.list_projects(A)} == {"active", "paused"}
    all_projects = await store.list_projects(A, include_archived=True)
    assert len(all_projects) == 3 and await store.list_projects(B) == []
    pages = [await store.list_projects(A, True, limit=1, offset=index) for index in range(3)]
    assert [page[0]["id"] for page in pages] == [row["id"] for row in all_projects]
    assert await store.list_projects(A, True, offset=3) == []
    with pytest.raises(ValueError):
        await store.list_projects(A, limit=101)


async def test_multiple_links_file_ownership_and_idempotency(store):
    project, conversation = await create(store)
    extra = await store.conversation(A, A, 11)
    foreign = await store.conversation(B, B, 10)
    linked = await store.attach_project(A, project["id"], extra["id"], source_key="attach")
    assert len(linked["conversation_ids"]) == 2
    assert await store.attach_project(A, project["id"], extra["id"], source_key="attach") == linked
    for owner, target in ((A, foreign["id"]), (B, foreign["id"])):
        with pytest.raises(ValueError):
            await store.attach_project(owner, project["id"], target)
    own_file, foreign_file = await artifact(store), await artifact(store, B)
    result = await store.attach_project_artifact(
        A, project["id"], own_file, conversation_id=conversation["id"], source_key="file"
    )
    assert result["artifact_ids"] == [str(own_file)]
    assert (
        await store.attach_project_artifact(
            A, project["id"], own_file, conversation_id=conversation["id"], source_key="file"
        )
        == result
    )
    assert await store.attach_project_artifact(A, project["id"], own_file) == result
    with pytest.raises(ValueError):
        await store.attach_project_artifact(A, project["id"], foreign_file)


async def test_detach_and_old_attach_replay_cannot_replace_new_binding(store):
    first, origin = await create(store)
    second, other = await create(store, thread=11)
    target = await store.conversation(A, A, 12)
    await store.attach_project(A, first["id"], target["id"], source_key="old-attach")
    await store.detach_project(A, target["id"], source_key="old-detach")
    await store.attach_project(A, second["id"], target["id"], source_key="new-attach")
    await store.detach_project(A, target["id"], source_key="old-detach")
    await store.attach_project(A, first["id"], target["id"], source_key="old-attach")
    assert (await store.get_project(A, conversation_id=target["id"]))["id"] == second["id"]
    assert await store.get_project(A, conversation_id=origin["id"])
    assert await store.get_project(A, conversation_id=other["id"])


async def test_noop_detach_receipt_cannot_remove_subsequent_attachment(store):
    project, _ = await create(store)
    target = await store.conversation(A, A, 12)
    assert await store.detach_project(A, target["id"], source_key="empty-detach") is None
    await store.attach_project(A, project["id"], target["id"])
    assert await store.detach_project(A, target["id"], source_key="empty-detach") is None
    assert (await store.get_project(A, conversation_id=target["id"]))["id"] == project["id"]


async def test_detach_reattach_without_key_and_conflicting_key(store):
    project, origin = await create(store)
    target = await store.conversation(A, A, 12)
    await store.attach_project(A, project["id"], target["id"])
    await store.detach_project(A, target["id"])
    linked = await store.attach_project(A, project["id"], target["id"])
    assert str(target["id"]) in linked["conversation_ids"]
    await change(store, linked, origin, "reserved-key", name="Новое имя")
    with pytest.raises(ValueError, match="another operation"):
        await store.detach_project(A, target["id"], source_key="reserved-key")
    assert await store.get_project(A, conversation_id=target["id"])


async def test_rls_and_composite_foreign_keys_prevent_cross_owner_sql_links(store):
    project, conversation = await create(store)
    foreign_project, foreign_conversation = await create(store, B)
    foreign_file = await artifact(store, B)
    async with store.connection(B) as conn:
        for table in ("projects", "project_conversations", "project_changes"):
            assert not await conn.fetchval(f"SELECT 1 FROM {table} WHERE user_id=$1", A)
    async with store.connection() as conn:
        assert await conn.fetchval("SELECT count(*) FROM projects") == 0
    for query, args in (
        (
            "INSERT INTO project_conversations(user_id,project_id,conversation_id) VALUES($1,$2,$3)",
            (A, UUID(project["id"]), foreign_conversation["id"]),
        ),
        (
            "INSERT INTO project_artifacts(user_id,project_id,artifact_id) VALUES($1,$2,$3)",
            (A, UUID(project["id"]), foreign_file),
        ),
        (
            "INSERT INTO project_artifacts(user_id,project_id,artifact_id) VALUES($1,$2,$3)",
            (A, UUID(foreign_project["id"]), foreign_file),
        ),
    ):
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            async with store.connection(A) as conn:
                await conn.execute(query, *args)
    with pytest.raises(asyncpg.InsufficientPrivilegeError):
        async with store.connection(B) as conn:
            await conn.execute(
                "INSERT INTO projects(id,user_id,name) VALUES($1,$2,'Forbidden')", uuid4(), A
            )
    assert await store.get_project(A, conversation_id=conversation["id"])


async def test_run_provenance_and_fence_are_checked_atomically(store):
    conversation = await store.conversation(A, A, 10)
    run = await source_run(store, conversation)
    args = {"name": "Учёба", "run_id": str(run["id"]), "run_fence": run["fence"]}
    project = await store.create_project(A, conversation["id"], args, "run-create")
    assert project["history"][0]["run_id"] == str(run["id"])
    target = await store.conversation(A, A, 11)
    async with store.connection() as conn:
        await conn.execute("UPDATE runs SET cancel_requested=true WHERE id=$1", run["id"])
    with pytest.raises(ValueError, match="no longer active"):
        await store.attach_project(
            A,
            project["id"],
            target["id"],
            run_id=run["id"],
            run_fence=run["fence"],
            source_key="cancelled",
        )
    async with store.connection() as conn:
        await conn.execute(
            "UPDATE runs SET cancel_requested=false,fence=fence+1 WHERE id=$1", run["id"]
        )
    with pytest.raises(ValueError, match="no longer active"):
        await change(
            store,
            project,
            conversation,
            "stale-fence",
            name="Stale",
            run_id=run["id"],
            run_fence=run["fence"],
        )
    assert (await store.get_project(A, project_id=project["id"]))["revision"] == 1


async def test_foreign_run_and_wrong_source_conversation_are_rejected(store):
    project, conversation = await create(store)
    foreign = await store.conversation(B, B, 10)
    run = await source_run(store, foreign)
    with pytest.raises(ValueError, match="source run"):
        await change(
            store,
            project,
            conversation,
            "foreign-run",
            name="No",
            run_id=run["id"],
            run_fence=run["fence"],
        )
    other = await store.conversation(A, A, 11)
    own_run = await source_run(store, other)
    with pytest.raises(ValueError, match="another conversation"):
        await change(
            store,
            project,
            conversation,
            "wrong-origin",
            name="No",
            run_id=own_run["id"],
            run_fence=own_run["fence"],
        )


async def test_chat_deletion_unlinks_and_removes_only_its_origin_history(store):
    project, origin = await create(store, state={"summary": "Общий план"})
    other = await store.conversation(A, A, 11)
    await store.attach_project(A, project["id"], other["id"])
    current = await store.get_project(A, project_id=project["id"])
    await change(store, current, other, "other-source", state={"next_step": "Продолжить"})
    file_id = await artifact(store)
    await store.attach_project_artifact(A, project["id"], file_id, conversation_id=origin["id"])
    async with store.connection(A) as conn:
        await conn.execute("DELETE FROM conversations WHERE user_id=$1 AND id=$2", A, origin["id"])
    result = await store.get_project(A, project_id=project["id"])
    assert result["conversation_ids"] == [str(other["id"])]
    assert result["artifact_ids"] == [str(file_id)]
    assert result["state"]["summary"] == "Общий план"
    assert all(row["conversation_id"] == str(other["id"]) for row in result["history"])
    async with store.connection(A) as conn:
        assert await conn.fetchval("SELECT count(*) FROM project_changes WHERE user_id=$1", A) == 2


async def test_full_privacy_erasure_removes_projects_but_preserves_billing_and_other_owner(store):
    project, conversation = await create(store)
    other_project, _ = await create(store, B)
    before = await store.ensure_user(A)
    request = await store.requests_prepare(A, conversation, "all", source_key="project-full-wipe")
    assert await store.confirm_privacy_request(A, request["id"], request["origin_thread_id"])
    await store.begin_privacy_erasure(request["id"], privacy_event_id(request["id"]))
    assert await store.get_project(A, project_id=project["id"]) is None
    assert await store.get_project(B, project_id=other_project["id"])
    after = await store.ensure_user(A)
    for key in ("plan", "balance_micro", "reserved_micro", "entitlement_micro", "topup_micro"):
        assert after[key] == before[key]
    async with store.connection(A) as conn:
        for table in ("projects", "project_conversations", "project_artifacts", "project_changes"):
            assert await conn.fetchval(f"SELECT count(*) FROM {table} WHERE user_id=$1", A) == 0


async def test_erasing_owner_cannot_create_or_update_projects(store):
    project, conversation = await create(store)
    async with store.connection() as conn:
        await conn.execute(
            """INSERT INTO privacy_requests(id,user_id,scope,chat_id,source_key,state)
            VALUES($1,$2,'all',$2,'synthetic-erasing','erasing')""",
            uuid4(),
            A,
        )
    with pytest.raises(ValueError, match="erasure"):
        await change(store, project, conversation, name="Blocked")
    with pytest.raises(ValueError, match="erasure"):
        await store.create_project(A, conversation["id"], {"name": "Blocked"}, "blocked-create")


async def test_migration_is_repeatable_without_changing_existing_project(store):
    project, _ = await create(store)
    conn = await asyncpg.connect(os.environ["ADMIN_DATABASE_URL"])
    try:
        sql = Path(__file__).parents[1].joinpath("src/cronos/projects.sql").read_text()
        for _ in range(2):
            async with conn.transaction():
                await conn.execute(sql)
    finally:
        await conn.close()
    assert await store.get_project(A, project_id=project["id"]) == project


@pytest.mark.parametrize(
    "patch",
    [
        {"name": " "},
        {"status": "unknown"},
        {"state": {"decisions": "not a list"}},
        {"state": {"summary": 1}},
        {"state": {"unknown": "value"}},
        {"revision": True},
    ],
)
async def test_invalid_updates_do_not_modify_state(store, patch):
    project, conversation = await create(store)
    with pytest.raises(ValueError):
        await change(store, project, conversation, **patch)
    assert await store.get_project(A, project_id=project["id"]) == project
