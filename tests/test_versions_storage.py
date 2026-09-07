"""Version lineage on real PostgreSQL, confined to two synthetic owners."""

import asyncio
import os
from uuid import uuid4

import asyncpg
import pytest
import pytest_asyncio

from cronos.settings import Settings
from cronos.storage import Store
from cronos.versions import ArtifactVersionConflict, VersionsStoreMixin

A, B = -986401, -986402
pytestmark = pytest.mark.asyncio(loop_scope="module")


class VersionStore(Store, VersionsStoreMixin):
    pass


async def cleanup(store, owners):
    for owner in owners:
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
    if not os.getenv("DATABASE_URL"):
        pytest.skip("Real PostgreSQL DATABASE_URL required; versions.sql must already be applied")
    value = VersionStore(Settings())
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
                assert inserted is not None, (
                    "Existing synthetic version owner must be inspected, not overwritten"
                )
            created.append(owner)
        yield
    finally:
        await cleanup(store, created)


async def artifact(store, owner=A):
    identifier = uuid4()
    async with store.connection(owner) as conn:
        await conn.execute(
            "INSERT INTO artifacts(id,user_id,filename,mime,path) VALUES($1,$2,'synthetic.txt','text/plain',$3)",
            identifier,
            owner,
            f"/synthetic/{identifier}.txt",
        )
    return identifier


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


async def test_branch_from_uploaded_original_is_atomic_and_keeps_files(store):
    original, second, branch, reference = [await artifact(store) for _ in range(4)]
    source = await run(store)
    v2 = await store.register_artifact_version(
        A,
        second,
        parent_artifact_id=original,
        reference_artifact_ids=[reference, original],
        change_summary="Изменено вступление",
        source_key="second",
        run=source,
    )
    v3 = await store.register_artifact_version(
        A,
        branch,
        parent_artifact_id=original,
        change_summary="Другая ветка",
        source_key="branch",
        run=source,
    )
    history = await store.artifact_versions(A, second)
    assert [row["version"] for row in history] == [1, 2, 3]
    assert [row["artifact_id"] for row in history] == [str(original), str(second), str(branch)]
    assert v2["family_id"] == v3["family_id"]
    assert v3["parent_artifact_id"] == str(original)
    assert history[1]["reference_artifact_ids"] == [str(reference), str(original)]
    assert history[0]["parent_artifact_id"] is None
    assert all(row["current_artifact_id"] == str(branch) for row in history)
    assert [
        row["version"] for row in await store.artifact_versions(A, original, limit=1, offset=1)
    ] == [2]
    async with store.connection(A) as conn:
        assert await conn.fetchval("SELECT count(*) FROM artifacts WHERE user_id=$1", A) == 4


async def test_restore_and_late_replays_never_rewind_newer_current_pointer(store):
    original, second, latest = [await artifact(store) for _ in range(3)]
    source = await run(store)
    await store.register_artifact_version(
        A, second, parent_artifact_id=original, source_key="register", run=source
    )
    restored = await store.restore_artifact_version(A, original, source_key="restore", run=source)
    assert restored["is_current"] and restored["version"] == 1
    await store.register_artifact_version(
        A, latest, parent_artifact_id=second, source_key="latest", run=source
    )
    replay = await store.restore_artifact_version(A, original, source_key="restore", run=source)
    assert not replay["is_current"] and replay["current_artifact_id"] == str(latest)
    replay = await store.register_artifact_version(
        A, second, parent_artifact_id=original, source_key="register", run=source
    )
    assert replay["current_artifact_id"] == str(latest)
    assert len(await store.artifact_versions(A, latest)) == 3
    async with store.connection(A) as conn:
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM artifact_version_operations WHERE user_id=$1", A
            )
            == 3
        )
    with pytest.raises(ArtifactVersionConflict):
        await store.restore_artifact_version(A, latest, source_key="register", run=source)


async def test_concurrent_registration_allocates_one_monotonic_family(store):
    original, left, right = [await artifact(store) for _ in range(3)]
    results = await asyncio.gather(
        *[
            store.register_artifact_version(
                A, child, parent_artifact_id=original, source_key=f"child:{child}"
            )
            for child in (left, right)
        ]
    )
    assert sorted(row["version"] for row in results) == [2, 3]
    assert len({row["family_id"] for row in results}) == 1
    await asyncio.gather(
        *[
            store.register_artifact_version(
                A, left, parent_artifact_id=original, source_key=f"child:{left}"
            )
            for _ in range(2)
        ]
    )
    assert len(await store.artifact_versions(A, original)) == 3


async def test_foreign_dependencies_and_sql_owner_fks_cannot_create_partial_family(store):
    original, result, foreign = (
        await artifact(store),
        await artifact(store),
        await artifact(store, B),
    )
    conversation = await store.conversation(B, B, 20)
    project = await store.create_project(
        B, conversation["id"], {"name": "Foreign project"}, "project"
    )
    for index, extra in enumerate(
        (
            {"parent_artifact_id": foreign},
            {"parent_artifact_id": original, "reference_artifact_ids": [foreign]},
            {"parent_artifact_id": original, "project_id": project["id"]},
        )
    ):
        with pytest.raises(ValueError):
            await store.register_artifact_version(A, result, source_key=f"foreign:{index}", **extra)
    async with store.connection(A) as conn:
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM artifact_version_families WHERE user_id=$1", A
            )
            == 0
        )
    own_version = await store.register_artifact_version(A, original, source_key="own")
    assert await store.get_artifact_version(B, original) is None
    assert await store.artifact_versions(B, original) == []
    async with store.connection(B) as conn:
        assert await conn.fetchval("SELECT count(*) FROM artifact_versions") == 0
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            async with conn.transaction():
                await conn.execute(
                    "INSERT INTO artifact_version_families(user_id,id,root_artifact_id) VALUES($1,$2,$3)",
                    B,
                    uuid4(),
                    original,
                )
    with pytest.raises(ArtifactVersionConflict):
        await store.register_artifact_version(
            A, original, change_summary="Переписать прошлое", source_key="rewrite"
        )
    async with store.connection(A) as conn:
        with pytest.raises(asyncpg.RaiseError, match="immutable"):
            async with conn.transaction():
                await conn.execute(
                    "UPDATE artifact_versions SET version=99 WHERE user_id=$1 AND artifact_id=$2",
                    A,
                    original,
                )
    assert (await store.get_artifact_version(A, original))["version"] == own_version["version"]


async def test_stale_cancelled_and_pre_reset_runs_cannot_change_versions(store):
    item = await artifact(store)
    source = await run(store)
    with pytest.raises(ValueError):
        await store.register_artifact_version(
            A, item, source_key="stale", run={**source, "fence": source["fence"] + 1}
        )
    async with store.connection(A) as conn:
        await conn.execute(
            "UPDATE runs SET cancel_requested=true WHERE id=$1 AND user_id=$2", source["id"], A
        )
    with pytest.raises(ValueError):
        await store.register_artifact_version(A, item, source_key="cancelled", run=source)
    async with store.connection(A) as conn:
        await conn.execute(
            "UPDATE runs SET cancel_requested=false WHERE id=$1 AND user_id=$2", source["id"], A
        )
        await conn.execute(
            "UPDATE users SET memory_revision=memory_revision+1,content_reset_at=clock_timestamp() WHERE user_id=$1",
            A,
        )
    with pytest.raises(ValueError, match="context reset"):
        await store.register_artifact_version(A, item, source_key="reset", run=source)
    assert await store.get_artifact_version(A, item) is None


async def test_forgotten_summary_stays_hidden_on_list_restore_and_replay(store):
    item = await artifact(store)
    source = await run(store)
    await store.register_artifact_version(
        A, item, change_summary="Личная подробность", source_key="version", run=source
    )
    async with store.connection(A) as conn:
        await conn.execute("UPDATE users SET memory_revision=memory_revision+1 WHERE user_id=$1", A)
    fresh_run = await run(store)
    values = [
        await store.get_artifact_version(A, item),
        *(await store.artifact_versions(A, item)),
        await store.restore_artifact_version(A, item, source_key="restore", run=fresh_run),
        await store.register_artifact_version(A, item, source_key="version", run=fresh_run),
    ]
    assert all(value["context_excluded"] and value["change_summary"] == "" for value in values)
    assert all(value["artifact_id"] == str(item) for value in values)
    assert await store.get_artifact(A, item)


async def test_deleted_source_hides_summary_but_keeps_original_artifact(store):
    item = await artifact(store)
    source = await run(store)
    await store.register_artifact_version(
        A, item, change_summary="Из удалённого чата", source_key="version", run=source
    )
    async with store.connection(A) as conn:
        await conn.execute("DELETE FROM runs WHERE user_id=$1 AND id=$2", A, source["id"])
        await conn.execute(
            "DELETE FROM conversations WHERE user_id=$1 AND id=$2", A, source["conversation_id"]
        )
    value = await store.get_artifact_version(A, item)
    assert value["context_excluded"] and not value["change_summary"]
    assert await store.get_artifact(A, item)


async def test_full_artifact_wipe_cascades_families_receipts_and_preserves_other_owner(store):
    original, child, reference = [await artifact(store) for _ in range(3)]
    foreign = await artifact(store, B)
    await store.register_artifact_version(
        A,
        child,
        parent_artifact_id=original,
        reference_artifact_ids=[reference],
        source_key="version",
    )
    await store.restore_artifact_version(A, original, source_key="restore", run=await run(store))
    other = await store.register_artifact_version(B, foreign, source_key="version")
    async with store.connection(A) as conn:
        await conn.execute("DELETE FROM artifacts WHERE user_id=$1", A)
        for table in (
            "artifact_version_families",
            "artifact_versions",
            "artifact_version_heads",
            "artifact_version_references",
            "artifact_version_operations",
        ):
            assert await conn.fetchval(f"SELECT count(*) FROM {table} WHERE user_id=$1", A) == 0
        assert await conn.fetchval("SELECT 1 FROM users WHERE user_id=$1", A) == 1
    assert (await store.get_artifact_version(B, foreign))["family_id"] == other["family_id"]
    assert await store.get_artifact(B, foreign)
