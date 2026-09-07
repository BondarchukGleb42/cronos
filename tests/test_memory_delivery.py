"""Real local PostgreSQL checks for forgetting derived context and pending delivery."""

import asyncio
import json
import os
from types import SimpleNamespace
from unittest.mock import AsyncMock
from urllib.parse import urlsplit
from uuid import UUID, uuid4

import pytest

from cronos.coordinator import Coordinator
from cronos.memory_privacy import invalidate_memory_context
from cronos.project_tools import project_context
from cronos.settings import Settings
from cronos.storage import Store

A, B = 931071000001, 931071000002
FACT = "synthetic forgotten project secret"


@pytest.fixture
async def case(tmp_path):
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        pytest.skip("Real local PostgreSQL DATABASE_URL is required")
    parsed = urlsplit(database_url)
    assert parsed.hostname in {"127.0.0.1", "localhost"} and parsed.path == "/cronos_test"
    store = Store(Settings(database_url=database_url))
    await store.open()
    created_users, event_ids = [], []
    try:
        for user_id in (A, B):
            async with store.connection(user_id) as conn:
                if await conn.fetchval("SELECT 1 FROM users WHERE user_id=$1", user_id):
                    pytest.fail("Memory test user already exists; do not erase another test's data")
            await store.ensure_user(user_id)
            created_users.append(user_id)
        conversation = await store.conversation(A, A, 77)
        other = await store.conversation(B, B, 88)
        await store.remember(A, FACT)
        await store.add_message(A, conversation["id"], "assistant", FACT)
        project = await store.create_project(
            A,
            conversation["id"],
            {
                "name": FACT,
                "goal": FACT,
                "state": {
                    "constraints": [FACT],
                    "decisions": [FACT],
                    "summary": FACT,
                },
            },
            f"memory-project:{uuid4()}",
        )

        async def make_run():
            event_id = uuid4()
            event_ids.append(event_id)
            async with store.connection() as conn:
                await conn.execute(
                    "INSERT INTO events(id,payload,state) VALUES($1,'{}','done')", event_id
                )
            return await store.start_run(event_id, A, conversation["id"])

        yield SimpleNamespace(
            store=store,
            conversation=conversation,
            other=other,
            project=project,
            make_run=make_run,
            tmp_path=tmp_path,
        )
    finally:
        for user_id in created_users:
            async with store.connection(user_id) as conn:
                await conn.execute(
                    "DELETE FROM occurrences WHERE schedule_id IN (SELECT id FROM schedules WHERE user_id=$1)",
                    user_id,
                )
                for table in (
                    "outbox",
                    "usage",
                    "ledger",
                    "reservations",
                    "operations",
                    "run_metrics",
                    "runs",
                    "schedules",
                    "messages",
                    "memory",
                    "projects",
                    "artifacts",
                    "conversations",
                    "privacy_requests",
                    "deleted_topics",
                    "users",
                ):
                    await conn.execute(f"DELETE FROM {table} WHERE user_id=$1", user_id)
                await conn.execute(
                    "DELETE FROM events WHERE id=ANY($1::uuid[]) OR (kind IN ('topic_title','topic_title_reset') AND payload->>'user_id'=$2)",
                    event_ids,
                    str(user_id),
                )
        await store.close()


async def delivery(store, user_id=A, thread=77, *, state="pending", owner=None):
    delivery_id = await store.enqueue(
        user_id, user_id, thread, {"text": FACT}, f"memory-delivery:{uuid4()}"
    )
    async with store.connection(user_id) as conn:
        await conn.execute(
            "UPDATE outbox SET state=$2,owner=$3,lease_until=now()+interval '1 minute' WHERE id=$1",
            delivery_id,
            state,
            owner,
        )
    return delivery_id


async def test_forget_cancels_queued_content_but_preserves_other_owner(case):
    first = await delivery(case.store)
    other = await delivery(case.store, B, 88)
    await case.store.forget(A, FACT)
    async with case.store.connection(A) as conn:
        row = await conn.fetchrow(
            "SELECT state,payload,owner,lease_until FROM outbox WHERE id=$1", first
        )
        assert dict(row) == {
            "state": "cancelled",
            "payload": {},
            "owner": None,
            "lease_until": None,
        }
        assert await conn.fetchval("SELECT count(*) FROM memory WHERE user_id=$1", A) == 0
        assert await conn.fetchval("SELECT bool_and(excluded) FROM messages WHERE user_id=$1", A)
    async with case.store.connection(B) as conn:
        row = await conn.fetchrow("SELECT state,payload FROM outbox WHERE id=$1", other)
        assert row["state"] == "pending" and row["payload"] == {"text": FACT}


async def test_sending_row_claimed_before_forget_is_fenced_before_telegram(case, monkeypatch):
    owner = "qa-memory-sender"
    delivery_id = await delivery(case.store, state="sending", owner=owner)
    stale = await case.store.claimed_delivery(delivery_id, owner)
    assert stale is not None
    await case.store.forget(A, FACT)
    coordinator = Coordinator.__new__(Coordinator)
    coordinator.store, coordinator.owner = case.store, owner
    coordinator.transport = SimpleNamespace(send=AsyncMock())
    coordinator.redis = SimpleNamespace(set=AsyncMock(return_value=True))
    monkeypatch.setattr(case.store, "next_delivery", AsyncMock(return_value=stale))
    assert await coordinator.delivery() is True
    coordinator.transport.send.assert_not_awaited()
    assert await case.store.claimed_delivery(delivery_id, owner) is None


async def test_forget_waits_for_delivery_lock_before_acknowledging(case):
    await delivery(case.store)
    task = None
    try:
        async with case.store.user_lock(A, purpose="delivery"):
            task = asyncio.create_task(case.store.forget(A, FACT))
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(asyncio.shield(task), timeout=0.1)
            async with case.store.connection(A) as conn:
                assert await conn.fetchval("SELECT count(*) FROM memory WHERE user_id=$1", A) == 1
        await asyncio.wait_for(task, timeout=2)
        async with case.store.connection(A) as conn:
            assert (
                await conn.fetchval(
                    "SELECT count(*) FROM outbox WHERE user_id=$1 AND state='pending'", A
                )
                == 0
            )
    finally:
        if task and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


async def test_invalidation_rolls_back_with_the_surrounding_forget_transaction(case):
    delivery_id = await delivery(case.store)
    with pytest.raises(RuntimeError, match="rollback probe"):
        async with case.store.user_lock(A, purpose="delivery"), case.store.connection(A) as conn:
            await invalidate_memory_context(conn, A)
            raise RuntimeError("rollback probe")
    async with case.store.connection(A) as conn:
        assert await conn.fetchval("SELECT state FROM outbox WHERE id=$1", delivery_id) == "pending"
        project = await conn.fetchrow(
            "SELECT context_excluded,revision,context_reset_revision FROM projects WHERE id=$1",
            UUID(case.project["id"]),
        )
        assert dict(project) == {
            "context_excluded": False,
            "revision": 1,
            "context_reset_revision": 0,
        }


async def test_current_forget_run_can_confirm_but_older_run_cannot_enqueue(case):
    old_run, current_run = await case.make_run(), await case.make_run()
    await case.store.enqueue_for_run(old_run, case.conversation, {"text": FACT}, f"old:{uuid4()}")
    await case.store.forget(A, FACT, current_run=current_run)
    assert (
        await case.store.enqueue_for_run(
            old_run, case.conversation, {"text": FACT}, f"late:{uuid4()}"
        )
        is None
    )
    confirmation = await case.store.enqueue_for_run(
        current_run, case.conversation, {"text": "Контекст сброшен"}, f"new:{uuid4()}"
    )
    assert confirmation is not None
    async with case.store.connection(A) as conn:
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM outbox WHERE user_id=$1 AND state='pending'", A
            )
            == 1
        )


async def test_forgotten_project_is_hidden_from_implicit_context_and_explicit_detail(case):
    await case.store.forget(A, FACT)
    implicit = await project_context(case.store, A, case.conversation["id"])
    assert implicit["current"] is None
    assert all(row["id"] != case.project["id"] for row in implicit["available"])
    explicit = await case.store.get_project(A, project_id=case.project["id"])
    assert set(explicit) == {"id", "revision", "status", "needs_context"}
    assert explicit["needs_context"] is True
    assert explicit["revision"] > case.project["revision"]
    assert FACT not in json.dumps(explicit)


async def test_explicit_project_update_restores_only_new_state_and_new_history(case):
    await case.store.forget(A, FACT)
    excluded = await case.store.get_project(A, project_id=case.project["id"])
    updated = await case.store.update_project(
        A,
        case.project["id"],
        {
            "revision": excluded["revision"],
            "conversation_id": case.conversation["id"],
            "name": "Новый проект",
            "goal": "Новая задача",
            "state": {"decisions": ["Новое решение"]},
        },
        f"new-project-context:{uuid4()}",
    )
    assert FACT not in json.dumps(updated)
    assert updated["state"]["constraints"] == []
    assert updated["state"]["decisions"] == ["Новое решение"]
    assert updated["state"]["summary"] == ""
    assert all(change["revision"] > excluded["revision"] for change in updated["history"])
    implicit = await project_context(case.store, A, case.conversation["id"])
    assert implicit["current"]["name"] == "Новый проект"
    assert FACT not in json.dumps(implicit)


async def test_forget_receipt_replay_does_not_cancel_new_delivery_or_new_project_state(case):
    run = await case.make_run()
    source_key = f"forget-replay:{uuid4()}"
    first = await case.store.forget(
        A, FACT, current_run=run["id"], source_key=source_key, run_fence=run["fence"]
    )
    excluded = await case.store.get_project(A, project_id=case.project["id"])
    updated = await case.store.update_project(
        A,
        case.project["id"],
        {
            "revision": excluded["revision"],
            "conversation_id": case.conversation["id"],
            "name": "Новый проект",
            "goal": "Новая цель",
        },
        f"project-after-forget:{uuid4()}",
    )
    new_delivery = await case.store.enqueue_for_run(
        run, case.conversation, {"text": "Новое подтверждение"}, f"new-after-forget:{uuid4()}"
    )
    before = await case.store.ensure_user(A)
    replay = await case.store.forget(
        A, FACT, current_run=run["id"], source_key=source_key, run_fence=run["fence"]
    )
    assert replay == first
    assert (await case.store.ensure_user(A))["memory_revision"] == before["memory_revision"]
    project = await case.store.get_project(A, project_id=case.project["id"])
    assert project["name"] == "Новый проект" and project["revision"] == updated["revision"]
    async with case.store.connection(A) as conn:
        assert (
            await conn.fetchval("SELECT state FROM outbox WHERE id=$1", new_delivery) == "pending"
        )


async def test_forget_preserves_original_file_links_and_financial_records(case):
    artifact_id = str(uuid4())
    path = case.tmp_path / "original.txt"
    await asyncio.to_thread(path.write_text, FACT)
    await case.store.save_artifact(
        A,
        {
            "id": artifact_id,
            "filename": "original.txt",
            "mime": "text/plain",
            "path": str(path),
            "extracted": {"text": FACT},
            "size_bytes": len(FACT),
        },
    )
    await case.store.attach_project_artifact(
        A,
        case.project["id"],
        artifact_id,
        conversation_id=case.conversation["id"],
        source_key=f"original-file:{uuid4()}",
    )
    await case.store.reserve(A, f"billing:{uuid4()}", 500)
    async with case.store.connection(A) as conn:
        await conn.execute(
            "INSERT INTO ledger(operation_id,user_id,kind,amount_micro,description) VALUES($1,$2,'test',500,'Account transaction')",
            f"ledger:{uuid4()}",
            A,
        )
        before = dict(
            await conn.fetchrow(
                "SELECT plan,balance_micro,reserved_micro,topup_micro FROM users WHERE user_id=$1",
                A,
            )
        )
    await case.store.forget(A, FACT)
    assert await asyncio.to_thread(path.read_text) == FACT
    artifact = await case.store.get_artifact(A, artifact_id)
    assert artifact["path"] == str(path) and artifact["extracted"] == {"text": FACT}
    async with case.store.connection(A) as conn:
        assert (
            dict(
                await conn.fetchrow(
                    "SELECT plan,balance_micro,reserved_micro,topup_micro FROM users WHERE user_id=$1",
                    A,
                )
            )
            == before
        )
        assert await conn.fetchval("SELECT count(*) FROM reservations WHERE user_id=$1", A) == 1
        assert await conn.fetchval("SELECT count(*) FROM ledger WHERE user_id=$1", A) == 1
        assert (
            await conn.fetchval("SELECT count(*) FROM project_artifacts WHERE user_id=$1", A) == 1
        )
