"""Real PostgreSQL tests; every user-owned write is limited to the two IDs below."""

import asyncio
import os
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import asyncpg
import pytest
import pytest_asyncio
from dateutil.relativedelta import relativedelta

from cronos.settings import Settings
from cronos.storage import PLANS, Store, uid

A, B = -910001, -910002
pytestmark = pytest.mark.asyncio(loop_scope="module")


async def cleanup(store):
    for user_id in (A, B):
        async with store.connection(user_id) as conn:
            tables = (
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
                "artifacts",
                "conversations",
                "users",
            )
            deletes = "\n".join(f"DELETE FROM {table} WHERE user_id={user_id};" for table in tables)
            # Only the two integer constants above are interpolated, never external input.
            await conn.execute(f"""DO $$ DECLARE ids uuid[]; BEGIN
                SELECT array_agg(event_id) INTO ids FROM (
                    SELECT event_id FROM runs WHERE user_id={user_id}
                    UNION SELECT o.event_id FROM occurrences o JOIN schedules s ON s.id=o.schedule_id
                    WHERE s.user_id={user_id}) selected;
                DELETE FROM occurrences WHERE schedule_id IN (SELECT id FROM schedules WHERE user_id={user_id});
                {deletes}
                DELETE FROM events WHERE id=ANY(ids) OR payload->>'test_user_id'='{user_id}';
                END $$""")


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def store():
    if not os.getenv("DATABASE_URL"):
        pytest.skip("Real PostgreSQL DATABASE_URL is required")
    settings = Settings()
    value = Store(settings)
    await value.open()
    try:
        yield value
    finally:
        await cleanup(value)
        await value.close()


@pytest_asyncio.fixture(autouse=True, loop_scope="module")
async def clean_users(store):
    await cleanup(store)
    for user_id in (A, B):
        await store.ensure_user(user_id)
    try:
        yield
    finally:
        await cleanup(store)


async def make_run(store, user_id=A, event_id=None, thread=1):
    conversation = await store.conversation(user_id, user_id, thread)
    event_id = event_id or uuid4()
    async with store.connection() as conn:
        await conn.execute(
            "INSERT INTO events(id,payload,state) VALUES($1,$2,'done') ON CONFLICT DO NOTHING",
            event_id,
            {"test_user_id": user_id},
        )
    return await store.start_run(event_id, user_id, conversation["id"]), conversation


async def test_rls_is_enforced_for_two_users_and_transaction_settings_do_not_leak(store):
    await store.remember(A, "private A")
    await store.remember(B, "private B")
    async with store.connection(A) as conn:
        role = await conn.fetchrow(
            "SELECT rolsuper,rolbypassrls FROM pg_roles WHERE rolname=current_user"
        )
        assert not role["rolsuper"] and not role["rolbypassrls"]
        assert [row["content"] for row in await conn.fetch("SELECT content FROM memory")] == [
            "private A"
        ]
        assert await conn.fetchval("SELECT count(*) FROM users WHERE user_id=$1", B) == 0
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            async with conn.transaction():
                await conn.execute(
                    "INSERT INTO memory(id,user_id,content) VALUES($1,$2,'injected')", uuid4(), B
                )
    async with store.connection() as conn:
        assert await conn.fetchval("SELECT count(*) FROM memory") == 0
    assert [row["content"] for row in await store.memories(B)] == ["private B"]


async def test_forget_preserves_current_ack_run_but_cancels_other_runs_only_for_owner(store):
    current, conversation = await make_run(store)
    other, _ = await make_run(store, thread=2)
    outsider, _ = await make_run(store, B)
    await store.remember(A, "secret fact")
    await store.remember(B, "secret fact")
    await store.add_message(A, conversation["id"], "user", "secret fact")
    before = await store.ensure_user(A)
    result = await store.forget(A, "secret", current_run=current)
    assert result["forgotten"] == ["secret fact"]
    assert await store.run_active(current["id"], current["fence"])
    assert not await store.run_active(other["id"], other["fence"])
    assert await store.run_active(outsider["id"], outsider["fence"])
    assert await store.history(A, conversation["id"]) == []
    assert (await store.ensure_user(A))["memory_revision"] == before["memory_revision"] + 1
    assert len(await store.memories(B)) == 1
    assert await store.enqueue_for_run(current, conversation, {"text": "forgotten"}, "ack-test")


async def test_fences_block_stale_finish_and_outbox_publish_and_dedupe_live_publish(store):
    old, conversation = await make_run(store)
    fresh = await store.start_run(old["event_id"], A, conversation["id"])
    assert fresh["fence"] == old["fence"] + 1
    assert await store.enqueue_for_run(old, conversation, {"text": "stale"}, "old-test") is None
    assert await store.finish_run(old["id"], fence=old["fence"]) is False
    first = await store.enqueue_for_run(fresh, conversation, {"text": "fresh"}, "fresh-test")
    assert (
        await store.enqueue_for_run(fresh, conversation, {"text": "duplicate"}, "fresh-test")
        == first
    )
    async with store.connection() as conn:
        await conn.execute("UPDATE runs SET cancel_requested=true WHERE id=$1", fresh["id"])
    assert await store.enqueue_for_run(fresh, conversation, {"text": "late"}, "late-test") is None
    assert await store.finish_run(fresh["id"], "cancelled", fence=fresh["fence"])


async def test_outbox_cannot_target_other_users_conversation(store):
    run, _ = await make_run(store)
    foreign = await store.conversation(B, B, 1)
    assert await store.enqueue_for_run(run, foreign, {"text": "wrong user"}, "cross-user") is None


async def test_user_lock_serializes_topics_and_waiters_leave_pool_capacity(store):
    entered = asyncio.Event()
    release = asyncio.Event()
    order = []

    async def first():
        async with store.user_lock(A):
            order.append("first")
            entered.set()
            await release.wait()

    async def second():
        async with store.user_lock(A):
            order.append("second")
            assert await store.ready()

    holder = asyncio.create_task(first())
    await entered.wait()
    waiters = [asyncio.create_task(second()) for _ in range(9)]
    try:
        await asyncio.wait_for(store.ready(), 10)
        assert order == ["first"]
    finally:
        release.set()
        # Nine serialized clients cross a real network tunnel; test liveness, not a 5s SLA.
        await asyncio.wait_for(asyncio.gather(holder, *waiters), 30)
    assert order == ["first"] + ["second"] * 9


async def test_lock_is_released_on_cancellation(store):
    entered = asyncio.Event()

    async def holder():
        async with store.user_lock(A):
            entered.set()
            await asyncio.Event().wait()

    task = asyncio.create_task(holder())
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    async with asyncio.timeout(10), store.user_lock(A):
        assert await store.ready()


async def test_usage_consumes_topup_after_period_and_replay_or_reset_cannot_restore_it(store):
    run, _ = await make_run(store)
    await store.top_up(A, 10_000, "topup-test")
    await store.top_up(A, 10_000, "topup-test")
    async with store.connection(A) as conn:
        await conn.execute("UPDATE users SET balance_micro=1000000 WHERE user_id=$1", A)
    await store.reserve(A, "usage-test", 5_000_000)
    receipt = {"cost_rub": "3", "model": "test", "prompt_tokens": 3, "completion_tokens": 2}
    await asyncio.gather(
        *(store.record_usage(A, run["id"], "usage-test", receipt) for _ in range(3))
    )
    user = await store.ensure_user(A)
    assert user["balance_micro"] == 0
    assert user["topup_micro"] == 8_000_000
    assert user["reserved_micro"] == 0
    async with store.connection(A) as conn:
        assert (
            await conn.fetchval("SELECT count(*) FROM ledger WHERE user_id=$1 AND kind='usage'", A)
            == 1
        )
        await conn.execute(
            "UPDATE users SET period_end=now()-interval '1 second' WHERE user_id=$1", A
        )
    user = await store.ensure_user(A)
    assert user["balance_micro"] == PLANS["FREE"]
    assert user["topup_micro"] == 8_000_000


async def test_unknown_usage_reconciles_once_and_released_reservation_never_charges_late(store):
    run, _ = await make_run(store)
    await store.reserve(A, "pending-test", 2_000_000)
    await store.record_usage(A, run["id"], "pending-test", {"model": "test", "cost_rub": None})
    assert (await store.ensure_user(A))["reserved_micro"] == 2_000_000
    await store.record_usage(
        A,
        run["id"],
        "pending-test",
        {"model": "test", "cost_rub": "1", "prompt_tokens": 11, "completion_tokens": 7},
    )
    async with store.connection(A) as conn:
        assert (
            await conn.fetchval("SELECT prompt_tokens FROM usage WHERE operation_id='pending-test'")
            == 11
        )
    before = await store.ensure_user(A)
    await store.reserve(A, "released-test", 2_000_000)
    async with store.connection(A) as conn:
        await conn.execute(
            "UPDATE reservations SET status='released' WHERE id='released-test' AND user_id=$1", A
        )
        await conn.execute(
            "UPDATE users SET reserved_micro=reserved_micro-2000000 WHERE user_id=$1", A
        )
    await store.record_usage(A, run["id"], "released-test", {"model": "test", "cost_rub": "1"})
    assert (await store.ensure_user(A))["balance_micro"] == before["balance_micro"]


async def test_upgrade_highwater_and_pending_downgrade_apply_only_next_period(store):
    await store.change_plan(A, "START", "start-test")
    await store.change_plan(A, "PREMIUM", "premium-test")
    before = await store.ensure_user(A)
    scheduled = await store.change_plan(A, "START", "downgrade-test")
    assert scheduled["plan"] == "PREMIUM" and scheduled["pending_plan"] == "START"
    await store.change_plan(A, "PREMIUM", "cancel-downgrade-test")
    assert (await store.ensure_user(A))["balance_micro"] == before["balance_micro"]
    await store.change_plan(A, "START", "downgrade-again-test")
    now = datetime.now(UTC)
    async with store.connection(A) as conn:
        await conn.execute(
            "UPDATE users SET period_end=$2,billing_anchor=$3 WHERE user_id=$1",
            A,
            now - timedelta(seconds=1),
            now - relativedelta(months=1),
        )
    user = await store.ensure_user(A)
    assert user["plan"] == "START" and user["pending_plan"] is None
    assert user["balance_micro"] == PLANS["START"]


async def test_schedule_duplicate_tick_and_late_cancel_invalidate_occurrence(store):
    conversation = await store.conversation(A, A, 1)
    due = datetime.now(UTC) - timedelta(seconds=2)
    first = await store.schedule(A, conversation, due, "test", source_key="schedule-test")
    again = await store.schedule(A, conversation, due, "duplicate", source_key="schedule-test")
    assert first["id"] == again["id"]
    ticks = await asyncio.gather(store.schedule_due(A), store.schedule_due(A))
    assert sum(ticks) == 1
    async with store.connection() as conn:
        rows = await conn.fetch("SELECT * FROM occurrences WHERE schedule_id=$1", uid(first["id"]))
    assert len(rows) == 1 and await store.occurrence_active(rows[0]["id"])
    await store.change_schedule(A, first["id"], "cancel")
    assert not await store.occurrence_active(rows[0]["id"])


async def test_wrong_ingress_owner_cannot_insert_or_advance_offset(store):
    offset = await store.get_poll_offset()
    update = {"update_id": -9100010001, "test_user_id": A}
    assert not await store.ingest_updates(
        [update], offset + 1, owner="test-nonowner-" + str(uuid4())
    )
    assert await store.get_poll_offset() == offset
    async with store.connection() as conn:
        assert not await conn.fetchval(
            "SELECT id FROM events WHERE update_id=$1", update["update_id"]
        )


async def test_ingress_replay_does_not_cancel_a_new_run_or_regress_offset(store):
    old, _ = await make_run(store)
    offset = await store.get_poll_offset()
    update = {
        "update_id": -9100010002,
        "test_user_id": A,
        "message": {"from": {"id": A}, "chat": {"id": A, "type": "private"}, "text": "стоп"},
    }
    assert await store.ingest_updates([update], offset)
    assert not await store.run_active(old["id"], old["fence"])
    new, _ = await make_run(store)
    assert await store.ingest_updates([update], max(0, offset - 1))
    assert await store.run_active(new["id"], new["fence"])
    assert await store.get_poll_offset() == offset


async def test_expired_ingress_lease_is_rejected(store):
    owner = "storage-test-" + str(uuid4())
    if not await store.acquire_poll_lease(owner):
        pytest.skip("An active application poller owns the production lease")
    try:
        offset = await store.get_poll_offset()
        assert await store.ingest_updates([], offset, owner=owner)
        async with store.connection() as conn:
            await conn.execute(
                "UPDATE service_leases SET expires_at=now()-interval '1 second' WHERE key='poller' AND owner=$1",
                owner,
            )
        assert not await store.ingest_updates(
            [{"update_id": -9100010003, "test_user_id": A}], offset, owner=owner
        )
    finally:
        await store.release_poll_lease(owner)


async def test_delivery_failure_preserves_partial_ids_and_success_sets_sent_at(store):
    delivery = await store.enqueue(A, A, 1, {"text": "test"}, "delivery-test")
    async with store.connection() as conn:
        await conn.execute(
            "UPDATE outbox SET owner='storage-test',state='sending',telegram_ids=$2 WHERE id=$1 AND user_id=$3",
            delivery,
            [101],
            A,
        )
    await store.delivery_result(delivery, "storage-test", error="temporary")
    async with store.connection() as conn:
        row = await conn.fetchrow(
            "SELECT telegram_ids,sent_at,state FROM outbox WHERE id=$1 AND user_id=$2", delivery, A
        )
    assert row["telegram_ids"] == [101] and row["sent_at"] is None and row["state"] == "pending"
    await store.delivery_result(delivery, "storage-test", ids=[101, 102])
    async with store.connection() as conn:
        row = await conn.fetchrow(
            "SELECT telegram_ids,sent_at,state FROM outbox WHERE id=$1 AND user_id=$2", delivery, A
        )
    assert row["telegram_ids"] == [101, 102] and row["sent_at"] and row["state"] == "sent"


async def test_expired_claims_stop_after_three_attempts_even_without_finish_event(store):
    event_id = uuid4()
    async with store.connection() as conn:
        await conn.execute(
            "INSERT INTO events(id,payload) VALUES($1,$2)", event_id, {"test_user_id": A}
        )
    for attempt in range(1, 4):
        claimed = await store.claim_event(f"test-worker-{attempt}", event_id)
        assert claimed["attempts"] == attempt
        async with store.connection() as conn:
            await conn.execute(
                "UPDATE events SET lease_until=now()-interval '1 second' WHERE id=$1", event_id
            )
    assert await store.claim_event("test-worker-4", event_id) is None
    async with store.connection() as conn:
        row = await conn.fetchrow("SELECT state,attempts FROM events WHERE id=$1", event_id)
    assert row["state"] == "failed" and row["attempts"] == 3


async def test_finish_event_does_not_count_attempt_twice_or_accept_old_owner(store):
    event_id = uuid4()
    async with store.connection() as conn:
        await conn.execute(
            "INSERT INTO events(id,payload) VALUES($1,$2)", event_id, {"test_user_id": A}
        )
    assert (await store.claim_event("worker-1", event_id))["attempts"] == 1
    await store.finish_event(event_id, "worker-1", "temporary")
    async with store.connection() as conn:
        row = await conn.fetchrow(
            "UPDATE events SET available_at=now() WHERE id=$1 RETURNING state,attempts", event_id
        )
    assert row["state"] == "pending" and row["attempts"] == 1
    assert (await store.claim_event("worker-2", event_id))["attempts"] == 2
    await store.finish_event(event_id, "worker-1")
    async with store.connection() as conn:
        assert await conn.fetchval("SELECT state FROM events WHERE id=$1", event_id) == "processing"
    await store.finish_event(event_id, "worker-2")
    async with store.connection() as conn:
        row = await conn.fetchrow("SELECT state,attempts FROM events WHERE id=$1", event_id)
    assert row["state"] == "done" and row["attempts"] == 2


async def test_stopped_draft_matches_thread_and_run_then_text_stop_accepts_first_token(store):
    first, _ = await make_run(store, thread=11)
    second, _ = await make_run(store, thread=22)
    outsider, _ = await make_run(store, B, thread=11)
    offset = await store.get_poll_offset()
    stopped = {
        "chat": {"id": A, "type": "private"},
        "message_thread_id": 22,
        "draft_id": first["id"].int % 2_000_000_000 + 1,
    }
    await store.ingest_updates(
        [{"update_id": -9100010010, "test_user_id": A, "stopped_message_generation": stopped}],
        offset,
    )
    assert await store.run_active(first["id"], first["fence"])
    assert await store.run_active(second["id"], second["fence"])
    stopped["message_thread_id"] = 11
    await store.ingest_updates(
        [{"update_id": -9100010011, "test_user_id": A, "stopped_message_generation": stopped}],
        offset,
    )
    assert not await store.run_active(first["id"], first["fence"])
    assert await store.run_active(second["id"], second["fence"])
    assert await store.run_active(outsider["id"], outsider["fence"])
    await store.ingest_updates(
        [
            {
                "update_id": -9100010012,
                "test_user_id": A,
                "message": {
                    "chat": {"id": A, "type": "private"},
                    "from": {"id": A},
                    "text": "STOP,\nпожалуйста",
                },
            }
        ],
        offset,
    )
    assert not await store.run_active(second["id"], second["fence"])
    assert await store.run_active(outsider["id"], outsider["fence"])
