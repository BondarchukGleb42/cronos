"""Real PostgreSQL checks confined to dedicated synthetic schedule owners."""

import os
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest
import pytest_asyncio

from cronos.settings import Settings
from cronos.storage import Store, uid

A, B = -950001, -950002
pytestmark = pytest.mark.asyncio(loop_scope="module")


@pytest_asyncio.fixture(loop_scope="module")
async def scheduled_store():
    if not os.getenv("DATABASE_URL"):
        pytest.skip("Real PostgreSQL DATABASE_URL is required")
    store = Store(Settings())
    await store.open()
    created = []
    try:
        for user_id in (A, B):
            async with store.connection(user_id) as conn:
                assert not await conn.fetchval("SELECT 1 FROM users WHERE user_id=$1", user_id), (
                    "Synthetic schedule owner already exists; inspect before rerunning"
                )
            await store.ensure_user(user_id)
            created.append(user_id)
        yield store
    finally:
        for user_id in created:
            async with store.connection(user_id) as conn:
                event_ids = await conn.fetch(
                    """SELECT event_id FROM runs WHERE user_id=$1
                    UNION SELECT o.event_id FROM occurrences o
                    JOIN schedules s ON s.id=o.schedule_id WHERE s.user_id=$1""",
                    user_id,
                )
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
                    "artifacts",
                    "conversations",
                    "users",
                ):
                    await conn.execute(f"DELETE FROM {table} WHERE user_id=$1", user_id)
                await conn.execute(
                    "DELETE FROM events WHERE id=ANY($1::uuid[]) OR payload->>'test_user_id'=$2",
                    [row["event_id"] for row in event_ids if row["event_id"] is not None],
                    str(user_id),
                )
        await store.close()


async def timer_case(store, user_id=A):
    conversation = await store.conversation(user_id, user_id, 1)
    info = await store.schedule(
        user_id,
        conversation,
        datetime.now(UTC) - timedelta(seconds=2),
        "Daily report",
        instruction="Create a fresh CSV",
        dynamic=True,
        interval_seconds=86400,
    )
    assert await store.schedule_due(user_id) == 1
    async with store.connection() as conn:
        event = await conn.fetchrow(
            "SELECT e.* FROM events e JOIN occurrences o ON o.event_id=e.id WHERE o.schedule_id=$1",
            uid(info["id"]),
        )
    run = await store.start_run(event["id"], user_id, conversation["id"])
    return SimpleNamespace(
        run=run,
        conversation=conversation,
        event=dict(event),
        schedule=await store.get_schedule(info["id"]),
    )


@pytest.mark.parametrize(
    "payload",
    [
        {"text": "Fresh prices"},
        {"document_path": "/tmp/prices.csv", "caption": "Prices"},
        {"photo_path": "/tmp/prices.png"},
    ],
)
async def test_every_timer_delivery_gets_authoritative_schedule_guards(scheduled_store, payload):
    store = scheduled_store
    case = await timer_case(store)
    incoming = {**payload, "schedule_id": str(uuid4()), "schedule_revision": 999}
    original = dict(incoming)
    # Caller-supplied event metadata must not replace the actual persisted run/event.
    run = {**case.run, "event_id": uuid4(), "kind": "telegram"}
    assert await store.run_active(run["id"], run["fence"])
    delivery_id = await store.enqueue_for_run(run, case.conversation, incoming, "scheduled-result")
    assert delivery_id is not None
    assert incoming == original
    async with store.connection() as conn:
        row = await conn.fetchrow("SELECT * FROM outbox WHERE id=$1", delivery_id)
    assert row["user_id"] == A and row["chat_id"] == A and row["thread_id"] == 1
    assert row["payload"] == {
        **payload,
        "schedule_id": str(case.schedule["id"]),
        "schedule_revision": case.schedule["revision"],
    }
    assert (
        await store.enqueue_for_run(run, case.conversation, payload, "scheduled-result")
        == delivery_id
    )


@pytest.mark.parametrize("action", ["cancel", "reschedule"])
async def test_cancelled_or_rescheduled_timer_stops_work_and_file_enqueue(scheduled_store, action):
    store = scheduled_store
    case = await timer_case(store)
    payload = {"document_path": "/tmp/prices.csv"}
    pending = await store.enqueue_for_run(case.run, case.conversation, payload, "before-change")
    assert pending is not None
    await store.change_schedule(
        A, case.schedule["id"], action, datetime.now(UTC) + timedelta(days=1)
    )
    assert not await store.run_active(case.run["id"], case.run["fence"])
    assert await store.enqueue_for_run(case.run, case.conversation, payload, "after-change") is None
    async with store.connection() as conn:
        deliveries = await conn.fetch("SELECT payload FROM outbox WHERE user_id=$1", A)
    assert len(deliveries) == 1
    # Already queued files carry the old revision for Coordinator.delivery to reject.
    assert deliveries[0]["payload"]["schedule_revision"] == case.schedule["revision"]
    assert deliveries[0]["payload"]["schedule_id"] == str(case.schedule["id"])
    assert (await store.get_schedule(case.schedule["id"]))["revision"] > case.schedule["revision"]


async def test_timer_cannot_use_another_owners_schedule_or_another_topic(scheduled_store):
    store = scheduled_store
    case = await timer_case(store)
    foreign = await timer_case(store, B)
    other_topic = await store.conversation(A, A, 2)
    assert (
        await store.enqueue_for_run(
            case.run, other_topic, {"document_path": "/tmp/private.csv"}, "wrong-topic"
        )
        is None
    )
    async with store.connection() as conn:
        await conn.execute(
            "UPDATE events SET payload=$2 WHERE id=$1",
            case.event["id"],
            {"schedule_id": str(foreign.schedule["id"]), "revision": foreign.schedule["revision"]},
        )
    assert not await store.run_active(case.run["id"], case.run["fence"])
    assert (
        await store.enqueue_for_run(
            case.run, case.conversation, {"text": "Wrong owner"}, "wrong-owner"
        )
        is None
    )
    assert await store.run_active(foreign.run["id"], foreign.run["fence"])
    async with store.connection() as conn:
        assert await conn.fetchval("SELECT count(*) FROM outbox WHERE user_id=$1", A) == 0


@pytest.mark.parametrize(
    "event_payload",
    [
        {},
        {"schedule_id": "invalid", "revision": 1},
        {"schedule_id": str(uuid4()), "revision": True},
    ],
)
async def test_malformed_timer_reference_cannot_enqueue(scheduled_store, event_payload):
    store = scheduled_store
    case = await timer_case(store)
    async with store.connection() as conn:
        await conn.execute(
            "UPDATE events SET payload=$2 WHERE id=$1", case.event["id"], event_payload
        )
    assert not await store.run_active(case.run["id"], case.run["fence"])
    assert (
        await store.enqueue_for_run(
            case.run, case.conversation, {"document_path": "/tmp/prices.csv"}, "malformed"
        )
        is None
    )


async def test_normal_run_deliveries_keep_existing_payload_and_fence_semantics(scheduled_store):
    store = scheduled_store
    conversation = await store.conversation(A, A, 1)
    event_id = uuid4()
    async with store.connection() as conn:
        await conn.execute(
            "INSERT INTO events(id,kind,payload) VALUES($1,'telegram',$2)",
            event_id,
            {"test_user_id": A, "schedule_id": str(uuid4()), "revision": 999},
        )
    run = await store.start_run(event_id, A, conversation["id"])
    payload = {"document_path": "/tmp/ordinary.csv", "caption": "Ordinary request"}
    assert await store.run_active(run["id"], run["fence"])
    delivery_id = await store.enqueue_for_run(run, conversation, payload, "normal-file")
    async with store.connection() as conn:
        assert await conn.fetchval("SELECT payload FROM outbox WHERE id=$1", delivery_id) == payload
    assert not await store.run_active(run["id"], run["fence"] + 1)
    assert (
        await store.enqueue_for_run(
            {**run, "fence": run["fence"] + 1}, conversation, payload, "stale-file"
        )
        is None
    )


async def test_schedule_results_and_listing_report_persisted_repeat_settings(scheduled_store):
    store = scheduled_store
    await store.preferences(A, {"timezone": "Europe/Moscow", "proactivity": True})
    conversation = await store.conversation(A, A, 1)
    due = datetime.now(UTC) + timedelta(days=1)
    result = await store.schedule(
        A,
        conversation,
        due,
        "Prices",
        instruction="Make a CSV",
        dynamic=True,
        proactive=True,
        interval_seconds=86400,
        source_key="repeat-settings",
    )
    expected = {
        "interval_seconds": 86400,
        "timezone": "Europe/Moscow",
        "dynamic": True,
        "proactive": True,
    }
    assert {key: result[key] for key in expected} == expected
    duplicate = await store.schedule(
        A,
        conversation,
        due,
        "Changed input",
        dynamic=False,
        proactive=False,
        interval_seconds=None,
        source_key="repeat-settings",
    )
    assert duplicate == result
    listed = await store.list_schedules(A)
    assert len(listed) == 1 and listed[0]["id"] == result["id"]
    assert {key: listed[0][key] for key in expected} == expected
    assert await store.list_schedules(B) == []
