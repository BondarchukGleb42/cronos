import asyncio
from contextlib import asynccontextmanager
from copy import deepcopy
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from cronos.coordinator import Coordinator
from cronos.initiative_tools import scope_initiative_tool
from cronos.providers import ProviderError
from cronos.storage import Store
from cronos.telegram import PartialDeliveryError
from cronos.worker import Worker


@pytest.fixture
def runtime():
    owner, policy_id, schedule_id = -987201, str(uuid4()), str(uuid4())
    conversation = {"id": uuid4(), "user_id": owner, "chat_id": owner, "thread_id": 7}
    policy = {
        "id": policy_id,
        "schedule_id": schedule_id,
        "project_id": str(uuid4()),
        "available": True,
        "revision": 2,
        "project_revision": 3,
        "allowed_tools": ["web_search", "file_create", "library_search", "file_read"],
    }
    schedule = {
        "id": schedule_id,
        "initiative_id": policy_id,
        "user_id": owner,
        "chat_id": owner,
        "thread_id": 7,
        "revision": 4,
        "proactive": True,
        "dynamic": True,
        "instruction": "Prepare a useful comparison",
        "fixed_text": "",
        "state": "active",
        "interval_seconds": 86400,
    }
    guard = {
        "initiative_id": policy_id,
        "initiative_decision_id": str(uuid4()),
        "initiative_revision": 2,
        "initiative_project_revision": 3,
        "initiative_fingerprint": "abc",
        "schedule_id": schedule_id,
        "schedule_revision": 4,
    }
    event = {
        "id": uuid4(),
        "attempts": 1,
        "payload": {"schedule_id": schedule_id, "revision": 4, "occurrence_id": str(uuid4())},
    }
    run = {
        "id": uuid4(),
        "user_id": owner,
        "fence": 1,
        "conversation_id": conversation["id"],
        "memory_revision": 0,
    }
    lock = {"held": False}

    @asynccontextmanager
    async def user_lock(user_id, purpose="work"):
        assert user_id == owner and not lock["held"]
        lock["held"] = True
        try:
            yield
        finally:
            lock["held"] = False

    conn = SimpleNamespace(execute=AsyncMock(), fetchval=AsyncMock(return_value=0))

    @asynccontextmanager
    async def connection(*args):
        yield conn

    store = SimpleNamespace(
        user_lock=user_lock,
        connection=connection,
        start_run=AsyncMock(return_value=run),
        finish_run=AsyncMock(),
        run_metrics=AsyncMock(),
        enqueue_for_run=AsyncMock(return_value=7),
        add_message=AsyncMock(),
        validate_initiative_delivery=AsyncMock(return_value=True),
        get_artifact=AsyncMock(
            side_effect=lambda user, key: {
                "path": f"/synthetic/{key}.csv",
                "filename": f"{key}.csv",
            }
        ),
        get_schedule=AsyncMock(return_value=schedule),
        get_initiative_for_schedule=AsyncMock(return_value=policy),
        occurrence_active=AsyncMock(return_value=True),
        mark_occurrence=AsyncMock(),
        conversation=AsyncMock(return_value=conversation),
        preferences=AsyncMock(return_value={"proactivity": True, "quiet_start": 0, "quiet_end": 0}),
        delivery_result=AsyncMock(),
    )
    worker: Any = Worker.__new__(Worker)
    worker.store = store
    worker.transport = SimpleNamespace(draft=AsyncMock())
    worker.agent = SimpleNamespace(
        run=AsyncMock(
            return_value={
                "text": "Подготовлено сравнение.",
                "prepared_artifact_ids": ["first", "second"],
                "initiative_decision": {
                    "should_send": True,
                    "reason": "Полезное изменение",
                    "delivery_guard": guard,
                },
            }
        )
    )
    row = {
        "id": 9,
        "user_id": owner,
        "chat_id": owner,
        "thread_id": 7,
        "attempts": 1,
        "payload": {"text": "Полезный результат", **guard},
    }
    store.next_delivery = AsyncMock(side_effect=lambda _: deepcopy(row))

    async def claimed(*unused):
        assert lock["held"]
        return deepcopy(row)

    store.claimed_delivery = AsyncMock(side_effect=claimed)
    coordinator: Any = Coordinator.__new__(Coordinator)
    coordinator.store, coordinator.owner = store, "initiative-sender"
    coordinator.redis = SimpleNamespace(set=AsyncMock(return_value=True))
    coordinator.transport = SimpleNamespace(send=AsyncMock(return_value=[101, 102]))
    return SimpleNamespace(
        worker=worker,
        coordinator=coordinator,
        store=store,
        conn=conn,
        policy=policy,
        schedule=schedule,
        guard=guard,
        run=run,
        event=event,
        conversation=conversation,
        row=row,
        lock=lock,
    )


async def answer(case):
    return await case.worker.answer(
        case.event,
        case.conversation,
        "Prepare",
        proactive=True,
        schedule=case.schedule,
        initiative=case.policy,
    )


async def test_prepared_text_and_all_files_share_one_guarded_outbox_and_no_draft(runtime):
    case = runtime
    assert await answer(case) == "done"
    case.worker.transport.draft.assert_not_awaited()
    assert case.worker.agent.run.await_args.kwargs["initiative"] is case.policy
    assert case.worker.agent.run.await_args.kwargs["proactive"] is True
    case.store.enqueue_for_run.assert_awaited_once()
    payload = case.store.enqueue_for_run.await_args.args[2]
    assert payload["_telegram_parts"] == [
        {"kind": "rich", "text": "Подготовлено сравнение."},
        {"kind": "document", "path": "/synthetic/first.csv", "caption": "first.csv"},
        {"kind": "document", "path": "/synthetic/second.csv", "caption": "second.csv"},
    ]
    assert all(payload[key] == value for key, value in case.guard.items())
    case.store.add_message.assert_not_awaited()


@pytest.mark.parametrize(
    "decision,status",
    [
        (None, "cancelled"),
        ({"should_send": False, "reason": "Нет изменений"}, "done"),
        ({"should_send": True}, "cancelled"),
    ],
)
async def test_missing_or_negative_decision_never_sends(runtime, decision, status):
    runtime.worker.agent.run.return_value["initiative_decision"] = decision
    assert await answer(runtime) == status
    runtime.store.enqueue_for_run.assert_not_awaited()
    runtime.store.get_artifact.assert_not_awaited()


async def test_policy_changed_during_preparation_blocks_all_files(runtime):
    runtime.store.validate_initiative_delivery.return_value = False
    assert await answer(runtime) == "cancelled"
    runtime.store.enqueue_for_run.assert_not_awaited()
    runtime.store.get_artifact.assert_not_awaited()


@pytest.mark.parametrize("error", [ProviderError("unavailable"), asyncio.CancelledError()])
async def test_failed_or_interrupted_preparation_never_enqueues_fallback(runtime, error):
    runtime.worker.agent.run.side_effect = error
    with pytest.raises(type(error)):
        await answer(runtime)
    runtime.store.enqueue_for_run.assert_not_awaited()
    expected = "interrupted" if isinstance(error, asyncio.CancelledError) else "failed"
    assert runtime.store.finish_run.await_args.args[1] == expected


@pytest.mark.parametrize("policy", [None, {"available": False, "reason": "project_context_reset"}])
async def test_marked_schedule_never_falls_back_to_legacy_when_policy_is_unavailable(
    runtime, policy
):
    runtime.store.get_initiative_for_schedule.return_value = policy
    runtime.worker.answer = AsyncMock()
    await runtime.worker.timer(runtime.event)
    runtime.worker.answer.assert_not_awaited()
    runtime.store.mark_occurrence.assert_awaited_once_with(
        runtime.event["payload"]["occurrence_id"], "cancelled"
    )


async def test_timer_passes_trusted_policy_to_answer(runtime):
    runtime.worker.answer = AsyncMock(return_value="done")
    await runtime.worker.timer(runtime.event)
    assert runtime.worker.answer.await_args is not None
    assert runtime.worker.answer.await_args.kwargs["initiative"] is runtime.policy
    assert runtime.worker.answer.await_args.kwargs["proactive"] is True


async def test_delivery_checks_live_guard_under_lock_and_preserves_success_ids(runtime):
    async def validate(owner, payload):
        assert runtime.lock["held"]
        return True

    runtime.store.validate_initiative_delivery.side_effect = validate
    assert await runtime.coordinator.delivery() is True
    runtime.coordinator.transport.send.assert_awaited_once()
    assert runtime.store.delivery_result.await_args.kwargs == {"ids": [101, 102]}


@pytest.mark.parametrize("missing", [False, True])
async def test_delivery_rejects_stale_or_missing_policy_decision(runtime, missing):
    if missing:
        runtime.row["payload"] = {
            "text": "Unapproved",
            "schedule_id": runtime.schedule["id"],
            "schedule_revision": 4,
        }
    else:
        runtime.store.validate_initiative_delivery.return_value = False
    assert await runtime.coordinator.delivery() is True
    runtime.coordinator.transport.send.assert_not_awaited()
    assert runtime.store.delivery_result.await_args.kwargs["permanent"] is True


async def test_partial_delivery_retains_every_policy_guard_and_does_not_ack_fingerprint(runtime):
    tail = {"_telegram_parts": [{"kind": "document", "path": "/synthetic/remaining.csv"}]}
    runtime.coordinator.transport.send.side_effect = PartialDeliveryError(
        [101], tail, TimeoutError()
    )
    assert await runtime.coordinator.delivery() is True
    args = runtime.conn.execute.await_args.args
    assert args[3] == {**tail, **runtime.guard}
    assert args[4] == [101] and args[5] == "pending"
    runtime.store.delivery_result.assert_not_awaited()


async def test_project_search_scope_and_unapproved_tool_execution_are_enforced(runtime):
    scoped = await scope_initiative_tool(
        runtime.store, "library_search", {"query": "цены"}, runtime.run, runtime.policy
    )
    assert (
        scoped["project_id"] == runtime.policy["project_id"]
        and scoped["scope"] == "current_project"
    )
    for name, args in (
        ("library_search", {"query": "цены", "scope": "account"}),
        ("file_create", {"project_id": str(uuid4())}),
        ("schedule_create", {}),
        ("upgrade", {}),
    ):
        with pytest.raises(ValueError):
            await scope_initiative_tool(runtime.store, name, args, runtime.run, runtime.policy)
    runtime.conn.fetchval.return_value = None
    with pytest.raises(ValueError, match="outside"):
        await scope_initiative_tool(
            runtime.store, "file_read", {"artifact_id": str(uuid4())}, runtime.run, runtime.policy
        )


@pytest.mark.parametrize("action", ["cancel", "reschedule"])
async def test_schedule_change_waits_for_in_flight_delivery_before_mutation(action):
    lock, calls = asyncio.Lock(), []
    schedule_id, owner = uuid4(), -987202
    conn = SimpleNamespace(fetchval=AsyncMock(return_value=schedule_id))

    @asynccontextmanager
    async def user_lock(user_id, *, purpose):
        calls.append((user_id, purpose))
        async with lock:
            yield

    @asynccontextmanager
    async def connection():
        assert lock.locked()
        yield conn

    store: Any = Store.__new__(Store)
    store.user_lock, store.connection = user_lock, connection
    task = None
    try:
        async with lock:
            task = asyncio.create_task(
                store.change_schedule(
                    owner,
                    schedule_id,
                    action,
                    datetime(2026, 9, 8, 10, tzinfo=UTC) if action == "reschedule" else None,
                )
            )
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(asyncio.shield(task), timeout=0.05)
            conn.fetchval.assert_not_awaited()
        assert await asyncio.wait_for(task, timeout=1) == {"id": str(schedule_id), "action": action}
        assert calls == [(owner, "delivery")]
        conn.fetchval.assert_awaited_once()
    finally:
        if task and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
