"""Real PostgreSQL tests; every user-owned write is limited to the two IDs below."""

import asyncio
import json
import os
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import asyncpg
import pytest
import pytest_asyncio
from dateutil.relativedelta import relativedelta

from cronos.settings import Settings
from cronos.storage import PLANS, Store, uid
from cronos.topics import create_chat

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
                DELETE FROM events WHERE id=ANY(ids) OR payload->>'test_user_id'='{user_id}'
                    OR (kind IN ('topic_title','topic_title_reset') AND payload->>'user_id'='{user_id}');
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


async def test_runless_operation_persists_and_replays_original_result(store):
    operation_id = "test-runless-topic-create"
    result = {"thread_id": 42, "name": "Новый чат"}
    await store.save_operation(operation_id, A, None, "topic_create", result)
    assert await store.operation(operation_id) == result
    await store.save_operation(
        operation_id, A, None, "topic_create", {"thread_id": 99, "name": "Duplicate"}
    )
    assert await store.operation(operation_id) == result
    async with store.connection() as conn:
        rows = await conn.fetch(
            "SELECT user_id,run_id,kind,status,result FROM operations WHERE id=$1", operation_id
        )
    assert [dict(row) for row in rows] == [
        {"user_id": A, "run_id": None, "kind": "topic_create", "status": "done", "result": result}
    ]


async def test_create_chat_with_real_store_persists_topic_result_and_deduplicates_welcome(store):
    operation_id = "test-real-topic-create"
    topic = {"message_thread_id": 42, "name": "Новый чат"}
    transport = SimpleNamespace(
        topic_capabilities=AsyncMock(return_value=SimpleNamespace(has_topics_enabled=True)),
        create_topic=AsyncMock(return_value=topic),
    )
    result = await create_chat(store, transport, A, A, operation_id)
    assert result["thread_id"] == 42 and result["name"] == "Новый чат"
    assert await store.operation(operation_id) == {"thread_id": 42, "name": "Новый чат"}

    replay = await create_chat(store, transport, A, A, operation_id)
    assert replay == result
    transport.topic_capabilities.assert_awaited_once()
    transport.create_topic.assert_awaited_once_with(A, "Новый чат")
    async with store.connection(A) as conn:
        conversations = await conn.fetch("SELECT * FROM conversations WHERE user_id=$1", A)
        operations = await conn.fetch(
            "SELECT id,run_id,kind,status FROM operations WHERE user_id=$1 ORDER BY id", A
        )
        deliveries = await conn.fetch("SELECT * FROM outbox WHERE user_id=$1", A)
    assert len(conversations) == 1
    assert str(conversations[0]["id"]) == result["id"]
    assert conversations[0]["chat_id"] == A and conversations[0]["thread_id"] == 42
    assert conversations[0]["title"] == "Новый чат"
    assert [dict(row) for row in operations] == [
        {"id": operation_id, "run_id": None, "kind": "topic_create", "status": "done"},
        {
            "id": operation_id + ":intent",
            "run_id": None,
            "kind": "topic_intent",
            "status": "started",
        },
    ]
    assert len(deliveries) == 1
    delivery = deliveries[0]
    assert delivery["chat_id"] == A and delivery["thread_id"] == 42
    assert delivery["state"] == "pending"
    assert delivery["dedupe_key"] == f"topic-welcome:{A}:42"
    assert "Новый чат готов" in delivery["payload"]["text"]
    assert delivery["payload"]["reply_markup"]["keyboard"]
    assert "inline_keyboard" not in delivery["payload"]["reply_markup"]


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


async def title_messages(store, conversation, start, stop):
    for index in range(start, stop):
        await store.add_message(
            conversation["user_id"],
            conversation["id"],
            "user" if index % 2 == 0 else "assistant",
            f"message {index}",
        )


async def test_topic_title_milestones_are_durable_deduplicated_and_coalesce_latest_context(store):
    conversation = await store.conversation(A, A, 7)
    assert await store.queue_topic_title(A, conversation["id"]) is False
    await title_messages(store, conversation, 0, 1)
    assert await store.topic_title_context(A, conversation["id"]) is None
    await title_messages(store, conversation, 1, 2)
    attempts = await asyncio.gather(
        *(store.queue_topic_title(A, conversation["id"]) for _ in range(6))
    )
    assert sum(attempts) == 1
    context = await store.topic_title_context(A, conversation["id"])
    assert context["message_count"] == 2
    assert context["conversation"]["title_auto"] is True
    assert context["conversation"]["title_message_count"] == 0
    assert await store.set_topic_title(A, conversation["id"], "Первая тема", 2, 0)
    assert await store.topic_title_context(A, conversation["id"]) is None
    assert await store.queue_topic_title(A, conversation["id"]) is False

    await title_messages(store, conversation, 2, 9)
    assert await store.queue_topic_title(A, conversation["id"]) is False
    await title_messages(store, conversation, 9, 10)
    assert await store.queue_topic_title(A, conversation["id"])
    await title_messages(store, conversation, 10, 19)
    assert await store.queue_topic_title(A, conversation["id"]) is False
    context = await store.topic_title_context(A, conversation["id"])
    assert context["message_count"] == 19
    assert await store.set_topic_title(A, conversation["id"], "Развившаяся тема", 19, 0)
    assert await store.topic_title_context(A, conversation["id"]) is None
    # A delayed first/bucket-10 job cannot replace a newer result in the same revision.
    assert await store.set_topic_title(A, conversation["id"], "Устаревшая тема", 2, 0) is False
    assert await store.set_topic_title(A, conversation["id"], "Старый порог", 10, 0) is False
    await title_messages(store, conversation, 19, 20)
    assert await store.queue_topic_title(A, conversation["id"])
    async with store.connection(A) as conn:
        events = await conn.fetch(
            "SELECT payload FROM events WHERE kind='topic_title' AND payload->>'conversation_id'=$1",
            str(conversation["id"]),
        )
    assert sorted(event["payload"]["threshold"] for event in events) == [2, 10, 20]
    assert all(event["payload"]["revision"] == 0 for event in events)
    assert all(event["payload"]["user_id"] == A for event in events)


async def test_topic_title_requires_user_and_assistant_pair_and_ignores_main_chat(store):
    conversation = await store.conversation(A, A, 3)
    await store.add_message(A, conversation["id"], "user", "one")
    await store.add_message(A, conversation["id"], "user", "two")
    await store.add_message(A, conversation["id"], "tool", "not an assistant answer")
    assert await store.topic_title_context(A, conversation["id"]) is None
    assert await store.queue_topic_title(A, conversation["id"]) is False
    await store.add_message(A, conversation["id"], "assistant", "answer")
    assert (await store.topic_title_context(A, conversation["id"]))["message_count"] == 3
    assert await store.queue_topic_title(A, conversation["id"])
    main = await store.conversation(A, A, 0, "Основной чат")
    await title_messages(store, main, 0, 12)
    assert await store.topic_title_context(A, main["id"]) is None
    assert await store.queue_topic_title(A, main["id"]) is False
    assert await store.set_topic_title(A, main["id"], "Changed", 12, 0) is False
    assert await store.sync_topic_title(A, A, 0, "Changed", manual=True) is None
    assert (await store.conversation(A, A, 0))["title"] == "Основной чат"


async def test_topic_title_context_excludes_hidden_tool_system_and_foreign_messages(store):
    conversation = await store.conversation(A, A, 1)
    foreign = await store.conversation(B, B, 1)
    await title_messages(store, conversation, 0, 14)
    await store.add_message(A, conversation["id"], "system", "system secret")
    await store.add_message(A, conversation["id"], "tool", "tool secret")
    await store.add_message(A, conversation["id"], "user", "forgotten", "excluded-title-test")
    await store.add_message(B, foreign["id"], "user", "foreign secret")
    async with store.connection(A) as conn:
        await conn.execute(
            "UPDATE messages SET excluded=true WHERE source_key='excluded-title-test'"
        )
    context = await store.topic_title_context(A, conversation["id"])
    assert context["message_count"] == 14
    assert [message["content"] for message in context["messages"]] == [
        f"message {index}" for index in range(2, 14)
    ]
    assert await store.topic_title_context(A, foreign["id"]) is None
    assert await store.queue_topic_title(A, foreign["id"]) is False
    assert await store.set_topic_title(A, foreign["id"], "Cross-owner title", 2, 0) is False
    assert (await store.conversation(B, B, 1))["title"] == ""


async def test_manual_topic_title_disables_automation_and_fences_old_work_and_echoes(store):
    conversation = await store.sync_topic_title(A, A, 1, "Новый чат")
    assert conversation["title_auto"] is True and conversation["revision"] == 0
    await title_messages(store, conversation, 0, 2)
    assert await store.queue_topic_title(A, conversation["id"])
    context = await store.topic_title_context(A, conversation["id"])
    manual = await store.sync_topic_title(A, A, 1, "Мой проект", manual=True)
    assert manual["title_auto"] is False
    assert manual["revision"] == context["conversation"]["revision"] + 1
    assert await store.topic_title_context(A, conversation["id"]) is None
    assert await store.queue_topic_title(A, conversation["id"]) is False
    assert await store.set_topic_title(A, conversation["id"], "Old generation", 2, 0) is False
    assert await store.set_topic_title(A, conversation["id"], "Wrongly enabled", 2, 1) is False
    repeated = await store.sync_topic_title(A, A, 1, "Мой проект", manual=True)
    assert repeated["revision"] == manual["revision"]
    echoed = await store.sync_topic_title(A, A, 1, "Old bot service event")
    assert echoed["title"] == "Мой проект" and echoed["title_auto"] is False
    renamed = await store.sync_topic_title(A, A, 1, "Новое ручное имя", manual=True)
    assert renamed["revision"] == manual["revision"] + 1


async def test_topic_creation_replay_preserves_generated_title_and_next_milestone(store):
    conversation = await store.sync_topic_title(A, A, 1, "Новый чат", created=True)
    await title_messages(store, conversation, 0, 2)
    assert await store.set_topic_title(A, conversation["id"], "Рабочий план", 2, 0)
    generated = await store.conversation(A, A, 1)
    replayed = await store.sync_topic_title(A, A, 1, "Новый чат", created=True)
    assert replayed == generated
    assert replayed["title"] == "Рабочий план"
    assert replayed["title_message_count"] == 2 and replayed["title_auto"] is True
    assert await store.topic_title_context(A, conversation["id"]) is None
    await title_messages(store, conversation, 2, 10)
    assert await store.queue_topic_title(A, conversation["id"])
    assert (await store.topic_title_context(A, conversation["id"]))["message_count"] == 10


async def test_manual_topic_events_reject_older_retries_but_allow_latest_api_retry(store):
    conversation = await store.conversation(A, A, 1)
    first = await store.sync_topic_title(A, A, 1, "План A", manual=True, update_id=100)
    assert first["title_update_id"] == 100 and first["revision"] == 1
    retry = await store.sync_topic_title(A, A, 1, "План A", manual=True, update_id=100)
    assert retry == first
    assert (
        await store.sync_topic_title(
            A, A, 1, "Changed duplicate payload", manual=True, update_id=100
        )
        == first
    )

    latest = await store.sync_topic_title(A, A, 1, "План B", manual=True, update_id=101)
    assert latest["title"] == "План B" and latest["revision"] == 2
    assert latest["title_update_id"] == 101 and latest["title_auto"] is False
    assert await store.sync_topic_title(A, A, 1, "План A", manual=True, update_id=100) is None
    assert await store.conversation(A, A, 1) == latest
    assert await store.sync_topic_title(A, A, 1, "План B", manual=True, update_id=101) == latest
    assert await store.set_topic_title(A, conversation["id"], "Stale auto title", 2, 0) is False

    # A later identical name still advances ordering, without changing the revision.
    advanced = await store.sync_topic_title(A, A, 1, "План B", manual=True, update_id=102)
    assert advanced["title_update_id"] == 102 and advanced["revision"] == 2
    assert await store.sync_topic_title(A, A, 1, "План B", manual=True, update_id=101) is None
    assert (
        await store.sync_topic_title(
            A, A, 1, "Creation replay", manual=True, created=True, update_id=99
        )
        == advanced
    )


async def test_manual_topic_event_ordering_is_scoped_to_owner_chat_and_thread(store):
    first = await store.sync_topic_title(A, A, 1, "Мой проект", manual=True, update_id=500)
    other_thread = await store.sync_topic_title(A, A, 2, "Другая тема", manual=True, update_id=1)
    other_chat = await store.sync_topic_title(A, 123456, 1, "Другой чат", manual=True, update_id=1)
    other_owner = await store.sync_topic_title(B, B, 1, "Чужая тема", manual=True, update_id=1)
    assert [row["title_update_id"] for row in (other_thread, other_chat, other_owner)] == [1, 1, 1]
    assert await store.sync_topic_title(A, A, 1, "Старое имя", manual=True, update_id=499) is None
    assert await store.conversation(A, A, 1) == first
    assert await store.conversation(B, B, 1) == other_owner


async def test_explicit_manual_creation_only_initializes_missing_topic(store):
    created = await store.sync_topic_title(
        A, A, 1, "Мой проект", manual=True, created=True, update_id=7
    )
    assert created["title"] == "Мой проект"
    assert created["title_auto"] is False and created["revision"] == 1
    assert created["title_update_id"] == 7
    renamed = await store.sync_topic_title(A, A, 1, "Новый проект", manual=True)
    replayed = await store.sync_topic_title(A, A, 1, "Мой проект", manual=True, created=True)
    assert replayed == renamed

    existing = await store.conversation(A, A, 2)
    replayed = await store.sync_topic_title(A, A, 2, "Старое имя", manual=True, created=True)
    assert replayed == existing


async def test_forget_resets_only_auto_topic_names_and_durably_queues_safe_resets(store):
    automatic = await store.conversation(A, A, 1, "Секретное автоназвание")
    manual = await store.sync_topic_title(A, A, 2, "Ручное название", manual=True)
    main = await store.conversation(A, A, 0, "Главный чат")
    foreign = await store.conversation(B, B, 1, "Чужая тема")
    await title_messages(store, automatic, 0, 2)
    assert await store.queue_topic_title(A, automatic["id"])
    assert await store.set_topic_title(A, automatic["id"], "Секретное автоназвание", 2, 0)
    await store.remember(A, "забываемый факт")
    result = await store.forget(A, "забываемый")
    assert result == {"forgotten": ["забываемый факт"], "context_reset": True}
    reset = await store.conversation(A, A, 1)
    assert reset["title"] == "Новый чат"
    assert reset["title_auto"] is True and reset["title_message_count"] == 0
    assert reset["revision"] == automatic["revision"] + 1
    unchanged_manual = await store.conversation(A, A, 2)
    assert unchanged_manual["title"] == manual["title"]
    assert unchanged_manual["title_auto"] is False
    assert unchanged_manual["revision"] == manual["revision"] + 1
    assert (await store.conversation(A, A, 0))["title"] == main["title"]
    assert await store.conversation(B, B, 1) == foreign
    assert await store.topic_title_context(A, automatic["id"]) is None
    assert await store.queue_topic_title(A, automatic["id"]) is False
    assert await store.set_topic_title(A, automatic["id"], "Stale secret", 2, 0) is False
    async with store.connection(A) as conn:
        events = await conn.fetch(
            "SELECT payload FROM events WHERE kind='topic_title_reset' AND payload->>'user_id'=$1",
            str(A),
        )
    assert [event["payload"] for event in events] == [
        {
            "user_id": A,
            "conversation_id": str(automatic["id"]),
            "revision": reset["revision"],
        }
    ]
    assert "Секретное" not in json.dumps([dict(event) for event in events], ensure_ascii=False)
    await title_messages(store, automatic, 2, 4)
    assert await store.queue_topic_title(A, automatic["id"])
    fresh_context = await store.topic_title_context(A, automatic["id"])
    assert fresh_context["message_count"] == 2
    assert await store.set_topic_title(A, automatic["id"], "Новая тема", 2, reset["revision"])
    async with store.connection(A) as conn:
        payloads = await conn.fetch(
            "SELECT payload FROM events WHERE kind='topic_title' AND payload->>'conversation_id'=$1",
            str(automatic["id"]),
        )
    assert len(payloads) == 2
    assert {row["payload"]["revision"] for row in payloads} == {0, 1}


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
