"""Privacy boundaries against real PostgreSQL; only these synthetic owners are mutated."""

import os
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
import pytest_asyncio

from cronos.settings import Settings
from cronos.storage import Store, privacy_event_id, sanitized_usage, uid

A, B = -960101, -960102
pytestmark = pytest.mark.asyncio(loop_scope="module")


def update(user=A, *, thread=10, text="private text", date=None, update_id=None):
    return {
        "update_id": update_id or uuid4().int % 9_000_000_000,
        "message": {
            "message_id": uuid4().int % 1_000_000,
            "date": date if date is not None else int(datetime.now(UTC).timestamp()),
            "chat": {"id": user, "type": "private"},
            "from": {"id": user, "is_bot": False, "first_name": "Private name"},
            "message_thread_id": thread,
            "text": text,
        },
    }


async def cleanup(store):
    for owner in (A, B):
        async with store.connection(owner) as conn:
            runs = await conn.fetch("SELECT id FROM runs WHERE user_id=$1", owner)
            for table in ("checkpoint_writes", "checkpoint_blobs", "checkpoints"):
                await conn.execute(
                    f"DELETE FROM langgraph.{table} WHERE thread_id=ANY($1::text[])",
                    [str(row["id"]) for row in runs],
                )
            await conn.execute(
                "DELETE FROM occurrences WHERE schedule_id IN (SELECT id FROM schedules WHERE user_id=$1)",
                owner,
            )
            events = await conn.fetch("SELECT event_id FROM runs WHERE user_id=$1", owner)
            requests = await conn.fetch("SELECT id FROM privacy_requests WHERE user_id=$1", owner)
            event_ids = [row["event_id"] for row in events] + [
                privacy_event_id(row["id"]) for row in requests
            ]
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
            await conn.execute(
                """DELETE FROM events WHERE id=ANY($1::uuid[]) OR payload->>'user_id'=$2
                OR payload#>>'{message,chat,id}'=$2 OR payload#>>'{edited_message,chat,id}'=$2
                OR payload#>>'{callback_query,from,id}'=$2""",
                event_ids,
                str(owner),
            )


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def store():
    if not os.getenv("DATABASE_URL") or not os.getenv("ADMIN_DATABASE_URL"):
        pytest.skip("Real PostgreSQL URLs are required")
    value = Store(Settings())
    await value.open()
    try:
        yield value
    finally:
        await cleanup(value)
        await value.close()


@pytest_asyncio.fixture(autouse=True, loop_scope="module")
async def clean_users(store):
    await cleanup(store)
    for owner in (A, B):
        await store.ensure_user(owner)
    yield
    await cleanup(store)


async def run_for(store, owner=A, thread=10):
    conversation = await store.conversation(owner, owner, thread, "Sensitive title")
    incoming = update(owner, thread=thread)
    await store.ingest_updates([incoming], incoming["update_id"] + 1)
    async with store.connection() as conn:
        event_id = await conn.fetchval(
            "SELECT id FROM events WHERE update_id=$1", incoming["update_id"]
        )
    run = await store.start_run(event_id, owner, conversation["id"])
    return run, conversation, incoming


async def prepare(store, conversation, scope="all", target=None, run=None, source=None):
    request = await store.requests_prepare(
        conversation["user_id"],
        conversation,
        scope,
        target,
        source_key=source or f"privacy-test:{uuid4()}",
        run_id=run["id"] if run else None,
    )
    return request


async def begin(store, request):
    confirmed = await store.confirm_privacy_request(A, request["id"], request["origin_thread_id"])
    assert confirmed
    return await store.begin_privacy_erasure(request["id"], privacy_event_id(request["id"]))


async def add_checkpoints(store, run):
    async with store.connection() as conn:
        await conn.execute(
            "INSERT INTO langgraph.checkpoints(thread_id,checkpoint_ns,checkpoint_id,checkpoint,metadata) VALUES($1,'','privacy','{}',$2)",
            str(run["id"]),
            {"private": "checkpoint secret"},
        )
        await conn.execute(
            "INSERT INTO langgraph.checkpoint_blobs(thread_id,checkpoint_ns,channel,version,type,blob) VALUES($1,'','messages','1','bytes',$2)",
            str(run["id"]),
            b"checkpoint secret",
        )
        await conn.execute(
            "INSERT INTO langgraph.checkpoint_writes(thread_id,checkpoint_ns,checkpoint_id,task_id,idx,channel,type,blob) VALUES($1,'','privacy','task',0,'messages','bytes',$2)",
            str(run["id"]),
            b"checkpoint secret",
        )


async def test_confirmation_is_owner_origin_ttl_and_replay_scoped(store):
    run, conversation, _ = await run_for(store)
    request = await prepare(store, conversation, run=run, source="same-privacy-operation")
    assert (await prepare(store, conversation, run=run, source="same-privacy-operation"))[
        "id"
    ] == request["id"]
    assert (await store.privacy_request_for_run(run["id"]))["id"] == request["id"]
    assert await store.get_privacy_request(B, request["id"]) is None
    assert await store.confirm_privacy_request(B, request["id"], 10) is None
    assert await store.confirm_privacy_request(A, request["id"], 11) is None
    assert (await store.pending_privacy_request(A, 10, "all"))["id"] == request["id"]
    async with store.connection() as conn:
        await conn.execute(
            "UPDATE privacy_requests SET expires_at=now()-interval '1 second' WHERE id=$1",
            uid(request["id"]),
        )
    assert await store.confirm_privacy_request(A, request["id"], 10) is None
    replacement = await prepare(store, conversation)
    assert (await store.get_privacy_request(A, request["id"]))["state"] == "cancelled"
    await begin(store, replacement)
    repeated = await store.confirm_privacy_request(A, replacement["id"], 10)
    assert repeated["state"] == "erasing"
    async with store.connection() as conn:
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM events WHERE id=$1", privacy_event_id(replacement["id"])
            )
            == 1
        )


async def test_full_erasure_removes_all_content_and_preserves_exact_billing(store):
    run, conversation, incoming = await run_for(store)
    other, _, _ = await run_for(store, B)
    await add_checkpoints(store, run)
    await add_checkpoints(store, other)
    await store.save_operation(
        f"{run['id']}:private", A, run["id"], "model", {"private": "operation secret"}
    )
    await store.record_usage(
        A,
        run["id"],
        "privacy-cost",
        {"model": "test", "cost_rub": "0.125", "prompt_tokens": 17, "completion_tokens": 8},
    )
    async with store.connection(A) as conn:
        await conn.execute(
            "UPDATE users SET plan='START',pending_plan='PREMIUM',topup_micro=333,preferences=$2,reserved_micro=777 WHERE user_id=$1",
            A,
            {"private": "preference secret"},
        )
        await conn.execute(
            "INSERT INTO reservations(id,user_id,amount_micro) VALUES('privacy-hold',$1,777)", A
        )
        await conn.execute(
            "UPDATE usage SET raw=$2 WHERE user_id=$1",
            A,
            {
                "request_id": "receipt-123",
                "cost_rub": "0.125",
                "raw": {"text": "secret"},
                "prompt": "secret",
            },
        )
        await conn.execute(
            "UPDATE ledger SET description='private arbitrary description' WHERE user_id=$1", A
        )
        await conn.execute(
            "INSERT INTO memory(id,user_id,content) VALUES($1,$2,'memory secret')", uuid4(), A
        )
        await conn.execute(
            "INSERT INTO messages(user_id,conversation_id,role,content,excluded) VALUES($1,$2,'user',$3,true)",
            A,
            uid(conversation["id"]),
            {"text": "excluded secret"},
        )
        await conn.execute(
            "INSERT INTO artifacts(id,user_id,filename,mime,path,extracted) VALUES($1,$2,'secret.txt','text/plain','/private/secret',$3)",
            uuid4(),
            A,
            {"text": "file secret"},
        )
        financial = dict(
            await conn.fetchrow(
                "SELECT plan,pending_plan,balance_micro,reserved_micro,entitlement_micro,topup_micro,period_start,period_end,billing_anchor FROM users WHERE user_id=$1",
                A,
            )
        )
    delivered = await store.enqueue(A, A, 0, {"text": "General secret"}, "privacy-general")
    async with store.connection() as conn:
        await conn.execute(
            "UPDATE outbox SET telegram_ids='[111,112]',state='sending',owner='delivery' WHERE id=$1",
            delivered,
        )
    request = await prepare(store, conversation, run=run)
    result = await begin(store, request)
    assert result["job"]["topic_ids"] == [10]
    assert result["job"]["general_message_ids"] == [111, 112]
    assert result["job"]["delete_user_files"] is True
    assert "run_ids" not in result["job"]
    assert await store.claimed_delivery(delivered, "delivery") is None
    assert await store.event_current(run["event_id"]) is None
    async with store.connection(A) as conn:
        for table in (
            "messages",
            "conversations",
            "memory",
            "artifacts",
            "runs",
            "operations",
            "outbox",
            "run_metrics",
            "schedules",
        ):
            assert await conn.fetchval(f"SELECT count(*) FROM {table} WHERE user_id=$1", A) == 0, (
                table
            )
        assert (
            dict(
                await conn.fetchrow(
                    "SELECT plan,pending_plan,balance_micro,reserved_micro,entitlement_micro,topup_micro,period_start,period_end,billing_anchor FROM users WHERE user_id=$1",
                    A,
                )
            )
            == financial
        )
        assert await conn.fetchval(
            "SELECT content_reset_at IS NOT NULL FROM users WHERE user_id=$1", A
        )
        receipt = await conn.fetchrow("SELECT * FROM usage WHERE operation_id='privacy-cost'")
        assert (
            receipt["cost_micro"] == 125_000
            and receipt["prompt_tokens"] == 17
            and receipt["run_id"] is None
        )
        assert receipt["raw"] == {"request_id": "receipt-123", "cost_rub": "0.125"}
        assert (
            await conn.fetchval("SELECT amount_micro FROM ledger WHERE operation_id='privacy-cost'")
            == -125_000
        )
        assert (
            await conn.fetchval("SELECT status FROM reservations WHERE id='privacy-hold'")
            == "reserved"
        )
        assert (
            await conn.fetchval(
                "SELECT payload FROM events WHERE update_id=$1", incoming["update_id"]
            )
            == {}
        )
        for table in ("checkpoints", "checkpoint_blobs", "checkpoint_writes"):
            assert (
                await conn.fetchval(
                    f"SELECT count(*) FROM langgraph.{table} WHERE thread_id=$1", str(run["id"])
                )
                == 0
            )
            assert (
                await conn.fetchval(
                    f"SELECT count(*) FROM langgraph.{table} WHERE thread_id=$1", str(other["id"])
                )
                == 1
            )
    assert await store.run_active(other["id"], other["fence"])
    await store.ingest_updates([incoming], incoming["update_id"] + 1)
    assert await store.event_current(run["event_id"]) is None


async def test_chat_erasure_includes_request_origin_but_keeps_shared_library_and_other_chat(store):
    target_run, target, _ = await run_for(store, thread=10)
    origin_run, origin, _ = await run_for(store, thread=20)
    other_run, _, _ = await run_for(store, thread=30)
    await add_checkpoints(store, origin_run)
    await store.save_operation(
        f"{origin_run['id']}:privacy",
        A,
        origin_run["id"],
        "privacy_request",
        {"title": "Sensitive title"},
    )
    async with store.connection(A) as conn:
        await conn.execute(
            "INSERT INTO memory(id,user_id,content) VALUES($1,$2,'Shared preference')", uuid4(), A
        )
        await conn.execute(
            "INSERT INTO artifacts(id,user_id,filename,mime,path) VALUES($1,$2,'shared.txt','text/plain','/shared')",
            uuid4(),
            A,
        )
        await conn.execute(
            "INSERT INTO messages(user_id,conversation_id,role,content) VALUES($1,$2,'user',$3)",
            A,
            uid(origin["id"]),
            {"text": "Keep unrelated origin history"},
        )
    request = await prepare(store, origin, "chat", target["id"], run=origin_run)
    await store.enqueue(A, A, 20, {"text": "Sensitive title"}, f"privacy-confirm:{request['id']}")
    job = (await begin(store, request))["job"]
    assert job["topic_ids"] == [10] and not job["delete_user_files"]
    async with store.connection(A) as conn:
        assert await conn.fetchval("SELECT count(*) FROM conversations WHERE user_id=$1", A) == 2
        assert await conn.fetchval("SELECT count(*) FROM messages WHERE user_id=$1", A) == 1
        assert await conn.fetchval("SELECT count(*) FROM memory WHERE user_id=$1", A) == 1
        assert await conn.fetchval("SELECT count(*) FROM artifacts WHERE user_id=$1", A) == 1
        assert await conn.fetchval("SELECT count(*) FROM outbox WHERE user_id=$1", A) == 0
        assert await conn.fetchval("SELECT count(*) FROM operations WHERE user_id=$1", A) == 0
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM langgraph.checkpoints WHERE thread_id=$1",
                str(origin_run["id"]),
            )
            == 0
        )
        assert await conn.fetchval("SELECT content_reset_at FROM users WHERE user_id=$1", A) is None
    assert await store.event_current(target_run["event_id"]) is None
    assert await store.event_current(origin_run["event_id"]) is None
    assert await store.run_active(other_run["id"], other_run["fence"])


async def test_finish_is_idempotent_minimal_and_honest_about_partial_cleanup(store):
    _, conversation, _ = await run_for(store)
    request = await prepare(store, conversation)
    await begin(store, request)
    result = {
        "files_erased": False,
        "files_failed": True,
        "topics_failed": 2,
        "messages_failed": 3,
        "general_history_limited": True,
    }
    assert await store.finish_privacy_erasure(request["id"], result)
    assert await store.finish_privacy_erasure(request["id"], result)
    assert (await store.confirm_privacy_request(A, request["id"], 10))["state"] == "done"
    row = await store.get_privacy_request(A, request["id"])
    assert row["job"] == {} and row["run_id"] is None and row["conversation_id"] is None
    async with store.connection() as conn:
        deliveries = await conn.fetch("SELECT payload,thread_id FROM outbox WHERE user_id=$1", A)
        assert len(deliveries) == 1 and deliveries[0]["thread_id"] == 0
        text = deliveries[0]["payload"]["text"]
        assert "Не удалось удалить файлы" in text and "тем: 2; сообщений: 3" in text
        assert "48 часов" in text and "баланс" in text and "Данные Cronos очищены." not in text
        assert (
            await conn.fetchval(
                "SELECT payload FROM events WHERE id=$1", privacy_event_id(request["id"])
            )
            == {}
        )
    new_conversation = await store.conversation(A, A, 0)
    new_request = await prepare(store, new_conversation)
    assert new_request["id"] != request["id"]


async def test_failed_erasing_job_resumes_explicitly_without_second_database_wipe(store):
    _, conversation, _ = await run_for(store)
    request = await prepare(store, conversation)
    initial = await begin(store, request)
    async with store.connection() as conn:
        await conn.execute(
            "UPDATE events SET state='failed',attempts=3 WHERE id=$1",
            privacy_event_id(request["id"]),
        )
    general = await store.conversation(A, A, 0)
    repeated = await prepare(store, general)
    assert repeated["id"] == request["id"]
    await store.confirm_privacy_request(A, repeated["id"], 0)
    resumed = await store.begin_privacy_erasure(request["id"], privacy_event_id(request["id"]))
    assert resumed["job"] == initial["job"]
    async with store.connection(A) as conn:
        assert await conn.fetchval("SELECT count(*) FROM conversations WHERE user_id=$1", A) == 1
        assert (
            await conn.fetchval(
                "SELECT attempts FROM events WHERE id=$1", privacy_event_id(request["id"])
            )
            == 0
        )


async def test_ingress_only_valid_confirmation_cancels_running_work(store):
    run, conversation, _ = await run_for(store)
    request = await prepare(store, conversation)
    for text in ("подтверждаю", "подтверждаю удаление чата", "согласен"):
        incoming = update(text=text)
        await store.ingest_updates([incoming], incoming["update_id"] + 1)
        assert await store.run_active(run["id"], run["fence"])
    incoming = update(text="подтверждаю полную очистку")
    await store.ingest_updates([incoming], incoming["update_id"] + 1)
    assert not await store.run_active(run["id"], run["fence"])
    assert (await store.get_privacy_request(A, request["id"]))["state"] == "pending"


async def test_ingress_after_erasure_drops_old_topics_dates_and_sanitizes_nested_content(store):
    _, conversation, _ = await run_for(store)
    request = await prepare(store, conversation)
    await begin(store, request)
    blocked = update(thread=0, text="during erase")
    await store.ingest_updates([blocked], blocked["update_id"] + 1)
    await store.finish_privacy_erasure(request["id"], {"files_erased": True})
    old = update(thread=0, date=int((datetime.now(UTC) - timedelta(days=1)).timestamp()))
    deleted_topic = update(thread=10)
    new = update(thread=0, text="fresh text", date=int(datetime.now(UTC).timestamp()) + 2)
    new["message"]["reply_to_message"] = {"text": "deleted secret", "message_id": 1}
    new["message"]["quote"] = {"text": "deleted secret"}
    for incoming in (old, deleted_topic, new):
        await store.ingest_updates([incoming], incoming["update_id"] + 1)
    async with store.connection() as conn:
        for incoming in (blocked, old, deleted_topic):
            assert (
                await conn.fetchval(
                    "SELECT payload FROM events WHERE update_id=$1", incoming["update_id"]
                )
                == {}
            )
        payload = await conn.fetchval(
            "SELECT payload FROM events WHERE update_id=$1", new["update_id"]
        )
        assert payload["message"]["text"] == "fresh text"
        assert "reply_to_message" not in payload["message"] and "quote" not in payload["message"]


async def test_late_receipt_cannot_reintroduce_provider_content_or_deleted_run(store):
    run, conversation, _ = await run_for(store)
    await store.reserve(A, "privacy-late", 1_000_000)
    await store.record_usage(
        A,
        run["id"],
        "privacy-late",
        {"cost_rub": None, "request_id": "receipt-pending", "raw": {"private": "secret"}},
    )
    request = await prepare(store, conversation)
    await begin(store, request)
    await store.record_usage(
        A,
        run["id"],
        "privacy-late",
        {
            "cost_rub": "0.5",
            "request_id": "receipt-pending",
            "model": "test",
            "raw": {"private": "secret"},
            "prompt": "secret",
        },
    )
    row = await store.reconciliation_usage(A, "privacy-late")
    assert row["run_id"] is None and row["cost_micro"] == 500_000
    assert row["raw"] == {"cost_rub": "0.5", "request_id": "receipt-pending", "model": "test"}
    async with store.connection(A) as conn:
        assert await conn.fetchval("SELECT reserved_micro FROM users WHERE user_id=$1", A) == 0
        assert (
            await conn.fetchval("SELECT count(*) FROM ledger WHERE operation_id='privacy-late'")
            == 1
        )


async def test_sanitized_usage_drops_unknown_or_structured_values(store):
    assert sanitized_usage(
        {
            "model": "test",
            "prompt_tokens": 42,
            "request_id": "receipt",
            "raw": {"text": "private"},
            "prompt": "secret",
            "provider": {"text": "secret"},
        }
    ) == {"model": "test", "prompt_tokens": 42, "request_id": "receipt"}


async def test_cross_owner_target_and_source_event_never_erased(store):
    _, own, _ = await run_for(store)
    other_run, other, _ = await run_for(store, B)
    with pytest.raises(ValueError, match="Чат не найден"):
        await prepare(store, own, "chat", other["id"])
    request = await prepare(store, own, "chat", source=f"privacy-request:{other_run['event_id']}")
    await begin(store, request)
    assert await store.event_current(other_run["event_id"]) is not None
    assert await store.run_active(other_run["id"], other_run["fence"])


async def test_general_aliases_share_erasure_scope_and_cutoff(store):
    zero_run, zero, _ = await run_for(store, thread=0)
    one_run, one, _ = await run_for(store, thread=1)
    await add_checkpoints(store, one_run)
    async with store.connection(A) as conn:
        for conversation in (zero, one):
            await conn.execute(
                "INSERT INTO messages(user_id,conversation_id,role,content,excluded) VALUES($1,$2,'user',$3,true)",
                A,
                uid(conversation["id"]),
                {"text": "General secret"},
            )
            await conn.execute(
                "INSERT INTO schedules(id,user_id,conversation_id,chat_id,thread_id,due_at,instruction,fixed_text) VALUES($1,$2,$3,$2,$4,now(),'secret','secret')",
                uuid4(),
                A,
                uid(conversation["id"]),
                conversation["thread_id"],
            )
    request = await prepare(store, zero, "chat")
    job = (await begin(store, request))["job"]
    assert job["topic_ids"] == [] and not job["delete_user_files"]
    async with store.connection(A) as conn:
        for table in ("conversations", "messages", "runs", "schedules"):
            assert await conn.fetchval(f"SELECT count(*) FROM {table} WHERE user_id=$1", A) == 0
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM langgraph.checkpoints WHERE thread_id=$1", str(one_run["id"])
            )
            == 0
        )
        deleted_at = await conn.fetchval(
            "SELECT deleted_at FROM deleted_topics WHERE user_id=$1 AND thread_id=0", A
        )
        assert await conn.fetchval("SELECT count(*) FROM deleted_topics WHERE user_id=$1", A) == 2
    await store.finish_privacy_erasure(request["id"], {})
    same_second = update(thread=0, date=int(deleted_at.timestamp()))
    await store.ingest_updates([same_second], same_second["update_id"] + 1)
    async with store.connection() as conn:
        assert (
            await conn.fetchval(
                "SELECT payload FROM events WHERE update_id=$1", same_second["update_id"]
            )
            == {}
        )
    assert await store.event_current(zero_run["event_id"]) is None


async def test_failed_remote_ids_retry_but_known_old_general_messages_are_excluded(store):
    _, conversation, _ = await run_for(store)
    old = update(thread=0, date=int(datetime.now(UTC).timestamp()) - 48 * 3600)
    recent = update(thread=0)
    for incoming in (old, recent):
        await store.ingest_updates([incoming], incoming["update_id"] + 1)
    old_delivery = await store.enqueue(A, A, 0, {"text": "old"}, "privacy-old-delivery")
    async with store.connection() as conn:
        await conn.execute(
            "UPDATE outbox SET telegram_ids='[9999999]',sent_at=now()-interval '49 hours' WHERE id=$1",
            old_delivery,
        )
        await conn.execute(
            "INSERT INTO deleted_topics(user_id,chat_id,thread_id) VALUES($1,$1,77)", A
        )
    request = await prepare(store, conversation)
    job = (await begin(store, request))["job"]
    assert job["topic_ids"] == [10, 77]
    assert job["general_message_ids"] == [recent["message"]["message_id"]]
    await store.finish_privacy_erasure(
        request["id"], {"files_erased": True, "topics_failed": 2, "messages_failed": 1}
    )
    new_conversation = await store.conversation(A, A, 0)
    retry = await prepare(store, new_conversation)
    retried = (await begin(store, retry))["job"]
    assert retried["topic_ids"] == [10, 77]
    assert retried["general_message_ids"] == job["general_message_ids"]


async def test_resume_callback_is_current_and_single_chat_scrubs_other_origin_callback(store):
    target_run, target, _ = await run_for(store)
    _, origin, _ = await run_for(store, thread=20)
    request = await prepare(store, origin, "chat", target["id"])
    incoming = update(thread=20, text="Sensitive title")
    message = incoming.pop("message")
    incoming["callback_query"] = {
        "id": "callback",
        "from": {"id": A},
        "message": message,
        "data": f"privacy:confirm:{uid(request['id']).hex}",
    }
    await store.ingest_updates([incoming], incoming["update_id"] + 1)
    await begin(store, request)
    async with store.connection() as conn:
        assert (
            await conn.fetchval(
                "SELECT payload FROM events WHERE update_id=$1", incoming["update_id"]
            )
            == {}
        )
        await conn.execute(
            "UPDATE events SET state='failed',attempts=3 WHERE id=$1",
            privacy_event_id(request["id"]),
        )
    # A new callback remains available while erasing, so explicit retries work.
    fresh = update(thread=20)
    fresh_message = fresh.pop("message")
    fresh["callback_query"] = {
        "id": "retry",
        "from": {"id": A},
        "message": fresh_message,
        "data": f"privacy:confirm:{uid(request['id']).hex}",
    }
    await store.ingest_updates([fresh], fresh["update_id"] + 1)
    async with store.connection() as conn:
        event_id = await conn.fetchval(
            "SELECT id FROM events WHERE update_id=$1", fresh["update_id"]
        )
    current = await store.event_current(event_id)
    assert (
        current and current["payload"]["callback_query"]["data"] == fresh["callback_query"]["data"]
    )
    assert "text" not in current["payload"]["callback_query"]["message"]
    assert current["payload"]["callback_query"]["message"]["from"] == {"id": A, "is_bot": False}
    assert "Private name" not in str(current["payload"])
    assert await store.event_current(target_run["event_id"]) is None


async def test_failed_erasure_can_resume_with_exact_advertised_text(store):
    _, conversation, _ = await run_for(store)
    request = await prepare(store, conversation)
    await begin(store, request)
    async with store.connection() as conn:
        await conn.execute(
            "UPDATE events SET state='failed',attempts=3 WHERE id=$1",
            privacy_event_id(request["id"]),
        )
    general = await store.conversation(A, A, 0)
    recovery = await prepare(store, general)
    incoming = update(thread=0, text="Подтверждаю полную очистку")
    await store.ingest_updates([incoming], incoming["update_id"] + 1)
    async with store.connection() as conn:
        event_id = await conn.fetchval(
            "SELECT id FROM events WHERE update_id=$1", incoming["update_id"]
        )
    assert await store.event_current(event_id)
    pending = await store.pending_privacy_request(A, 0, "all")
    assert pending["id"] == recovery["id"]
    assert await store.pending_privacy_request(A, 0) is None
    assert await store.confirm_privacy_request(A, pending["id"], 0)
    async with store.connection() as conn:
        assert (
            await conn.fetchval(
                "SELECT state FROM events WHERE id=$1", privacy_event_id(request["id"])
            )
            == "pending"
        )
    await store.finish_privacy_erasure(request["id"], {"files_erased": True})
    async with store.connection(A) as conn:
        assert await conn.fetchval("SELECT count(*) FROM conversations WHERE user_id=$1", A) == 0
        assert await conn.fetchval("SELECT payload FROM events WHERE id=$1", event_id) == {}
