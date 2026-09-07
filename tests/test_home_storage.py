"""Real PostgreSQL tests, limited to the explicitly listed synthetic accounts."""

import asyncio
import hashlib
import os
from uuid import UUID, uuid4

import asyncpg
import pytest
import pytest_asyncio

from cronos.settings import Settings
from cronos.storage import HOME_TITLE, Store, migrate, privacy_event_id, uid

A, B = -970101, -970102
EXISTING, EMPTY, RESET = 9_790_000_001, 9_790_000_002, 9_790_000_003
HOMED, ERASING = 9_790_000_004, 9_790_000_005
OWNERS = (A, B, EXISTING, EMPTY, RESET, HOMED, ERASING)
pytestmark = pytest.mark.asyncio(loop_scope="module")


async def cleanup(store):
    for owner in OWNERS:
        async with store.connection(owner) as conn:
            events = await conn.fetch("SELECT event_id FROM runs WHERE user_id=$1", owner)
            requests = await conn.fetch("SELECT id FROM privacy_requests WHERE user_id=$1", owner)
            event_ids = [row["event_id"] for row in events] + [
                privacy_event_id(row["id"]) for row in requests
            ]
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
            await conn.execute(
                "DELETE FROM events WHERE id=ANY($1::uuid[]) OR payload->>'user_id'=$2",
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
    for owner in OWNERS:
        await store.ensure_user(owner)
    yield
    await cleanup(store)


async def financial(store, owner=A):
    async with store.connection(owner) as conn:
        return dict(
            await conn.fetchrow(
                """SELECT plan,pending_plan,balance_micro,reserved_micro,
            entitlement_micro,topup_micro,period_start,period_end,billing_anchor FROM users WHERE user_id=$1""",
                owner,
            )
        )


async def prepare_erasure(store, conversation, scope):
    owner = conversation["user_id"]
    request = await store.requests_prepare(
        owner, conversation, scope, source_key=f"home-privacy-test:{uuid4()}"
    )
    assert await store.confirm_privacy_request(owner, request["id"], conversation["thread_id"])
    begun = await store.begin_privacy_erasure(request["id"], privacy_event_id(request["id"]))
    assert begun
    return request


async def test_home_owner_unique_idempotent_and_key_stable(store):
    first = await store.conversation(A, A, 10, "First")
    second = await store.conversation(A, A, 11, "Second")
    foreign = await store.conversation(B, B, 10, "Foreign")
    before = await financial(store)
    key = await store.home_creation_key(A)
    assert key.startswith(f"home-create:{A}:")
    UUID(key.rsplit(":", 1)[1])
    assert await store.get_home(A) is None
    with pytest.raises(ValueError, match="Чат не найден"):
        await store.set_home(A, foreign["id"])
    home = await store.set_home(A, first["id"])
    assert home["title"] == HOME_TITLE and home["is_home"] and not home["title_auto"]
    assert home["revision"] == first["revision"] + 1
    repeated = await store.set_home(A, first["id"])
    assert repeated == home
    assert (await store.get_home(A))["id"] == uid(first["id"])
    assert await store.get_home(B) is None
    assert await store.home_creation_key(A) == key
    with pytest.raises(ValueError, match="уже существует"):
        await store.set_home(A, second["id"])
    async with store.connection(A) as conn:
        with pytest.raises(asyncpg.UniqueViolationError):
            await conn.execute(
                "UPDATE conversations SET is_home=true WHERE user_id=$1 AND id=$2",
                A,
                uid(second["id"]),
            )
    assert await financial(store) == before
    listed = await store.list_conversations(A)
    assert sum(row["is_home"] for row in listed) == 1


async def test_home_promotion_invalidates_queued_titles_and_manual_renames(store):
    conversation = await store.conversation(A, A, 10)
    await store.add_message(A, conversation["id"], "user", "Tell me a story")
    await store.add_message(A, conversation["id"], "assistant", "A story")
    context = await store.topic_title_context(A, conversation["id"])
    assert context and await store.queue_topic_title(A, conversation["id"])
    home = await store.set_home(A, conversation["id"])
    assert await store.topic_title_context(A, home["id"]) is None
    assert not await store.queue_topic_title(A, home["id"])
    assert not await store.set_topic_title(
        A, home["id"], "Story", 2, context["conversation"]["revision"]
    )
    manual = await store.sync_topic_title(A, A, 10, "Changed by user", manual=True, update_id=100)
    assert manual["title"] == HOME_TITLE and manual["revision"] == home["revision"]
    assert manual["title_update_id"] == 100 and not manual["title_auto"]
    assert await store.sync_topic_title(A, A, 10, "Stale", manual=True, update_id=99) is None
    replay = await store.sync_topic_title(A, A, 10, "Replayed", manual=True, update_id=100)
    created = await store.sync_topic_title(A, A, 10, "Created replay", manual=True, created=True)
    automated = await store.sync_topic_title(A, A, 10, "Automated")
    assert all(row["title"] == HOME_TITLE for row in (replay, created, automated))
    await store.forget(A, "anything")
    forgotten = await store.get_home(A)
    assert forgotten["title"] == HOME_TITLE and not forgotten["title_auto"]
    async with store.connection(A) as conn:
        # Even an inconsistent legacy flag cannot re-enable auto titles for home.
        await conn.execute("UPDATE conversations SET title_auto=true WHERE id=$1", home["id"])
        await conn.execute("UPDATE messages SET excluded=false WHERE user_id=$1", A)
    assert await store.topic_title_context(A, home["id"]) is None
    assert not await store.set_topic_title(A, home["id"], "Wrong title", 2, forgotten["revision"])


async def test_reset_home_is_atomic_deduplicated_and_keeps_history(store):
    conversation = await store.conversation(A, A, 10)
    await store.set_home(A, conversation["id"])
    await store.add_message(A, conversation["id"], "user", "Keep this message")
    original_key = await store.home_creation_key(A)
    before = await financial(store)
    keys = await asyncio.gather(
        *(store.reset_home(A, conversation["id"], source_key="home-reset-test") for _ in range(3))
    )
    assert len(set(keys)) == 1 and keys[0] != original_key
    assert await store.home_creation_key(A) == keys[0]
    assert await store.get_home(A) is None
    assert await store.history(A, conversation["id"]) == [
        {"role": "user", "content": "Keep this message"}
    ]
    assert await financial(store) == before
    async with store.connection(A) as conn:
        marker = await conn.fetchrow("SELECT * FROM operations WHERE id='home-reset-test'")
        assert marker["kind"] == "home_reset" and marker["run_id"] is None
        assert marker["result"] == {"key": keys[0]} and marker["status"] == "done"
    with pytest.raises(ValueError, match="недоступна"):
        await store.reset_home(B, source_key="home-reset-test")


async def test_reset_foreign_or_non_home_is_rejected_without_rotation(store):
    conversation = await store.conversation(A, A, 10)
    foreign = await store.conversation(B, B, 10)
    await store.set_home(B, foreign["id"])
    key = await store.home_creation_key(A)
    for target in (conversation["id"], foreign["id"]):
        with pytest.raises(ValueError, match="Домашняя тема не найдена"):
            await store.reset_home(A, target, source_key=f"invalid-reset:{target}")
        assert await store.home_creation_key(A) == key
    rotated = await store.reset_home(A, source_key="ambiguous-reset")
    assert rotated != key
    assert await store.reset_home(A, source_key="ambiguous-reset") == rotated


@pytest.mark.parametrize(
    "scope,make_home,rotated",
    [("chat", True, True), ("chat", False, False), ("all", True, True), ("all", False, True)],
)
async def test_erasure_rotates_home_generation_once_and_preserves_finances(
    store, scope, make_home, rotated
):
    conversation = await store.conversation(A, A, 10)
    if make_home:
        await store.set_home(A, conversation["id"])
    async with store.connection(A) as conn:
        await conn.execute(
            "UPDATE users SET plan='START',pending_plan='PREMIUM',balance_micro=123456,reserved_micro=7,topup_micro=333 WHERE user_id=$1",
            A,
        )
    before = await financial(store)
    key = await store.home_creation_key(A)
    request = await prepare_erasure(store, conversation, scope)
    new_key = await store.home_creation_key(A)
    assert (new_key != key) is rotated
    assert await store.get_home(A) is None
    await store.begin_privacy_erasure(request["id"], privacy_event_id(request["id"]))
    assert await store.home_creation_key(A) == new_key
    await store.finish_privacy_erasure(request["id"], {"files_erased": scope == "all"})
    assert await store.home_creation_key(A) == new_key
    assert await financial(store) == before


async def test_bootstrap_migration_is_idempotent_and_only_seeds_active_unhomed_accounts(store):
    existing = await store.conversation(EXISTING, EXISTING, 10)
    await store.conversation(RESET, RESET, 10)
    await store.conversation(A, A, 10)
    homed = await store.conversation(HOMED, HOMED, 10)
    await store.set_home(HOMED, homed["id"])
    erasing = await store.conversation(ERASING, ERASING, 10)
    await prepare_erasure(store, erasing, "all")
    # /clearall recovery can transiently create an empty General while erasing.
    await store.conversation(ERASING, ERASING, 0)
    async with store.connection(RESET) as conn:
        await conn.execute("UPDATE users SET content_reset_at=now() WHERE user_id=$1", RESET)
    await migrate(store.settings)
    await migrate(store.settings)
    expected_id = UUID(hashlib.md5(f"cronos:home-bootstrap:v1:{EXISTING}".encode()).hexdigest())
    async with store.connection() as conn:
        rows = await conn.fetch(
            "SELECT * FROM events WHERE kind='home_init' AND payload->>'user_id'=ANY($1::text[])",
            [str(owner) for owner in OWNERS],
        )
    by_owner = {row["payload"]["user_id"]: row for row in rows}
    assert set(by_owner) == {EXISTING, RESET}
    assert by_owner[EXISTING]["id"] == expected_id
    original_generation = (await store.home_creation_key(EXISTING)).rsplit(":", 1)[1]
    assert by_owner[EXISTING]["payload"] == {"user_id": EXISTING, "generation": original_generation}
    assert by_owner[EXISTING]["state"] == "awaiting_home_worker"
    assert await store.event_current(expected_id) is None
    assert await store.claim_event("legacy-worker", expected_id) is None
    assert await store.activate_home_bootstrap() == 2
    assert await store.activate_home_bootstrap() == 0
    assert await store.event_current(expected_id)
    request = await prepare_erasure(store, existing, "all")
    await store.finish_privacy_erasure(request["id"], {"files_erased": True})
    assert await store.event_current(expected_id) is None
    await migrate(store.settings)
    async with store.connection() as conn:
        assert not await conn.fetchval("SELECT 1 FROM events WHERE id=$1", expected_id)
    assert await store.get_home(EXISTING) is None
    # A new conversation is explicit return after wiping; it now qualifies for backfill.
    await store.conversation(EXISTING, EXISTING, 0)
    await migrate(store.settings)
    assert await store.event_current(expected_id) is None
    assert await store.activate_home_bootstrap() == 1
    returned = await store.event_current(expected_id)
    assert (
        returned["payload"]["generation"]
        == (await store.home_creation_key(EXISTING)).rsplit(":", 1)[1]
    )
    assert returned["payload"]["generation"] != original_generation


async def test_queue_home_is_idempotent_owner_scoped_and_privacy_guarded(store):
    assert not await store.queue_home(-979999)
    assert await store.get_home(A) is None
    assert await store.queue_home(A)
    assert not await store.queue_home(A)
    expected_generation = (await store.home_creation_key(A)).rsplit(":", 1)[1]
    async with store.connection() as conn:
        rows = await conn.fetch(
            "SELECT * FROM events WHERE kind='home_init' AND payload->>'user_id'=$1", str(A)
        )
    assert len(rows) == 1
    assert rows[0]["payload"] == {"user_id": A, "generation": expected_generation}
    assert rows[0]["id"] == UUID(
        hashlib.md5(f"cronos:home-init:{A}:{expected_generation}".encode()).hexdigest()
    )
    other = await store.conversation(B, B, 10)
    await store.set_home(B, other["id"])
    assert not await store.queue_home(B)
    # A deletion invalidates the old event; erasing blocks a new one until explicit activity after finish.
    conversation = await store.conversation(A, A, 10)
    request = await prepare_erasure(store, conversation, "all")
    assert not await store.queue_home(A)
    assert await store.event_current(rows[0]["id"]) is None
    await store.finish_privacy_erasure(request["id"], {"files_erased": True})
    async with store.connection() as conn:
        assert not await conn.fetchval(
            "SELECT 1 FROM events WHERE kind='home_init' AND payload->>'user_id'=$1", str(A)
        )
    # Worker invokes this only after a new interactive message, never automatically after wipe.
    await store.conversation(A, A, 20)
    assert await store.queue_home(A)
    assert not await store.queue_home(A)


async def test_proactivity_callback_replay_cannot_undo_newer_off_or_repeat_cancellation(store):
    conversation = await store.conversation(A, A, 10)
    other = await store.conversation(B, B, 10)
    async with store.connection(A) as conn:
        await conn.execute(
            "UPDATE users SET preferences=preferences||$2::jsonb WHERE user_id=$1",
            A,
            {"private_profile": "Do not copy into operation"},
        )
        await conn.execute(
            "INSERT INTO schedules(id,user_id,conversation_id,chat_id,due_at,instruction,fixed_text,proactive) VALUES($1,$2,$3,$2,now(),'initiative','initiative',true)",
            uuid4(),
            A,
            uid(conversation["id"]),
        )
        await conn.execute(
            "INSERT INTO schedules(id,user_id,conversation_id,chat_id,due_at,instruction,fixed_text,proactive) VALUES($1,$2,$3,$2,now(),'explicit task','explicit task',false)",
            uuid4(),
            A,
            uid(conversation["id"]),
        )
    async with store.connection(B) as conn:
        await conn.execute(
            "INSERT INTO schedules(id,user_id,conversation_id,chat_id,due_at,instruction,fixed_text,proactive) VALUES($1,$2,$3,$2,now(),'other owner','other owner',true)",
            uuid4(),
            B,
            uid(other["id"]),
        )
    assert await store.set_proactivity(A, True, "callback:on:proactivity") == {"proactivity": True}
    assert await store.set_proactivity(A, False, "callback:off:proactivity") == {
        "proactivity": False
    }
    assert await store.set_proactivity(A, True, "callback:on:proactivity") == {"proactivity": True}
    assert await store.set_proactivity(A, False, "callback:off:proactivity") == {
        "proactivity": False
    }
    preferences = await store.preferences(A)
    assert (
        preferences["proactivity"] is False
        and preferences["private_profile"] == "Do not copy into operation"
    )
    async with store.connection(A) as conn:
        schedules = await conn.fetch(
            "SELECT proactive,state,revision FROM schedules WHERE user_id=$1 ORDER BY proactive DESC",
            A,
        )
        assert [dict(row) for row in schedules] == [
            {"proactive": True, "state": "cancelled", "revision": 2},
            {"proactive": False, "state": "active", "revision": 1},
        ]
        markers = await conn.fetch("SELECT result,kind,run_id FROM operations WHERE user_id=$1", A)
        assert len(markers) == 2
        assert all(
            row["kind"] == "home_preference"
            and row["run_id"] is None
            and set(row["result"]) == {"proactivity"}
            for row in markers
        )
    with pytest.raises(ValueError, match="недоступна"):
        await store.set_proactivity(B, False, "callback:on:proactivity")
    async with store.connection(B) as conn:
        assert await conn.fetchval("SELECT state FROM schedules WHERE user_id=$1", B) == "active"
    await store.save_operation("callback:wrong-kind", A, None, "model", {"proactivity": True})
    with pytest.raises(ValueError, match="недоступна"):
        await store.set_proactivity(A, True, "callback:wrong-kind")
    assert (await store.preferences(A))["proactivity"] is False


async def test_home_panel_latest_order_supersedes_pending_and_sending_without_losing_dedupe(store):
    old = await store.enqueue_home_panel(
        A, A, 10, {"text": "FREE", "edit_message_id": 100}, "home-panel:old", 10
    )
    async with store.connection() as conn:
        await conn.execute(
            "UPDATE outbox SET state='sending',owner='coordinator',lease_until=now()+interval '1 minute' WHERE id=$1",
            old,
        )
    pending = await store.enqueue(
        A, A, 10, {"text": "Legacy FREE", "edit_message_id": 100}, "home-panel:legacy-pending"
    )
    latest = await store.enqueue_home_panel(
        A, A, 10, {"text": "PRO", "edit_message_id": 100}, "home-panel:latest", 20
    )
    assert latest and latest != old
    assert await store.claimed_delivery(old, "coordinator") is None
    assert (
        await store.enqueue_home_panel(
            A, A, 10, {"text": "Retry FREE", "edit_message_id": 100}, "home-panel:old", 10
        )
        == old
    )
    assert (
        await store.enqueue_home_panel(
            A, A, 10, {"text": "Late FREE", "edit_message_id": 100}, "home-panel:late-old", 15
        )
        is None
    )
    async with store.connection() as conn:
        rows = {
            row["id"]: dict(row)
            for row in await conn.fetch(
                "SELECT id,state,owner,lease_until,error,payload FROM outbox WHERE user_id=$1", A
            )
        }
        assert len(rows) == 3
        for stale_id in (old, pending):
            assert rows[stale_id]["state"] == "cancelled" and rows[stale_id]["owner"] is None
            assert (
                rows[stale_id]["lease_until"] is None
                and rows[stale_id]["error"] == "Superseded home panel"
            )
        assert rows[latest]["state"] == "pending" and rows[latest]["payload"]["text"] == "PRO"
        assert rows[latest]["payload"]["home_panel_order"] == 20


async def test_home_panel_order_is_scoped_to_owner_chat_and_message(store):
    other_owner = await store.enqueue_home_panel(
        B, B, 10, {"text": "Other owner", "edit_message_id": 100}, "home-panel:other-owner", 100
    )
    other_message = await store.enqueue_home_panel(
        A, A, 10, {"text": "Other message", "edit_message_id": 101}, "home-panel:other-message", 100
    )
    other_chat = await store.enqueue_home_panel(
        A, -123, 10, {"text": "Other chat", "edit_message_id": 100}, "home-panel:other-chat", 100
    )
    ordinary = await store.enqueue(
        A, A, 10, {"text": "Regular edit", "edit_message_id": 100}, "ordinary-edit"
    )
    latest = await store.enqueue_home_panel(
        A, A, 10, {"text": "Main", "edit_message_id": 100}, "home-panel:main", 1
    )
    assert latest
    first_send = await store.enqueue_home_panel(
        A, A, 10, {"text": "A new panel"}, "home-panel:new-message", 0
    )
    assert (
        await store.enqueue_home_panel(
            A, A, 10, {"text": "Ignored replay"}, "home-panel:new-message", 0
        )
        == first_send
    )
    async with store.connection() as conn:
        for delivery in (other_owner, other_message, other_chat, ordinary, latest, first_send):
            assert (
                await conn.fetchval("SELECT state FROM outbox WHERE id=$1", delivery) == "pending"
            )
    with pytest.raises(ValueError, match="недоступна"):
        await store.enqueue_home_panel(B, B, 10, {"text": "Wrong owner"}, "home-panel:main", 999)
