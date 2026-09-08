"""Durable album collection against isolated PostgreSQL and synthetic Telegram updates."""

import asyncio
import os
from datetime import UTC, datetime
from uuid import uuid4

import pytest
import pytest_asyncio

from cronos.settings import Settings
from cronos.storage import MEDIA_GROUP_DEBOUNCE_SECONDS, Store, privacy_event_id, uid

A, B = -980101, -980102
USED_UPDATES = set()
pytestmark = pytest.mark.asyncio(loop_scope="module")


def photo(message_id, *, user=A, thread=10, group="album-1", caption=None, date=None):
    update_id = uuid4().int % 8_000_000_000_000 + 10_000_000_000
    USED_UPDATES.add(update_id)
    message = {
        "message_id": message_id,
        "date": date if date is not None else int(datetime.now(UTC).timestamp()),
        "chat": {"id": user, "type": "private", "first_name": "Private profile"},
        "from": {"id": user, "is_bot": False, "first_name": "Private profile"},
        "message_thread_id": thread,
        "photo": [
            {
                "file_id": f"photo-{message_id}",
                "file_unique_id": f"unique-{message_id}",
                "width": 100,
                "height": 100,
            }
        ],
    }
    if group is not None:
        message["media_group_id"] = group
    if caption is not None:
        message["caption"] = caption
    return {"update_id": update_id, "message": message}


async def cleanup(store):
    for owner in (A, B):
        async with store.connection(owner) as conn:
            requests = await conn.fetch("SELECT id FROM privacy_requests WHERE user_id=$1", owner)
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
                """DELETE FROM events WHERE payload->>'user_id'=$1
                OR payload#>>'{message,chat,id}'=$1 OR payload#>>'{message,from,id}'=$1
                OR id=ANY($2::uuid[])""",
                str(owner),
                [privacy_event_id(row["id"]) for row in requests],
            )
    async with store.connection() as conn:
        await conn.execute(
            "DELETE FROM events WHERE update_id=ANY($1::bigint[])", list(USED_UPDATES)
        )
    USED_UPDATES.clear()


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
    for user in (A, B):
        await store.ensure_user(user)
    yield
    await cleanup(store)


async def ingest(store, updates, owner=None):
    return await store.ingest_updates(
        updates, max(item["update_id"] for item in updates) + 1, owner=owner
    )


async def albums(store, user=A):
    async with store.connection() as conn:
        return [
            dict(row)
            for row in await conn.fetch(
                """SELECT * FROM events
            WHERE media_group_key IS NOT NULL AND payload#>>'{message,from,id}'=$1
            ORDER BY created_at,id""",
                str(user),
            )
        ]


async def make_due(store, event_id):
    async with store.connection() as conn:
        await conn.execute(
            "UPDATE events SET available_at=now()-interval '1 second' WHERE id=$1", uid(event_id)
        )


async def test_one_album_is_one_sorted_captioned_event_and_minimal_receipts(store):
    first, second = photo(100), photo(101, caption="Use the logo and this avatar")
    assert await ingest(store, [second, first])
    rows = await albums(store)
    assert len(rows) == 1 and rows[0]["update_id"] is None and rows[0]["state"] == "pending"
    payload = rows[0]["payload"]
    assert payload["media_group_messages"] == [first["message"], second["message"]]
    assert payload["message"] == second["message"]
    assert payload["update_id"] == second["update_id"]
    assert not payload.get("media_group_continuation")
    assert MEDIA_GROUP_DEBOUNCE_SECONDS == 2
    assert (rows[0]["available_at"] - rows[0]["created_at"]).total_seconds() >= 2
    async with store.connection() as conn:
        receipts = await conn.fetch(
            "SELECT payload,state FROM events WHERE update_id=ANY($1::bigint[])",
            [first["update_id"], second["update_id"]],
        )
        assert len(receipts) == 2 and all(row["state"] == "done" for row in receipts)
        for receipt in receipts:
            assert receipt["payload"]["media_group_receipt"] is True
            assert "file_id" not in str(receipt["payload"]) and "caption" not in str(
                receipt["payload"]
            )
            assert "Private profile" not in str(receipt["payload"])
            assert receipt["payload"]["message"]["chat"] == {"id": A, "type": "private"}
        # Explicit future time avoids timing-sensitive assertions on busy CI runners.
        await conn.execute(
            "UPDATE events SET available_at=now()+interval '1 minute' WHERE id=$1", rows[0]["id"]
        )
    assert await store.claim_event("worker", rows[0]["id"]) is None
    await make_due(store, rows[0]["id"])
    claimed = await store.claim_event("worker", rows[0]["id"])
    assert claimed["payload"] == payload


async def test_separate_batches_and_restart_extend_only_on_new_items(store):
    first, second, third = photo(100), photo(101, caption="Logo"), photo(102, caption="Avatar")
    await ingest(store, [first])
    original = (await albums(store))[0]
    async with store.connection() as conn:
        await conn.execute(
            "UPDATE events SET available_at=now()+interval '0.1 second',notified_at=now() WHERE id=$1",
            original["id"],
        )
    await ingest(store, [second])
    appended = (await albums(store))[0]
    assert appended["id"] == original["id"] and appended["notified_at"] is None
    assert appended["available_at"] > original["created_at"]
    await ingest(store, [first, second])
    duplicate = (await albums(store))[0]
    assert (
        duplicate["available_at"] == appended["available_at"]
        and duplicate["payload"] == appended["payload"]
    )
    fresh_process = Store(store.settings)
    await fresh_process.open()
    try:
        await ingest(fresh_process, [third])
    finally:
        await fresh_process.close()
    final = (await albums(store))[0]
    assert final["id"] == original["id"]
    assert [message["message_id"] for message in final["payload"]["media_group_messages"]] == [
        100,
        101,
        102,
    ]
    assert [message.get("caption") for message in final["payload"]["media_group_messages"]] == [
        None,
        "Logo",
        "Avatar",
    ]


async def test_concurrent_batches_use_one_durable_group_and_duplicate_message_is_not_appended(
    store,
):
    first, second = photo(100), photo(101)
    await asyncio.gather(ingest(store, [first]), ingest(store, [second]))
    original = (await albums(store))[0]
    assert len(await albums(store)) == 1
    assert len(original["payload"]["media_group_messages"]) == 2
    same_message_new_update = photo(101)
    await ingest(store, [same_message_new_update])
    repeated = (await albums(store))[0]
    assert repeated["payload"] == original["payload"]
    assert repeated["available_at"] == original["available_at"]


@pytest.mark.parametrize(
    "state,attempts", [("processing", 1), ("done", 1), ("failed", 3), ("pending", 1)]
)
async def test_late_part_is_explicit_cumulative_continuation_and_old_input_is_immutable(
    store, state, attempts
):
    first, second, third = photo(100, caption="Combine logo and avatar"), photo(101), photo(102)
    await ingest(store, [first])
    initial = (await albums(store))[0]
    async with store.connection() as conn:
        await conn.execute(
            "UPDATE events SET state=$2,attempts=$3 WHERE id=$1", initial["id"], state, attempts
        )
    await ingest(store, [second])
    await ingest(store, [third, second])
    rows = await albums(store)
    assert len(rows) == 2
    assert rows[0]["payload"] == initial["payload"] and rows[0]["state"] == state
    continued = rows[1]["payload"]
    assert continued["media_group_continuation"] is True
    assert continued["message"]["caption"] == "Combine logo and avatar"
    assert continued["media_group_messages"] == [
        first["message"],
        second["message"],
        third["message"],
    ]
    assert continued["update_id"] == second["update_id"]
    assert rows[1]["state"] == "pending" and rows[1]["attempts"] == 0


async def test_media_group_scope_separates_owner_thread_chat_and_group_id(store):
    items = [photo(100), photo(100, user=B), photo(101, thread=11), photo(102, group="album-2")]
    group_chat = photo(103)
    group_chat["message"]["chat"] = {"id": -777, "type": "supergroup"}
    items.append(group_chat)
    await ingest(store, items)
    ours, theirs = await albums(store, A), await albums(store, B)
    assert len(ours) == 4 and len(theirs) == 1
    assert len({row["media_group_key"] for row in ours + theirs}) == 5
    assert all(len(row["payload"]["media_group_messages"]) == 1 for row in ours + theirs)


async def test_normal_photo_edited_message_and_invalid_lease_keep_original_ingress_behavior(store):
    ordinary = photo(100, group=None)
    edited = photo(101)
    edited["edited_message"] = edited.pop("message")
    blocked = photo(102)
    assert not await ingest(store, [blocked], owner="not-the-leader")
    assert not await albums(store)
    await ingest(store, [ordinary, edited])
    async with store.connection() as conn:
        rows = await conn.fetch(
            "SELECT * FROM events WHERE update_id=ANY($1::bigint[]) ORDER BY update_id",
            [ordinary["update_id"], edited["update_id"]],
        )
    assert len(rows) == 2 and all(
        row["state"] == "pending" and row["media_group_key"] is None for row in rows
    )
    assert {row["update_id"] for row in rows} == {ordinary["update_id"], edited["update_id"]}


@pytest.mark.parametrize("scope,thread", [("all", 0), ("chat", 10)])
async def test_privacy_clears_full_album_and_receipts_and_prevents_late_resurrection(
    store, scope, thread
):
    old_date = int(datetime.now(UTC).timestamp()) - 10
    first, second = (
        photo(100, thread=thread, date=old_date),
        photo(101, thread=thread, date=old_date),
    )
    other = photo(200, user=B, thread=thread, date=old_date)
    await ingest(store, [first, second, other])
    aggregate = (await albums(store))[0]
    foreign = (await albums(store, B))[0]
    conversation = await store.conversation(A, A, thread)
    request = await store.requests_prepare(
        A, conversation, scope, source_key=f"media-privacy:{uuid4()}"
    )
    assert await store.confirm_privacy_request(A, request["id"], thread)
    erasing = await store.begin_privacy_erasure(request["id"], privacy_event_id(request["id"]))
    if thread == 0:
        assert erasing["job"]["general_message_ids"] == [100, 101]
    else:
        assert erasing["job"]["topic_ids"] == [thread]
    assert await store.event_current(aggregate["id"]) is None
    assert (await albums(store, B))[0]["payload"] == foreign["payload"]
    await store.finish_privacy_erasure(request["id"], {"files_erased": scope == "all"})
    late = photo(102, thread=thread, date=old_date)
    await ingest(store, [late, first, second])
    assert await albums(store) == []
    async with store.connection() as conn:
        receipts = await conn.fetch(
            "SELECT payload,state FROM events WHERE update_id=ANY($1::bigint[])",
            [item["update_id"] for item in (first, second, late)],
        )
    assert len(receipts) == 3 and all(
        row["payload"] == {} and row["state"] == "done" for row in receipts
    )


async def test_privacy_sanitizes_each_album_item_and_stale_publish_does_not_delay_debounce(store):
    async with store.connection(A) as conn:
        await conn.execute(
            "UPDATE users SET content_reset_at=now()-interval '1 day' WHERE user_id=$1", A
        )
    first, second = photo(100), photo(101)
    for item in (first, second):
        item["message"]["reply_to_message"] = {"text": "deleted secret"}
        item["message"]["quote"] = {"text": "deleted secret"}
    await ingest(store, [first, second])
    aggregate = (await albums(store))[0]
    assert "deleted secret" not in str(aggregate["payload"])
    async with store.connection() as conn:
        await conn.execute(
            "UPDATE events SET available_at=now()+interval '1 minute',notified_at=NULL WHERE id=$1",
            aggregate["id"],
        )
    await store.notified(aggregate["id"])
    assert (await albums(store))[0]["notified_at"] is None
    await make_due(store, aggregate["id"])
    assert str(aggregate["id"]) in await store.pending_events()
    await store.notified(aggregate["id"])
    assert (await albums(store))[0]["notified_at"] is not None


def instruction(message_id, text="Use these photos together", *, user=A, thread=10):
    value = photo(message_id, user=user, thread=thread, group=None)
    value["message"].pop("photo")
    value["message"]["text"] = text
    return value


@pytest.mark.parametrize("split_batches", [False, True])
async def test_captionless_album_followed_by_instruction_is_one_request(store, split_batches):
    first, second, text = photo(100), photo(101), instruction(102)
    if split_batches:
        await ingest(store, [first, second])
        await ingest(store, [text])
    else:
        await ingest(store, [first, second, text])
    rows = await albums(store)
    assert len(rows) == 1
    payload = rows[0]["payload"]
    assert payload["media_group_messages"] == [first["message"], second["message"], text["message"]]
    assert payload["message"] == text["message"]
    assert not payload.get("media_group_continuation")
    async with store.connection() as conn:
        receipt = await conn.fetchrow(
            "SELECT state,payload FROM events WHERE update_id=$1", text["update_id"]
        )
        assert receipt["state"] == "done" and receipt["payload"]["media_group_receipt"]
        assert "Use these photos" not in str(receipt["payload"])
    before = rows[0]["available_at"]
    await ingest(store, [text])
    assert (await albums(store))[0]["available_at"] == before
    await make_due(store, rows[0]["id"])
    assert (await store.claim_event("album-worker", rows[0]["id"]))["payload"] == payload


async def test_commands_controls_other_thread_and_owner_are_not_joined(store):
    first = photo(100)
    await ingest(store, [first])
    texts = [
        instruction(200 + index, text)
        for index, text in enumerate(
            [
                "/new",
                "/delete",
                "/clearall",
                "стоп",
                "отмена",
                "Подтверждаю полную очистку",
                "Подтверждаю удаление чата",
                "➕ Новый чат",
                "🗑 Удалить чат",
                "🪐 Главное меню",
                "🤖 Модель",
                "🧠 Режим рассуждения",
            ]
        )
    ]
    texts.extend([instruction(300, thread=11), instruction(301, user=B)])
    await ingest(store, texts)
    assert (await albums(store))[0]["payload"]["media_group_messages"] == [first["message"]]
    async with store.connection() as conn:
        rows = await conn.fetch(
            "SELECT state,payload,media_group_key FROM events WHERE update_id=ANY($1::bigint[])",
            [value["update_id"] for value in texts],
        )
    assert len(rows) == len(texts)
    assert all(row["state"] == "pending" and row["media_group_key"] is None for row in rows)


@pytest.mark.parametrize("expired", [False, True])
async def test_text_after_started_or_expired_album_remains_normal(store, expired):
    first, text = photo(100), instruction(101)
    await ingest(store, [first])
    row = (await albums(store))[0]
    async with store.connection() as conn:
        if expired:
            await conn.execute(
                "UPDATE events SET available_at=now()-interval '1 second' WHERE id=$1", row["id"]
            )
        else:
            await conn.execute(
                "UPDATE events SET state='processing',attempts=1 WHERE id=$1", row["id"]
            )
    await ingest(store, [text])
    assert len(await albums(store)) == 1
    async with store.connection() as conn:
        normal = await conn.fetchrow(
            "SELECT state,payload FROM events WHERE update_id=$1", text["update_id"]
        )
    assert normal["state"] == "pending" and normal["payload"] == text


async def test_text_join_rechecks_claim_race_before_consuming_receipt(store, monkeypatch):
    first, text = photo(100), instruction(101)
    await ingest(store, [first])
    event = (await albums(store))[0]
    original_lookup = store._pending_media_group_for_text

    async def claimed_after_lookup(conn, update):
        key = await original_lookup(conn, update)
        if key is not None:
            await conn.execute(
                "UPDATE events SET state='processing',attempts=1 WHERE id=$1", event["id"]
            )
        return key

    monkeypatch.setattr(store, "_pending_media_group_for_text", claimed_after_lookup)
    await ingest(store, [text])
    assert len(await albums(store)) == 1
    async with store.connection() as conn:
        normal = await conn.fetchrow(
            "SELECT state,payload FROM events WHERE update_id=$1", text["update_id"]
        )
    assert normal["state"] == "pending" and normal["payload"] == text
