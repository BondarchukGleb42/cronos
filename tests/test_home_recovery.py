"""Recover only a positively identified missing home; never probe by renaming."""

import os
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock
from urllib.parse import urlparse
from uuid import uuid4

import pytest
import pytest_asyncio
from aiogram.exceptions import TelegramBadRequest, TelegramNetworkError, TelegramRetryAfter
from aiogram.methods import SendMessage

from cronos.coordinator import Coordinator
from cronos.home import HOME_TITLE, is_missing_topic_error
from cronos.settings import Settings
from cronos.storage import Store
from cronos.telegram import PartialDeliveryError
from cronos.worker import Worker

A, B = -975501, -975502


def missing_error(description="Bad Request: TOPIC_NOT_FOUND"):
    return TelegramBadRequest(
        method=SendMessage(chat_id=A, message_thread_id=77, text="panel"), message=description
    )


@pytest.mark.parametrize(
    "description",
    [
        "Bad Request: TOPIC_NOT_FOUND",
        "message thread not found",
        "Bad Request: forum topic not found",
    ],
)
def test_only_exact_missing_topic_response_authorizes_recovery(description):
    assert is_missing_topic_error(missing_error(description))


@pytest.mark.parametrize(
    "description",
    ["TOPIC_ID_INVALID", "CHAT_ADMIN_REQUIRED", "chat not found", "TOPIC_NOT_FOUND extra detail"],
)
def test_ambiguous_or_unrelated_bad_request_does_not_authorize_recovery(description):
    assert not is_missing_topic_error(missing_error(description))


def coordinator(store, error):
    instance = Coordinator.__new__(Coordinator)
    instance.owner = "recovery-coordinator"
    instance.store = store
    instance.redis = SimpleNamespace(set=AsyncMock(return_value=True))
    instance.transport = SimpleNamespace(send=AsyncMock(side_effect=error))
    return instance


@pytest.mark.parametrize("partial", [False, True])
async def test_coordinator_recovers_exact_missing_home_without_sending_elsewhere(partial):
    row = {
        "id": 1,
        "user_id": A,
        "chat_id": A,
        "thread_id": 77,
        "attempts": 1,
        "telegram_ids": [10] if partial else [],
        "payload": {"text": "home panel"},
    }
    locked = False

    @asynccontextmanager
    async def lock(user_id, *, purpose):
        nonlocal locked
        assert user_id == A and purpose == "delivery"
        locked = True
        try:
            yield
        finally:
            locked = False

    async def recover(*args, **kwargs):
        assert locked
        return True

    store = SimpleNamespace(
        next_delivery=AsyncMock(return_value=row),
        claimed_delivery=AsyncMock(return_value=row),
        user_lock=lock,
        recover_missing_home_delivery=AsyncMock(side_effect=recover),
        delivery_result=AsyncMock(),
    )
    remainder = {"_telegram_parts": [{"kind": "text", "text": "remaining"}]}
    error = PartialDeliveryError([11], remainder, missing_error()) if partial else missing_error()
    instance = coordinator(store, error)
    assert await instance.delivery()
    store.recover_missing_home_delivery.assert_awaited_once_with(
        1,
        instance.owner,
        **({"telegram_ids": [10, 11], "remaining_payload": remainder} if partial else {}),
    )
    store.delivery_result.assert_not_awaited()
    instance.transport.send.assert_awaited_once_with(A, 77, row["payload"])


@pytest.mark.parametrize("kind", ["missing_nonhome", "bad_request", "network", "rate_limit"])
async def test_coordinator_keeps_normal_delivery_handling_without_confirmed_home(kind):
    store = SimpleNamespace(
        recover_missing_home_delivery=AsyncMock(return_value=False),
        delivery_result=AsyncMock(),
    )
    method = SendMessage(chat_id=A, text="panel")
    error = {
        "missing_nonhome": missing_error(),
        "bad_request": missing_error("CHAT_ADMIN_REQUIRED"),
        "network": TelegramNetworkError(method=method, message="TOPIC_NOT_FOUND"),
        "rate_limit": TelegramRetryAfter(method=method, message="TOPIC_NOT_FOUND", retry_after=2),
    }[kind]
    instance = coordinator(store, error)
    assert await instance.deliver_claimed(
        {"id": 1, "user_id": A, "chat_id": A, "thread_id": 77, "attempts": 1, "payload": {}}
    )
    assert store.recover_missing_home_delivery.await_count == (kind == "missing_nonhome")
    store.delivery_result.assert_awaited_once()
    assert store.delivery_result.await_args.kwargs.get("permanent", False) == (
        kind in {"missing_nonhome", "bad_request"}
    )


async def cleanup(store):
    for user_id in (A, B):
        async with store.connection(user_id) as conn:
            for table in (
                "privacy_requests",
                "outbox",
                "operations",
                "messages",
                "conversations",
                "users",
            ):
                await conn.execute(f"DELETE FROM {table} WHERE user_id=$1", user_id)
            await conn.execute("DELETE FROM events WHERE payload->>'user_id'=$1", str(user_id))


@pytest_asyncio.fixture
async def store():
    database_url = os.getenv("DATABASE_URL")
    if not database_url or not os.getenv("ADMIN_DATABASE_URL"):
        pytest.skip("Real local PostgreSQL URLs are required")
    assert urlparse(database_url).hostname in {"localhost", "127.0.0.1", "::1"}
    value = Store(Settings())
    await value.open()
    try:
        await cleanup(value)
        for user_id in (A, B):
            await value.ensure_user(user_id)
        yield value
    finally:
        await cleanup(value)
        await value.close()


async def claimed(store, *, user_id=A, chat_id=A, thread_id=77, key=None):
    delivery_id = await store.enqueue(
        user_id, chat_id, thread_id, {"text": "panel"}, key or f"home-panel:{uuid4()}"
    )
    async with store.connection() as conn:
        await conn.execute(
            """UPDATE outbox SET state='sending',owner='recovery-coordinator',
            lease_until=now()+interval '1 minute' WHERE id=$1""",
            delivery_id,
        )
    return delivery_id


@pytest.mark.parametrize("partial", [False, True])
async def test_real_missing_home_recovers_after_restart_once_preserving_history_and_finances(
    store, partial
):
    old = await store.conversation(A, A, 77, HOME_TITLE)
    await store.set_home(A, old["id"])
    await store.add_message(A, old["id"], "user", "Saved conversation")
    before = await store.ensure_user(A)
    delivery_id = await claimed(store)
    stale_panel = await store.enqueue(A, A, 77, {"text": "old panel"}, f"home-panel:{uuid4()}")
    other_chat = await store.enqueue(A, A, 78, {"text": "other chat"}, f"home-panel:{uuid4()}")
    remainder = {"_telegram_parts": [{"kind": "text", "text": "remaining"}]}
    kwargs = {"telegram_ids": [10, 11], "remaining_payload": remainder} if partial else {}
    async with store.user_lock(A, purpose="delivery"):
        assert await store.recover_missing_home_delivery(
            delivery_id, "recovery-coordinator", **kwargs
        )
        assert not await store.recover_missing_home_delivery(
            delivery_id, "recovery-coordinator", **kwargs
        )
    assert await store.get_home(A) is None
    async with store.connection(A) as conn:
        events = await conn.fetch(
            "SELECT * FROM events WHERE kind='home_init' AND payload->>'user_id'=$1", str(A)
        )
        failed = await conn.fetchrow("SELECT * FROM outbox WHERE id=$1", delivery_id)
        assert failed["state"] == "failed" and failed["lease_until"] is None
        if partial:
            assert failed["telegram_ids"] == [10, 11] and failed["payload"] == remainder
        assert (
            await conn.fetchval("SELECT state FROM outbox WHERE id=$1", stale_panel) == "cancelled"
        )
        assert await conn.fetchval("SELECT state FROM outbox WHERE id=$1", other_chat) == "pending"
        assert len(events) == 1 and events[0]["state"] == "pending"
    after = await store.ensure_user(A)
    assert after["home_generation"] != before["home_generation"]
    assert {k: v for k, v in after.items() if k != "home_generation"} == {
        k: v for k, v in before.items() if k != "home_generation"
    }

    # A new worker uses the durable event left by the coordinator, not process memory.
    worker = Worker.__new__(Worker)
    worker.store = store
    worker.transport = SimpleNamespace(
        topic_capabilities=AsyncMock(return_value=SimpleNamespace(has_topics_enabled=True)),
        create_topic=AsyncMock(return_value={"message_thread_id": 88, "name": HOME_TITLE}),
        edit_topic=AsyncMock(),
    )
    await worker.home_init(dict(events[0]))
    await worker.home_init(dict(events[0]))
    home = await store.get_home(A)
    assert home["thread_id"] == 88 and home["id"] != old["id"]
    worker.transport.create_topic.assert_awaited_once_with(A, HOME_TITLE)
    worker.transport.edit_topic.assert_not_awaited()
    assert await store.history(A, old["id"]) == [{"role": "user", "content": "Saved conversation"}]
    async with store.connection(A) as conn:
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM outbox WHERE dedupe_key=$1", f"home-welcome:{home['id']}"
            )
            == 1
        )


@pytest.mark.parametrize(
    "case",
    [
        "nonhome",
        "general",
        "other_chat",
        "other_user",
        "replaced_home",
        "cancelled",
        "wrong_owner",
        "erasing",
        "erased",
    ],
)
async def test_real_stale_or_unauthorized_missing_delivery_does_not_reset_current_home(store, case):
    home = await store.conversation(A, A, 77, HOME_TITLE)
    await store.set_home(A, home["id"])
    args = {"user_id": A, "chat_id": A, "thread_id": 77}
    if case == "nonhome":
        args["thread_id"] = 78
    elif case == "general":
        args["thread_id"] = 0
    elif case == "other_chat":
        args["chat_id"] = -123
    elif case == "other_user":
        args["user_id"] = B
    delivery_id = await claimed(store, **args)
    if case == "replaced_home":
        await store.reset_home(A, home["id"], source_key="test-replaced-home")
        home = await store.conversation(A, A, 88, HOME_TITLE)
        await store.set_home(A, home["id"])
    async with store.connection(A) as conn:
        if case == "cancelled":
            await conn.execute("UPDATE outbox SET state='cancelled' WHERE id=$1", delivery_id)
        elif case == "erased":
            await conn.execute("DELETE FROM outbox WHERE id=$1", delivery_id)
        elif case == "erasing":
            await conn.execute(
                """INSERT INTO privacy_requests(id,user_id,scope,chat_id,source_key,state)
                VALUES($1,$2,'all',$2,$3,'erasing')""",
                uuid4(),
                A,
                str(uuid4()),
            )
    generation = await store.home_creation_key(A)
    async with store.user_lock(args["user_id"], purpose="delivery"):
        assert not await store.recover_missing_home_delivery(
            delivery_id, "wrong" if case == "wrong_owner" else "recovery-coordinator"
        )
    assert (await store.get_home(A))["id"] == home["id"]
    assert await store.home_creation_key(A) == generation
    async with store.connection(A) as conn:
        assert (
            await conn.fetchval("SELECT count(*) FROM events WHERE payload->>'user_id'=$1", str(A))
            == 0
        )


async def test_real_queue_failure_rolls_back_retirement_so_retry_can_recover(store):
    home = await store.conversation(A, A, 77, HOME_TITLE)
    await store.set_home(A, home["id"])
    delivery_id = await claimed(store)
    generation = await store.home_creation_key(A)
    connection = store.connection

    class FailQueue:
        def __init__(self, conn):
            self.conn = conn

        def __getattr__(self, name):
            return getattr(self.conn, name)

        async def execute(self, query, *args):
            if "INSERT INTO events" in query:
                raise RuntimeError("queue unavailable")
            return await self.conn.execute(query, *args)

    @asynccontextmanager
    async def broken_connection(user_id=None):
        async with connection(user_id) as conn:
            yield FailQueue(conn)

    store.connection = broken_connection
    try:
        async with store.user_lock(A, purpose="delivery"):
            with pytest.raises(RuntimeError, match="queue unavailable"):
                await store.recover_missing_home_delivery(delivery_id, "recovery-coordinator")
    finally:
        store.connection = connection
    assert await store.home_creation_key(A) == generation
    assert (await store.get_home(A))["id"] == home["id"]
    assert await store.claimed_delivery(delivery_id, "recovery-coordinator")
    async with store.user_lock(A, purpose="delivery"):
        assert await store.recover_missing_home_delivery(delivery_id, "recovery-coordinator")
