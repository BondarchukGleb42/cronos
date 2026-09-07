from contextlib import asynccontextmanager
from copy import deepcopy
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from cronos.coordinator import Coordinator, initiative_delay
from cronos.telegram import PartialDeliveryError


class DeliveryStore(SimpleNamespace):
    """Represent the current durable row separately from the earlier claim snapshot."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.current_row = deepcopy(self.next_delivery.return_value)
        self.locked_user = None
        self.lock_calls = []
        self.claimed_delivery = AsyncMock(side_effect=self.recheck)

    @asynccontextmanager
    async def user_lock(self, user_id, *, purpose):
        assert purpose == "delivery"
        assert self.locked_user is None
        self.lock_calls.append((user_id, purpose))
        self.locked_user = user_id
        try:
            yield
        finally:
            self.locked_user = None

    async def recheck(self, delivery_id, owner):
        selected = self.next_delivery.return_value
        assert self.locked_user == selected["user_id"]
        assert delivery_id == selected["id"]
        assert (owner,) == self.next_delivery.await_args.args
        return self.current_row


@pytest.mark.parametrize(
    ("hour", "expected_day", "expected_hour"),
    [(21, None, None), (22, 6, 9), (0, 5, 9), (8, 5, 9), (9, None, None)],
)
def test_overnight_quiet_hours(hour, expected_day, expected_hour):
    now = datetime(2026, 9, 5, hour, tzinfo=UTC)
    delay = initiative_delay({"quiet_start": 22, "quiet_end": 9}, now, 0)
    if expected_day is None:
        assert delay is None
    else:
        assert delay == datetime(2026, 9, expected_day, expected_hour, tzinfo=UTC)


def test_quiet_hours_follow_user_timezone_and_daily_cap():
    now = datetime(2026, 9, 5, 16, 20, tzinfo=UTC)  # 23:20 Novosibirsk
    prefs = {
        "timezone": "Asia/Novosibirsk",
        "quiet_start": 22,
        "quiet_end": 9,
        "initiative_limit": 2,
    }
    assert initiative_delay(prefs, now, 0).astimezone(UTC) == datetime(2026, 9, 6, 2, tzinfo=UTC)
    daytime = datetime(2026, 9, 5, 5, tzinfo=UTC)  # 12:00 local
    assert initiative_delay(prefs, daytime, 1) is None
    assert initiative_delay(prefs, daytime, 2).astimezone(UTC) == datetime(
        2026, 9, 6, 2, tzinfo=UTC
    )


def test_equal_quiet_boundaries_disable_quiet_hours_but_preserve_cap():
    now = datetime(2026, 9, 5, 12, tzinfo=UTC)
    prefs = {"quiet_start": 9, "quiet_end": 9, "initiative_limit": 1}
    assert initiative_delay(prefs, now, 0) is None
    assert initiative_delay(prefs, now, 1) == datetime(2026, 9, 6, 9, tzinfo=UTC)


@pytest.mark.parametrize(
    "schedule", [None, {"state": "cancelled", "revision": 1}, {"state": "active", "revision": 2}]
)
async def test_delivery_rechecks_schedule_before_sending(schedule):
    coordinator = Coordinator.__new__(Coordinator)
    coordinator.owner = "test-owner"
    coordinator.store = DeliveryStore(
        next_delivery=AsyncMock(
            return_value={
                "id": 1,
                "user_id": -920005,
                "chat_id": -920005,
                "thread_id": 0,
                "attempts": 1,
                "payload": {
                    "text": "do not send",
                    "schedule_id": "test-id",
                    "schedule_revision": 1,
                },
            }
        ),
        get_schedule=AsyncMock(return_value=schedule),
        delivery_result=AsyncMock(),
    )
    coordinator.transport = SimpleNamespace(send=AsyncMock())
    assert await coordinator.delivery() is True
    coordinator.transport.send.assert_not_called()
    assert coordinator.store.delivery_result.call_args.kwargs["permanent"] is True


async def test_delivery_honours_revoked_proactivity():
    coordinator = Coordinator.__new__(Coordinator)
    coordinator.owner = "test-owner"
    coordinator.store = DeliveryStore(
        next_delivery=AsyncMock(
            return_value={
                "id": 2,
                "user_id": -920005,
                "chat_id": -920005,
                "thread_id": 0,
                "attempts": 1,
                "payload": {
                    "text": "do not send",
                    "schedule_id": "test-id",
                    "schedule_revision": 1,
                },
            }
        ),
        get_schedule=AsyncMock(return_value={"state": "active", "revision": 1, "proactive": True}),
        preferences=AsyncMock(return_value={"proactivity": False}),
        delivery_result=AsyncMock(),
    )
    coordinator.transport = SimpleNamespace(send=AsyncMock())
    assert await coordinator.delivery() is True
    coordinator.transport.send.assert_not_called()
    assert coordinator.store.delivery_result.call_args.kwargs["error"] == "Proactivity disabled"


async def test_partial_delivery_persists_ids_remainder_and_schedule_guards():
    coordinator = Coordinator.__new__(Coordinator)
    coordinator.owner = "test-owner"
    conn = SimpleNamespace(execute=AsyncMock())

    @asynccontextmanager
    async def connection():
        yield conn

    row = {
        "id": 3,
        "user_id": -920005,
        "chat_id": -920005,
        "thread_id": 0,
        "attempts": 1,
        "telegram_ids": [10],
        "payload": {
            "text": "report",
            "schedule_id": "schedule-test",
            "schedule_revision": 4,
        },
    }
    remainder = {"_telegram_parts": [{"kind": "document", "path": "/data/report.pdf"}]}
    coordinator.store = DeliveryStore(
        next_delivery=AsyncMock(return_value=row),
        get_schedule=AsyncMock(return_value={"state": "active", "revision": 4, "proactive": False}),
        connection=connection,
        delivery_result=AsyncMock(),
    )
    coordinator.redis = SimpleNamespace(set=AsyncMock(return_value=True))
    coordinator.transport = SimpleNamespace(
        send=AsyncMock(side_effect=PartialDeliveryError([11], remainder, TimeoutError()))
    )
    assert await coordinator.delivery() is True
    args = conn.execute.call_args.args
    assert args[1:3] == (3, "test-owner")
    assert args[3] == {**remainder, "schedule_id": "schedule-test", "schedule_revision": 4}
    assert args[4] == [10, 11]
    assert args[5] == "pending"


async def test_successful_remainder_keeps_previously_delivered_ids():
    coordinator = Coordinator.__new__(Coordinator)
    coordinator.owner = "test-owner"
    coordinator.store = DeliveryStore(
        next_delivery=AsyncMock(
            return_value={
                "id": 4,
                "user_id": -920005,
                "chat_id": -920005,
                "thread_id": 0,
                "attempts": 2,
                "telegram_ids": [10],
                "payload": {"text": "remaining"},
            }
        ),
        delivery_result=AsyncMock(),
    )
    coordinator.redis = SimpleNamespace(set=AsyncMock(return_value=True))
    coordinator.transport = SimpleNamespace(send=AsyncMock(return_value=[11]))
    assert await coordinator.delivery() is True
    assert coordinator.store.delivery_result.call_args.kwargs["ids"] == [10, 11]


async def test_erased_claimed_row_does_not_send_stale_in_memory_content():
    coordinator = Coordinator.__new__(Coordinator)
    coordinator.owner = "test-owner"
    coordinator.store = DeliveryStore(
        next_delivery=AsyncMock(
            return_value={
                "id": 5,
                "user_id": -920005,
                "chat_id": -920005,
                "thread_id": 77,
                "attempts": 1,
                "payload": {"text": "content erased before lock acquisition"},
            }
        ),
        delivery_result=AsyncMock(),
    )
    coordinator.store.current_row = None
    coordinator.transport = SimpleNamespace(send=AsyncMock())

    assert await coordinator.delivery() is True

    coordinator.store.claimed_delivery.assert_awaited_once_with(5, "test-owner")
    assert coordinator.store.lock_calls == [(-920005, "delivery")]
    assert coordinator.store.locked_user is None
    coordinator.transport.send.assert_not_called()
    coordinator.store.delivery_result.assert_not_called()


async def test_actual_send_and_receipt_hold_user_lock_and_use_current_payload():
    coordinator = Coordinator.__new__(Coordinator)
    coordinator.owner = "test-owner"
    coordinator.store = DeliveryStore(
        next_delivery=AsyncMock(
            return_value={
                "id": 6,
                "user_id": -920005,
                "chat_id": -920005,
                "thread_id": 77,
                "attempts": 1,
                "payload": {"text": "stale snapshot"},
            }
        ),
        delivery_result=AsyncMock(),
    )
    coordinator.store.current_row["payload"] = {"text": "current durable content"}
    coordinator.redis = SimpleNamespace(set=AsyncMock(return_value=True))

    async def send(chat_id, thread_id, payload):
        assert coordinator.store.locked_user == -920005
        assert coordinator.store.claimed_delivery.await_count == 1
        assert (chat_id, thread_id) == (-920005, 77)
        assert payload == {"text": "current durable content"}
        return [15]

    async def save_receipt(*args, **kwargs):
        assert coordinator.store.locked_user == -920005
        assert kwargs == {"ids": [15]}

    coordinator.transport = SimpleNamespace(send=AsyncMock(side_effect=send))
    coordinator.store.delivery_result.side_effect = save_receipt

    assert await coordinator.delivery() is True

    coordinator.transport.send.assert_awaited_once()
    coordinator.store.delivery_result.assert_awaited_once_with(6, "test-owner", ids=[15])
    assert coordinator.store.lock_calls == [(-920005, "delivery")]
    assert coordinator.store.locked_user is None
