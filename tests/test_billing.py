import asyncio
import os
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from cronos.billing import Reconciler
from cronos.providers import ProviderError
from cronos.settings import Settings
from cronos.storage import Store


class Receipts:
    def __init__(self, values):
        self.values = values
        self.calls = []
        self.closed = False

    async def receipt(self, request_id):
        self.calls.append(request_id)
        value = self.values[request_id]
        if isinstance(value, Exception):
            raise value
        return value

    async def close(self):
        self.closed = True


class FakeStore:
    def __init__(self, age, raw, *, stored=True):
        self.reservation = {"id": "operation", "user_id": 7, "created_at": datetime.now(UTC) - age}
        self.usage = {"raw": raw, "run_id": uuid4(), "model": "original-model"} if stored else None
        self.state = "reserved"
        self.recorded = []

    async def pending_reservations(self):
        return [self.reservation] if self.state == "reserved" else []

    async def reconciliation_usage(self, user_id, operation_id):
        assert (user_id, operation_id) == (7, "operation")
        return self.usage

    async def record_usage(self, user_id, run_id, operation_id, receipt):
        self.recorded.append(receipt)
        self.state = "settled"

    async def release_unresolved_reservation(self, user_id, operation_id):
        self.state = "released"
        return True


async def test_receipt_settles_once_and_preserves_original_model_when_receipt_omits_it():
    store = FakeStore(
        timedelta(minutes=3), {"request_id": "receipt-1", "reconciliation": "pending"}
    )
    receipts = Receipts(
        {
            "receipt-1": {
                "cost_rub": "0.125",
                "model": None,
                "prompt_tokens": 10,
                "completion_tokens": 5,
            }
        }
    )
    reconciler = Reconciler(None, store, provider=receipts)
    assert (await reconciler.tick())["settled"] == 1
    assert (await reconciler.tick())["processed"] == 0
    assert store.recorded[0]["model"] == "original-model"
    assert store.recorded[0]["reconciliation"] == "reconciled"
    assert receipts.calls == ["receipt-1"]
    await reconciler.close()
    assert receipts.closed


@pytest.mark.parametrize("unavailable", [None, ProviderError("Unavailable"), TimeoutError()])
async def test_unknown_receipt_remains_reserved_before_24_hours(unavailable):
    store = FakeStore(timedelta(hours=23), {"request_id": "receipt-1"})
    receipts = Receipts(
        {"receipt-1": unavailable if isinstance(unavailable, Exception) else {"cost_rub": None}}
    )
    result = await Reconciler(None, store, provider=receipts).tick()
    assert result["pending"] == 1 and result["errors"] == 0
    assert store.state == "reserved" and not store.recorded


@pytest.mark.parametrize("stored", [False, True])
async def test_requestless_or_crashed_call_is_released_only_after_24_hours(stored):
    store = FakeStore(timedelta(hours=25), {}, stored=stored)
    receipts = Receipts({})
    result = await Reconciler(None, store, provider=receipts).tick()
    assert result["released"] == 1 and not receipts.calls and not store.recorded


async def test_requestless_call_before_deadline_stays_pending():
    store = FakeStore(timedelta(hours=23), {}, stored=False)
    receipts = Receipts({})
    result = await Reconciler(None, store, provider=receipts).tick()
    assert result["pending"] == 1 and store.state == "reserved" and not receipts.calls


async def test_malformed_receipt_cannot_extend_expired_reservation():
    store = FakeStore(timedelta(hours=25), {"request_id": "bad-receipt"})
    receipts = Receipts({"bad-receipt": ValueError("Malformed receipt")})
    result = await Reconciler(None, store, provider=receipts).tick()
    assert result["released"] == 1 and result["errors"] == 1


USER = -940001


class ScopedStore(Store):
    async def pending_reservations(self, *, limit=100, user_id=None):
        return await super().pending_reservations(limit=limit, user_id=USER)


async def cleanup(store):
    async with store.connection(USER) as conn:
        # The single literal test ID is the only scope this fixture may delete.
        await conn.execute("""DO $$ BEGIN
            DELETE FROM usage WHERE user_id=-940001;
            DELETE FROM ledger WHERE user_id=-940001;
            DELETE FROM reservations WHERE user_id=-940001;
            DELETE FROM users WHERE user_id=-940001;
        END $$""")


async def test_real_postgres_reconciliation_release_and_late_receipt_never_double_charge():
    if not os.getenv("DATABASE_URL"):
        pytest.skip("Real PostgreSQL DATABASE_URL is required")
    store = ScopedStore(Settings())
    await store.open()
    ids = {kind: f"billing-{kind}-{uuid4()}" for kind in ("known", "expired", "orphan", "awaiting")}
    run_id = uuid4()
    receipt = {
        "cost_rub": "1.5",
        "model": "test-model",
        "prompt_tokens": 10,
        "completion_tokens": 2,
    }
    receipts = Receipts({"known": receipt, "expired": ProviderError("Not ready")})
    reconciler = Reconciler(None, store, provider=receipts)
    try:
        await cleanup(store)
        await store.ensure_user(USER)
        for kind, amount in (("known", 3), ("expired", 2), ("orphan", 1), ("awaiting", 1)):
            await store.reserve(USER, ids[kind], amount * 1_000_000)
            if kind != "orphan":
                usage = {"cost_rub": None, "model": "test-model"}
                if kind != "awaiting":
                    usage["request_id"] = kind
                await store.record_usage(USER, run_id, ids[kind], usage)
        async with store.connection(USER) as conn:
            await conn.execute(
                """UPDATE reservations SET created_at=now()-
                CASE WHEN id=ANY($2::text[]) THEN interval '25 hours' ELSE interval '3 minutes' END
                WHERE user_id=$1""",
                USER,
                [ids["expired"], ids["orphan"]],
            )
        assert await store.reconciliation_usage(-940002, ids["known"]) is None
        assert not await store.release_unresolved_reservation(USER, ids["awaiting"])
        before = await store.balance(USER)
        assert before["pending_cost_operations"] == 4 and before["tokens_pending_cost"] == 7000
        result = await reconciler.tick()
        assert result == {"processed": 4, "settled": 1, "released": 2, "pending": 1, "errors": 0}
        balance = await store.balance(USER)
        assert balance["tokens_remaining"] == 23500 and balance["tokens_spent"] == 1500
        assert balance["pending_cost_operations"] == 1 and balance["tokens_pending_cost"] == 1000
        assert balance["unresolved_cost_operations"] == 2
        await store.record_usage(USER, run_id, ids["expired"], {**receipt, "cost_rub": "9"})
        await asyncio.gather(
            *(store.record_usage(USER, run_id, ids["known"], receipt) for _ in range(3))
        )
        assert (await reconciler.tick())["processed"] == 1
        after = await store.balance(USER)
        assert after["tokens_remaining"] == balance["tokens_remaining"]
        assert after["tokens_reserved"] == 1000 and after["unresolved_cost_operations"] == 2
        late = await store.reconciliation_usage(USER, ids["expired"])
        assert late["cost_micro"] == 9_000_000 and late["raw"]["charged_to"] == "project"
        assert late["raw"]["reconciliation"] == "unresolved_alpha" and late["raw"]["late_receipt"]
    finally:
        await reconciler.close()
        await cleanup(store)
        await store.close()
