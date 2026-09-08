"""Real owner-scoped persistence and replay tests for dialogue controls."""

import os

import pytest
import pytest_asyncio

from cronos.model_preferences import DIALOGUE_MODELS
from cronos.settings import Settings
from cronos.storage import Store

A, B = -989201, -989202
pytestmark = pytest.mark.asyncio(loop_scope="module")


async def cleanup(store):
    for owner in (A, B):
        async with store.connection(owner) as conn:
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
            await conn.execute("DELETE FROM events WHERE payload->>'user_id'=$1", str(owner))


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


async def financial(store, owner):
    async with store.connection(owner) as conn:
        return dict(
            await conn.fetchrow(
                """SELECT plan,pending_plan,balance_micro,reserved_micro,entitlement_micro,
            topup_micro,period_start,period_end,billing_anchor FROM users WHERE user_id=$1""",
                owner,
            )
        )


async def test_old_model_and_reasoning_replay_preserves_new_choice_and_other_preferences(store):
    await store.preferences(A, {"private_profile": "Never copy into a receipt", "tone": "Коротко"})
    before = await financial(store, A)
    old = await store.set_model_preferences(A, {"model": "luna", "reasoning": False}, "control:old")
    latest = await store.set_model_preferences(
        A, {"model": "sol", "reasoning": True}, "control:new"
    )
    assert old == {"model": DIALOGUE_MODELS["luna"], "reasoning": False}
    assert latest == {"model": DIALOGUE_MODELS["sol"], "reasoning": True}
    assert (
        await store.set_model_preferences(A, {"model": "luna", "reasoning": False}, "control:old")
        == old
    )
    prefs = await store.preferences(A)
    assert prefs["model"] == DIALOGUE_MODELS["sol"] and prefs["reasoning"] is True
    assert prefs["private_profile"] == "Never copy into a receipt" and prefs["tone"] == "Коротко"
    assert await financial(store, A) == before
    async with store.connection(A) as conn:
        receipts = await conn.fetch("SELECT kind,result,run_id FROM operations WHERE user_id=$1", A)
        assert len(receipts) == 2
        assert all(
            row["kind"] == "model_preference"
            and row["run_id"] is None
            and set(row["result"]) == {"model", "reasoning"}
            for row in receipts
        )


async def test_alias_normalization_and_partial_updates_keep_the_other_control(store):
    assert await store.set_model_preferences(A, {"model": "Solar"}, "control:solar") == {
        "model": DIALOGUE_MODELS["sol"]
    }
    await store.set_model_preferences(A, {"reasoning": True}, "control:deep")
    await store.set_model_preferences(A, {"model": "GPT-5.6 Terra"}, "control:terra")
    preferences = await store.preferences(A)
    assert preferences["reasoning"] is True and preferences["model"] == DIALOGUE_MODELS["terra"]
    await store.set_model_preferences(A, {"reasoning": False}, "control:off")
    await store.set_model_preferences(A, {"reasoning": True}, "control:deep")
    preferences = await store.preferences(A)
    assert preferences["reasoning"] is False and preferences["model"] == DIALOGUE_MODELS["terra"]


async def test_receipt_owner_and_operation_kind_are_checked_before_writing(store):
    await store.set_model_preferences(A, {"model": "luna"}, "control:owned")
    before_b = await store.preferences(B)
    with pytest.raises(ValueError, match="недоступна"):
        await store.set_model_preferences(B, {"model": "terra"}, "control:owned")
    assert await store.preferences(B) == before_b
    await store.save_operation("control:wrong-kind", A, None, "model", {"model": "terra"})
    with pytest.raises(ValueError, match="недоступна"):
        await store.set_model_preferences(A, {"model": "sol"}, "control:wrong-kind")
    assert (await store.preferences(A))["model"] == DIALOGUE_MODELS["luna"]


async def test_invalid_patch_never_partially_updates_the_other_control(store):
    await store.set_model_preferences(A, {"model": "luna", "reasoning": False}, "control:initial")
    with pytest.raises(ValueError):
        await store.set_model_preferences(
            A, {"model": "unsupported", "reasoning": True}, "control:invalid"
        )
    with pytest.raises(ValueError):
        await store.set_model_preferences(
            A, {"model": "sol", "reasoning": "false"}, "control:string"
        )
    prefs = await store.preferences(A)
    assert prefs["model"] == DIALOGUE_MODELS["luna"] and prefs["reasoning"] is False
    assert await store.operation("control:invalid") is None
    assert await store.operation("control:string") is None


async def test_erasing_account_rejects_new_preference_writes(store):
    conversation = await store.conversation(A, A, 10)
    request = await store.requests_prepare(A, conversation, "all", source_key="control:privacy")
    async with store.connection(A) as conn:
        await conn.execute("UPDATE privacy_requests SET state='erasing' WHERE id=$1", request["id"])
    before = await store.preferences(A)
    with pytest.raises(ValueError, match="очистки"):
        await store.set_model_preferences(A, {"reasoning": True}, "control:erasing")
    assert await store.preferences(A) == before
    assert await store.operation("control:erasing") is None
