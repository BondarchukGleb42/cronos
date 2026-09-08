"""One-time proactivity defaults preserve explicit opt-outs and existing account data."""

import json
import os
from types import SimpleNamespace
from uuid import uuid4

import asyncpg
import pytest
import pytest_asyncio

from cronos.proactivity import APPLY_DEFAULT_SQL
from cronos.settings import Settings
from cronos.storage import Store, privacy_event_id

A, B = -989301, -989302
pytestmark = pytest.mark.asyncio(loop_scope="module")


async def cleanup(store):
    for owner in (A, B):
        async with store.connection(owner) as conn:
            requests = await conn.fetch("SELECT id FROM privacy_requests WHERE user_id=$1", owner)
            events = await conn.fetch("SELECT event_id FROM runs WHERE user_id=$1", owner)
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
                "DELETE FROM events WHERE payload->>'user_id'=$1 OR id=ANY($2::uuid[])",
                str(owner),
                [row["event_id"] for row in events if row["event_id"] is not None]
                + [privacy_event_id(row["id"]) for row in requests],
            )


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def db():
    if not os.getenv("DATABASE_URL") or not os.getenv("ADMIN_DATABASE_URL"):
        pytest.skip("Real PostgreSQL URLs are required")
    settings = Settings()
    store = Store(settings)
    await store.open()
    admin = await asyncpg.connect(settings.admin_database_url.get_secret_value())
    try:
        yield SimpleNamespace(store=store, admin=admin)
    finally:
        await cleanup(store)
        await admin.close()
        await store.close()


@pytest_asyncio.fixture(autouse=True, loop_scope="module")
async def clean_users(db):
    await cleanup(db.store)
    yield
    await cleanup(db.store)


async def legacy_user(db, owner=A, preferences=None):
    """Simulate an old worker inserting a row without the newly added marker column."""
    conversation_id = uuid4()
    async with db.store.connection(owner) as conn:
        await conn.execute(
            "INSERT INTO users(user_id,preferences) VALUES($1,$2)",
            owner,
            {"proactivity": False, "timezone": "Europe/Moscow", "tone": "Коротко"}
            if preferences is None
            else preferences,
        )
        row = await conn.fetchrow(
            """INSERT INTO conversations(id,user_id,chat_id,thread_id,title)
            VALUES($1,$2,$2,10,'План') RETURNING *""",
            conversation_id,
            owner,
        )
    return dict(row)


async def user_row(db, owner=A):
    async with db.store.connection(owner) as conn:
        return dict(await conn.fetchrow("SELECT * FROM users WHERE user_id=$1", owner))


async def apply_default(db, owner=A):
    # The same statement is used globally by migrate and lazily by ensure_user.
    # Restrict the admin test call to our synthetic owner to avoid other fixtures.
    await db.admin.execute(APPLY_DEFAULT_SQL, owner)
    return await user_row(db, owner)


async def preference_receipt(
    db, conversation, arguments=None, *, model_present=True, compact=False
):
    owner, run_id = conversation["user_id"], uuid4()
    async with db.store.connection(owner) as conn:
        await conn.execute(
            "INSERT INTO runs(id,user_id,conversation_id,status) VALUES($1,$2,$3,'done')",
            run_id,
            owner,
            conversation["id"],
        )
    if model_present:
        await db.store.save_operation(
            f"{run_id}:model:0",
            owner,
            run_id,
            "model",
            {
                "message": {
                    "role": "assistant",
                    "content": None,
                    "tool_calls": [
                        {
                            "id": "setting-call",
                            "type": "function",
                            "function": {
                                "name": "preferences_set",
                                "arguments": json.dumps(
                                    arguments, separators=(",", ":") if compact else None
                                ),
                            },
                        },
                    ],
                }
            },
        )
    await db.store.save_operation(
        f"{run_id}:tool:1:0",
        owner,
        run_id,
        "preferences_set",
        {"proactivity": False, "timezone": "Europe/Moscow", "tone": "Коротко"},
    )


async def test_new_user_starts_enabled_and_marks_the_default_applied(db):
    row = await db.store.ensure_user(A)
    assert row["preferences"]["proactivity"] is True
    assert row["proactivity_default_applied"] is True
    assert (await db.store.preferences(A))["proactivity"] is True


@pytest.mark.parametrize("preferences", [{}, {"proactivity": False}, {"proactivity": True}])
async def test_legacy_defaults_are_enabled_once_by_migration(db, preferences):
    await legacy_user(db, preferences=preferences)
    assert (await user_row(db))["proactivity_default_applied"] is False
    row = await apply_default(db)
    assert row["preferences"] == {**preferences, "proactivity": True}
    assert row["proactivity_default_applied"] is True


async def test_ui_explicit_off_is_preserved(db):
    await legacy_user(db)
    await db.store.save_operation(
        "default:ui-off", A, None, "home_preference", {"proactivity": False}
    )
    row = await apply_default(db)
    assert row["preferences"]["proactivity"] is False
    assert row["proactivity_default_applied"] is True


async def test_unrelated_preferences_result_false_is_not_an_explicit_opt_out(db):
    conversation = await legacy_user(db)
    await preference_receipt(db, conversation, {"timezone": "Europe/Moscow"})
    row = await apply_default(db)
    assert row["preferences"]["proactivity"] is True
    assert row["preferences"]["timezone"] == "Europe/Moscow"
    assert row["proactivity_default_applied"] is True


@pytest.mark.parametrize("compact", [False, True])
async def test_explicit_freeform_off_is_preserved_from_same_run_model_arguments(db, compact):
    conversation = await legacy_user(db)
    await preference_receipt(db, conversation, {"proactivity": False}, compact=compact)
    row = await apply_default(db)
    assert row["preferences"]["proactivity"] is False
    assert row["proactivity_default_applied"] is True


async def test_orphan_preferences_receipt_conservatively_preserves_off(db):
    conversation = await legacy_user(db)
    await preference_receipt(db, conversation, model_present=False)
    row = await apply_default(db)
    assert row["preferences"]["proactivity"] is False
    assert row["proactivity_default_applied"] is True


async def test_second_migration_and_lazy_access_do_not_undo_a_later_explicit_off(db):
    await legacy_user(db)
    assert (await apply_default(db))["preferences"]["proactivity"] is True
    await db.store.set_proactivity(A, False, "default:later-off")
    assert (await apply_default(db))["preferences"]["proactivity"] is False
    assert (await db.store.ensure_user(A))["preferences"]["proactivity"] is False


async def test_lazy_default_catches_users_created_by_old_worker_after_migration(db):
    await db.admin.execute(APPLY_DEFAULT_SQL, A)
    await legacy_user(db)
    row = await db.store.ensure_user(A)
    assert row["preferences"]["proactivity"] is True
    assert row["proactivity_default_applied"] is True


async def test_backfill_leaves_an_erasing_account_and_its_marker_untouched(db):
    await legacy_user(db)
    async with db.store.connection(A) as conn:
        await conn.execute(
            """INSERT INTO privacy_requests(id,user_id,scope,chat_id,source_key,state)
            VALUES($1,$2,'all',$2,'default:erasing','erasing')""",
            uuid4(),
            A,
        )
    before = await user_row(db)
    after = await apply_default(db)
    assert after == before
    assert after["preferences"]["proactivity"] is False
    assert after["proactivity_default_applied"] is False


async def test_targeted_backfill_and_opt_out_proof_are_owner_scoped(db):
    await legacy_user(db, A)
    await legacy_user(db, B)
    await db.store.save_operation(
        "default:owner-b-off", B, None, "home_preference", {"proactivity": False}
    )
    assert (await apply_default(db, A))["preferences"]["proactivity"] is True
    untouched_b = await user_row(db, B)
    assert untouched_b["preferences"]["proactivity"] is False
    assert untouched_b["proactivity_default_applied"] is False
    assert (await apply_default(db, B))["preferences"]["proactivity"] is False


async def test_backfill_preserves_finances_other_preferences_and_every_schedule(db):
    preferences = {
        "proactivity": False,
        "timezone": "Asia/Tokyo",
        "tone": "Коротко",
        "reasoning": True,
        "model": "openai/gpt-5.6-terra",
        "nested": {"keep": [1, 2]},
    }
    conversation = await legacy_user(db, preferences=preferences)
    async with db.store.connection(A) as conn:
        await conn.execute(
            """UPDATE users SET plan='START',pending_plan='PREMIUM',balance_micro=123456,
            reserved_micro=777,entitlement_micro=700000000,topup_micro=333,
            period_end=now()+interval '10 days',billing_anchor=now() WHERE user_id=$1""",
            A,
        )
        await conn.execute(
            "INSERT INTO usage(operation_id,user_id,model,prompt_tokens,completion_tokens,cost_micro,raw) VALUES('default:usage',$1,'test/model',12,34,456,$2)",
            A,
            {"cost_rub": "0.000456", "request_id": "synthetic-receipt"},
        )
        await conn.execute(
            "INSERT INTO ledger(operation_id,user_id,kind,amount_micro,description) VALUES('default:ledger',$1,'charge',-456,'Synthetic usage')",
            A,
        )
        await conn.execute(
            "INSERT INTO reservations(id,user_id,amount_micro) VALUES('default:hold',$1,777)", A
        )
        for proactive, state in ((True, "active"), (True, "cancelled"), (False, "active")):
            await conn.execute(
                """INSERT INTO schedules(id,user_id,conversation_id,chat_id,thread_id,due_at,
                timezone,interval_seconds,instruction,fixed_text,dynamic,proactive,state,revision)
                VALUES($1,$2,$3,$2,10,now()+interval '1 day','Asia/Tokyo',86400,'Synthetic task','',true,$4,$5,4)""",
                uuid4(),
                A,
                conversation["id"],
                proactive,
                state,
            )
        before_tables = {
            table: [
                dict(row)
                for row in await conn.fetch(f"SELECT * FROM {table} WHERE user_id=$1 ORDER BY 1", A)
            ]
            for table in ("usage", "ledger", "reservations", "schedules")
        }
    before = await user_row(db)
    after = await apply_default(db)
    assert after["preferences"] == {**preferences, "proactivity": True}
    assert {
        k: v for k, v in after.items() if k not in {"preferences", "proactivity_default_applied"}
    } == {
        k: v for k, v in before.items() if k not in {"preferences", "proactivity_default_applied"}
    }
    async with db.store.connection(A) as conn:
        for table, expected in before_tables.items():
            actual = [
                dict(row)
                for row in await conn.fetch(f"SELECT * FROM {table} WHERE user_id=$1 ORDER BY 1", A)
            ]
            assert actual == expected, table


async def test_full_clear_restores_enabled_default_without_resetting_billing(db):
    await db.store.ensure_user(A)
    conversation = await db.store.conversation(A, A, 10)
    await db.store.set_proactivity(A, False, "default:before-clear-off")
    before = await user_row(db)
    request = await db.store.requests_prepare(A, conversation, "all", source_key="default:clear")
    assert await db.store.confirm_privacy_request(A, request["id"], 10)
    async with db.store.user_lock(A), db.store.user_lock(A, purpose="delivery"):
        await db.store.begin_privacy_erasure(request["id"], privacy_event_id(request["id"]))
    after = await user_row(db)
    assert after["preferences"]["proactivity"] is True
    assert after["proactivity_default_applied"] is True
    for key in (
        "plan",
        "pending_plan",
        "balance_micro",
        "entitlement_micro",
        "topup_micro",
        "period_start",
        "period_end",
        "billing_anchor",
    ):
        assert after[key] == before[key], key
    assert await db.store.operation("default:before-clear-off") is None
