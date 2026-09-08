"""Consented project initiative on local PostgreSQL; no providers or Telegram calls."""

import asyncio
import os
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from urllib.parse import urlsplit
from uuid import UUID, uuid4

import pytest

import cronos.initiative as initiative_module
from cronos.initiative import (
    InitiativeStoreMixin,
    invalidate_initiative_context,
)
from cronos.settings import Settings
from cronos.storage import Store

A, B = -987101, -987102


class InitiativeStore(Store, InitiativeStoreMixin):
    pass


@pytest.fixture
async def case():
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        pytest.skip("Real local PostgreSQL DATABASE_URL is required")
    parsed = urlsplit(database_url)
    assert parsed.hostname in {"localhost", "127.0.0.1"} and parsed.path == "/cronos_test"
    store = InitiativeStore(Settings(database_url=database_url))
    await store.open()
    created, events = [], []
    try:
        for owner in (A, B):
            async with store.connection(owner) as conn:
                assert not await conn.fetchval("SELECT 1 FROM users WHERE user_id=$1", owner), (
                    "Synthetic initiative owner already exists; do not erase another test's data"
                )
            await store.ensure_user(owner)
            created.append(owner)
        conversation = await store.conversation(A, A, 720)
        other = await store.conversation(B, B, 721)
        project = await store.create_project(
            A,
            conversation["id"],
            {"name": "Цены овощей", "goal": "Проверять свежие цены"},
            f"initiative-project:{uuid4()}",
        )
        await store.preferences(A, {"proactivity": True})

        async def make_run(schedule=None, target=None):
            target = target or conversation
            event_id = uuid4()
            events.append(event_id)
            payload = {"user_id": target["user_id"]}
            if schedule:
                payload.update(schedule_id=str(schedule["id"]), revision=schedule["revision"])
            async with store.connection() as conn:
                await conn.execute(
                    "INSERT INTO events(id,kind,payload) VALUES($1,$2,$3)",
                    event_id,
                    "timer" if schedule else "telegram",
                    payload,
                )
            return await store.start_run(event_id, target["user_id"], target["id"])

        yield SimpleNamespace(
            store=store, conversation=conversation, other=other, project=project, make_run=make_run
        )
    finally:
        for owner in created:
            async with store.connection(owner) as conn:
                await conn.execute(
                    "DELETE FROM occurrences WHERE schedule_id IN (SELECT id FROM schedules WHERE user_id=$1)",
                    owner,
                )
                for table in (
                    "outbox",
                    "operations",
                    "usage",
                    "reservations",
                    "run_metrics",
                    "artifacts",
                    "runs",
                    "projects",
                    "schedules",
                    "memory",
                    "messages",
                    "ledger",
                    "conversations",
                    "users",
                ):
                    await conn.execute(f"DELETE FROM {table} WHERE user_id=$1", owner)
                await conn.execute(
                    "DELETE FROM events WHERE payload->>'user_id'=$1",
                    str(owner),
                )
        await store.close()


def args(**patch):
    return {
        "purpose": "Подготовить полезное сравнение цен",
        "instruction": "Проверь цены огурцов в Москве, отправь только значимое изменение с источниками.",
        "allowed_tools": ["project_get", "web_search", "file_create"],
        "due_at": datetime.now(UTC) + timedelta(hours=1),
        "interval_seconds": 86400,
        **patch,
    }


async def configure(case, **patch):
    return await case.store.configure_initiative(
        A, case.project["id"], args(**patch), f"configure:{uuid4()}", await case.make_run()
    )


async def decide(case, policy, evidence="Москва: огурцы 130 руб/кг", **patch):
    schedule = await case.store.get_schedule(policy["schedule_id"])
    parameters = {
        "user_id": A,
        "id": policy["id"],
        "revision": policy["revision"],
        "summary": "Найдена подтверждённая существенная скидка.",
        "evidence_key": evidence,
        "send": True,
        "source_key": f"decision:{uuid4()}",
        "run": await case.make_run(schedule),
        "expected_project_revision": policy["project_revision"],
        **patch,
    }
    return await case.store.decide_initiative(**parameters)


async def test_configuration_respects_explicit_off_and_rejects_mutating_tools(case):
    await case.store.preferences(A, {"proactivity": False})
    with pytest.raises(ValueError, match="Proactivity is disabled"):
        await configure(case)
    await case.store.preferences(A, {"proactivity": True})
    for tool in (
        "upgrade",
        "schedule_create",
        "memory_write",
        "image_generate",
        "privacy_request",
        "arbitrary_webhook",
    ):
        with pytest.raises(ValueError, match="allowed_tools"):
            await configure(case, allowed_tools=[tool])
    async with case.store.connection(A) as conn:
        assert await conn.fetchval("SELECT count(*) FROM schedules WHERE user_id=$1", A) == 0


async def test_configure_replay_is_one_atomic_policy_schedule_and_receipt(case):
    run, source_key = await case.make_run(), "one-configuration"
    policy = await case.store.configure_initiative(A, case.project["id"], args(), source_key, run)
    replay = await case.store.configure_initiative(A, case.project["id"], args(), source_key, run)
    assert replay == policy and policy["available"] is True
    assert await case.store.list_initiatives(A, case.project["id"]) == [policy]
    assert await case.store.list_initiatives(B) == []
    schedule = await case.store.get_schedule(policy["schedule_id"])
    assert schedule["dynamic"] and schedule["proactive"]
    assert str(schedule["initiative_id"]) == policy["id"]
    assert schedule["chat_id"] == A and schedule["thread_id"] == 720
    async with case.store.connection(A) as conn:
        assert (
            await conn.fetchval("SELECT count(*) FROM initiative_policies WHERE user_id=$1", A) == 1
        )
        assert await conn.fetchval("SELECT count(*) FROM schedules WHERE user_id=$1", A) == 1
        assert (
            await conn.fetchval("SELECT count(*) FROM initiative_operations WHERE user_id=$1", A)
            == 1
        )


async def test_configure_transaction_failure_leaves_no_orphan_schedule(case, monkeypatch):
    async def fail(*unused, **unused_kwargs):
        raise RuntimeError("crash before receipt")

    monkeypatch.setattr(initiative_module, "_save", fail)
    with pytest.raises(RuntimeError, match="crash"):
        await configure(case)
    async with case.store.connection(A) as conn:
        assert await conn.fetchval("SELECT count(*) FROM schedules WHERE user_id=$1", A) == 0
        assert (
            await conn.fetchval("SELECT count(*) FROM initiative_policies WHERE user_id=$1", A) == 0
        )


async def test_foreign_owner_and_cancelled_run_cannot_configure_or_decide(case):
    foreign_run = await case.make_run(target=case.other)
    with pytest.raises(ValueError, match="source run"):
        await case.store.configure_initiative(A, case.project["id"], args(), "foreign", foreign_run)
    policy = await configure(case)
    assert await case.store.get_initiative_for_schedule(B, policy["schedule_id"]) is None
    run = await case.make_run(await case.store.get_schedule(policy["schedule_id"]))
    async with case.store.connection(A) as conn:
        await conn.execute("UPDATE runs SET cancel_requested=true WHERE id=$1", run["id"])
    with pytest.raises(ValueError, match="no longer active"):
        await decide(case, policy, run=run)
    with pytest.raises(ValueError, match="own scheduled run"):
        await decide(case, policy, run=await case.make_run())


async def test_decision_and_replay_do_not_consume_fingerprint_before_delivery(case):
    policy = await configure(case)
    source_key = "same-decision"
    run = await case.make_run(await case.store.get_schedule(policy["schedule_id"]))
    result = await decide(case, policy, run=run, source_key=source_key)
    replay = await decide(case, policy, run=run, source_key=source_key)
    assert result == replay and result["should_send"] is True
    assert await case.store.validate_initiative_delivery(A, result["delivery_guard"]) is True
    assert await case.store.validate_initiative_delivery(B, result["delivery_guard"]) is False
    async with case.store.connection(A) as conn:
        assert (
            await conn.fetchval(
                "SELECT last_fingerprint FROM initiative_policies WHERE id=$1", UUID(policy["id"])
            )
            is None
        )


async def test_pending_suppression_failed_retry_and_sent_fingerprint(case):
    policy = await configure(case)
    first = await decide(case, policy)
    payload = {"text": "Подготовленное сравнение", **first["delivery_guard"]}
    outbox_id = await case.store.enqueue(A, A, 720, payload, "initiative-delivery")
    assert (await decide(case, policy))["reason"] == "delivery_pending"
    async with case.store.connection(A) as conn:
        await conn.execute("UPDATE outbox SET state='failed' WHERE id=$1", outbox_id)
    retry = await decide(case, policy)
    assert retry["should_send"] is True
    async with case.store.connection() as conn:
        await conn.execute(
            "UPDATE outbox SET state='sending',owner='initiative-test' WHERE id=$1", outbox_id
        )
    await case.store.delivery_result(outbox_id, "initiative-test", ids=[101])
    delivered = await decide(case, policy)
    assert delivered == {"should_send": False, "reason": "already_delivered"}
    changed = await decide(case, policy, evidence="Москва: огурцы 90 руб/кг")
    assert changed["should_send"] is True


async def test_sent_files_join_project_search_only_after_full_ack_and_replay_once(case):
    store = case.store
    policy = await configure(case)
    schedule = await store.get_schedule(policy["schedule_id"])
    run, other_run = await case.make_run(schedule), await case.make_run(schedule)
    prepared, unrelated, excluded = (str(uuid4()) for _ in range(3))
    for artifact_id, source_run in (
        (prepared, run),
        (unrelated, other_run),
        (excluded, run),
    ):
        await store.save_artifact(
            A,
            {
                "id": artifact_id,
                "filename": "prepared.csv",
                "mime": "text/csv",
                "path": f"/synthetic/{artifact_id}.csv",
                "extracted": {"text": "acknowledgedreport: огурцы 90 руб/кг"},
            },
        )
        await store.register_artifact_version(
            A,
            artifact_id,
            project_id=policy["project_id"],
            source_key=f"prepared:{artifact_id}",
            run=source_run,
        )
    async with store.connection(A) as conn:
        await conn.execute(
            "UPDATE artifact_versions SET context_excluded=true WHERE artifact_id=$1",
            UUID(excluded),
        )
    decision = await decide(case, policy, run=run)
    payload = {
        "_telegram_parts": [
            {"kind": "rich", "text": "Подготовлено сравнение."},
            {"kind": "document", "path": f"/synthetic/{prepared}.csv"},
        ],
        "initiative_artifact_ids": [prepared, unrelated, excluded, prepared],
        **decision["delivery_guard"],
    }
    outbox_id = await store.enqueue_for_run(run, case.conversation, payload, "prepared-ack")
    assert outbox_id is not None

    async def snapshot():
        async with store.connection(A) as conn:
            links = await conn.fetch(
                "SELECT artifact_id FROM project_artifacts WHERE project_id=$1",
                UUID(policy["project_id"]),
            )
            revision = await conn.fetchval(
                "SELECT revision FROM projects WHERE id=$1", UUID(policy["project_id"])
            )
            journal = await conn.fetch(
                "SELECT * FROM project_changes WHERE project_id=$1 AND kind='attach_artifact'",
                UUID(policy["project_id"]),
            )
        search = await store.library_search(
            A, "acknowledgedreport", kinds=["artifact"], project_id=policy["project_id"]
        )
        return {str(row["artifact_id"]) for row in links}, revision, journal, search["hits"]

    assert await snapshot() == (set(), policy["project_revision"], [], [])
    # A partial transport success only stores the unacknowledged tail and known IDs.
    async with store.connection(A) as conn:
        await conn.execute(
            """UPDATE outbox SET payload=$2,telegram_ids='[101]',state='pending',
            owner='initiative-ack-test' WHERE id=$1""",
            outbox_id,
            {**payload, "_telegram_parts": payload["_telegram_parts"][1:]},
        )
    assert await snapshot() == (set(), policy["project_revision"], [], [])
    await store.delivery_result(outbox_id, "initiative-ack-test", error="Failed", permanent=True)
    assert await snapshot() == (set(), policy["project_revision"], [], [])
    async with store.connection(A) as conn:
        await conn.execute("UPDATE outbox SET state='sending' WHERE id=$1", outbox_id)
        await conn.execute("UPDATE runs SET status='done' WHERE id=$1", run["id"])
    async with store.user_lock(A, purpose="delivery"):
        await store.delivery_result(outbox_id, "initiative-ack-test", ids=[101, 102])
    links, revision, journal, hits = await snapshot()
    assert links == {prepared} and revision == policy["project_revision"] + 1
    assert len(journal) == 1 and journal[0]["patch"] == {"artifact_ids": [prepared]}
    assert journal[0]["run_id"] == run["id"]
    assert journal[0]["conversation_id"] == case.conversation["id"]
    assert journal[0]["source_key"] == (
        f"initiative:delivered:{decision['delivery_guard']['initiative_decision_id']}"
    )
    assert [hit["id"] for hit in hits] == [prepared]
    async with store.user_lock(A, purpose="delivery"):
        await store.delivery_result(outbox_id, "initiative-ack-test", ids=[101, 102])
    assert await snapshot() == (links, revision, journal, hits)


async def test_skip_and_empty_reason_cannot_authorize_delivery(case):
    policy = await configure(case)
    skipped = await decide(case, policy, send=False, summary="Существенных изменений нет.")
    assert skipped == {"should_send": False, "reason": "Существенных изменений нет."}
    assert (
        await case.store.validate_initiative_delivery(A, {"initiative_id": policy["id"]}) is False
    )
    with pytest.raises(ValueError, match="decision reason"):
        await decide(case, policy, summary=" ")


async def test_store_fences_every_prepared_timer_outbox_before_enqueue(case):
    policy = await configure(case)
    schedule = await case.store.get_schedule(policy["schedule_id"])
    run = await case.make_run(schedule)
    assert (
        await case.store.enqueue_for_run(
            run, case.conversation, {"document_path": "/synthetic/unapproved.csv"}, "early-file"
        )
        is None
    )
    decision = await decide(case, policy, run=run)
    payload = {"text": "Подготовлено", **decision["delivery_guard"]}
    assert await case.store.enqueue_for_run(run, case.conversation, payload, "approved") is not None
    await case.store.initiative_feedback(
        A, policy["id"], "stop", "stop-before-second-enqueue", await case.make_run()
    )
    assert (
        await case.store.enqueue_for_run(run, case.conversation, payload, "stale-after-stop")
        is None
    )


@pytest.mark.parametrize(
    "mutation",
    [
        "project_revision",
        "project_completed",
        "project_reset",
        "consent",
        "policy_pause",
        "schedule_cancel",
        "conversation_detach",
    ],
)
async def test_delivery_and_new_decisions_recheck_live_policy_project_and_consent(case, mutation):
    policy = await configure(case)
    decision = await decide(case, policy)
    async with case.store.connection(A) as conn:
        if mutation == "project_revision":
            await conn.execute(
                "UPDATE projects SET revision=revision+1 WHERE id=$1", UUID(policy["project_id"])
            )
        elif mutation == "project_completed":
            await conn.execute(
                "UPDATE projects SET status='completed' WHERE id=$1", UUID(policy["project_id"])
            )
        elif mutation == "project_reset":
            await conn.execute(
                "UPDATE projects SET context_reset_revision=revision+1,revision=revision+1,context_excluded=true WHERE id=$1",
                UUID(policy["project_id"]),
            )
        elif mutation == "consent":
            await conn.execute(
                "UPDATE users SET preferences=preferences||'{\"proactivity\":false}' WHERE user_id=$1",
                A,
            )
        elif mutation == "policy_pause":
            await conn.execute(
                "UPDATE initiative_policies SET status='paused',revision=revision+1 WHERE id=$1",
                UUID(policy["id"]),
            )
        elif mutation == "schedule_cancel":
            await conn.execute(
                "UPDATE schedules SET state='cancelled',revision=revision+1 WHERE id=$1",
                UUID(policy["schedule_id"]),
            )
        else:
            await conn.execute("DELETE FROM project_conversations WHERE user_id=$1", A)
    assert await case.store.validate_initiative_delivery(A, decision["delivery_guard"]) is False
    assert (await decide(case, policy))["should_send"] is False


async def test_feedback_is_idempotent_and_waits_for_actual_delivery_lock(case):
    policy, run = await configure(case), await case.make_run()
    more = await case.store.initiative_feedback(A, policy["id"], "more", "more-once", run)
    again = await case.store.initiative_feedback(A, policy["id"], "more", "more-once", run)
    assert more == again and more["interval_seconds"] == 43200
    task = None
    try:
        async with case.store.user_lock(A, purpose="delivery"):
            task = asyncio.create_task(
                case.store.initiative_feedback(A, policy["id"], "pause", "pause-once", run)
            )
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(asyncio.shield(task), timeout=0.1)
        paused = await asyncio.wait_for(task, timeout=2)
        assert paused["available"] is False
        assert paused["status"] == "paused"
        assert await case.store.list_initiatives(A, include_paused=False) == []
    finally:
        if task and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    resumed = await case.store.initiative_feedback(A, policy["id"], "resume", "resume", run)
    assert resumed["available"] is True
    await case.store.preferences(A, {"proactivity": False})
    with pytest.raises(ValueError, match="proactivity_disabled"):
        await case.store.initiative_feedback(A, policy["id"], "resume", "bad-resume", run)


async def test_forget_scrubs_policy_and_receipts_and_old_context_cannot_resume(case):
    policy = await configure(case)
    await decide(case, policy)
    async with case.store.user_lock(A, purpose="delivery"), case.store.connection(A) as conn:
        await conn.execute("SELECT user_id FROM users WHERE user_id=$1 FOR UPDATE", A)
        await conn.execute(
            "UPDATE projects SET context_excluded=true,context_reset_revision=revision+1,revision=revision+1 WHERE user_id=$1",
            A,
        )
        await invalidate_initiative_context(conn, A)
    result = await case.store.get_initiative_for_schedule(A, policy["schedule_id"])
    assert result["available"] is False and "instruction" not in result
    async with case.store.connection(A) as conn:
        assert await conn.fetchval(
            "SELECT bool_and(result='{}') FROM initiative_operations WHERE user_id=$1", A
        )
        assert (
            await conn.fetchval(
                "SELECT instruction FROM schedules WHERE id=$1", UUID(policy["schedule_id"])
            )
            == ""
        )
        await conn.execute(
            "UPDATE projects SET context_excluded=false,name='Новый проект',goal='Новая цель',revision=revision+1 WHERE user_id=$1",
            A,
        )
    with pytest.raises(ValueError, match="project_context_reset"):
        await case.store.initiative_feedback(
            A, policy["id"], "resume", "cannot-restore-old-purpose", await case.make_run()
        )


async def test_project_deletion_leaves_safe_schedule_marker_instead_of_legacy_fallback(case):
    policy = await configure(case)
    decision = await decide(case, policy)
    async with case.store.connection(A) as conn:
        await conn.execute("DELETE FROM projects WHERE id=$1", UUID(policy["project_id"]))
        assert (
            await conn.fetchval("SELECT count(*) FROM initiative_operations WHERE user_id=$1", A)
            == 0
        )
    result = await case.store.get_initiative_for_schedule(A, policy["schedule_id"])
    assert result == {"id": policy["id"], "available": False, "reason": "policy_unavailable"}
    assert await case.store.validate_initiative_delivery(A, decision["delivery_guard"]) is False


async def test_legacy_schedule_has_no_prepared_initiative_policy(case):
    schedule = await case.store.schedule(
        A, case.conversation, datetime.now(UTC), "Проверить цель", dynamic=True, proactive=True
    )
    assert await case.store.get_initiative_for_schedule(A, schedule["id"]) is None
