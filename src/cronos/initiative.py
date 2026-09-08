"""Consented project preparation, durable decisions and delivery-time policy guards."""

import hashlib
from contextlib import AbstractAsyncContextManager
from datetime import datetime
from uuid import UUID, uuid4

import asyncpg

from cronos.projects import _origin, _owner_lock

INITIATIVE_ALLOWED_TOOLS = frozenset(
    {
        "project_get",
        "library_search",
        "library_read",
        "file_read",
        "table_analyze",
        "web_search",
        "file_create",
        "skill_info",
    }
)
MIN_INTERVAL, MAX_INTERVAL = 3600, 31_536_000
_ROW = """SELECT i.*,p.revision AS project_revision,p.status AS project_status,
  p.context_excluded,p.context_reset_revision AS live_context_reset_revision,
  s.state AS schedule_state,s.revision AS schedule_revision,s.interval_seconds,s.due_at,
  s.proactive,s.dynamic,s.initiative_id AS schedule_initiative_id,u.preferences,
  EXISTS(SELECT 1 FROM project_conversations pc WHERE pc.user_id=i.user_id
    AND pc.project_id=i.project_id AND pc.conversation_id=i.conversation_id) AS conversation_bound
  FROM initiative_policies i JOIN projects p ON p.user_id=i.user_id AND p.id=i.project_id
  JOIN schedules s ON s.user_id=i.user_id AND s.id=i.schedule_id
  JOIN users u ON u.user_id=i.user_id WHERE i.user_id=$1 AND i.id=$2"""


def _text(value, name, maximum):
    if not isinstance(value, str) or not value.strip() or len(value) > maximum:
        raise ValueError(f"{name} requires 1-{maximum} characters")
    return value.strip()


def _integer(value, name, minimum, maximum):
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise ValueError(f"{name} must be between {minimum} and {maximum}")
    return value


def _inactive_reason(row):
    if row["preferences"].get("proactivity") is not True:
        return "proactivity_disabled"
    if (
        row["context_excluded"]
        or row["context_reset_revision"] != row["live_context_reset_revision"]
    ):
        return "project_context_reset"
    if row["project_status"] != "active":
        return "project_inactive"
    if not row["conversation_bound"]:
        return "conversation_detached"
    if row["status"] != "active":
        return "policy_paused"
    if row["schedule_state"] != "active":
        return "schedule_inactive"
    if not row["proactive"] or not row["dynamic"] or row["schedule_initiative_id"] != row["id"]:
        return "schedule_policy_mismatch"
    return None


def _public(row):
    result = {
        "id": str(row["id"]),
        "project_id": str(row["project_id"]),
        "schedule_id": str(row["schedule_id"]),
        "revision": row["revision"],
        "status": row["status"],
    }
    reason = _inactive_reason(row)
    if reason:
        return {**result, "available": False, "reason": reason}
    return {
        **result,
        "available": True,
        "conversation_id": str(row["conversation_id"]),
        "project_revision": row["project_revision"],
        "purpose": row["purpose"],
        "instruction": row["instruction"],
        "allowed_tools": row["allowed_tools"],
        "interval_seconds": row["interval_seconds"],
        "due_at": row["due_at"].isoformat(),
    }


async def _run(conn, user_id, run):
    if not isinstance(run, dict) or run.get("user_id") != user_id or not run.get("id"):
        raise ValueError("Initiative source run is unavailable")
    return await _origin(conn, user_id, run.get("conversation_id"), run.get("id"), run.get("fence"))


async def _receipt(conn, user_id, source_key, kind, initiative_id=None):
    row = await conn.fetchrow(
        "SELECT * FROM initiative_operations WHERE user_id=$1 AND source_key=$2",
        user_id,
        source_key,
    )
    if row and (row["kind"] != kind or (initiative_id and row["initiative_id"] != initiative_id)):
        raise ValueError("Initiative source_key belongs to another operation")
    return row


async def _save(
    conn, user_id, policy_id, kind, source_key, origin, run_id, result, operation_id=None
):
    await conn.execute(
        """INSERT INTO initiative_operations
        (id,user_id,initiative_id,kind,source_key,run_id,conversation_id,result)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8)""",
        operation_id or uuid4(),
        user_id,
        policy_id,
        kind,
        source_key,
        run_id,
        origin,
        result,
    )


async def _decision_valid(conn, user_id, payload):
    """Read only. The caller holds the delivery lock through its actual Telegram send."""
    if not isinstance(payload, dict):
        return False
    try:
        policy_id = UUID(str(payload["initiative_id"]))
        decision_id = UUID(str(payload["initiative_decision_id"]))
    except KeyError, ValueError, TypeError:
        return False
    row = await conn.fetchrow(_ROW, user_id, policy_id)
    if not row or _inactive_reason(row):
        return False
    if (
        payload.get("initiative_revision") != row["revision"]
        or payload.get("initiative_project_revision") != row["project_revision"]
        or payload.get("schedule_id") != str(row["schedule_id"])
        or payload.get("schedule_revision") != row["schedule_revision"]
    ):
        return False
    result = await conn.fetchval(
        """SELECT result FROM initiative_operations WHERE user_id=$1 AND id=$2
        AND initiative_id=$3 AND kind='decide'""",
        user_id,
        decision_id,
        policy_id,
    )
    if not isinstance(result, dict) or result.get("should_send") is not True:
        return False
    guard = result.get("delivery_guard")
    return isinstance(guard, dict) and all(
        payload.get(key) == value for key, value in guard.items()
    )


async def mark_initiative_sent(conn, payload):
    """Call inside Store.delivery_result's success transaction, using its stored outbox payload.

    Never call for failed/partial sends. A failed delivery therefore does not consume the
    evidence fingerprint and a later run can prepare the useful result again.
    """
    if not conn.is_in_transaction():
        raise RuntimeError("Initiative acknowledgement requires the delivery transaction")
    if not isinstance(payload, dict) or not payload.get("initiative_id"):
        return
    try:
        policy_id = UUID(str(payload["initiative_id"]))
        decision_id = UUID(str(payload["initiative_decision_id"]))
    except KeyError, ValueError, TypeError:
        return
    origin = await conn.fetchrow(
        """UPDATE initiative_policies i SET last_fingerprint=$3,updated_at=clock_timestamp()
        FROM initiative_operations d WHERE i.id=$1 AND d.id=$2 AND d.initiative_id=i.id
        AND d.user_id=i.user_id AND d.kind='decide' AND d.result->>'should_send'='true'
        AND d.result->'delivery_guard'->>'initiative_fingerprint'=$3
        AND i.revision=$4 AND i.status='active'
        RETURNING i.user_id,i.project_id,d.run_id,d.conversation_id""",
        policy_id,
        decision_id,
        payload.get("initiative_fingerprint"),
        payload.get("initiative_revision"),
    )
    artifact_ids = payload.get("initiative_artifact_ids")
    if not origin or not isinstance(artifact_ids, list) or not artifact_ids:
        return
    try:
        artifact_ids = list(dict.fromkeys(UUID(str(value)) for value in artifact_ids))
    except ValueError, TypeError:
        return
    if not await _decision_valid(conn, origin["user_id"], payload):
        return
    # The sender already owns the delivery lock. Attach directly in this transaction;
    # calling the public attach method would try to acquire that same lock again.
    added = await conn.fetch(
        """INSERT INTO project_artifacts(user_id,project_id,artifact_id)
        SELECT v.user_id,v.project_id,v.artifact_id FROM artifact_versions v
        JOIN users u ON u.user_id=v.user_id
        JOIN runs r ON r.user_id=v.user_id AND r.id=v.run_id
        JOIN projects p ON p.user_id=v.user_id AND p.id=v.project_id
        WHERE v.user_id=$1 AND v.project_id=$2 AND v.run_id=$3
        AND v.conversation_id=$4 AND v.artifact_id=ANY($5::uuid[])
        AND NOT v.context_excluded AND v.memory_revision=u.memory_revision
        AND r.memory_revision=u.memory_revision AND NOT r.cancel_requested
        AND NOT p.context_excluded AND p.status='active' AND p.revision=$6
        ON CONFLICT DO NOTHING RETURNING artifact_id""",
        origin["user_id"],
        origin["project_id"],
        origin["run_id"],
        origin["conversation_id"],
        artifact_ids,
        payload.get("initiative_project_revision"),
    )
    if not added:
        return
    revision = await conn.fetchval(
        """UPDATE projects SET revision=revision+1,updated_at=clock_timestamp()
        WHERE user_id=$1 AND id=$2 RETURNING revision""",
        origin["user_id"],
        origin["project_id"],
    )
    await conn.execute(
        """INSERT INTO project_changes
        (id,user_id,project_id,revision,kind,source_key,conversation_id,run_id,patch)
        VALUES($1,$2,$3,$4,'attach_artifact',$5,$6,$7,$8)""",
        uuid4(),
        origin["user_id"],
        origin["project_id"],
        revision,
        f"initiative:delivered:{decision_id}",
        origin["conversation_id"],
        origin["run_id"],
        {"artifact_ids": sorted(str(row["artifact_id"]) for row in added)},
    )


async def validate_initiative_payload(conn, user_id, payload):
    """Use the caller's owner-scoped transaction when fencing a new outbox row."""
    return await _decision_valid(conn, user_id, payload)


async def invalidate_initiative_context(conn, user_id):
    """Call inside forget's owner/delivery-locked transaction; preserve original artifacts."""
    if not conn.is_in_transaction():
        raise RuntimeError("Initiative invalidation requires the forget transaction")
    await conn.execute(
        """UPDATE schedules SET state='cancelled',revision=revision+1,instruction='',fixed_text=''
        WHERE user_id=$1 AND initiative_id IS NOT NULL""",
        user_id,
    )
    await conn.execute(
        """UPDATE initiative_policies SET status='paused',revision=revision+1,purpose='',
        instruction='',last_fingerprint=NULL,updated_at=clock_timestamp() WHERE user_id=$1""",
        user_id,
    )
    await conn.execute("UPDATE initiative_operations SET result='{}' WHERE user_id=$1", user_id)


class InitiativeStoreMixin:
    def connection(
        self, user_id: int | None = None
    ) -> AbstractAsyncContextManager[asyncpg.Connection]:
        raise NotImplementedError

    def user_lock(self, user_id: int, *, purpose="work") -> AbstractAsyncContextManager:
        raise NotImplementedError

    async def configure_initiative(self, user_id, project_id, args, source_key, run):
        project_id = UUID(str(project_id))
        source_key = _text(source_key, "source_key", 500)
        if not isinstance(args, dict) or set(args) - {
            "purpose",
            "instruction",
            "allowed_tools",
            "due_at",
            "interval_seconds",
        }:
            raise ValueError("Unknown initiative fields")
        purpose = _text(args.get("purpose"), "purpose", 500)
        instruction = _text(args.get("instruction"), "instruction", 4000)
        allowed = args.get("allowed_tools")
        if (
            not isinstance(allowed, list)
            or not allowed
            or any(not isinstance(x, str) or x not in INITIATIVE_ALLOWED_TOOLS for x in allowed)
        ):
            raise ValueError(
                "Initiative allowed_tools must be explicit approved read/search/file tools"
            )
        interval = _integer(
            args.get("interval_seconds", 86400), "interval_seconds", MIN_INTERVAL, MAX_INTERVAL
        )
        due = args.get("due_at")
        if isinstance(due, str):
            due = datetime.fromisoformat(due)
        if not isinstance(due, datetime) or due.tzinfo is None or due.utcoffset() is None:
            raise ValueError("Initiative due_at requires a timezone")
        async with self.connection(user_id) as conn:
            await _owner_lock(conn, user_id)
            origin, run_id = await _run(conn, user_id, run)
            prior = await _receipt(conn, user_id, source_key, "configure")
            if prior:
                row = await conn.fetchrow(_ROW, user_id, prior["initiative_id"])
                if row["project_id"] != project_id:
                    raise ValueError("Initiative source_key belongs to another project")
                return _public(row)
            prefs = await conn.fetchval("SELECT preferences FROM users WHERE user_id=$1", user_id)
            if prefs.get("proactivity") is not True:
                raise ValueError("Proactivity is disabled")
            project = await conn.fetchrow(
                """SELECT p.* FROM projects p JOIN project_conversations pc
                ON pc.user_id=p.user_id AND pc.project_id=p.id
                WHERE p.user_id=$1 AND p.id=$2 AND pc.conversation_id=$3 FOR SHARE OF p""",
                user_id,
                project_id,
                origin,
            )
            if not project or project["status"] != "active" or project["context_excluded"]:
                raise ValueError("Active project and its bound source conversation are required")
            policy_id, schedule_id = uuid4(), uuid4()
            conversation = await conn.fetchrow(
                "SELECT * FROM conversations WHERE user_id=$1 AND id=$2", user_id, origin
            )
            # Same durable schedules as Store.schedule, but atomically paired with the policy.
            await conn.execute(
                """INSERT INTO schedules(id,user_id,conversation_id,chat_id,thread_id,due_at,
                timezone,interval_seconds,instruction,fixed_text,dynamic,proactive,source_key,initiative_id)
                VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,'',true,true,$10,$11)""",
                schedule_id,
                user_id,
                origin,
                conversation["chat_id"],
                conversation["thread_id"],
                due,
                prefs.get("timezone", "UTC"),
                interval,
                instruction,
                f"initiative:{user_id}:{source_key}",
                policy_id,
            )
            await conn.execute(
                """INSERT INTO initiative_policies(id,user_id,project_id,schedule_id,conversation_id,
                purpose,instruction,allowed_tools,context_reset_revision,source_key)
                VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)""",
                policy_id,
                user_id,
                project_id,
                schedule_id,
                origin,
                purpose,
                instruction,
                sorted(set(allowed)),
                project["context_reset_revision"],
                source_key,
            )
            await _save(
                conn,
                user_id,
                policy_id,
                "configure",
                source_key,
                origin,
                run_id,
                {"id": str(policy_id)},
            )
            return _public(await conn.fetchrow(_ROW, user_id, policy_id))

    async def get_initiative_for_schedule(self, user_id, schedule_id):
        async with self.connection(user_id) as conn:
            marker = await conn.fetchval(
                "SELECT initiative_id FROM schedules WHERE user_id=$1 AND id=$2",
                user_id,
                UUID(str(schedule_id)),
            )
            if marker is None:
                return None
            row = await conn.fetchrow(_ROW, user_id, marker)
            if row is None:
                return {"id": str(marker), "available": False, "reason": "policy_unavailable"}
            return _public(row)

    async def list_initiatives(self, user_id, project_id=None, *, include_paused=True, limit=30):
        limit = _integer(limit, "limit", 1, 100)
        if not isinstance(include_paused, bool):
            raise ValueError("include_paused must be a boolean")
        project_id = UUID(str(project_id)) if project_id is not None else None
        sql = _ROW.replace("i.id=$2", "($2::uuid IS NULL OR i.project_id=$2)")
        async with self.connection(user_id) as conn:
            rows = await conn.fetch(
                sql + " AND ($3 OR i.status='active') ORDER BY i.created_at DESC,i.id LIMIT $4",
                user_id,
                project_id,
                include_paused,
                limit,
            )
        return [_public(row) for row in rows]

    async def initiative_feedback(self, user_id, id, action, source_key, run):
        policy_id, source_key = UUID(str(id)), _text(source_key, "source_key", 500)
        if action not in {"stop", "pause", "less", "more", "resume"}:
            raise ValueError("Unknown initiative feedback")
        async with self.user_lock(user_id, purpose="delivery"), self.connection(user_id) as conn:
            await _owner_lock(conn, user_id)
            origin, run_id = await _run(conn, user_id, run)
            row = await conn.fetchrow(_ROW, user_id, policy_id)
            if row is None:
                raise ValueError("Initiative is unavailable")
            prior = await _receipt(conn, user_id, source_key, "feedback", policy_id)
            if prior:
                if prior["result"].get("action") != action:
                    raise ValueError("Initiative source_key belongs to another feedback")
                return {**_public(row), "action": action}
            if action not in {"stop", "pause"}:
                reason = _inactive_reason(row)
                if reason not in {None, "policy_paused", "schedule_inactive"}:
                    raise ValueError(f"Initiative cannot resume: {reason}")
            interval = row["interval_seconds"]
            if action == "less":
                interval = min(MAX_INTERVAL, interval * 2)
            elif action == "more":
                interval = max(MIN_INTERVAL, interval // 2)
            active = action == "resume" or (
                action in {"less", "more"} and row["status"] == "active"
            )
            await conn.execute(
                "UPDATE initiative_policies SET status=$3,revision=revision+1,updated_at=clock_timestamp() WHERE user_id=$1 AND id=$2",
                user_id,
                policy_id,
                "active" if active else "paused",
            )
            await conn.execute(
                """UPDATE schedules SET state=$3,revision=revision+1,interval_seconds=$4,
                due_at=clock_timestamp()+$4::bigint*interval '1 second' WHERE user_id=$1 AND id=$2""",
                user_id,
                row["schedule_id"],
                "active" if active else "cancelled",
                interval,
            )
            await _save(
                conn, user_id, policy_id, "feedback", source_key, origin, run_id, {"action": action}
            )
            return {**_public(await conn.fetchrow(_ROW, user_id, policy_id)), "action": action}

    async def decide_initiative(
        self,
        user_id,
        id,
        revision,
        summary,
        evidence_key,
        send,
        source_key,
        run,
        *,
        expected_project_revision,
    ):
        policy_id, source_key = UUID(str(id)), _text(source_key, "source_key", 500)
        revision = _integer(revision, "revision", 1, 2**63 - 1)
        expected_project_revision = _integer(
            expected_project_revision, "expected_project_revision", 1, 2**63 - 1
        )
        summary = _text(summary, "summary / decision reason", 1500)
        evidence_key = _text(evidence_key, "evidence_key", 4000)
        if not isinstance(send, bool):
            raise ValueError("send must be a boolean")
        fingerprint = hashlib.sha256(" ".join(evidence_key.casefold().split()).encode()).hexdigest()
        async with self.connection(user_id) as conn:
            await _owner_lock(conn, user_id)
            origin, run_id = await _run(conn, user_id, run)
            row = await conn.fetchrow(_ROW, user_id, policy_id)
            if row is None:
                raise ValueError("Initiative is unavailable")
            event = await conn.fetchrow(
                "SELECT e.kind,e.payload FROM events e JOIN runs r ON r.event_id=e.id WHERE r.user_id=$1 AND r.id=$2",
                user_id,
                run_id,
            )
            if (
                not event
                or event["kind"] != "timer"
                or event["payload"].get("schedule_id") != str(row["schedule_id"])
                or origin != row["conversation_id"]
            ):
                raise ValueError("Initiative decision requires its own scheduled run")
            prior = await _receipt(conn, user_id, source_key, "decide", policy_id)
            if prior:
                result = prior["result"]
                if result.get("should_send") and not await _decision_valid(
                    conn, user_id, result.get("delivery_guard")
                ):
                    return {"should_send": False, "reason": "decision_no_longer_valid"}
                return result or {"should_send": False, "reason": "context_cleared"}
            reason = _inactive_reason(row)
            if not reason and (
                row["revision"] != revision
                or row["project_revision"] != expected_project_revision
                or event["payload"].get("revision") != row["schedule_revision"]
            ):
                reason = "stale_revision"
            if not reason and row["last_fingerprint"] == fingerprint:
                reason = "already_delivered"
            if not reason and await conn.fetchval(
                """SELECT 1 FROM outbox WHERE user_id=$1 AND state IN ('pending','sending')
                AND payload->>'initiative_id'=$2 AND payload->>'initiative_fingerprint'=$3
                AND payload->>'initiative_revision'=$4 AND payload->>'initiative_project_revision'=$5 LIMIT 1""",
                user_id,
                str(policy_id),
                fingerprint,
                str(revision),
                str(expected_project_revision),
            ):
                reason = "delivery_pending"
            operation_id = uuid4()
            result = {"should_send": send and reason is None, "reason": reason or summary}
            if result["should_send"]:
                result["delivery_guard"] = {
                    "initiative_id": str(policy_id),
                    "initiative_decision_id": str(operation_id),
                    "initiative_revision": revision,
                    "initiative_project_revision": expected_project_revision,
                    "initiative_fingerprint": fingerprint,
                    "schedule_id": str(row["schedule_id"]),
                    "schedule_revision": row["schedule_revision"],
                }
            await _save(
                conn, user_id, policy_id, "decide", source_key, origin, run_id, result, operation_id
            )
            return result

    async def validate_initiative_delivery(self, user_id, payload):
        async with self.connection(user_id) as conn:
            return await _decision_valid(conn, user_id, payload)
