"""Explicit scoped memories with validity, revisions and content-free replay receipts."""

from contextlib import AbstractAsyncContextManager
from datetime import UTC, datetime
from uuid import uuid4

import asyncpg

from cronos.projects import _conversation, _origin, _owner_lock, _source_key, _uid

MEMORY_SCOPES = {"global", "conversation", "project"}


class MemoryRevisionConflict(ValueError):
    """Reload a changed fact before editing it."""


def _text(value, field: str, *, empty=False) -> str:
    if not isinstance(value, str) or (not empty and not value.strip()):
        raise ValueError(f"Memory {field} must be {'text' if empty else 'nonempty text'}")
    return value.strip()


def _expiry(value):
    if value is None:
        return None
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value)
        except ValueError:
            raise ValueError("Memory expiry must be an ISO timestamp with timezone") from None
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Memory expiry requires a timezone")
    return value.astimezone(UTC)


def _public(row) -> dict:
    result = dict(row)
    for field in ("id", "conversation_id", "project_id", "supersedes_id"):
        if result.get(field) is not None:
            result[field] = str(result[field])
    for field in ("created_at", "updated_at", "expires_at"):
        if result.get(field) is not None:
            result[field] = result[field].isoformat()
    return result


async def _scope(conn, user_id, scope, conversation_id=None, project_id=None):
    if not isinstance(scope, str) or scope not in MEMORY_SCOPES:
        raise ValueError("Memory scope must be global, conversation or project")
    if scope == "global":
        if conversation_id is not None or project_id is not None:
            raise ValueError("Global memory cannot have a scoped target")
    elif scope == "conversation":
        if conversation_id is None or project_id is not None:
            raise ValueError("Conversation memory requires only its conversation id")
        conversation_id = await _conversation(conn, user_id, conversation_id)
    else:
        if project_id is None or conversation_id is not None:
            raise ValueError("Project memory requires only its project id")
        project_id = _uid(project_id)
        if not await conn.fetchval(
            "SELECT id FROM projects WHERE user_id=$1 AND id=$2", user_id, project_id
        ):
            raise ValueError("Memory project is unavailable")
    return conversation_id, project_id


async def _run_origin(conn, user_id, run, conversation_id=None):
    if run is not None:
        if not isinstance(run, dict) or "id" not in run or "fence" not in run:
            raise ValueError("Memory source run requires id and fence")
        return await _origin(conn, user_id, None, run["id"], run["fence"])
    return conversation_id, None


async def _row(conn, user_id, memory_id):
    row = await conn.fetchrow(
        "SELECT * FROM memory WHERE user_id=$1 AND id=$2", user_id, _uid(memory_id)
    )
    if row is None:
        raise ValueError("Memory is unavailable")
    return row


async def _receipt(conn, user_id, source_key, kind, memory_id=None):
    previous = await conn.fetchrow(
        "SELECT memory_id,kind FROM memory_operations WHERE user_id=$1 AND source_key=$2",
        user_id,
        source_key,
    )
    if previous and (
        previous["kind"] != kind
        or (memory_id is not None and previous["memory_id"] != _uid(memory_id))
    ):
        raise ValueError("Memory source_key was already used for another operation")
    return previous


async def _save_receipt(conn, user_id, source_key, memory_id, kind, revision, origin, run_id):
    await conn.execute(
        """INSERT INTO memory_operations
        (user_id,source_key,memory_id,kind,revision,conversation_id,run_id)
        VALUES($1,$2,$3,$4,$5,$6,$7)""",
        user_id,
        source_key,
        memory_id,
        kind,
        revision,
        origin,
        run_id,
    )


class MemoryStoreMixin:
    def connection(
        self, user_id: int | None = None
    ) -> AbstractAsyncContextManager[asyncpg.Connection]:
        """Implemented by Store as an owner-scoped transaction."""
        raise NotImplementedError

    async def write_memory(
        self,
        user_id: int,
        content: str,
        category: str = "preference",
        source: str = "",
        *,
        scope: str = "global",
        conversation_id=None,
        project_id=None,
        expires_at=None,
        supersedes_id=None,
        source_key: str,
        run=None,
    ) -> dict:
        content, category = _text(content, "content"), _text(category, "category")
        source, source_key = _text(source, "source", empty=True), _source_key(source_key)
        expires_at = _expiry(expires_at)
        async with self.connection(user_id) as conn:
            await _owner_lock(conn, user_id)
            conversation_id, project_id = await _scope(
                conn, user_id, scope, conversation_id, project_id
            )
            origin, run_id = await _run_origin(conn, user_id, run, conversation_id)
            receipt = await _receipt(conn, user_id, source_key, "write")
            if receipt:
                return _public(await _row(conn, user_id, receipt["memory_id"]))
            previous = None
            if supersedes_id is not None:
                previous = await _row(conn, user_id, supersedes_id)
                if (
                    previous["scope"] != scope
                    or previous["conversation_id"] != conversation_id
                    or previous["project_id"] != project_id
                ):
                    raise ValueError("A memory can only replace another fact in the same scope")
                if previous["status"] == "superseded":
                    raise MemoryRevisionConflict(
                        "Memory already has a replacement; reload current facts"
                    )
            # An expired version should not prevent explicitly recording a fact again.
            await conn.execute(
                """UPDATE memory SET status='inactive',revision=revision+1,updated_at=clock_timestamp()
                WHERE user_id=$1 AND scope=$2 AND conversation_id IS NOT DISTINCT FROM $3::uuid
                AND project_id IS NOT DISTINCT FROM $4::uuid AND content=$5
                AND status='active' AND expires_at<=now()""",
                user_id,
                scope,
                conversation_id,
                project_id,
                content,
            )
            existing = await conn.fetchrow(
                """SELECT * FROM memory WHERE user_id=$1 AND scope=$2
                AND conversation_id IS NOT DISTINCT FROM $3::uuid
                AND project_id IS NOT DISTINCT FROM $4::uuid AND content=$5 AND status='active'""",
                user_id,
                scope,
                conversation_id,
                project_id,
                content,
            )
            if previous is not None and (existing is None or previous["id"] != existing["id"]):
                await conn.execute(
                    """UPDATE memory SET status='superseded',revision=revision+1,updated_at=clock_timestamp()
                    WHERE user_id=$1 AND id=$2""",
                    user_id,
                    previous["id"],
                )
            if existing is not None:
                # A fresh exact write is also a legitimate category/validity correction.
                expiry = expires_at if expires_at is not None else existing["expires_at"]
                predecessor = (
                    previous["id"]
                    if previous and previous["id"] != existing["id"]
                    else existing["supersedes_id"]
                )
                if (category, expiry, predecessor) != (
                    existing["category"],
                    existing["expires_at"],
                    existing["supersedes_id"],
                ):
                    await conn.execute(
                        """UPDATE memory SET category=$3,expires_at=$4,supersedes_id=$5,
                        revision=revision+1,updated_at=clock_timestamp() WHERE user_id=$1 AND id=$2""",
                        user_id,
                        existing["id"],
                        category,
                        expiry,
                        predecessor,
                    )
                memory_id = existing["id"]
            else:
                memory_id = uuid4()
                await conn.execute(
                    """INSERT INTO memory
                    (id,user_id,content,category,source,scope,conversation_id,project_id,expires_at,supersedes_id)
                    VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)""",
                    memory_id,
                    user_id,
                    content,
                    category,
                    source,
                    scope,
                    conversation_id,
                    project_id,
                    expires_at,
                    previous["id"] if previous else None,
                )
            current = await _row(conn, user_id, memory_id)
            await _save_receipt(
                conn, user_id, source_key, memory_id, "write", current["revision"], origin, run_id
            )
            return _public(current)

    async def query_memories(
        self,
        user_id: int,
        *,
        conversation_id=None,
        project_id=None,
        all_scopes: bool = False,
        query: str | None = None,
        include_inactive: bool = False,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict]:
        if (
            not isinstance(limit, int)
            or isinstance(limit, bool)
            or not 1 <= limit <= 100
            or not isinstance(offset, int)
            or isinstance(offset, bool)
            or offset < 0
        ):
            raise ValueError("Memory listing requires limit 1..100 and a nonnegative offset")
        query = _text(query, "query", empty=True) if query is not None else None
        async with self.connection(user_id) as conn:
            if conversation_id is not None:
                conversation_id = await _conversation(conn, user_id, conversation_id)
            if project_id is not None:
                _, project_id = await _scope(conn, user_id, "project", project_id=project_id)
            elif conversation_id is not None:
                project_id = await conn.fetchval(
                    """SELECT pc.project_id FROM project_conversations pc JOIN projects p
                    ON p.user_id=pc.user_id AND p.id=pc.project_id
                    WHERE pc.user_id=$1 AND pc.conversation_id=$2 AND NOT p.context_excluded""",
                    user_id,
                    conversation_id,
                )
            sql = """SELECT * FROM memory WHERE user_id=$1
                AND ($2 OR scope='global' OR (scope='conversation' AND conversation_id=$3)
                     OR (scope='project' AND project_id=$4))
                AND ($5 OR (status='active' AND (expires_at IS NULL OR expires_at>now())))
                ORDER BY updated_at DESC,id"""
            params = (user_id, all_scopes, conversation_id, project_id, include_inactive)
            if not query:
                rows = await conn.fetch(sql + " LIMIT $6 OFFSET $7", *params, limit, offset)
                return [_public(row) for row in rows]
            # PostgreSQL's C locale does not fold Cyrillic. Apply Unicode matching
            # while streaming scoped rows, and paginate matches rather than candidates.
            needle, results, skipped = query.casefold(), [], 0
            async for row in conn.cursor(sql, *params, prefetch=100):
                if needle not in row["content"].casefold():
                    continue
                if skipped < offset:
                    skipped += 1
                    continue
                results.append(_public(row))
                if len(results) == limit:
                    break
            return results

    async def revise_memory(
        self,
        user_id: int,
        memory_id,
        changes: dict,
        source_key: str,
        run=None,
    ) -> dict:
        allowed = {"revision", "content", "category", "expires_at", "status", "source"}
        if not isinstance(changes, dict) or set(changes) - allowed:
            raise ValueError("Unknown memory revision fields; scope cannot be changed")
        revision = changes.get("revision")
        if not isinstance(revision, int) or isinstance(revision, bool) or revision < 1:
            raise ValueError("Current memory revision is required")
        patch = {key: value for key, value in changes.items() if key != "revision"}
        if not patch:
            raise ValueError("Memory revision has no changed fields")
        for field in ("content", "category", "source"):
            if field in patch:
                patch[field] = _text(patch[field], field, empty=field == "source")
        if "status" in patch and patch["status"] not in ("active", "inactive"):
            raise ValueError("Memory status can only be active or inactive")
        if "expires_at" in patch:
            patch["expires_at"] = _expiry(patch["expires_at"])
        source_key, memory_id = _source_key(source_key), _uid(memory_id)
        async with self.connection(user_id) as conn:
            await _owner_lock(conn, user_id)
            current = await _row(conn, user_id, memory_id)
            origin, run_id = await _run_origin(conn, user_id, run, current["conversation_id"])
            if await _receipt(conn, user_id, source_key, "revise", memory_id):
                return _public(current)
            if current["revision"] != revision:
                raise MemoryRevisionConflict("Memory changed; reload its current revision")
            if current["status"] == "superseded":
                raise MemoryRevisionConflict("Revise the replacement instead of an obsolete memory")
            merged = {**current, **patch}
            duplicate = await conn.fetchval(
                """SELECT id FROM memory WHERE user_id=$1 AND id<>$2 AND scope=$3
                AND conversation_id IS NOT DISTINCT FROM $4::uuid
                AND project_id IS NOT DISTINCT FROM $5::uuid AND content=$6 AND status='active'""",
                user_id,
                memory_id,
                current["scope"],
                current["conversation_id"],
                current["project_id"],
                merged["content"],
            )
            if merged["status"] == "active" and duplicate is not None:
                raise ValueError("An active memory with this content already exists in the scope")
            updated = await conn.fetchrow(
                """UPDATE memory SET content=$3,category=$4,source=$5,expires_at=$6,status=$7,
                revision=revision+1,updated_at=clock_timestamp()
                WHERE user_id=$1 AND id=$2 AND revision=$8 RETURNING *""",
                user_id,
                memory_id,
                merged["content"],
                merged["category"],
                merged["source"],
                merged["expires_at"],
                merged["status"],
                revision,
            )
            if updated is None:
                raise MemoryRevisionConflict("Memory changed; reload its current revision")
            await _save_receipt(
                conn, user_id, source_key, memory_id, "revise", updated["revision"], origin, run_id
            )
            return _public(updated)
