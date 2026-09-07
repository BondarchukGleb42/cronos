"""Owner-scoped project persistence, mixed into Store without importing it."""

from contextlib import AbstractAsyncContextManager
from functools import wraps
from uuid import UUID, uuid4

import asyncpg

PROJECT_STATUSES = {"active", "paused", "completed"}
STATE_LISTS = {"constraints", "decisions", "open_questions"}
STATE_FIELDS = STATE_LISTS | {"next_step", "summary"}
PROJECT_FIELDS = {"name", "goal", "status", "state"}


def serialize_project_delivery(function):
    """A project change cannot acknowledge while an older initiative is sending."""

    @wraps(function)
    async def wrapped(self, user_id, *args, **kwargs):
        async with self.user_lock(user_id, "delivery"):
            return await function(self, user_id, *args, **kwargs)

    return wrapped


class ProjectRevisionConflict(ValueError):
    """The caller must reload the project before applying a stale edit."""


def _uid(value) -> UUID:
    return value if isinstance(value, UUID) else UUID(str(value))


def _source_key(value) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("Project source_key is required")
    return value


def _patch(args: dict, *, creating: bool = False) -> dict:
    allowed = PROJECT_FIELDS | {"revision", "conversation_id", "run_id", "run_fence"}
    if not isinstance(args, dict) or set(args) - allowed:
        raise ValueError("Unknown project fields")
    result = {key: value for key, value in args.items() if key in PROJECT_FIELDS}
    if creating and "name" not in result:
        raise ValueError("Project name is required")
    for key in ("name", "goal"):
        if key in result:
            if not isinstance(result[key], str) or (key == "name" and not result[key].strip()):
                raise ValueError(f"Invalid project {key}")
            result[key] = result[key].strip()
    if "status" in result and (
        not isinstance(result["status"], str) or result["status"] not in PROJECT_STATUSES
    ):
        raise ValueError("Unknown project status")
    if "state" in result:
        state = result["state"]
        if not isinstance(state, dict) or set(state) - STATE_FIELDS:
            raise ValueError("Unknown project state fields")
        for key, value in state.items():
            if key in STATE_LISTS:
                if not isinstance(value, list) or any(not isinstance(x, str) for x in value):
                    raise ValueError(f"Project {key} must be a list of strings")
            elif not isinstance(value, str):
                raise ValueError(f"Project {key} must be text")
    return result


async def _owner_lock(conn, user_id: int):
    # Serialize source-key replay and partial updates with privacy's account-row lock.
    if (
        await conn.fetchval("SELECT user_id FROM users WHERE user_id=$1 FOR UPDATE", user_id)
        is None
    ):
        raise ValueError("Project owner does not exist")
    if await conn.fetchval(
        "SELECT 1 FROM privacy_requests WHERE user_id=$1 AND state='erasing'", user_id
    ):
        raise ValueError("Data erasure is in progress")


async def _conversation(conn, user_id: int, conversation_id):
    conversation_id = _uid(conversation_id)
    if not await conn.fetchval(
        "SELECT id FROM conversations WHERE user_id=$1 AND id=$2", user_id, conversation_id
    ):
        raise ValueError("Project conversation is unavailable")
    return conversation_id


async def _origin(conn, user_id: int, conversation_id=None, run_id=None, run_fence=None):
    if run_id is not None:
        run_id = _uid(run_id)
        run = await conn.fetchrow(
            """SELECT conversation_id,status,cancel_requested,fence FROM runs
            WHERE user_id=$1 AND id=$2 FOR SHARE""",
            user_id,
            run_id,
        )
        if run is None or (
            conversation_id is not None and _uid(conversation_id) != run["conversation_id"]
        ):
            raise ValueError("Project source run is unavailable or belongs to another conversation")
        if (
            not isinstance(run_fence, int)
            or isinstance(run_fence, bool)
            or run["status"] != "running"
            or run["cancel_requested"]
            or run["fence"] != run_fence
        ):
            raise ValueError("Project source run is no longer active")
        conversation_id = run["conversation_id"]
    elif run_fence is not None:
        raise ValueError("Project source fence requires a run")
    if conversation_id is None:
        raise ValueError("Project source conversation is required")
    return await _conversation(conn, user_id, conversation_id), run_id


async def _project_row(conn, user_id: int, project_id):
    row = await conn.fetchrow(
        "SELECT * FROM projects WHERE user_id=$1 AND id=$2", user_id, _uid(project_id)
    )
    if row is None:
        raise ValueError("Project is unavailable")
    return row


async def _replay(conn, user_id, source_key, kind, project_id=None):
    row = await conn.fetchrow(
        "SELECT project_id,kind FROM project_changes WHERE user_id=$1 AND source_key=$2",
        user_id,
        _source_key(source_key),
    )
    if row and (row["kind"] != kind or (project_id and row["project_id"] != _uid(project_id))):
        raise ValueError("Project source_key was already used for another operation")
    return row


async def _change(conn, user_id, project_id, revision, kind, source_key, origin, run_id, patch):
    await conn.execute(
        """INSERT INTO project_changes
        (id,user_id,project_id,revision,kind,source_key,conversation_id,run_id,patch)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)""",
        uuid4(),
        user_id,
        project_id,
        revision,
        kind,
        source_key,
        origin,
        run_id,
        patch,
    )


def _public(row) -> dict:
    if row.get("context_excluded", False):
        return {
            "id": str(row["id"]),
            "revision": row["revision"],
            "status": row["status"],
            "needs_context": True,
        }
    return {
        **dict(row),
        "id": str(row["id"]),
        "created_at": row["created_at"].isoformat(),
        "updated_at": row["updated_at"].isoformat(),
    }


async def _detail(conn, user_id, project_id) -> dict:
    result = _public(await _project_row(conn, user_id, project_id))
    if result.get("needs_context"):
        return result
    project_id = _uid(project_id)
    result["conversation_ids"] = [
        str(row["conversation_id"])
        for row in await conn.fetch(
            """SELECT conversation_id FROM project_conversations
            WHERE user_id=$1 AND project_id=$2 ORDER BY created_at,conversation_id""",
            user_id,
            project_id,
        )
    ]
    result["artifact_ids"] = [
        str(row["artifact_id"])
        for row in await conn.fetch(
            """SELECT artifact_id FROM project_artifacts
            WHERE user_id=$1 AND project_id=$2 ORDER BY created_at,artifact_id""",
            user_id,
            project_id,
        )
    ]
    # History is durable; only the latest 20 changes are included in model context.
    changes = await conn.fetch(
        """SELECT revision,kind,conversation_id,run_id,patch,created_at
        FROM project_changes WHERE user_id=$1 AND project_id=$2 AND revision>$3
        ORDER BY revision DESC,created_at DESC LIMIT 21""",
        user_id,
        project_id,
        result.get("context_reset_revision", 0),
    )
    result["history_has_more"] = len(changes) > 20
    result["history"] = [
        {
            **dict(row),
            "conversation_id": str(row["conversation_id"]) if row["conversation_id"] else None,
            "run_id": str(row["run_id"]) if row["run_id"] else None,
            "created_at": row["created_at"].isoformat(),
        }
        for row in reversed(changes[:20])
    ]
    return result


class ProjectsStoreMixin:
    def connection(
        self, user_id: int | None = None
    ) -> AbstractAsyncContextManager[asyncpg.Connection]:
        """Implemented by Store; every use is an owner-scoped transaction."""
        raise NotImplementedError

    async def create_project(
        self, user_id: int, conversation_id, args: dict, source_key: str
    ) -> dict:
        patch = _patch(args, creating=True)
        source_key = _source_key(source_key)
        async with self.connection(user_id) as conn:
            await _owner_lock(conn, user_id)
            origin, run_id = await _origin(
                conn, user_id, conversation_id, args.get("run_id"), args.get("run_fence")
            )
            existing = await _replay(conn, user_id, source_key, "create")
            if existing:
                return await _detail(conn, user_id, existing["project_id"])
            if await conn.fetchval(
                "SELECT 1 FROM project_conversations WHERE user_id=$1 AND conversation_id=$2",
                user_id,
                origin,
            ):
                raise ValueError("Conversation is already attached to a project")
            state = {
                **{key: [] for key in STATE_LISTS},
                "next_step": "",
                "summary": "",
                **patch.get("state", {}),
            }
            project_id = uuid4()
            await conn.execute(
                """INSERT INTO projects(id,user_id,name,goal,status,state)
                VALUES($1,$2,$3,$4,$5,$6)""",
                project_id,
                user_id,
                patch["name"],
                patch.get("goal", ""),
                patch.get("status", "active"),
                state,
            )
            await conn.execute(
                "INSERT INTO project_conversations(user_id,project_id,conversation_id) VALUES($1,$2,$3)",
                user_id,
                project_id,
                origin,
            )
            await _change(conn, user_id, project_id, 1, "create", source_key, origin, run_id, patch)
            return await _detail(conn, user_id, project_id)

    async def list_projects(
        self, user_id: int, include_archived: bool = False, limit: int = 50, offset: int = 0
    ) -> list[dict]:
        if (
            not isinstance(limit, int)
            or isinstance(limit, bool)
            or not 1 <= limit <= 100
            or not isinstance(offset, int)
            or isinstance(offset, bool)
            or offset < 0
        ):
            raise ValueError("Project listing requires limit 1..100 and a nonnegative offset")
        async with self.connection(user_id) as conn:
            return [
                _public(row)
                for row in await conn.fetch(
                    """SELECT * FROM projects WHERE user_id=$1 AND ($2 OR status<>'completed')
                    ORDER BY updated_at DESC,id LIMIT $3 OFFSET $4""",
                    user_id,
                    include_archived,
                    limit,
                    offset,
                )
            ]

    async def get_project(self, user_id: int, project_id=None, conversation_id=None) -> dict | None:
        if project_id is None and conversation_id is None:
            raise ValueError("A project or conversation id is required")
        async with self.connection(user_id) as conn:
            found = await conn.fetchval(
                """SELECT p.id FROM projects p WHERE p.user_id=$1
                AND ($2::uuid IS NULL OR p.id=$2)
                AND ($3::uuid IS NULL OR EXISTS(
                    SELECT 1 FROM project_conversations pc WHERE pc.user_id=$1
                    AND pc.project_id=p.id AND pc.conversation_id=$3))""",
                user_id,
                _uid(project_id) if project_id is not None else None,
                _uid(conversation_id) if conversation_id is not None else None,
            )
            return await _detail(conn, user_id, found) if found else None

    @serialize_project_delivery
    async def update_project(self, user_id: int, project_id, args: dict, source_key: str) -> dict:
        patch = _patch(args)
        source_key = _source_key(source_key)
        revision = args.get("revision")
        if not isinstance(revision, int) or isinstance(revision, bool) or revision < 1:
            raise ValueError("Current project revision is required")
        if not patch:
            raise ValueError("Project update has no changed fields")
        project_id = _uid(project_id)
        async with self.connection(user_id) as conn:
            await _owner_lock(conn, user_id)
            origin, run_id = await _origin(
                conn,
                user_id,
                args.get("conversation_id"),
                args.get("run_id"),
                args.get("run_fence"),
            )
            current = await _project_row(conn, user_id, project_id)
            if await _replay(conn, user_id, source_key, "update", project_id):
                return await _detail(conn, user_id, project_id)
            if current["revision"] != revision:
                raise ProjectRevisionConflict("Project changed; reload its current revision")
            excluded = current.get("context_excluded", False)
            if excluded and not {"name", "goal"} <= patch.keys():
                raise ValueError(
                    "Project context was cleared; provide a new explicit name and goal"
                )
            base_state = (
                {**{key: [] for key in STATE_LISTS}, "next_step": "", "summary": ""}
                if excluded
                else current["state"]
            )
            updated = await conn.fetchval(
                """UPDATE projects SET name=$3,goal=$4,status=$5,state=$6,
                revision=revision+1,updated_at=clock_timestamp(),context_excluded=false
                WHERE user_id=$1 AND id=$2 AND revision=$7 RETURNING revision""",
                user_id,
                project_id,
                patch.get("name", current["name"]),
                patch.get("goal", current["goal"]),
                patch.get("status", "active" if excluded else current["status"]),
                {**base_state, **patch.get("state", {})},
                revision,
            )
            if updated is None:
                raise ProjectRevisionConflict("Project changed; reload its current revision")
            await _change(
                conn, user_id, project_id, revision + 1, "update", source_key, origin, run_id, patch
            )
            return await _detail(conn, user_id, project_id)

    @serialize_project_delivery
    async def attach_project(
        self,
        user_id: int,
        project_id,
        conversation_id,
        *,
        source_key: str | None = None,
        run_id=None,
        run_fence=None,
    ) -> dict:
        project_id = _uid(project_id)
        source_key = _source_key(source_key) if source_key is not None else None
        async with self.connection(user_id) as conn:
            await _owner_lock(conn, user_id)
            current = await _project_row(conn, user_id, project_id)
            target = await _conversation(conn, user_id, conversation_id)
            origin, run_id = await _origin(
                conn, user_id, None if run_id else target, run_id, run_fence
            )
            if source_key and await _replay(
                conn, user_id, source_key, "attach_conversation", project_id
            ):
                return await _detail(conn, user_id, project_id)
            previous = await conn.fetchval(
                "SELECT project_id FROM project_conversations WHERE user_id=$1 AND conversation_id=$2",
                user_id,
                target,
            )
            if previous is not None:
                if previous != project_id:
                    raise ValueError("Conversation is already attached to another project")
                if source_key is None:
                    return await _detail(conn, user_id, project_id)
            else:
                await conn.execute(
                    "INSERT INTO project_conversations(user_id,project_id,conversation_id) VALUES($1,$2,$3)",
                    user_id,
                    project_id,
                    target,
                )
                await conn.execute(
                    """UPDATE projects SET revision=revision+1,updated_at=clock_timestamp()
                    WHERE user_id=$1 AND id=$2""",
                    user_id,
                    project_id,
                )
            await _change(
                conn,
                user_id,
                project_id,
                current["revision"] + (previous is None),
                "attach_conversation",
                source_key or f"project:conversation:{uuid4()}",
                origin,
                run_id,
                {"conversation_id": str(target)},
            )
            return await _detail(conn, user_id, project_id)

    @serialize_project_delivery
    async def detach_project(
        self,
        user_id: int,
        conversation_id,
        *,
        source_key: str | None = None,
        run_id=None,
        run_fence=None,
    ) -> dict | None:
        source_key = _source_key(source_key) if source_key is not None else None
        async with self.connection(user_id) as conn:
            await _owner_lock(conn, user_id)
            target = await _conversation(conn, user_id, conversation_id)
            origin, run_id = await _origin(
                conn, user_id, None if run_id else target, run_id, run_fence
            )
            replay = (
                await _replay(conn, user_id, source_key, "detach_conversation")
                if source_key
                else None
            )
            if replay:
                return (
                    await _detail(conn, user_id, replay["project_id"])
                    if replay["project_id"]
                    else None
                )
            project_id = await conn.fetchval(
                """DELETE FROM project_conversations WHERE user_id=$1 AND conversation_id=$2
                RETURNING project_id""",
                user_id,
                target,
            )
            revision = 0
            if project_id:
                revision = await conn.fetchval(
                    """UPDATE projects SET revision=revision+1,updated_at=clock_timestamp()
                    WHERE user_id=$1 AND id=$2 RETURNING revision""",
                    user_id,
                    project_id,
                )
            if project_id or source_key:
                await _change(
                    conn,
                    user_id,
                    project_id,
                    revision,
                    "detach_conversation",
                    source_key or f"project:detach:{uuid4()}",
                    origin,
                    run_id,
                    {"conversation_id": str(target)},
                )
            return await _detail(conn, user_id, project_id) if project_id else None

    @serialize_project_delivery
    async def attach_project_artifact(
        self,
        user_id: int,
        project_id,
        artifact_id,
        *,
        conversation_id=None,
        run_id=None,
        source_key: str | None = None,
        run_fence=None,
    ) -> dict:
        project_id, artifact_id = _uid(project_id), _uid(artifact_id)
        provided_source_key = source_key is not None
        async with self.connection(user_id) as conn:
            await _owner_lock(conn, user_id)
            current = await _project_row(conn, user_id, project_id)
            if not await conn.fetchval(
                "SELECT id FROM artifacts WHERE user_id=$1 AND id=$2", user_id, artifact_id
            ):
                raise ValueError("Project artifact is unavailable")
            origin = None
            if conversation_id is not None or run_id is not None:
                origin, run_id = await _origin(conn, user_id, conversation_id, run_id, run_fence)
            source_key = _source_key(source_key or f"project:artifact:{uuid4()}")
            if await _replay(conn, user_id, source_key, "attach_artifact", project_id):
                return await _detail(conn, user_id, project_id)
            added = await conn.fetchval(
                """INSERT INTO project_artifacts(user_id,project_id,artifact_id)
                VALUES($1,$2,$3) ON CONFLICT DO NOTHING RETURNING artifact_id""",
                user_id,
                project_id,
                artifact_id,
            )
            if not added and not provided_source_key:
                return await _detail(conn, user_id, project_id)
            if added:
                await conn.execute(
                    """UPDATE projects SET revision=revision+1,updated_at=clock_timestamp()
                    WHERE user_id=$1 AND id=$2""",
                    user_id,
                    project_id,
                )
            await _change(
                conn,
                user_id,
                project_id,
                current["revision"] + bool(added),
                "attach_artifact",
                source_key,
                origin,
                run_id,
                {"artifact_id": str(artifact_id)},
            )
            return await _detail(conn, user_id, project_id)
