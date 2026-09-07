"""Immutable artifact lineage and a movable current-version pointer."""

from contextlib import AbstractAsyncContextManager
from uuid import uuid4

import asyncpg

from cronos.projects import _origin, _owner_lock, _project_row, _source_key, _uid


class ArtifactVersionConflict(ValueError):
    pass


async def _artifact(conn, user_id, artifact_id):
    if (
        await conn.fetchval(
            "SELECT id FROM artifacts WHERE user_id=$1 AND id=$2", user_id, artifact_id
        )
        is None
    ):
        raise ValueError("Artifact is unavailable")


async def _write_context(conn, user_id, run):
    await _owner_lock(conn, user_id)
    user = await conn.fetchrow(
        "SELECT memory_revision,content_reset_at FROM users WHERE user_id=$1", user_id
    )
    conversation_id = run_id = None
    if run is not None:
        if not isinstance(run, dict) or run.get("user_id") != user_id:
            raise ValueError("Artifact version run belongs to another owner")
        conversation_id, run_id = await _origin(
            conn, user_id, run_id=run.get("id"), run_fence=run.get("fence")
        )
        source = await conn.fetchrow(
            "SELECT memory_revision,created_at FROM runs WHERE user_id=$1 AND id=$2",
            user_id,
            run_id,
        )
        if source["memory_revision"] != user["memory_revision"] or (
            user["content_reset_at"] is not None
            and source["created_at"] <= user["content_reset_at"]
        ):
            raise ValueError("Artifact version run predates the context reset")
    return conversation_id, run_id, user["memory_revision"]


async def _version_row(conn, user_id, artifact_id):
    return await conn.fetchrow(
        "SELECT * FROM artifact_versions WHERE user_id=$1 AND artifact_id=$2", user_id, artifact_id
    )


async def _detail(conn, user_id, artifact_id):
    row = await _version_row(conn, user_id, artifact_id)
    if row is None:
        return None
    current = await conn.fetchval(
        "SELECT artifact_id FROM artifact_version_heads WHERE user_id=$1 AND family_id=$2",
        user_id,
        row["family_id"],
    )
    revision = await conn.fetchval("SELECT memory_revision FROM users WHERE user_id=$1", user_id)
    excluded = row["context_excluded"] or row["memory_revision"] != revision
    references = await conn.fetch(
        "SELECT reference_artifact_id FROM artifact_version_references WHERE user_id=$1 AND artifact_id=$2 ORDER BY ordinal",
        user_id,
        artifact_id,
    )
    return {
        "artifact_id": str(row["artifact_id"]),
        "family_id": str(row["family_id"]),
        "version": row["version"],
        "parent_artifact_id": str(row["parent_artifact_id"]) if row["parent_artifact_id"] else None,
        "reference_artifact_ids": [str(item["reference_artifact_id"]) for item in references],
        "change_summary": "" if excluded else row["change_summary"],
        "project_id": str(row["project_id"]) if row["project_id"] else None,
        "current_artifact_id": str(current) if current else None,
        "is_current": current == artifact_id,
        "context_excluded": bool(excluded),
        "created_at": row["created_at"].isoformat(),
    }


async def _receipt(conn, user_id, source_key, kind, artifact_id):
    row = await conn.fetchrow(
        "SELECT kind,artifact_id FROM artifact_version_operations WHERE user_id=$1 AND source_key=$2",
        user_id,
        source_key,
    )
    if row and (row["kind"] != kind or row["artifact_id"] != artifact_id):
        raise ArtifactVersionConflict("Version source_key already belongs to another operation")
    return row is not None


async def _save_receipt(conn, user_id, source_key, kind, artifact_id, family_id):
    await conn.execute(
        "INSERT INTO artifact_version_operations(user_id,source_key,kind,artifact_id,family_id) VALUES($1,$2,$3,$4,$5)",
        user_id,
        source_key,
        kind,
        artifact_id,
        family_id,
    )


async def _set_head(conn, user_id, family_id, artifact_id):
    await conn.execute(
        """INSERT INTO artifact_version_heads(user_id,family_id,artifact_id) VALUES($1,$2,$3)
    ON CONFLICT(user_id,family_id) DO UPDATE SET artifact_id=EXCLUDED.artifact_id""",
        user_id,
        family_id,
        artifact_id,
    )


async def _insert_version(
    conn, user_id, artifact_id, family_id, version, parent, summary, project_id, context
):
    conversation_id, run_id, memory_revision = context
    await conn.execute(
        """INSERT INTO artifact_versions
    (user_id,artifact_id,family_id,version,parent_artifact_id,change_summary,project_id,conversation_id,run_id,memory_revision)
    VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)""",
        user_id,
        artifact_id,
        family_id,
        version,
        parent,
        summary,
        project_id,
        conversation_id,
        run_id,
        memory_revision,
    )


async def _new_family(conn, user_id, root_id, summary, project_id, context):
    family_id = uuid4()
    await conn.execute(
        "INSERT INTO artifact_version_families(user_id,id,root_artifact_id) VALUES($1,$2,$3)",
        user_id,
        family_id,
        root_id,
    )
    await _insert_version(conn, user_id, root_id, family_id, 1, None, summary, project_id, context)
    await _set_head(conn, user_id, family_id, root_id)
    return family_id


class VersionsStoreMixin:
    def connection(
        self, user_id: int | None = None
    ) -> AbstractAsyncContextManager[asyncpg.Connection]:
        raise NotImplementedError

    async def version_operation(self, user_id, source_key) -> dict | None:
        """Recover a prepared artifact before repeating paid generation."""
        async with self.connection(user_id) as conn:
            row = await conn.fetchrow(
                "SELECT artifact_id,kind FROM artifact_version_operations WHERE user_id=$1 AND source_key=$2",
                user_id,
                source_key,
            )
            if row is None:
                return None
            if row["kind"] != "register":
                raise ArtifactVersionConflict("Operation belongs to another version action")
            return await _detail(conn, user_id, row["artifact_id"])

    async def register_artifact_version(
        self,
        user_id,
        artifact_id,
        *,
        parent_artifact_id=None,
        reference_artifact_ids=None,
        change_summary="",
        project_id=None,
        source_key,
        run=None,
    ) -> dict:
        """Register once; a late retry never changes the family's current pointer."""
        artifact_id, source_key = _uid(artifact_id), _source_key(source_key)
        parent = _uid(parent_artifact_id) if parent_artifact_id is not None else None
        project_id = _uid(project_id) if project_id is not None else None
        if reference_artifact_ids is not None and not isinstance(reference_artifact_ids, list):
            raise ValueError("reference_artifact_ids must be a list")
        refs = list(dict.fromkeys(_uid(item) for item in (reference_artifact_ids or [])))
        if parent == artifact_id or artifact_id in refs:
            raise ValueError("An artifact cannot be its own parent or reference")
        if not isinstance(change_summary, str):
            raise ValueError("change_summary must be text")
        async with self.connection(user_id) as conn:
            context = await _write_context(conn, user_id, run)
            await _artifact(conn, user_id, artifact_id)
            if await _receipt(conn, user_id, source_key, "register", artifact_id):
                return await _detail(conn, user_id, artifact_id)
            # Validate every dependency before bootstrapping an uploaded parent.
            for dependency in [*refs, *([parent] if parent else [])]:
                await _artifact(conn, user_id, dependency)
            if project_id:
                project = await _project_row(conn, user_id, project_id)
                if project.get("context_excluded", False):
                    raise ValueError("Project requires fresh context before a new version")
            existing = await _version_row(conn, user_id, artifact_id)
            if existing:
                original_refs = await conn.fetch(
                    "SELECT reference_artifact_id FROM artifact_version_references WHERE user_id=$1 AND artifact_id=$2 ORDER BY ordinal",
                    user_id,
                    artifact_id,
                )
                if (
                    existing["parent_artifact_id"],
                    existing["project_id"],
                    existing["change_summary"],
                    [row["reference_artifact_id"] for row in original_refs],
                ) != (parent, project_id, change_summary, refs):
                    raise ArtifactVersionConflict("Artifact version metadata is immutable")
                await _save_receipt(
                    conn, user_id, source_key, "register", artifact_id, existing["family_id"]
                )
                return await _detail(conn, user_id, artifact_id)
            if parent:
                parent_version = await _version_row(conn, user_id, parent)
                family_id = (
                    parent_version["family_id"]
                    if parent_version
                    else await _new_family(conn, user_id, parent, "", None, context)
                )
                version = await conn.fetchval(
                    "UPDATE artifact_version_families SET next_version=next_version+1 WHERE user_id=$1 AND id=$2 RETURNING next_version-1",
                    user_id,
                    family_id,
                )
                await _insert_version(
                    conn,
                    user_id,
                    artifact_id,
                    family_id,
                    version,
                    parent,
                    change_summary,
                    project_id,
                    context,
                )
            else:
                family_id = await _new_family(
                    conn, user_id, artifact_id, change_summary, project_id, context
                )
            for ordinal, reference in enumerate(refs):
                await conn.execute(
                    "INSERT INTO artifact_version_references(user_id,artifact_id,reference_artifact_id,ordinal) VALUES($1,$2,$3,$4)",
                    user_id,
                    artifact_id,
                    reference,
                    ordinal,
                )
            await _set_head(conn, user_id, family_id, artifact_id)
            await _save_receipt(conn, user_id, source_key, "register", artifact_id, family_id)
            return await _detail(conn, user_id, artifact_id)

    async def get_artifact_version(self, user_id, artifact_id) -> dict | None:
        async with self.connection(user_id) as conn:
            return await _detail(conn, user_id, _uid(artifact_id))

    async def artifact_versions(self, user_id, artifact_id, limit=20, offset=0) -> list[dict]:
        if (
            isinstance(limit, bool)
            or not isinstance(limit, int)
            or not 1 <= limit <= 100
            or isinstance(offset, bool)
            or not isinstance(offset, int)
            or offset < 0
        ):
            raise ValueError("Invalid version pagination")
        async with self.connection(user_id) as conn:
            origin = await _version_row(conn, user_id, _uid(artifact_id))
            if origin is None:
                return []
            rows = await conn.fetch(
                "SELECT artifact_id FROM artifact_versions WHERE user_id=$1 AND family_id=$2 ORDER BY version LIMIT $3 OFFSET $4",
                user_id,
                origin["family_id"],
                limit,
                offset,
            )
            return [await _detail(conn, user_id, row["artifact_id"]) for row in rows]

    async def restore_artifact_version(self, user_id, artifact_id, *, source_key, run) -> dict:
        """Point to an existing version without generating/copying a file."""
        artifact_id, source_key = _uid(artifact_id), _source_key(source_key)
        if run is None:
            raise ValueError("Restoring a version requires an active run")
        async with self.connection(user_id) as conn:
            await _write_context(conn, user_id, run)
            row = await _version_row(conn, user_id, artifact_id)
            if row is None:
                raise ValueError("Artifact version is unavailable")
            if not await _receipt(conn, user_id, source_key, "restore", artifact_id):
                await _set_head(conn, user_id, row["family_id"], artifact_id)
                await _save_receipt(
                    conn, user_id, source_key, "restore", artifact_id, row["family_id"]
                )
            return await _detail(conn, user_id, artifact_id)
