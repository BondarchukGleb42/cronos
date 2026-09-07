"""Artifact versions share the normal owner-checked delivery path."""

VERSION_TOOL_NAMES = frozenset({"artifact_versions", "artifact_restore"})


async def recover_version_link(store, version, op, run, conversation, *, proactive=False):
    """Finish a link if the process stopped after the immutable version receipt."""
    if version.get("project_id") and not proactive and not version.get("context_excluded"):
        await store.attach_project_artifact(
            run["user_id"],
            version["project_id"],
            version["artifact_id"],
            source_key=op + ":project-file",
            run_id=run["id"],
            run_fence=run["fence"],
            conversation_id=conversation["id"],
        )


async def version_project(store, owner, conversation, requested=None):
    project = (
        await store.get_project(owner, project_id=requested)
        if requested
        else await store.get_project(owner, conversation_id=conversation["id"])
    )
    if requested and (not project or project.get("needs_context")):
        raise ValueError("Проект недоступен или требует нового контекста")
    return project["id"] if project and not project.get("needs_context") else None


async def register_generated_version(
    store, artifact, args, op, run, conversation, *, refs=None, project_id=None, proactive=False
):
    version = await store.register_artifact_version(
        run["user_id"],
        artifact["id"],
        parent_artifact_id=args.get("parent_artifact_id")
        or (refs[0] if refs and len(refs) == 1 else None),
        reference_artifact_ids=refs or [],
        change_summary=args.get("change_summary", ""),
        project_id=project_id,
        source_key=op,
        run=run,
    )
    await recover_version_link(store, version, op, run, conversation, proactive=proactive)
    return version


async def execute_version_tool(store, name, args, op, run, conversation):
    user = run["user_id"]
    if name == "artifact_versions":
        return await store.artifact_versions(
            user, args["artifact_id"], limit=args.get("limit", 20), offset=args.get("offset", 0)
        )
    version = await store.restore_artifact_version(
        user, args["artifact_id"], source_key=op, run=run
    )
    artifact = await store.get_artifact(user, version["artifact_id"])
    if artifact["mime"].startswith("image/"):
        return {**version, "delivery": "prepared", "media_type": "image"}
    await store.enqueue_for_run(
        run, conversation, {"document_path": artifact["path"], "caption": artifact["filename"]}, op
    )
    return {**version, "filename": artifact["filename"], "delivery": "queued"}
