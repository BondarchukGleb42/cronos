"""Project tool dispatch; ownership always comes from the executing run."""

PROJECT_TOOL_NAMES = frozenset(
    {
        "project_create",
        "project_list",
        "project_get",
        "project_update",
        "project_link",
        "project_attach_file",
    }
)


async def execute_project_tool(store, name, args, op, run, conversation):
    user = run["user_id"]
    if name == "project_create":
        return await store.create_project(
            user,
            conversation["id"],
            {**args, "run_id": str(run["id"]), "run_fence": run["fence"]},
            op,
        )
    if name == "project_list":
        return await store.list_projects(
            user,
            include_archived=args.get("include_completed", False),
            limit=args.get("limit", 50),
            offset=args.get("offset", 0),
        )
    if name == "project_get":
        project = await store.get_project(
            user,
            project_id=args.get("project_id"),
            conversation_id=None if args.get("project_id") else conversation["id"],
        )
        return project or {
            "found": False,
            "message": "Проект не найден или чат пока не связан с проектом.",
        }
    if name == "project_update":
        changes = {key: value for key, value in args.items() if key != "project_id"}
        return await store.update_project(
            user,
            args["project_id"],
            {
                **changes,
                "conversation_id": str(conversation["id"]),
                "run_id": str(run["id"]),
                "run_fence": run["fence"],
            },
            op,
        )
    if name == "project_link":
        target = args.get("conversation_id") or conversation["id"]
        if args.get("action", "attach") == "detach":
            return await store.detach_project(
                user, target, source_key=op, run_id=run["id"], run_fence=run["fence"]
            )
        return await store.attach_project(
            user,
            args["project_id"],
            target,
            source_key=op,
            run_id=run["id"],
            run_fence=run["fence"],
        )
    if name == "project_attach_file":
        return await store.attach_project_artifact(
            user,
            args["project_id"],
            args["artifact_id"],
            source_key=op,
            run_id=run["id"],
            run_fence=run["fence"],
            conversation_id=conversation["id"],
        )
    raise ValueError("Неизвестная операция проекта")


async def project_context(store, user_id, conversation_id):
    current = await store.get_project(user_id, conversation_id=conversation_id)
    projects = await store.list_projects(user_id, limit=20)
    return {
        "current": current,
        "available": [
            {key: row[key] for key in ("id", "name", "status", "revision")} for row in projects
        ],
    }
