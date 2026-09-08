"""Explicit, scoped memory operations for the personal agent."""

MEMORY_TOOL_NAMES = frozenset({"memory_write", "memory_update", "memory_list"})


def _optional_id(value):
    """Some models emit empty optional IDs instead of JSON null."""
    return None if isinstance(value, str) and not value.strip() else value


async def execute_memory_tool(store, name, args, op, run, conversation):
    owner = run["user_id"]
    if name == "memory_list":
        return await store.query_memories(
            owner,
            all_scopes=True,
            query=args.get("query"),
            include_inactive=args.get("include_inactive", False),
            limit=args.get("limit", 100),
            offset=args.get("offset", 0),
        )
    if name == "memory_update":
        return await store.revise_memory(
            owner,
            args["memory_id"],
            {
                **{key: value for key, value in args.items() if key != "memory_id"},
                "source": str(run["event_id"]),
            },
            op,
            run=run,
        )
    scope = args.get("scope", "global")
    project_id = _optional_id(args.get("project_id"))
    if scope == "project" and project_id is None:
        project = await store.get_project(owner, conversation_id=conversation["id"])
        if not project:
            raise ValueError("Сначала выбери проект для этого факта")
        project_id = project["id"]
    return await store.write_memory(
        owner,
        args["content"],
        args.get("category", "preference"),
        str(run["event_id"]),
        scope=scope,
        conversation_id=conversation["id"] if scope == "conversation" else None,
        project_id=project_id,
        expires_at=args.get("expires_at"),
        supersedes_id=_optional_id(args.get("supersedes_id")),
        source_key=op,
        run=run,
    )
