"""Source-backed retrieval, separate from the account's implicit conversation context."""

LIBRARY_TOOL_NAMES = frozenset({"library_search", "library_read"})


def recall_query(prompt) -> str:
    if isinstance(prompt, str):
        return prompt[:1000]
    if isinstance(prompt, list):
        return "\n".join(
            part["text"]
            for part in prompt
            if isinstance(part, dict)
            and part.get("type") == "text"
            and isinstance(part.get("text"), str)
        )[:1000]
    return ""


async def execute_library_tool(store, name, args, run, conversation):
    owner = run["user_id"]
    if name == "library_read":
        return await store.library_read(
            owner,
            args["kind"],
            args["id"],
            offset=args.get("offset", 0),
            length=args.get("length", 4000),
        )
    if args.get("scope", "current_project") not in {"current_project", "account"}:
        raise ValueError("Область поиска должна быть current_project или account")
    project_id = args.get("project_id")
    if not project_id and args.get("scope", "current_project") == "current_project":
        current = await store.get_project(owner, conversation_id=conversation["id"])
        project_id = current["id"] if current and not current.get("needs_context") else None
    return await store.library_search(
        owner,
        args["query"],
        kinds=args.get("kinds"),
        project_id=project_id,
        limit=args.get("limit", 10),
        offset=args.get("offset", 0),
    )
