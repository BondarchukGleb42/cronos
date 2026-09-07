"""Personal workflow tools; the executing run supplies ownership and receipts."""

from cronos.workflows import WORKFLOW_TEMPLATES

WORKFLOW_TOOL_NAMES = frozenset(
    {"workflow_templates", "workflow_start", "workflow_get", "workflow_observe", "workflow_replan"}
)


async def workflow_context(store, user_id, project):
    if not project or project.get("needs_context"):
        return None
    current = await store.workflow_get(user_id, project["id"], limit=5)
    return current if current and not current.get("needs_context") else None


async def execute_workflow_tool(store, name, args, op, run, conversation):
    if name == "workflow_templates":
        kind = args.get("kind")
        if kind is None:
            return {
                "templates": [
                    {key: value[key] for key in ("kind", "title", "instructions")}
                    for value in WORKFLOW_TEMPLATES.values()
                ],
                "next_step": "Укажи kind, чтобы получить точные schemas и пример parameters/plan/observation.",
            }
        if kind not in WORKFLOW_TEMPLATES:
            raise ValueError("Неизвестный шаблон личного плана")
        return WORKFLOW_TEMPLATES[kind]
    owner = run["user_id"]
    requested = args.get("project_id")
    project = await store.get_project(
        owner,
        project_id=requested,
        conversation_id=None if requested else conversation["id"],
    )
    if not project or project.get("needs_context"):
        if name == "workflow_get":
            return {
                "found": False,
                "needs_context": bool(project and project.get("needs_context")),
                "message": "Проект не выбран или его контекст нужно восстановить.",
            }
        raise ValueError("Сначала выбери или явно восстанови проект для личного плана")
    if name == "workflow_get":
        return await store.workflow_get(
            owner, project["id"], limit=args.get("limit", 20), offset=args.get("offset", 0)
        ) or {"found": False, "message": "В этом проекте ещё нет личного плана."}
    changes = {key: value for key, value in args.items() if key != "project_id"}
    if name == "workflow_start":
        return await store.workflow_start(owner, project["id"], changes, op, run=run)
    if name == "workflow_observe":
        return await store.workflow_observe(owner, project["id"], changes, op, run=run)
    if name == "workflow_replan":
        return await store.workflow_replan(owner, project["id"], changes, op, run=run)
    raise ValueError("Неизвестная операция личного плана")
