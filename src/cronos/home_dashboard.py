"""Deterministic personal home: authoritative rows, no model or copied personal text."""

from uuid import UUID

from cronos.home_views import main_panel, project_panel
from cronos.topics import create_chat


async def home_overview(store, user_id):
    async with store.connection(user_id) as conn:
        preferences = (
            await conn.fetchval("SELECT preferences FROM users WHERE user_id=$1", user_id) or {}
        )
        hidden = preferences.get("home_hidden_projects", {})
        if not isinstance(hidden, dict):
            hidden = {}
        projects = [
            dict(row)
            for row in await conn.fetch(
                """SELECT id,name,goal,state,revision,updated_at FROM projects
            WHERE user_id=$1 AND status='active' AND NOT context_excluded
            ORDER BY updated_at DESC,id LIMIT 100""",
                user_id,
            )
        ]
        visible = [p for p in projects if hidden.get(str(p["id"])) != p["revision"]]
        results = [
            dict(row)
            for row in await conn.fetch(
                """SELECT a.id,a.filename,v.version FROM artifact_version_heads h
            JOIN artifact_versions v ON (v.user_id,v.artifact_id)=(h.user_id,h.artifact_id)
            JOIN artifacts a ON (a.user_id,a.id)=(v.user_id,v.artifact_id)
            JOIN users u ON u.user_id=a.user_id
            JOIN runs r ON (r.user_id,r.id)=(v.user_id,v.run_id)
            JOIN events e ON e.id=r.event_id
            WHERE a.user_id=$1 AND NOT v.context_excluded AND v.memory_revision=u.memory_revision
            AND r.status='done' AND e.kind='telegram'
            ORDER BY v.created_at DESC LIMIT 3""",
                user_id,
            )
        ]
        tasks = await conn.fetchval(
            "SELECT count(*) FROM schedules WHERE user_id=$1 AND state='active'",
            user_id,
        )
    return {
        "projects": [{**p, "id": str(p["id"])} for p in visible[:3]],
        "project_count": len(projects),
        "hidden_count": len(projects) - len(visible),
        "results": [{**r, "id": str(r["id"])} for r in results],
        "tasks": tasks,
        "suggestions": preferences.get("home_suggestions", True) is not False,
        "proactivity": bool(preferences.get("proactivity", True)),
    }


async def personal_main_panel(store, user_id):
    return main_panel(await home_overview(store, user_id))


async def hide_home_project(store, user_id, project_id):
    """Hide the current revision; a changed next step may be proposed again."""
    async with store.connection(user_id) as conn:
        await conn.fetchval("SELECT user_id FROM users WHERE user_id=$1 FOR UPDATE", user_id)
        project = await conn.fetchrow(
            "SELECT id,revision FROM projects WHERE user_id=$1 AND id=$2 AND status='active' AND NOT context_excluded",
            user_id,
            UUID(str(project_id)),
        )
        if not project:
            return False
        preferences = await conn.fetchval("SELECT preferences FROM users WHERE user_id=$1", user_id)
        hidden = preferences.get("home_hidden_projects", {})
        hidden = dict(hidden) if isinstance(hidden, dict) else {}
        # Store only IDs/revisions of current owner projects, bounded in size.
        available = {
            str(r["id"])
            for r in await conn.fetch("SELECT id FROM projects WHERE user_id=$1", user_id)
        }
        hidden = {k: v for k, v in hidden.items() if k in available}
        hidden[str(project["id"])] = project["revision"]
        hidden = dict(list(hidden.items())[-100:])
        await conn.execute(
            "UPDATE users SET preferences=preferences||$2::jsonb WHERE user_id=$1",
            user_id,
            {"home_hidden_projects": hidden},
        )
        return True


async def home_project_panel(store, user_id, project_id):
    project = await store.get_project(user_id, project_id=project_id)
    if not project or project.get("needs_context") or project["status"] != "active":
        return project_panel(None)
    conversations = await store.list_conversations(user_id)
    linked = set(project["conversation_ids"])
    chats = [row for row in conversations if str(row["id"]) in linked and not row.get("is_home")]
    return project_panel(project, chats)


async def continue_home_project(store, transport, user_id, chat_id, project_id, event_id):
    project = await store.get_project(user_id, project_id=project_id)
    if not project or project.get("needs_context") or project["status"] != "active":
        return project_panel(None)
    key = f"home-project:{event_id}"
    result = await create_chat(
        store,
        transport,
        user_id,
        chat_id,
        key,
        project["name"][:100],
        show_welcome=False,
    )
    if result.get("error"):
        return {"text": result["error"]}
    await store.attach_project(user_id, project_id, result["id"], source_key=key + ":link")
    next_step = project["state"].get("next_step") or "Напиши, с чего хочешь продолжить."
    await store.enqueue(
        user_id,
        chat_id,
        result["thread_id"],
        {
            "text": f"Проект «{project['name']}» подключён.\n\n{next_step}\n\nНапиши здесь — продолжим с сохранённым контекстом."
        },
        key + ":welcome",
    )
    panel = project_panel(project)
    panel["text"] = "Чат проекта готов. Выбери его в списке тем Telegram.\n\n" + panel["text"]
    return panel
