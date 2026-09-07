"""Agent adapters: trusted policy scope and explicit durable usefulness decisions."""

import json
from uuid import UUID

from cronos.initiative import INITIATIVE_ALLOWED_TOOLS

INITIATIVE_TOOL_NAMES = frozenset(
    {"initiative_configure", "initiative_feedback", "initiative_decide", "initiative_skip"}
)
INITIATIVE_DECISION_TOOLS = frozenset({"initiative_decide", "initiative_skip"})


def initiative_result(state):
    """Derive file references and the last decision from actual current-run tool receipts."""
    calls, prepared, decision = {}, [], None
    for message in state.get("messages", []):
        if message.get("role") == "assistant":
            for call in message.get("tool_calls", []):
                calls[call.get("id")] = call.get("function", {}).get("name")
        elif message.get("role") == "tool":
            name = calls.get(message.get("tool_call_id"))
            try:
                result = json.loads(message.get("content", ""))
            except ValueError, TypeError:
                result = {}
            if not isinstance(result, dict):
                result = {}
            if name in INITIATIVE_DECISION_TOOLS:
                decision = result if isinstance(result.get("should_send"), bool) else None
            else:
                # A decision made before further search/file work cannot authorize its result.
                decision = None
                if (
                    name == "file_create"
                    and result.get("delivery") == "prepared"
                    and not result.get("error")
                ):
                    artifact_id = result.get("artifact_id")
                    if isinstance(artifact_id, str) and artifact_id not in prepared:
                        prepared.append(artifact_id)
    return {
        "text": state.get("answer") or "",
        "prepared_artifact_ids": prepared,
        "initiative_decision": decision or {"should_send": False, "reason": "decision_missing"},
    }


async def execute_initiative_tool(store, name, args, op, run, conversation, *, policy=None):
    user = run["user_id"]
    if name == "initiative_configure":
        project_id = args.get("project_id")
        if not project_id:
            current = await store.get_project(user, conversation_id=conversation["id"])
            if not current or current.get("needs_context"):
                raise ValueError("Сначала выбери активный проект с актуальным контекстом")
            project_id = current["id"]
        values = {key: value for key, value in args.items() if key != "project_id"}
        return await store.configure_initiative(user, project_id, values, op, run)
    if name == "initiative_feedback":
        return await store.initiative_feedback(user, args["id"], args["action"], op, run)
    if name not in INITIATIVE_DECISION_TOOLS or not policy or not policy.get("available"):
        raise ValueError("Prepared initiative decision requires its trusted active policy")
    if args.get("id") and UUID(str(args["id"])) != UUID(str(policy["id"])):
        raise ValueError("Initiative decision belongs to another policy")
    return await store.decide_initiative(
        user,
        policy["id"],
        policy["revision"],
        args["summary"],
        args.get("evidence_key", "no-evidence")
        if name == "initiative_skip"
        else args["evidence_key"],
        False if name == "initiative_skip" else args["send"],
        op,
        run,
        expected_project_revision=policy["project_revision"],
    )


async def scope_initiative_tool(store, name, args, run, policy):
    """Enforce the approved tool list and project boundary before executing any read/write.

    This applies to actual execution as well as the model's tool schema. Generated files
    from this run can be read for verification; unrelated account files cannot be read.
    """
    if not policy or not policy.get("available"):
        raise ValueError("Prepared initiative policy is unavailable")
    if name in INITIATIVE_DECISION_TOOLS:
        return dict(args)
    if name not in INITIATIVE_ALLOWED_TOOLS or name not in policy.get("allowed_tools", []):
        raise ValueError("Tool is not approved for this initiative")
    current = await store.get_initiative_for_schedule(run["user_id"], policy["schedule_id"])
    if (
        not current
        or not current.get("available")
        or current["id"] != policy["id"]
        or current["revision"] != policy["revision"]
        or current["project_revision"] != policy["project_revision"]
    ):
        raise ValueError("Initiative policy changed during preparation")
    args = dict(args)
    owner, project = run["user_id"], UUID(str(policy["project_id"]))
    if name in {"project_get", "library_search", "file_create"}:
        if args.get("project_id") and UUID(str(args["project_id"])) != project:
            raise ValueError("Prepared initiative cannot access another project")
        args["project_id"] = str(project)
    if name == "library_search":
        if args.get("scope") == "account":
            raise ValueError("Prepared initiative search is limited to its project")
        args["scope"] = "current_project"
    source_kind = source_id = None
    if name in {"file_read", "table_analyze"}:
        source_kind, source_id = "artifact", args["artifact_id"]
    elif name == "file_create" and args.get("parent_artifact_id"):
        source_kind, source_id = "artifact", args["parent_artifact_id"]
    elif name == "library_read":
        source_kind, source_id = args["kind"], args["id"]
    if source_kind is not None:
        if source_id is None:
            raise ValueError("Initiative source reference is required")
        async with store.connection(owner) as conn:
            if source_kind == "artifact":
                allowed = await conn.fetchval(
                    """SELECT 1 FROM artifacts a WHERE a.user_id=$1 AND a.id=$2 AND (
                    EXISTS(SELECT 1 FROM project_artifacts pa WHERE pa.user_id=$1 AND pa.project_id=$3 AND pa.artifact_id=a.id)
                    OR EXISTS(SELECT 1 FROM artifact_versions v JOIN users u ON u.user_id=v.user_id
                      WHERE v.user_id=$1 AND v.artifact_id=a.id AND v.project_id=$3 AND v.run_id=$4
                      AND NOT v.context_excluded AND v.memory_revision=u.memory_revision))""",
                    owner,
                    UUID(str(source_id)),
                    project,
                    UUID(str(run["id"])),
                )
            elif source_kind == "message":
                allowed = await conn.fetchval(
                    """SELECT 1 FROM messages m JOIN project_conversations pc
                    ON pc.user_id=m.user_id AND pc.conversation_id=m.conversation_id
                    WHERE m.user_id=$1 AND m.id=$2 AND pc.project_id=$3 AND NOT m.excluded""",
                    owner,
                    int(source_id),
                    project,
                )
            elif source_kind == "project":
                allowed = UUID(str(source_id)) == project
            else:
                allowed = False
            if not allowed:
                raise ValueError("Source is outside the approved initiative project")
    return args
