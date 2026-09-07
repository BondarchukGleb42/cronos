"""Versioned user recipes: declarations and receipt-checked applications, never code."""

from collections import Counter
from contextlib import AbstractAsyncContextManager
from math import isfinite
from uuid import uuid4

import asyncpg
from pydantic import BaseModel, ConfigDict, Field, field_validator

from cronos.projects import _origin, _owner_lock, _source_key, _uid

RECIPE_STEP_TOOLS = frozenset(
    {
        "deep_reason",
        "web_search",
        "file_read",
        "file_create",
        "table_analyze",
        "image_analyze",
        "image_generate",
        "library_search",
        "library_read",
        "workflow_get",
        "workflow_observe",
        "workflow_replan",
    }
)
CONTENT_FIELDS = {"name", "description", "inputs", "steps", "output_requirements"}


class RecipeRevisionConflict(ValueError):
    pass


class Data(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")


class RecipeInput(Data):
    name: str = Field(min_length=1, max_length=64)
    type: str
    description: str = Field(default="", max_length=2000)
    required: bool = True

    @field_validator("name")
    @classmethod
    def name_valid(cls, value):
        if value != value.strip() or not value.strip():
            raise ValueError("Input name must be nonempty without surrounding whitespace")
        return value

    @field_validator("type")
    @classmethod
    def type_valid(cls, value):
        if value not in {"string", "number", "boolean", "artifact"}:
            raise ValueError("Unknown recipe input type")
        return value


class RecipeStep(Data):
    tool: str
    instruction: str = Field(min_length=1, max_length=8000)

    @field_validator("tool")
    @classmethod
    def tool_valid(cls, value):
        if value not in RECIPE_STEP_TOOLS:
            raise ValueError("Tool is not allowed in recipes")
        return value

    @field_validator("instruction")
    @classmethod
    def instruction_valid(cls, value):
        if not value.strip():
            raise ValueError("Step instruction is required")
        return value.strip()


class RecipeDefinition(Data):
    name: str = Field(min_length=1, max_length=200)
    description: str = Field(default="", max_length=4000)
    inputs: list[RecipeInput] = Field(default_factory=list, max_length=30)
    steps: list[RecipeStep] = Field(min_length=1, max_length=20)
    output_requirements: list[str] = Field(default_factory=list, max_length=30)
    status: str = "active"

    @field_validator("name")
    @classmethod
    def name_valid(cls, value):
        if not value.strip():
            raise ValueError("Recipe name is required")
        return value.strip()

    @field_validator("inputs")
    @classmethod
    def unique_inputs(cls, value):
        if len({item.name for item in value}) != len(value):
            raise ValueError("Recipe input names must be unique")
        return value

    @field_validator("output_requirements")
    @classmethod
    def requirements_valid(cls, value):
        if any(not item.strip() or len(item) > 4000 for item in value):
            raise ValueError("Output requirements must be nonempty short text")
        return value

    @field_validator("status")
    @classmethod
    def status_valid(cls, value):
        if value not in {"active", "inactive"}:
            raise ValueError("Unknown recipe status")
        return value


async def _context(conn, user_id, run):
    await _owner_lock(conn, user_id)
    if not isinstance(run, dict) or run.get("user_id") != user_id:
        raise ValueError("Recipe requires an active run owned by the user")
    conversation, run_id = await _origin(
        conn, user_id, run_id=run.get("id"), run_fence=run.get("fence")
    )
    revision = await conn.fetchval(
        """SELECT u.memory_revision FROM users u JOIN runs r ON r.user_id=u.user_id
        WHERE u.user_id=$1 AND r.id=$2 AND r.memory_revision=u.memory_revision
        AND (u.content_reset_at IS NULL OR r.created_at>u.content_reset_at)""",
        user_id,
        run_id,
    )
    if revision is None:
        raise ValueError("Recipe run predates the context reset")
    return conversation, run_id, revision


async def _version(conn, user_id, recipe_id, version=None):
    return await conn.fetchrow(
        """SELECT v.*,u.memory_revision AS current_memory_revision FROM recipes r
        JOIN recipe_versions v ON v.user_id=r.user_id AND v.recipe_id=r.id
          AND v.version=COALESCE($3::bigint,r.revision)
        JOIN users u ON u.user_id=r.user_id WHERE r.user_id=$1 AND r.id=$2""",
        user_id,
        recipe_id,
        version,
    )


def _public(row, *, compact=False):
    excluded = row["memory_revision"] != row["current_memory_revision"]
    result = {
        "id": str(row["recipe_id"]),
        "version": row["version"],
        "revision": row["version"],
        "context_excluded": excluded,
    }
    if excluded:
        return {**result, "needs_resave": True}
    definition = row["definition"]
    return {
        **result,
        **(
            {key: definition[key] for key in ("name", "description", "status")}
            if compact
            else definition
        ),
    }


async def _receipt(conn, user_id, source_key, kind, recipe_id=None, application_id=None):
    row = await conn.fetchrow(
        "SELECT * FROM recipe_operations WHERE user_id=$1 AND source_key=$2", user_id, source_key
    )
    if row and (
        row["kind"] != kind
        or (recipe_id is not None and row["recipe_id"] != recipe_id)
        or (application_id is not None and row["application_id"] != application_id)
    ):
        raise ValueError("Recipe source_key belongs to another operation")
    return row


async def _save_receipt(conn, user_id, source_key, kind, recipe_id, version, application_id=None):
    await conn.execute(
        """INSERT INTO recipe_operations(user_id,source_key,kind,recipe_id,version,application_id)
        VALUES($1,$2,$3,$4,$5,$6)""",
        user_id,
        source_key,
        kind,
        recipe_id,
        version,
        application_id,
    )


async def _inputs(conn, user_id, definition, values):
    if not isinstance(values, dict):
        raise ValueError("Recipe inputs must be an object")
    schema = {item["name"]: item for item in definition["inputs"]}
    if set(values) - set(schema):
        raise ValueError("Unknown recipe input")
    for name, value in values.items():
        kind = schema[name]["type"]
        if kind in {"string", "artifact"} and not isinstance(value, str):
            raise ValueError(f"Input {name} must be {kind}")
        if kind == "boolean" and not isinstance(value, bool):
            raise ValueError(f"Input {name} must be boolean")
        if kind == "number" and (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or (isinstance(value, float) and not isfinite(value))
        ):
            raise ValueError(f"Input {name} must be a finite number")
        if kind == "artifact" and not await conn.fetchval(
            "SELECT 1 FROM artifacts WHERE user_id=$1 AND id=$2", user_id, _uid(value)
        ):
            raise ValueError("Recipe input artifact is unavailable")
    return [
        item["name"] for item in schema.values() if item["required"] and item["name"] not in values
    ]


async def _application(conn, user_id, application_id):
    row = await conn.fetchrow(
        "SELECT * FROM recipe_applications WHERE user_id=$1 AND id=$2", user_id, application_id
    )
    if row is None:
        return None
    version = await _version(conn, user_id, row["recipe_id"], row["version"])
    excluded = version["memory_revision"] != version["current_memory_revision"]
    base = {
        "id": str(row["id"]),
        "recipe_id": str(row["recipe_id"]),
        "version": row["version"],
        "status": row["status"],
        "context_excluded": excluded,
        "completed": row["status"] == "completed" and not excluded,
    }
    if excluded:
        return {
            **base,
            "needs_resave": True,
            "steps": [],
            "inputs": {},
            "requirements": [],
            "missing_inputs": [],
        }
    outputs = await conn.fetch(
        """SELECT artifact_id FROM recipe_application_receipts WHERE user_id=$1 AND application_id=$2
        AND artifact_id IS NOT NULL ORDER BY operation_id""",
        user_id,
        application_id,
    )
    definition = version["definition"]
    return {
        **base,
        "name": definition["name"],
        "inputs": row["inputs"],
        "input_schema": definition["inputs"],
        "missing_inputs": row["missing_inputs"],
        "steps": definition["steps"] if row["status"] in {"ready", "completed"} else [],
        "requirements": definition["output_requirements"],
        "output_artifact_ids": [str(item["artifact_id"]) for item in outputs],
        "semantic_requirements_verified": False,
    }


def _successful(tool, result):
    """Recognize actual built-in receipt shapes, not arbitrary truthy JSON."""
    if not isinstance(result, dict) or "error" in result:
        return False
    if "completed" in result and result["completed"] is not True:
        return False
    status = result.get("status")
    if status is not None and not isinstance(status, str):
        return False
    if status in {"error", "failed", "cancelled", "awaiting_input"}:
        return False
    if tool in {"file_create", "image_generate"}:
        return (
            isinstance(result.get("artifact_id"), str)
            and bool(result["artifact_id"])
            and result.get("delivery") == ("queued" if tool == "file_create" else "prepared")
        )
    if tool == "deep_reason":
        return isinstance(result.get("analysis"), str) and bool(result["analysis"].strip())
    if tool == "web_search":
        return (
            bool(result.get("text"))
            and isinstance(result.get("text"), str)
            and bool(result.get("sources"))
            and isinstance(result["sources"], list)
            and all(
                isinstance(x, dict)
                and isinstance(x.get("url"), str)
                and x["url"].startswith(("https://", "http://"))
                for x in result["sources"]
            )
        )
    if tool in {"file_read", "image_analyze", "library_read"}:
        return isinstance(result.get("text"), str)
    if tool == "table_analyze":
        return (
            isinstance(result.get("value"), (str, int, float))
            and isinstance(result.get("numeric_rows"), int)
            and not isinstance(result["numeric_rows"], bool)
        )
    if tool == "library_search":
        return isinstance(result.get("hits"), list) and isinstance(result.get("has_more"), bool)
    if tool in {"workflow_get", "workflow_observe", "workflow_replan"}:
        return (
            isinstance(result.get("id"), str)
            and isinstance(result.get("plan"), dict)
            and isinstance(result.get("revision"), int)
            and not result.get("needs_context")
        )
    return False


class RecipesStoreMixin:
    def connection(
        self, user_id: int | None = None
    ) -> AbstractAsyncContextManager[asyncpg.Connection]:
        raise NotImplementedError

    async def recipe_save(self, user_id, args, source_key, run) -> dict:
        source_key = _source_key(source_key)
        if not isinstance(args, dict) or set(args) - (
            CONTENT_FIELDS | {"status", "recipe_id", "revision"}
        ):
            raise ValueError("Unknown recipe fields")
        recipe_id = _uid(args["recipe_id"]) if "recipe_id" in args else None
        async with self.connection(user_id) as conn:
            _, _, revision = await _context(conn, user_id, run)
            receipt = await _receipt(conn, user_id, source_key, "save", recipe_id)
            if receipt:
                return _public(
                    await _version(conn, user_id, receipt["recipe_id"], receipt["version"])
                )
            patch = {
                key: value for key, value in args.items() if key in CONTENT_FIELDS | {"status"}
            }
            if recipe_id:
                current = await _version(conn, user_id, recipe_id)
                if current is None:
                    raise ValueError("Recipe is unavailable")
                if type(args.get("revision")) is not int or args["revision"] != current["version"]:
                    raise RecipeRevisionConflict(
                        "Reload the recipe before updating a stale revision"
                    )
                if current["memory_revision"] != revision and not CONTENT_FIELDS <= patch.keys():
                    raise ValueError("A forgotten recipe requires an explicit full re-save")
                definition = RecipeDefinition.model_validate(
                    {**current["definition"], **patch}
                ).model_dump()
                number = current["version"] + 1
                await conn.execute(
                    "UPDATE recipes SET revision=$3,updated_at=clock_timestamp() WHERE user_id=$1 AND id=$2",
                    user_id,
                    recipe_id,
                    number,
                )
            else:
                if "revision" in args:
                    raise ValueError("Recipe revision requires recipe_id")
                definition = RecipeDefinition.model_validate(patch).model_dump()
                recipe_id, number = uuid4(), 1
                await conn.execute(
                    "INSERT INTO recipes(user_id,id,revision) VALUES($1,$2,1)", user_id, recipe_id
                )
            await conn.execute(
                "INSERT INTO recipe_versions(user_id,recipe_id,version,definition,memory_revision) VALUES($1,$2,$3,$4,$5)",
                user_id,
                recipe_id,
                number,
                definition,
                revision,
            )
            await _save_receipt(conn, user_id, source_key, "save", recipe_id, number)
            return _public(await _version(conn, user_id, recipe_id, number))

    async def recipe_get(self, user_id, recipe_id) -> dict | None:
        async with self.connection(user_id) as conn:
            row = await _version(conn, user_id, _uid(recipe_id))
            return _public(row) if row else None

    async def recipe_list(self, user_id, query=None, limit=20, offset=0) -> list[dict]:
        if type(limit) is not int or not 1 <= limit <= 100 or type(offset) is not int or offset < 0:
            raise ValueError("Invalid recipe pagination")
        if query is not None and not isinstance(query, str):
            raise ValueError("Recipe query must be text")
        async with self.connection(user_id) as conn:
            rows = await conn.fetch(
                """SELECT v.*,u.memory_revision AS current_memory_revision FROM recipes r
                JOIN recipe_versions v ON v.user_id=r.user_id AND v.recipe_id=r.id AND v.version=r.revision
                JOIN users u ON u.user_id=r.user_id WHERE r.user_id=$1 AND v.memory_revision=u.memory_revision
                AND strpos(translate(lower((v.definition->>'name')||' '||(v.definition->>'description')),
                    'АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ','абвгдеёжзийклмнопрстуфхцчшщъыьэюя'),$2)>0
                ORDER BY r.updated_at DESC,r.id LIMIT $3 OFFSET $4""",
                user_id,
                (query or "").casefold(),
                limit,
                offset,
            )
            return [_public(row, compact=True) for row in rows]

    async def recipe_apply(
        self, user_id, recipe_id, inputs, source_key, run, conversation_id
    ) -> dict:
        recipe_id, conversation_id, source_key = (
            _uid(recipe_id),
            _uid(conversation_id),
            _source_key(source_key),
        )
        async with self.connection(user_id) as conn:
            origin, run_id, revision = await _context(conn, user_id, run)
            if origin != conversation_id:
                raise ValueError("Recipe application belongs to another conversation")
            receipt = await _receipt(conn, user_id, source_key, "apply", recipe_id)
            if receipt:
                return await _application(conn, user_id, receipt["application_id"])
            version = await _version(conn, user_id, recipe_id)
            if version is None or version["memory_revision"] != revision:
                raise ValueError("Recipe is unavailable or requires a full re-save")
            if version["definition"]["status"] != "active":
                raise ValueError("Recipe is inactive")
            waiting = await conn.fetchrow(
                """SELECT a.* FROM recipe_applications a JOIN recipe_versions v
                ON v.user_id=a.user_id AND v.recipe_id=a.recipe_id AND v.version=a.version
                WHERE a.user_id=$1 AND a.recipe_id=$2 AND a.conversation_id=$3
                AND a.status='awaiting_input' AND v.memory_revision=$4""",
                user_id,
                recipe_id,
                conversation_id,
                revision,
            )
            if not isinstance(inputs, dict):
                raise ValueError("Recipe inputs must be an object")
            if waiting:
                version = await _version(conn, user_id, recipe_id, waiting["version"])
                inputs = {**waiting["inputs"], **inputs}
            missing = await _inputs(conn, user_id, version["definition"], inputs)
            status = "awaiting_input" if missing else "ready"
            if await conn.fetchval(
                "SELECT 1 FROM recipe_applications WHERE user_id=$1 AND run_id=$2 AND status='ready'",
                user_id,
                run_id,
            ):
                raise ValueError("Complete the current recipe application before starting another")
            # Old waiting applications are superseded even if a reset hid their content.
            await conn.execute(
                "UPDATE recipe_applications SET status='superseded' WHERE user_id=$1 AND recipe_id=$2 AND conversation_id=$3 AND status='awaiting_input'",
                user_id,
                recipe_id,
                conversation_id,
            )
            identifier = uuid4()
            await conn.execute(
                """INSERT INTO recipe_applications(user_id,id,recipe_id,version,run_id,conversation_id,status,inputs,missing_inputs)
                VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)""",
                user_id,
                identifier,
                recipe_id,
                version["version"],
                run_id,
                conversation_id,
                status,
                inputs,
                missing,
            )
            await _save_receipt(
                conn, user_id, source_key, "apply", recipe_id, version["version"], identifier
            )
            return await _application(conn, user_id, identifier)

    async def list_pending_recipe(self, user_id, conversation_id) -> list[dict]:
        async with self.connection(user_id) as conn:
            rows = await conn.fetch(
                """SELECT a.id FROM recipe_applications a JOIN recipe_versions v
                ON v.user_id=a.user_id AND v.recipe_id=a.recipe_id AND v.version=a.version
                JOIN users u ON u.user_id=a.user_id WHERE a.user_id=$1 AND a.conversation_id=$2
                AND a.status='awaiting_input' AND v.memory_revision=u.memory_revision ORDER BY a.created_at DESC LIMIT 5""",
                user_id,
                _uid(conversation_id),
            )
            return [await _application(conn, user_id, row["id"]) for row in rows]

    async def recipe_complete(self, user_id, application_id, source_key, run) -> dict:
        application_id, source_key = _uid(application_id), _source_key(source_key)
        async with self.connection(user_id) as conn:
            _, run_id, revision = await _context(conn, user_id, run)
            row = await conn.fetchrow(
                "SELECT * FROM recipe_applications WHERE user_id=$1 AND id=$2",
                user_id,
                application_id,
            )
            if row is None:
                raise ValueError("Recipe application is unavailable")
            receipt = await _receipt(
                conn, user_id, source_key, "complete", application_id=application_id
            )
            if receipt:
                return await _application(conn, user_id, application_id)
            version = await _version(conn, user_id, row["recipe_id"], row["version"])
            if version["memory_revision"] != revision:
                raise ValueError("Recipe application predates the context reset")
            if row["run_id"] != run_id:
                raise ValueError("Recipe completion requires the application's run")
            if row["status"] == "completed":
                await _save_receipt(
                    conn,
                    user_id,
                    source_key,
                    "complete",
                    row["recipe_id"],
                    row["version"],
                    application_id,
                )
                return await _application(conn, user_id, application_id)
            if row["status"] != "ready":
                raise ValueError("Recipe inputs are incomplete or application was superseded")
            required = Counter(step["tool"] for step in version["definition"]["steps"])
            operations = await conn.fetch(
                """SELECT o.id,o.kind,o.result FROM operations o
                WHERE o.user_id=$1 AND o.run_id=$2 AND o.created_at>=$3 AND o.status='done'
                AND o.kind=ANY($4::text[]) AND NOT EXISTS(SELECT 1 FROM recipe_application_receipts rr
                  WHERE rr.user_id=o.user_id AND rr.operation_id=o.id)
                ORDER BY o.created_at,o.id""",
                user_id,
                run_id,
                row["created_at"],
                list(required),
            )
            selected = []
            for operation in operations:
                tool, result = operation["kind"], operation["result"]
                if required[tool] <= 0 or not _successful(tool, result):
                    continue
                artifact_id = None
                if tool in {"file_create", "image_generate"}:
                    try:
                        artifact_id = _uid(result["artifact_id"])
                    except ValueError, TypeError, AttributeError:
                        continue
                    if not await conn.fetchval(
                        "SELECT 1 FROM artifacts WHERE user_id=$1 AND id=$2", user_id, artifact_id
                    ):
                        continue
                selected.append((operation["id"], tool, artifact_id))
                required[tool] -= 1
            missing = {tool: count for tool, count in required.items() if count > 0}
            if missing:
                raise ValueError(
                    "Recipe steps lack successful receipts: "
                    + ", ".join(f"{tool} x{count}" for tool, count in sorted(missing.items()))
                )
            for operation_id, tool, artifact_id in selected:
                await conn.execute(
                    "INSERT INTO recipe_application_receipts(user_id,application_id,operation_id,tool,artifact_id) VALUES($1,$2,$3,$4,$5)",
                    user_id,
                    application_id,
                    operation_id,
                    tool,
                    artifact_id,
                )
            await conn.execute(
                "UPDATE recipe_applications SET status='completed',completed_at=clock_timestamp() WHERE user_id=$1 AND id=$2",
                user_id,
                application_id,
            )
            await _save_receipt(
                conn,
                user_id,
                source_key,
                "complete",
                row["recipe_id"],
                row["version"],
                application_id,
            )
            return await _application(conn, user_id, application_id)
