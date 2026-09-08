from contextlib import asynccontextmanager
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest

from cronos.capabilities import TOOLS
from cronos.memory import MemoryStoreMixin
from cronos.memory_tools import execute_memory_tool


@pytest.fixture
def case():
    return SimpleNamespace(
        store=SimpleNamespace(
            write_memory=AsyncMock(return_value={"id": "fact"}),
            revise_memory=AsyncMock(return_value={"revision": 2}),
            query_memories=AsyncMock(return_value=[]),
            get_project=AsyncMock(return_value={"id": "owned-project"}),
        ),
        run={"id": uuid4(), "event_id": uuid4(), "user_id": 42, "fence": 3},
        conversation={"id": uuid4()},
    )


async def test_project_memory_uses_current_project_and_actual_source(case):
    await execute_memory_tool(
        case.store,
        "memory_write",
        {"content": "Официальный стиль", "scope": "project"},
        "memory-operation",
        case.run,
        case.conversation,
    )
    case.store.write_memory.assert_awaited_once_with(
        42,
        "Официальный стиль",
        "preference",
        str(case.run["event_id"]),
        scope="project",
        project_id="owned-project",
        conversation_id=None,
        expires_at=None,
        supersedes_id=None,
        source_key="memory-operation",
        run=case.run,
    )


async def test_missing_project_never_saves_as_global(case):
    case.store.get_project.return_value = None
    with pytest.raises(ValueError, match="проект"):
        await execute_memory_tool(
            case.store,
            "memory_write",
            {"content": "Стиль", "scope": "project"},
            "op",
            case.run,
            case.conversation,
        )
    case.store.write_memory.assert_not_awaited()


async def test_correction_preserves_revision_and_provenance(case):
    await execute_memory_tool(
        case.store,
        "memory_update",
        {"memory_id": "old", "revision": 2, "status": "inactive"},
        "revision-op",
        case.run,
        case.conversation,
    )
    case.store.revise_memory.assert_awaited_once_with(
        42,
        "old",
        {"revision": 2, "status": "inactive", "source": str(case.run["event_id"])},
        "revision-op",
        run=case.run,
    )


async def test_memory_list_supports_explicit_history_and_pagination(case):
    await execute_memory_tool(
        case.store,
        "memory_list",
        {"query": "город", "offset": 100, "include_inactive": True},
        "list",
        case.run,
        case.conversation,
    )
    case.store.query_memories.assert_awaited_once_with(
        42,
        all_scopes=True,
        query="город",
        include_inactive=True,
        limit=100,
        offset=100,
    )


@pytest.mark.parametrize("project_id", [None, "", " \t\n"])
@pytest.mark.parametrize("supersedes_id", [None, "", " \t\n"])
async def test_global_memory_normalizes_only_empty_optional_ids(case, project_id, supersedes_id):
    args = {
        "content": "  Я предпочитаю краткие ответы  ",
        "category": "preference",
        "scope": "global",
        "project_id": project_id,
        "supersedes_id": supersedes_id,
        "expires_at": None,
    }
    original = deepcopy(args)
    assert await execute_memory_tool(
        case.store, "memory_write", args, "op", case.run, case.conversation
    ) == {"id": "fact"}
    case.store.write_memory.assert_awaited_once_with(
        42,
        args["content"],
        "preference",
        str(case.run["event_id"]),
        scope="global",
        conversation_id=None,
        project_id=None,
        supersedes_id=None,
        expires_at=None,
        source_key="op",
        run=case.run,
    )
    case.store.get_project.assert_not_awaited()
    assert args == original


@pytest.mark.parametrize("project_id", ["", " \t\n"])
async def test_blank_project_id_resolves_only_explicit_project_scope(case, project_id):
    await execute_memory_tool(
        case.store,
        "memory_write",
        {"content": "Стиль", "scope": "project", "project_id": project_id},
        "op",
        case.run,
        case.conversation,
    )
    case.store.get_project.assert_awaited_once_with(42, conversation_id=case.conversation["id"])
    assert case.store.write_memory.call_args.kwargs["project_id"] == "owned-project"


@pytest.mark.parametrize(
    "value", ["not-a-uuid", "00000000-0000-0000-0000-000000000042", " UUID with spaces ", 0, False]
)
async def test_nonblank_ids_reach_storage_validation_unchanged(case, value):
    await execute_memory_tool(
        case.store,
        "memory_write",
        {"content": "Стиль", "scope": "project", "project_id": value, "supersedes_id": value},
        "op",
        case.run,
        case.conversation,
    )
    case.store.get_project.assert_not_awaited()
    kwargs = case.store.write_memory.call_args.kwargs
    assert kwargs["project_id"] == value and kwargs["supersedes_id"] == value


@pytest.mark.parametrize(
    "scope,project_id,error",
    [
        (
            "global",
            "00000000-0000-0000-0000-000000000042",
            "Global memory cannot have a scoped target",
        ),
        ("project", "not-a-uuid", "UUID"),
        ("project", "00000000-0000-0000-0000-000000000042", "Memory project is unavailable"),
    ],
)
async def test_boundary_keeps_real_store_scope_uuid_and_owner_rejections(
    case, scope, project_id, error
):
    conn = SimpleNamespace(fetchval=AsyncMock(side_effect=[42, None, None]))

    class ValidationStore(MemoryStoreMixin):
        @asynccontextmanager
        async def connection(self, user_id=None):
            assert user_id == 42
            yield conn

    case.store.write_memory = ValidationStore().write_memory
    with pytest.raises(ValueError, match=error):
        await execute_memory_tool(
            case.store,
            "memory_write",
            {"content": "Факт", "scope": scope, "project_id": project_id},
            "op",
            case.run,
            case.conversation,
        )
    case.store.get_project.assert_not_awaited()
    if error == "Memory project is unavailable":
        conn.fetchval.assert_awaited_with(
            "SELECT id FROM projects WHERE user_id=$1 AND id=$2", 42, UUID(project_id)
        )


async def test_blank_expiry_remains_invalid_instead_of_making_memory_permanent(case):
    case.store.write_memory = MemoryStoreMixin().write_memory
    with pytest.raises(ValueError, match="expiry"):
        await execute_memory_tool(
            case.store,
            "memory_write",
            {"content": "Временно", "expires_at": " "},
            "op",
            case.run,
            case.conversation,
        )


def test_memory_write_optional_ids_explicitly_allow_json_null():
    definition = next(
        tool["function"] for tool in TOOLS if tool["function"]["name"] == "memory_write"
    )
    parameters = definition["parameters"]
    for name in ("project_id", "supersedes_id"):
        assert parameters["properties"][name]["type"] == ["string", "null"]
        assert "null" in parameters["properties"][name]["description"]
        assert name not in parameters["required"]
