from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

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
