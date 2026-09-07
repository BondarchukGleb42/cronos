from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from cronos.library_tools import execute_library_tool, recall_query


async def test_account_search_requires_explicit_scope_when_a_project_is_bound():
    store = SimpleNamespace(
        get_project=AsyncMock(return_value={"id": "current-project"}),
        library_search=AsyncMock(return_value={"hits": []}),
    )
    run, conversation = {"user_id": 42}, {"id": "conversation"}
    await execute_library_tool(store, "library_search", {"query": "бюджет"}, run, conversation)
    assert store.library_search.call_args.kwargs["project_id"] == "current-project"
    await execute_library_tool(
        store, "library_search", {"query": "бюджет", "scope": "account"}, run, conversation
    )
    assert store.library_search.call_args.kwargs["project_id"] is None
    with pytest.raises(ValueError):
        await execute_library_tool(
            store, "library_search", {"query": "бюджет", "scope": "unknown"}, run, conversation
        )
    assert store.library_search.await_count == 2


def test_recall_never_searches_binary_payloads_or_attachment_identifiers():
    assert (
        recall_query(
            [
                {"type": "text", "text": "Предыдущее решение"},
                {"type": "image_ref", "artifact_id": "private-id"},
                {"type": "image_url", "image_url": {"url": "data:image/png;base64,private"}},
            ]
        )
        == "Предыдущее решение"
    )
    assert recall_query([{"type": "image_ref", "artifact_id": "private-id"}]) == ""
