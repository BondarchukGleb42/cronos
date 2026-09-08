"""Optional IDs emitted as null/blank must reach the intended tool branch."""

from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest

from cronos.agent import Agent
from cronos.settings import Settings


@pytest.fixture
def case(tmp_path):
    owner = 44107
    conversation = {"id": uuid4(), "user_id": owner, "chat_id": owner, "thread_id": 37}
    run = {"id": uuid4(), "user_id": owner, "conversation_id": conversation["id"], "fence": 2}
    project = {"id": str(uuid4()), "name": "Учёба", "revision": 1}
    recipe = {"id": str(uuid4()), "version": 1}

    async def get_project(user_id, *, project_id=None, conversation_id=None):
        # Match Store.get_project: even an empty string is parsed when non-null.
        parsed = UUID(str(project_id)) if project_id is not None else None
        assert user_id == owner
        assert parsed == UUID(project["id"]) if parsed else conversation_id == conversation["id"]
        return project

    async def library_search(user_id, query, *, project_id=None, **kwargs):
        # The real library validates a non-null ID before issuing its query.
        if project_id is not None:
            UUID(str(project_id))
        assert user_id == owner
        return {"hits": [], "query": query, "project_id": project_id}

    async def recipe_save(user_id, args, source_key, saved_run):
        # Presence, rather than truthiness, chooses recipe creation vs editing.
        parsed = UUID(str(args["recipe_id"])) if "recipe_id" in args else None
        assert (user_id, source_key, saved_run) == (owner, "optional-id-op", run)
        return {**recipe, "created": parsed is None}

    store = SimpleNamespace(
        get_project=AsyncMock(side_effect=get_project),
        workflow_get=AsyncMock(return_value={"project_id": project["id"], "kind": "learning"}),
        library_search=AsyncMock(side_effect=library_search),
        recipe_save=AsyncMock(side_effect=recipe_save),
    )
    provider = SimpleNamespace(complete=AsyncMock())
    agent = Agent(
        Settings(database_url="postgresql://unused", artifacts_dir=str(tmp_path)),
        store,
        provider,
        None,
    )
    return SimpleNamespace(
        agent=agent,
        store=store,
        provider=provider,
        owner=owner,
        conversation=conversation,
        run=run,
        project=project,
    )


@pytest.mark.parametrize("optional_id", [None, "", " \t "])
@pytest.mark.parametrize("name", ["project_get", "workflow_get"])
async def test_blank_project_id_resolves_current_project_through_agent(case, name, optional_id):
    args = {"project_id": optional_id}
    result = await case.agent.execute(name, args, "optional-id-op", case.run, case.conversation)

    case.store.get_project.assert_awaited_once_with(
        case.owner, project_id=None, conversation_id=case.conversation["id"]
    )
    if name == "workflow_get":
        case.store.workflow_get.assert_awaited_once_with(
            case.owner, case.project["id"], limit=20, offset=0
        )
        assert result["project_id"] == case.project["id"]
    else:
        assert result == case.project
    assert args == {"project_id": optional_id}
    case.provider.complete.assert_not_awaited()


@pytest.mark.parametrize("optional_id", [None, "", " \t "])
async def test_account_library_search_omits_blank_project_without_changing_scope(case, optional_id):
    args = {"query": "бюджет", "scope": "account", "project_id": optional_id}
    result = await case.agent.execute(
        "library_search", args, "optional-id-op", case.run, case.conversation
    )

    case.store.get_project.assert_not_awaited()
    case.store.library_search.assert_awaited_once_with(
        case.owner, "бюджет", kinds=None, project_id=None, limit=10, offset=0
    )
    assert result["project_id"] is None
    assert args["project_id"] == optional_id
    case.provider.complete.assert_not_awaited()


@pytest.mark.parametrize("optional_id", [None, "", " \t "])
async def test_recipe_save_removes_blank_key_to_choose_creation(case, optional_id):
    definition = {
        "name": "Утренний обзор",
        "description": "Краткая сводка",
        "input_schema": [],
        "steps": [{"tool": "web_search", "instruction": "Найди новости"}],
        "requirements": ["Приведи источники"],
    }
    args = {**definition, "recipe_id": optional_id}
    result = await case.agent.execute(
        "recipe_save", args, "optional-id-op", case.run, case.conversation
    )

    case.store.recipe_save.assert_awaited_once_with(
        case.owner, definition, "optional-id-op", case.run
    )
    assert result["created"] is True
    assert args == {**definition, "recipe_id": optional_id}
    case.provider.complete.assert_not_awaited()


@pytest.mark.parametrize("name", ["project_get", "workflow_get"])
async def test_nonempty_invalid_optional_id_is_not_silently_replaced_by_current_project(case, name):
    with pytest.raises(ValueError):
        await case.agent.execute(
            name, {"project_id": "not-a-uuid"}, "optional-id-op", case.run, case.conversation
        )
    case.store.get_project.assert_awaited_once_with(
        case.owner, project_id="not-a-uuid", conversation_id=None
    )
    case.store.workflow_get.assert_not_awaited()
