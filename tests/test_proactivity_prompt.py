"""Default-on guidance must retain current preferences and contextual boundaries."""

import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from langgraph.checkpoint.memory import InMemorySaver

from cronos.agent import Agent
from cronos.capabilities import SKILLS
from cronos.settings import Settings


@pytest.mark.parametrize("proactivity", [True, False])
async def test_real_graph_prompt_uses_current_toggle_without_asking_to_opt_in(
    tmp_path, monkeypatch, proactivity
):
    prefs = {"proactivity": proactivity, "timezone": "Europe/Moscow"}
    store = SimpleNamespace(
        preferences=AsyncMock(return_value=prefs),
        ensure_user=AsyncMock(return_value={}),
        get_project=AsyncMock(return_value=None),
        list_projects=AsyncMock(return_value=[]),
        recipe_list=AsyncMock(return_value=[]),
        list_pending_recipe=AsyncMock(return_value=[]),
        query_memories=AsyncMock(return_value=[]),
        history=AsyncMock(return_value=[]),
        conversation_recall=AsyncMock(return_value=[]),
        list_artifacts=AsyncMock(return_value=[]),
        list_schedules=AsyncMock(return_value=[]),
        run_active=AsyncMock(return_value=True),
        operation=AsyncMock(return_value=None),
        save_operation=AsyncMock(),
        configure_initiative=AsyncMock(),
        schedule=AsyncMock(),
    )
    provider = SimpleNamespace(
        complete=AsyncMock(return_value={"message": {"role": "assistant", "content": "Привет!"}})
    )
    agent = Agent(
        Settings(database_url="postgresql://unused", artifacts_dir=str(tmp_path)),
        store,
        provider,
        None,
    )

    @asynccontextmanager
    async def graph_connection():
        yield SimpleNamespace(execute=AsyncMock())

    async def paid(run, op, function, **kwargs):
        return await function()

    monkeypatch.setattr(agent, "paid", paid)
    monkeypatch.setattr(
        "cronos.agent.psycopg.AsyncConnection.connect",
        AsyncMock(side_effect=lambda *args, **kwargs: graph_connection()),
    )
    saver = InMemorySaver()
    monkeypatch.setattr("cronos.agent.FencedSaver", lambda conn, run: saver)
    owner = 44108
    conversation = {"id": uuid4(), "chat_id": owner, "thread_id": 2}
    run = {"id": uuid4(), "user_id": owner, "fence": 1}

    assert await agent.run(run, conversation, "Привет") == "Привет!"
    system = provider.complete.call_args.args[0][0]["content"]
    assert json.dumps(prefs, ensure_ascii=False) in system
    assert "Инициатива включена по умолчанию" in system
    assert "при true не спрашивай повторного общего разрешения" in system
    assert "При false не создавай самостоятельные обращения" in system
    assert "вернуть его можно по явной просьбе пользователя" in system
    assert "без выдуманного\nпрофиля и новых целей" in system
    assert "не разрешает рассылки другим людям, покупки или внешние действия" in system
    assert "сначала\nнужно согласие" not in system
    store.configure_initiative.assert_not_awaited()
    store.schedule.assert_not_awaited()


async def test_default_on_does_not_invent_a_project_for_prepared_initiative(tmp_path):
    store = SimpleNamespace(
        get_project=AsyncMock(return_value=None), configure_initiative=AsyncMock()
    )
    agent = Agent(
        Settings(database_url="postgresql://unused", artifacts_dir=str(tmp_path)), store, None, None
    )
    with pytest.raises(ValueError, match="активный проект"):
        await agent.execute(
            "initiative_configure", {}, "configure-op", {"user_id": 44108}, {"id": uuid4()}
        )
    store.configure_initiative.assert_not_awaited()


def test_skills_explain_default_on_and_still_respect_explicit_off():
    for name in ("onboarding", "proactivity", "initiative"):
        instructions = SKILLS[name]["instructions"]
        assert "повторно" in instructions
        assert "false" in instructions
        assert "после отдельного согласия" not in instructions
