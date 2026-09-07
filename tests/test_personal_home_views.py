"""Personal dashboard formatting: truthful state, usable controls and bounded text."""

from copy import deepcopy
from uuid import uuid4

from cronos.home_views import main_panel, project_panel, projects_panel


def callbacks(panel):
    return [
        button["callback_data"]
        for row in panel["reply_markup"]["inline_keyboard"]
        for button in row
    ]


def valid(panel):
    assert 0 < len(panel["text"].encode("utf-16-le")) // 2 < 3900
    assert "parse_mode" not in panel
    for row in panel["reply_markup"]["inline_keyboard"]:
        for button in row:
            assert button["text"] and 0 < len(button["callback_data"].encode()) <= 64
            assert "url" not in button


def project(index=0, **extra):
    return {
        "id": str(uuid4()),
        "name": f"Проект {index}",
        "goal": "Результат",
        "status": "active",
        "state": {"summary": "Проверено", "next_step": "Следующий согласованный шаг"},
        **extra,
    }


def test_first_use_has_examples_without_fabricated_saved_projects():
    panel = main_panel({"projects": [], "results": [], "tasks": 0, "suggestions": True})
    valid(panel)
    assert "Например" in panel["text"] and "Вот где можно продолжить" not in panel["text"]
    assert not any(
        value.startswith(("home:project:", "home:result:")) for value in callbacks(panel)
    )
    assert "home:projects:0" in callbacks(panel) and "chat:new" in callbacks(panel)


def test_personal_state_is_rendered_with_three_cards_and_no_mutation():
    projects = [project(index) for index in range(4)]
    results = [
        {"id": str(uuid4()), "filename": f"report-{i}.csv", "version": i + 1} for i in range(4)
    ]
    overview = {
        "projects": projects,
        "results": results,
        "tasks": 2,
        "suggestions": True,
        "hidden_count": 1,
    }
    original = deepcopy(overview)
    panel = main_panel(overview)
    valid(panel)
    assert len([c for c in callbacks(panel) if c.startswith("home:project:")]) == 3
    assert len([c for c in callbacks(panel) if c.startswith("home:result:")]) == 3
    assert "Следующий согласованный шаг" in panel["text"] and "Активных заданий: 2" in panel["text"]
    assert "home:show" in callbacks(panel) and overview == original


def test_disabled_suggestions_hide_cards_and_onboarding_but_keep_explicit_navigation():
    panel = main_panel({"projects": [project(name="Secret subject")], "suggestions": False})
    assert "Secret subject" not in panel["text"] and "Например" not in panel["text"]
    assert not any(c.startswith("home:project:") for c in callbacks(panel))
    assert "home:projects:0" in callbacks(panel)


def test_project_list_excludes_completed_forgotten_and_paginates_actual_active_projects():
    projects = [project(i) for i in range(8)] + [
        project(name="Completed secret", status="completed"),
        project(name="Forgotten secret", needs_context=True),
    ]
    first, second = projects_panel(projects), projects_panel(projects, page=1)
    for panel in (first, second):
        valid(panel)
        assert "Completed secret" not in panel["text"] and "Forgotten secret" not in panel["text"]
    assert "home:projects:1" in callbacks(first) and "home:projects:0" in callbacks(second)
    assert len([c for c in callbacks(second) if c.startswith("home:project:")]) == 2


def test_project_card_stale_state_and_extreme_unicode_remain_safe_and_explicit():
    stale = project_panel(None)
    assert callbacks(stale) == ["home:main"]
    card = project(
        name="🪐" * 3000,
        goal="Д" * 10000,
        state={"summary": "<b>literal</b>" * 1000, "next_step": "🧠" * 10000},
    )
    panel = project_panel(card, [{"title": "Обычный чат"}])
    valid(panel)
    assert f"home:continue:{card['id']}" in callbacks(panel)
    assert f"home:hide:{card['id']}" in callbacks(panel)
    assert "списка тем Telegram" in panel["text"] and "<b>literal</b>" in panel["text"]
    assert "t.me/" not in panel["text"] and "tg://" not in panel["text"]
