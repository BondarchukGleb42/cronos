from copy import deepcopy

import pytest

from cronos.home_views import (
    chats_panel,
    guide_panel,
    main_panel,
    memory_panel,
    plans_panel,
    settings_panel,
    tasks_panel,
)


def callbacks(panel):
    return [
        button["callback_data"]
        for row in panel["reply_markup"]["inline_keyboard"]
        for button in row
    ]


def assert_valid(panel, *, home=True):
    assert set(panel) == {"text", "reply_markup"}
    assert 0 < len(panel["text"].encode("utf-16-le")) // 2 < 3900
    assert "parse_mode" not in panel and "format" not in panel
    for row in panel["reply_markup"]["inline_keyboard"]:
        assert row
        for button in row:
            assert set(button) == {"text", "callback_data"}
            assert 1 <= len(button["callback_data"].encode()) <= 64
            assert 1 <= len(button["text"].encode()) <= 64
    if home:
        assert panel["reply_markup"]["inline_keyboard"][-1] == [
            {"text": "🪐 Главное меню", "callback_data": "home:main"}
        ]


def test_main_navigation_routes_match_public_contract():
    panel = main_panel()
    assert_valid(panel, home=False)
    assert panel["text"].startswith("🪐 Cronos")
    assert callbacks(panel) == [
        "chat:new",
        "home:projects:0",
        "home:chats:0",
        "home:tasks:0",
        "home:plans",
        "home:guide",
        "home:memory:0",
        "home:settings",
    ]
    assert "Инициатива включена" in panel["text"]


def test_guide_lists_supported_files_and_natural_controls():
    panel = guide_panel()
    assert_valid(panel)
    assert all(name in panel["text"] for name in ("PDF", "CSV", "XLSX", "DOCX", "TXT", "фото"))
    assert "источники" in panel["text"]
    assert "История у каждого чата своя" in panel["text"]
    assert "подтверждения" in panel["text"]


def chats(count=13):
    return [
        {"thread_id": index + 2, "title": f"Тема {index + 1}", "is_home": False}
        for index in range(count)
    ]


def test_chats_exclude_home_and_both_general_aliases_and_use_native_selection():
    data = [
        {"thread_id": 77, "is_home": True, "title": "Hidden home"},
        {"thread_id": 0, "title": "Hidden General0"},
        {"thread_id": 1, "title": "Hidden General1"},
        *chats(),
    ]
    original = deepcopy(data)
    first = chats_panel(data)
    middle = chats_panel(data, 1)
    last = chats_panel(data, 2)
    assert "Hidden" not in first["text"]
    assert "6. Тема 6" in first["text"] and "7. Тема 7" not in first["text"]
    assert "7. Тема 7" in middle["text"] and "12. Тема 12" in middle["text"]
    assert "13. Тема 13" in last["text"]
    assert "home:chats:1" in callbacks(first)
    assert "home:chats:0" in callbacks(middle) and "home:chats:2" in callbacks(middle)
    assert "home:chats:3" not in callbacks(last)
    assert "списке тем Telegram" in first["text"] and "https://" not in first["text"]
    assert data == original
    for panel in (first, middle, last):
        assert_valid(panel)


@pytest.mark.parametrize("page", [-100, None, "1", True])
def test_bad_page_values_return_first_page(page):
    assert chats_panel(chats(), page) == chats_panel(chats())


def test_out_of_range_page_clamps_to_last_and_empty_pages_offer_help():
    assert chats_panel(chats(), 10**100) == chats_panel(chats(), 2)
    for panel in (chats_panel([]), tasks_panel([]), memory_panel([])):
        assert_valid(panel)
        assert not any(value.rsplit(":", 1)[-1].isdigit() for value in callbacks(panel))
    assert "Создай первый" in chats_panel([])["text"]
    assert "10:00 по Москве" in tasks_panel([])["text"]
    assert "Запомни" in memory_panel([])["text"]


def schedule(**extra):
    return {
        "id": "private-id",
        "state": "active",
        "due_at": "2026-09-08T07:00:00+00:00",
        "timezone": "Europe/Moscow",
        "instruction": "Проверить цены",
        "fixed_text": "Старый текст",
        "interval_seconds": 86400,
        "dynamic": True,
        "proactive": False,
        **extra,
    }


def test_tasks_show_local_time_recurrence_and_execution_kind_without_ids():
    panel = tasks_panel(
        [
            schedule(),
            schedule(instruction="Отдых", proactive=True, dynamic=False, interval_seconds=604800),
            schedule(state="cancelled", instruction="Hidden cancelled"),
        ]
    )
    assert_valid(panel)
    text = panel["text"]
    assert "08.09.2026, 10:00" in text and "Europe/Moscow (UTC+03:00)" in text
    assert "ежедневно" in text and "еженедельно" in text
    assert "Новый результат при запуске" in text and "Сохранённый текст" in text
    assert "Инициатива Cronos" in text and "По твоему запросу" in text
    assert "Hidden" not in text and "private-id" not in text and "Старый текст" not in text
    assert callbacks(panel) == ["home:main"]


def test_task_pagination_and_unknown_dates_do_not_invent_times_or_calendar_months():
    data = [schedule(instruction=f"Задание {index}") for index in range(1, 7)]
    assert "5. Задание 5" in tasks_panel(data)["text"]
    assert "6. Задание 6" not in tasks_panel(data)["text"]
    assert "6. Задание 6" in tasks_panel(data, 1)["text"]
    panel = tasks_panel(
        [
            schedule(due_at="bad", interval_seconds=2592000),
            schedule(due_at="2026-09-08T10:00:00", timezone=None, interval_seconds=None),
        ]
    )
    assert "время не указано" in panel["text"]
    assert "часовой пояс не указан" in panel["text"]
    assert "каждые 30 дн." in panel["text"] and "ежемесячно" not in panel["text"]
    assert "однократно" in panel["text"]


def test_plans_use_actual_balance_and_pending_plan_with_explicit_test_payment():
    balance = {
        "plan": "PREMIUM",
        "pending_plan": "START",
        "tokens_remaining": 123456,
        "period_end": "2026-10-07T00:00:00Z",
        "soft_limits": True,
    }
    original = deepcopy(balance)
    panel = plans_panel(balance)
    assert_valid(panel)
    assert "Сейчас: PREMIUM" in panel["text"]
    assert "123 456 токенов" in panel["text"]
    assert "Со следующего периода: START" in panel["text"]
    assert "07.10.2026, 00:00 · UTC" in panel["text"]
    assert "не блокирует общение" in panel["text"]
    assert "оплаты и списания денег нет" in panel["text"]
    assert callbacks(panel) == ["plan:FREE", "plan:START", "plan:PREMIUM", "plan:PRO", "home:main"]
    assert balance == original
    assert "мягкие" not in plans_panel({**balance, "soft_limits": False})["text"]


@pytest.mark.parametrize("proactivity", [False, True])
def test_settings_show_real_preferences_and_opposite_toggle(proactivity):
    panel = settings_panel(
        {"timezone": "Asia/Tokyo", "tone": "Коротко и спокойно", "proactivity": proactivity}
    )
    assert_valid(panel)
    assert "Asia/Tokyo" in panel["text"] and "Коротко и спокойно" in panel["text"]
    assert callbacks(panel) == [
        f"home:proactivity:{'off' if proactivity else 'on'}",
        "home:clearall",
        "home:main",
    ]
    assert "после отдельного подтверждения" in panel["text"]


def test_missing_proactivity_defaults_to_enabled_in_home_and_settings():
    assert "Инициатива включена" in main_panel()["text"]
    panel = settings_panel({})
    assert "Писать первым: включено" in panel["text"]
    assert callbacks(panel)[0] == "home:proactivity:off"
    assert "включена по умолчанию" in guide_panel()["text"]


def test_home_respects_explicit_off_without_suggesting_another_consent_step():
    panel = main_panel({"proactivity": False})
    assert "Инициатива выключена" in panel["text"]
    assert "Явные напоминания продолжают работать" in panel["text"]
    assert "Инициатива включена" not in panel["text"]
    assert "Писать первым: выключено" in settings_panel({"proactivity": False})["text"]


def test_memory_paginates_eight_records_and_keeps_shared_memory_explanation():
    records = [
        {"id": f"secret-id-{i}", "content": f"Факт {i}", "category": "preference"}
        for i in range(1, 10)
    ]
    first, second = memory_panel(records), memory_panel(records, 1)
    assert "8. Факт 8" in first["text"] and "9. Факт 9" not in first["text"]
    assert "9. Факт 9" in second["text"]
    assert "secret-id" not in first["text"]
    assert "для проекта или отдельного чата" in first["text"]
    assert "home:memory:1" in callbacks(first) and "home:memory:0" in callbacks(second)
    assert_valid(first)
    assert_valid(second)


def test_extreme_unicode_fields_remain_plaintext_bounded_and_do_not_change_callbacks():
    long = "😀" * 10000 + "\nprivate suffix"
    panels = [
        chats_panel([{**row, "title": long} for row in chats()]),
        tasks_panel([schedule(instruction=long, timezone=long) for _ in range(6)]),
        memory_panel([{"content": long} for _ in range(9)]),
        settings_panel({"tone": long, "timezone": long}),
        plans_panel({"plan": long, "pending_plan": long, "tokens_remaining": float("nan")}),
    ]
    for panel in panels:
        assert_valid(panel)
        assert "private suffix" not in panel["text"]
        assert all("😀" not in value for value in callbacks(panel))
    assert "не указано" in panels[-1]["text"]
    assert "**literal** <tag>" in memory_panel([{"content": "**literal** <tag>"}])["text"]
