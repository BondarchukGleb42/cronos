"""Pure plaintext Telegram panels; callers supply already authorized user data."""

from datetime import datetime
from decimal import Decimal, InvalidOperation
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

HOME_TITLE = "🪐 Cronos"


def _clip(text: str, limit: int) -> str:
    text = "".join(char for char in text if not 0xD800 <= ord(char) <= 0xDFFF)
    if sum(2 if ord(char) > 0xFFFF else 1 for char in text) <= limit:
        return text
    remaining, result = limit - 1, []
    for char in text:
        width = 2 if ord(char) > 0xFFFF else 1
        if width > remaining:
            break
        result.append(char)
        remaining -= width
    return "".join(result).rstrip() + "…"


def _line(value, limit: int, fallback: str = "") -> str:
    return _clip(" ".join(str(value or fallback).split()), limit)


def _button(text: str, callback: str) -> dict:
    return {"text": text, "callback_data": callback}


def _panel(text: str, rows: list[list[dict]], *, home: bool = True) -> dict:
    if home:
        rows = [*rows, [_button("🪐 Главное меню", "home:main")]]
    return {"text": _clip(text, 3899), "reply_markup": {"inline_keyboard": rows}}


def _page(items: list[dict], page: int, size: int, route: str):
    pages = max(1, (len(items) + size - 1) // size)
    page = min(
        max(page if isinstance(page, int) and not isinstance(page, bool) else 0, 0), pages - 1
    )
    start = page * size
    navigation = []
    if page:
        navigation.append(_button("← Назад", f"home:{route}:{page - 1}"))
    if page + 1 < pages:
        navigation.append(_button("Далее →", f"home:{route}:{page + 1}"))
    return (
        items[start : start + size],
        start,
        f"Страница {page + 1} из {pages}",
        ([navigation] if navigation else []),
    )


def _number(value) -> str:
    try:
        number = Decimal(str(value))
        if number.is_finite() and abs(number) < Decimal("1e18"):
            return f"{number:,.0f}".replace(",", " ")
    except InvalidOperation, ValueError, TypeError:
        pass
    return "не указано"


def _when(value, timezone=None) -> str:
    try:
        moment = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
    except ValueError, TypeError:
        return "время не указано"
    zone = None
    if timezone:
        try:
            zone = ZoneInfo(str(timezone))
        except ZoneInfoNotFoundError, ValueError, TypeError:
            pass
    if moment.tzinfo is None:
        if zone is None:
            return moment.strftime("%d.%m.%Y, %H:%M") + " · часовой пояс не указан"
        moment = moment.replace(tzinfo=zone)
    elif zone is not None:
        moment = moment.astimezone(zone)
    offset = moment.strftime("%z")
    offset = "UTC" if offset == "+0000" else f"UTC{offset[:3]}:{offset[3:]}"
    label = f"{zone.key} ({offset})" if zone is not None else offset
    return moment.strftime("%d.%m.%Y, %H:%M") + " · " + _line(label, 100)


def _recurrence(seconds) -> str:
    if seconds is None:
        return "однократно"
    if not isinstance(seconds, (int, float)) or isinstance(seconds, bool) or seconds <= 0:
        return "интервал не указан"
    if seconds == 86400:
        return "ежедневно"
    if seconds == 604800:
        return "еженедельно"
    for divisor, unit in ((86400, "дн."), (3600, "ч."), (60, "мин.")):
        if seconds % divisor == 0:
            return f"каждые {_number(seconds / divisor)} {unit}"
    return f"каждые {_number(seconds)} сек."


def main_panel(overview: dict | None = None) -> dict:
    overview = overview or {}
    rows = []
    text = f"{HOME_TITLE}\n\nПривет! Я твой личный AI-агент для жизни и работы. "
    projects = overview.get("projects", [])
    if projects and overview.get("suggestions", True):
        text += "Вот где можно продолжить:\n"
        for project in projects[:3]:
            text += f"\n• {_line(project['name'], 100)} — {_line(project.get('state', {}).get('next_step'), 220, 'выбрать следующий шаг')}"
            rows.append(
                [
                    _button(
                        _line("Продолжить: " + project["name"], 22), f"home:project:{project['id']}"
                    )
                ]
            )
        text += "\n\nПоказаны активные проекты и сохранённые следующие шаги. Предложение можно скрыть в карточке проекта."
    else:
        text += (
            "Со мной можно просто поговорить или поручить задачу: найти информацию, "
            "разобраться в документе, подготовить файл или спланировать день."
        )
        if overview.get("suggestions", True):
            text += "\n\nНачни со своей задачи. Например: «Хочу заниматься дома по 20 минут» или «Помоги подготовиться к собеседованию». Анкета не нужна; уточним детали по ходу."
    if overview.get("results"):
        text += "\n\nНедавние результаты:"
        for result in overview["results"][:3]:
            text += f"\n• {_line(result['filename'], 120)} · версия {result['version']}"
            rows.append(
                [_button(_line("📎 " + result["filename"], 22), f"home:result:{result['id']}")]
            )
    if overview.get("tasks"):
        text += f"\n\nАктивных заданий: {overview['tasks']}. Они доступны в «Мои задачи»."
    text += "\n\nС твоего разрешения могу писать первым. Всё доступно обычными сообщениями; меню помогает начать."
    if overview.get("hidden_count"):
        rows.append([_button("Показать скрытые предложения", "home:show")])
    return _panel(
        text,
        rows
        + [
            [_button("➕ Новый чат", "chat:new")],
            [_button("🎯 Мои проекты", "home:projects:0")],
            [_button("💬 Мои чаты", "home:chats:0"), _button("📋 Мои задачи", "home:tasks:0")],
            [_button("💳 Мой тариф", "home:plans"), _button("📖 Что могу", "home:guide")],
            [_button("🧠 Память", "home:memory:0"), _button("⚙️ Настройки", "home:settings")],
        ],
        home=False,
    )


def guide_panel() -> dict:
    return _panel(
        "📖 Что умеет Cronos\n\n"
        "1. Отдельные чаты\nСоздай чат для новой темы. Название появится после моего ответа. "
        "История у каждого чата своя; проекты можно продолжать в разных чатах. Переключайся через список тем Telegram.\n\n"
        "2. Жизнь и работа\nПомогу с планами, учёбой, письмами, идеями и повседневными вопросами. "
        "Можно просто поговорить. Попроси напоминание или регулярный отчёт; "
        "для моих собственных инициатив сначала нужно твоё разрешение.\n\n"
        "3. Файлы и фото\nПришли PDF, CSV, XLSX, DOCX, TXT или фото — помогу разобраться. "
        "Могу подготовить таблицу, документ или PDF по твоей задаче.\n\n"
        "4. Поиск в интернете\nНайду свежую информацию и сохраню ссылки на источники.\n\n"
        "5. Долгие дела\n«Сохрани как проект»; «Где остановились?»; «Эту вводную запомни только для проекта до пятницы». "
        "У проекта есть цель, решения и следующий шаг.\n\n"
        "6. Библиотека и версии\n«Найди прежнюю таблицу»; «Верни предыдущую версию». "
        "При изменении файла исходник сохраняется, поиск показывает источники.\n\n"
        "7. Сценарии и рецепты\nПланы питания, тренировок, обучения, контента и дневник самочувствия "
        "учитывают твою обратную связь. «Сохрани этот порядок как рецепт» — для повторения с новыми материалами.\n\n"
        "8. Управление обычными словами\nНапиши, что запомнить, забыть, поменять в настройках "
        "или заданиях. Инициативу можно настроить для проекта с конкретными условиями и отключить в любой момент. "
        "Удаление чата и полная очистка требуют отдельного подтверждения.",
        [[_button("➕ Новый чат", "chat:new"), _button("⚙️ Настройки", "home:settings")]],
    )


def projects_panel(projects: list[dict], page: int = 0):
    active = [p for p in projects if p.get("status") == "active" and not p.get("needs_context")]
    selected, start, footer, navigation = _page(active, page, 6, "projects")
    text = "🎯 Мои проекты\n\n" + (
        "\n".join(
            f"{start + i}. {_line(p['name'], 120)} — {_line(p.get('goal'), 160)}"
            for i, p in enumerate(selected, 1)
        )
        or "Пока нет активных проектов. Расскажи о длительном деле и попроси сохранить его как проект."
    )
    rows = [[_button(_line(p["name"], 22), f"home:project:{p['id']}")] for p in selected]
    if active:
        text += "\n\n" + footer
    return _panel(text, rows + navigation)


def project_panel(project: dict | None, chats: list[dict] | None = None):
    if not project:
        return _panel(
            "Проект завершён, удалён или его контекст очищен. Открой актуальное меню.", []
        )
    state = project.get("state", {})
    text = f"🎯 {_line(project['name'], 180)}\n\nЦель: {_line(project.get('goal'), 500)}"
    if state.get("summary"):
        text += f"\n\nГде остановились: {_line(state['summary'], 800)}"
    text += f"\n\nСледующий шаг: {_line(state.get('next_step'), 600, 'пока не выбран')}"
    if chats:
        text += "\n\nМожно продолжить в существующем чате из списка тем Telegram: " + ", ".join(
            _line(c.get("title"), 80, "Новый чат") for c in chats[:3]
        )
    text += "\n\nКарточка основана на сохранённом состоянии проекта. Исправить его можно обычным сообщением."
    return _panel(
        text,
        [
            [_button("Продолжить в новом чате", f"home:continue:{project['id']}")],
            [_button("Скрыть предложение", f"home:hide:{project['id']}")],
        ],
    )


def chats_panel(conversations: list[dict], page: int = 0) -> dict:
    chats = [row for row in conversations if not row.get("is_home") and row.get("thread_id", 0) > 1]
    rows, start, footer, navigation = _page(chats, page, 6, "chats")
    body = "\n".join(
        f"{start + index}. {_line(row.get('title'), 160, 'Новый чат')}"
        for index, row in enumerate(rows, 1)
    )
    text = "💬 Мои чаты\n\n" + (body or "Пока нет отдельных чатов. Создай первый для своей задачи.")
    text += "\n\nЧтобы открыть чат, выбери его в списке тем Telegram."
    if chats:
        text += "\n" + footer
    return _panel(text, [[_button("➕ Новый чат", "chat:new")], *navigation])


def tasks_panel(schedules: list[dict], page: int = 0) -> dict:
    active = [row for row in schedules if row.get("state", "active") == "active"]
    rows, start, footer, navigation = _page(active, page, 5, "tasks")
    entries = []
    for index, row in enumerate(rows, start + 1):
        subject = _line(row.get("instruction") or row.get("fixed_text"), 220, "Задание")
        mode = "Новый результат при запуске" if row.get("dynamic") else "Сохранённый текст"
        source = "Инициатива Cronos" if row.get("proactive") else "По твоему запросу"
        entries.append(
            f"{index}. {subject}\n{_when(row.get('due_at'), row.get('timezone'))}\n"
            f"{_recurrence(row.get('interval_seconds'))} · {mode}\n{source}"
        )
    text = "📋 Мои задачи\n\n" + (
        "\n\n".join(entries)
        or "Активных заданий пока нет. "
        "Например, напиши: «Каждый день в 10:00 по Москве присылай сводку новостей» "
        "или «Напомни завтра в 18:00 по Москве позвонить». Уточни время и часовой пояс."
    )
    if active:
        text += "\n\nИзменить или отменить задание можно обычным сообщением.\n" + footer
    return _panel(text, navigation)


def plans_panel(balance: dict) -> dict:
    text = (
        f"💳 Мой тариф\n\nСейчас: {_line(balance.get('plan'), 32, 'не указан')}\n"
        f"Остаток: {_number(balance.get('tokens_remaining'))} токенов\n"
        f"Обновление лимита: {_when(balance.get('period_end'))}"
    )
    if balance.get("pending_plan"):
        text += f"\nСо следующего периода: {_line(balance['pending_plan'], 32)}"
    if balance.get("soft_limits"):
        text += "\nВ альфе лимиты мягкие: исчерпание баланса не блокирует общение."
    text += (
        "\n\nFREE — 25 000 токенов в день\n"
        "START — 700 000 токенов в месяц · 990 ₽/мес.\n"
        "PREMIUM — 1 700 000 токенов в месяц · 1 990 ₽/мес.\n"
        "PRO — 3 700 000 токенов в месяц · 3 990 ₽/мес.\n\n"
        "Сейчас переключение тарифа тестовое: оплаты и списания денег нет. "
        "Повышение применяется сразу, понижение — со следующего периода."
    )
    return _panel(
        text,
        [
            [_button("FREE · бесплатный", "plan:FREE")],
            *[
                [_button(f"{name} · тест, без оплаты", f"plan:{name}")]
                for name in ("START", "PREMIUM", "PRO")
            ],
        ],
    )


def settings_panel(preferences: dict) -> dict:
    proactive = bool(preferences.get("proactivity"))
    return _panel(
        "⚙️ Настройки\n\n"
        f"Часовой пояс: {_line(preferences.get('timezone'), 100, 'не указан')}\n"
        f"Стиль общения: {_line(preferences.get('tone'), 200, 'не указан')}\n"
        f"Писать первым: {'разрешено' if proactive else 'выключено'}\n\n"
        "Настройки можно менять обычным сообщением. Например: «Отвечай короче» "
        "или «Мой часовой пояс — Europe/Moscow».\n\n"
        "Полная очистка удаляет персональные данные после отдельного подтверждения; тариф и баланс сохраняются.",
        [
            [
                _button(
                    "Выключить инициативу" if proactive else "Разрешить инициативу",
                    f"home:proactivity:{'off' if proactive else 'on'}",
                )
            ],
            [_button("🗑 Полная очистка", "home:clearall")],
        ],
    )


def memory_panel(memories: list[dict], page: int = 0) -> dict:
    rows, start, footer, navigation = _page(memories, page, 8, "memory")
    entries = []
    for index, row in enumerate(rows, 1):
        scope = {
            "global": "Для всех чатов",
            "project": "Для проекта",
            "conversation": "Для одного чата",
        }.get(row.get("scope", "global"), "Область не указана")
        if row.get("expires_at"):
            scope += " · до " + _when(row["expires_at"])
        entries.append(f"{start + index}. {_line(row.get('content'), 200, 'Без текста')}\n{scope}")
    text = "🧠 Память\n\n" + (
        "\n\n".join(entries)
        or "Пока ничего не сохранено. Расскажи о своих предпочтениях или напиши: «Запомни, что…»."
    )
    text += "\n\nПамять бывает общей, для проекта или отдельного чата; у временных фактов есть срок. Чтобы убрать факт, напиши: «Забудь, что…»."
    if memories:
        text += "\n" + footer
    return _panel(text, navigation)
