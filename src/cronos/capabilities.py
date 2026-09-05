"""The executable skill registry is also the agent's source of self-knowledge."""

SKILLS = {
    "daily": {
        "summary": "Повседневные задачи, семья, питание, тренировки, работа, обучение и творчество.",
        "instructions": "Сначала помоги с текущей задачей. Адаптируй ответ под цели и стиль человека. Не проводи анкетирование: уточняй один действительно нужный факт за раз. Предлагай уместное продолжение: проверить прогресс, пересмотреть план, обсудить результат. Для поддержки самочувствия используй бережный разговор, не ставь диагнозы.",
    },
    "memory": {
        "summary": "Общая память между темами: предпочтения, цели и договорённости; показать, запомнить, забыть.",
        "instructions": "Сохраняй устойчивые факты, явно сообщённые пользователем, с источником текущего сообщения. Не превращай предположение в факт. Не запоминай пароли. Показывай и удаляй память по просьбе. Удаление сбрасывает контекст прошлых диалогов; не повторяй удалённое в ответе. Не пиши 'запомнил', пока memory_write не завершился успешно.",
    },
    "proactivity": {
        "summary": "Разовые и повторяющиеся сообщения, сопровождение планов, проверки прогресса, персональный тон.",
        "instructions": "Явное 'напомни' выполняй без дополнительного согласия, если известны дата и часовой пояс. Если время неоднозначно — уточни. При планировании питания, тренировок, учёбы или работы предложи уместное сопровождение. Возможность инициативных сообщений согласуется один раз через preferences_set(proactivity=true). Включённая проактивность разрешает создавать уместные follow-up самостоятельно. Не более согласованной частоты, учитывай quiet_start/quiet_end. Точное напоминание: dynamic=false. Контекстный check-in: dynamic=true, proactive=true, instruction описывает цель, text содержит готовый запасной текст. Все изменения доступны обычным текстом.",
    },
    "search": {
        "summary": "Поиск в интернете с реальными источниками; автоматически используется модель с подтверждённым поиском.",
        "instructions": "Для актуальных фактов, ссылок и просьб найти используй web_search. Поиск переключается на проверенный perplexity/sonar независимо от выбранной разговорной модели. Сохраняй ссылки в финальном ответе. Ошибка поиска не разрешает выдумывать источники.",
    },
    "files": {
        "summary": "Чтение PDF, CSV, Excel, DOCX и текста; анализ данных; создание PDF, XLSX, CSV, DOCX, TXT.",
        "instructions": "Файлы пользователя перечислены в контексте. Получай содержимое через file_read; большие документы читай последовательными порциями. Для числовых таблиц используй table_analyze — арифметику считает код. Генерируй файл через file_create, готовый результат отправляется пользователю. PDF без текстового слоя можно распознать через image_analyze с номером страницы. .xls, макросы и произвольное выполнение кода пока не поддерживаются; предложи сохранить в XLSX.",
    },
    "images": {
        "summary": "Анализ фотографий и страниц PDF; генерация и редактирование изображений.",
        "instructions": "Для изображения используй image_analyze; для генерации image_generate. Для редактирования передай artifact_id исходника. Не говори, что изображение создано без успешного результата инструмента.",
    },
    "topics": {
        "summary": "Отдельные темы диалога при общей личной памяти.",
        "instructions": "По просьбе пользователя создай Telegram topic через topic_create. Название — короткое и понятное. При недоступных private topics честно сообщи об этом; обычный чат продолжит работать. Список тем доступен через topics_list.",
    },
    "account": {
        "summary": "Модели, автоматическое усиление сложных задач через deep_reason, reasoning, баланс и тарифы; тестовое повышение тарифа и пополнение без оплаты.",
        "instructions": "В альфе тарифы не блокируют разговор. balance показывает фактические расходы и остаток. upgrade показывает кнопки START/PREMIUM/PRO, нажатие сразу применяет тестовый тариф. По прямой просьбе пользователя plan_change тоже меняет тариф без оплаты. Деньги не списываются. Модель и reasoning меняются preferences_set, список моделей через models_list. При несовместимости инструментов система использует подходящую модель.",
    },
}


def tool(name, description, properties=None, required=()):
    return {
        "type": "function",
        "function": {
            "name": name,
            "description": description,
            "parameters": {
                "type": "object",
                "properties": properties or {},
                "required": list(required),
                "additionalProperties": False,
            },
        },
    }


def string(description="", enum=None):
    value = {"type": "string", "description": description}
    if enum:
        value["enum"] = enum
    return value


TOOLS = [
    tool(
        "deep_reason",
        "Для сложной логики, математики, сравнения стратегий или неоднозначного плана передай подзадачу reasoning-модели. Выбирай автоматически по сложности.",
        {"problem": string("Полная постановка с нужными данными и ограничениями")},
        ["problem"],
    ),
    tool(
        "skill_info",
        "Подробные инструкции об одном из собственных навыков.",
        {"skill": string(enum=list(SKILLS))},
        ["skill"],
    ),
    tool("memory_list", "Показать, что Cronos помнит о пользователе."),
    tool(
        "memory_write",
        "Запомнить устойчивый факт, явно сообщённый пользователем.",
        {"content": string(), "category": string()},
        ["content"],
    ),
    tool(
        "memory_forget",
        "Забыть факт и исключить старые диалоги из активного контекста.",
        {"query": string()},
        ["query"],
    ),
    tool(
        "preferences_set",
        "Изменить предпочтения явно по просьбе или согласию пользователя.",
        {
            "timezone": string("Часовой пояс IANA, например Asia/Novosibirsk"),
            "tone": string(),
            "proactivity": {"type": "boolean"},
            "initiative_limit": {"type": "integer", "minimum": 1, "maximum": 10},
            "quiet_start": {"type": "integer", "minimum": 0, "maximum": 23},
            "quiet_end": {"type": "integer", "minimum": 0, "maximum": 23},
            "model": string(),
            "reasoning": {"type": "boolean"},
        },
    ),
    tool(
        "web_search",
        "Найти актуальную информацию в интернете с проверяемыми ссылками.",
        {"query": string()},
        ["query"],
    ),
    tool(
        "schedule_create",
        "Создать напоминание или инициативное продолжение разговора.",
        {
            "due_at": string(
                "ISO 8601 с часовым поясом. Если пояс пользователя неизвестен — сначала уточни."
            ),
            "text": string("Готовый текст на случай недоступности модели"),
            "instruction": string("Цель будущего обращения"),
            "dynamic": {"type": "boolean"},
            "proactive": {"type": "boolean"},
            "interval_seconds": {"type": "integer", "minimum": 60},
        },
        ["due_at", "text"],
    ),
    tool("schedules_list", "Все активные запланированные действия."),
    tool(
        "schedule_change",
        "Отменить или перенести запланированное действие.",
        {"id": string(), "action": string(enum=["cancel", "reschedule"]), "due_at": string()},
        ["id", "action"],
    ),
    tool("files_list", "Список файлов пользователя."),
    tool(
        "file_read",
        "Прочитать документ или таблицу, порциями для большого текста.",
        {
            "artifact_id": string(),
            "offset": {"type": "integer", "minimum": 0},
            "length": {"type": "integer", "minimum": 100, "maximum": 24000},
        },
        ["artifact_id"],
    ),
    tool(
        "table_analyze",
        "Реальные вычисления над столбцом таблицы: sum, mean, min, max, count.",
        {
            "artifact_id": string(),
            "table": {"type": "integer", "minimum": 0},
            "column": string(),
            "operation": string(enum=["sum", "mean", "min", "max", "count"]),
        },
        ["artifact_id", "column", "operation"],
    ),
    tool(
        "file_create",
        "Создать и отправить документ или таблицу. Для CSV/XLSX передавай columns и rows. Сохраняй язык, названия колонок и значения точно по запросу пользователя, не переводи их.",
        {
            "format": string(enum=["pdf", "xlsx", "csv", "docx", "txt"]),
            "filename": string(),
            "content": string(
                "Текст документа; при заполненных columns и rows можно передать пустую строку. Для CSV также допустим готовый CSV-текст с разделителями, без Markdown."
            ),
            "columns": {
                "type": "array",
                "items": string(),
                "description": "Названия колонок в порядке и на языке пользователя, без перевода.",
            },
            "rows": {
                "type": "array",
                "description": "Строки ячеек в том же порядке, что columns. Числа передавай числами; текстовые значения сохраняй без перевода.",
                "items": {
                    "type": "array",
                    "items": {"type": ["string", "number", "boolean", "null"]},
                },
            },
        },
        ["format", "filename", "content"],
    ),
    tool(
        "image_analyze",
        "Рассмотреть изображение или страницу PDF, прочитать скан.",
        {"artifact_id": string(), "question": string(), "page": {"type": "integer", "minimum": 1}},
        ["artifact_id", "question"],
    ),
    tool(
        "image_generate",
        "Сгенерировать изображение или отредактировать исходное; отправить результат.",
        {"prompt": string(), "artifact_id": string()},
        ["prompt"],
    ),
    tool("topics_list", "Показать отдельные темы диалога."),
    tool(
        "topic_create",
        "Создать отдельную тему Telegram по просьбе пользователя.",
        {"name": string()},
        ["name"],
    ),
    tool("models_list", "Доступные модели провайдера.", {"query": string()}),
    tool("balance", "Тариф, остаток и фактически потраченные токены."),
    tool("upgrade", "Показать кнопки повышения тарифа без реальной оплаты."),
    tool(
        "plan_change",
        "Изменить тестовый тариф по прямой просьбе пользователя.",
        {"plan": string(enum=["FREE", "START", "PREMIUM", "PRO"])},
        ["plan"],
    ),
    tool(
        "top_up",
        "Тестовое пополнение токенов без оплаты по просьбе пользователя.",
        {"tokens": {"type": "integer", "minimum": 1}},
        ["tokens"],
    ),
]


def catalog_context():
    return "\n".join(f"- {name}: {value['summary']}" for name, value in SKILLS.items())
