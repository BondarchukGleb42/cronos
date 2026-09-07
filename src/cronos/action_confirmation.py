"""Conservative schedule-claim guard, not a general natural-language verifier.

Callers must supply only tool results from the current run and freshly verified
active schedules. User text or remembered assistant promises are not evidence.
"""

import re
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

SCHEDULE_REPAIR_PROMPT = (
    "В предыдущем ответе есть неподтверждённое обещание о задании по расписанию. "
    "Сверь текущие результаты инструментов и фактически активные расписания. "
    "Согласие пользователя и прошлые слова ассистента не подтверждают сохранение. "
    "Если пользователь явно просит напоминание или регулярный отчёт/файл и известны время и часовой пояс, "
    "выполни доступный schedule_create с proactive=false либо schedule_change. "
    "Не создавай дубль уже подходящего активного расписания. "
    "Не считай ошибку инструмента успехом и не подменяй действие обещанием. "
    "Если действие нельзя подтвердить, честно сообщи это без обещания будущей отправки."
)
UNCONFIRMED_SCHEDULE_TEXT = (
    "Не удалось подтвердить создание или изменение задания по расписанию. "
    "Пока не считай это действие выполненным."
)

_SCHEDULE = re.compile(
    r"\b(?:напоминан\w*|уведомлен\w*|таймер\w*|reminders?|notifications?|alerts?)\b"
)
_TIMETABLE = re.compile(r"\b(?:расписан\w*|schedules?)\b")
_TEMPORAL = re.compile(
    r"\b(?:кажд\w*|ежеднев\w*|еженедель\w*|ежемесяч\w*|завтра|послезавтра|сегодня|"
    r"через\s+\d+|в\s+\d{1,2}\b|по\s+(?:утрам|вечерам)|daily|weekly|monthly|every|each|"
    r"tomorrow|tonight|at\s+\d{1,2}\b|in\s+\d+\s+(?:minutes?|hours?|days?))\b"
)
_DELIVERY = re.compile(r"\b(?:рассылк\w*|отправк\w*|доставк\w*|delivery|sending)\b")
_VERBS = {
    "create": re.compile(
        r"\b(?:создал[аи]?|создан[оаы]?|настроил[аи]?|настроен[оаы]?|добавил[аи]?|добавлен[оаы]?|"
        r"поставил[аи]?|поставлен[оаы]?|запланировал[аи]?|запланирован[оаы]?|установил[аи]?|установлен[оаы]?|"
        r"сохранил[аи]?|сохран[её]н[оаы]?|created|scheduled|added|saved|set\s+up|set)\b"
    ),
    "change": re.compile(
        r"\b(?:перен[её]с(?:ла|ли)?|перенес[её]н[оаы]?|изменил[аи]?|изменен[оаы]?|"
        r"обновил[аи]?|обновлен[оаы]?|rescheduled|updated|moved|changed)\b"
    ),
    "cancel": re.compile(
        r"\b(?:отменил[аи]?|отменен[оаы]?|удалил[аи]?|удален[оаы]?|cancelled|canceled|deleted|removed)\b"
    ),
}
_FUTURE = re.compile(
    r"\b(?:(?:буду|будем)\s+(?:\w+\s+){0,3}(?:присылать|отправлять|напоминать|уведомлять|сообщать)|"
    r"пришлю|отправлю|напомню|уведомлю|сообщу|"
    r"i(?:['’]ll|\s+will)\s+(?:\w+\s+){0,3}(?:send(?:ing)?|remind(?:ing)?|notify(?:ing)?)|"
    r"(?:напоминан\w*|уведомлен\w*)\s+буд(?:ет|ут)\s+(?:приходить|отправляться))\b"
)
_EXISTING = re.compile(
    r"\b(?:уже\s+(?:есть|создан\w*|настроен\w*|активн\w*)|существующ\w*|действующ\w*|активн\w*|already\s+(?:exists?|scheduled|set)|existing|active)\b"
)
_NONASSERTIVE = re.compile(
    r"\b(?:если|чтобы|чтоб|при\s+желании|хочешь|хотите|могу|можем|можно|предлагаю|например|"
    r"должно|следует|потребуется|необходимо|нельзя\s+(?:сказать|утверждать)|"
    r"не\s+(?:могу|удалось)\s+подтвердить|неправда|"
    r"if|unless|could|would|can\s+(?:create|set|schedule)|for\s+example|should|must|"
    r"(?:вы|ты|пользователь|you|user)\s+(?:написал\w*|сказал\w*|просил\w*|wrote|said|asked))\b"
)
_DAILY = re.compile(
    r"\b(?:ежеднев\w*|кажд\w*\s+(?:день|утро|вечер)|по\s+(?:утрам|вечерам)|daily|(?:every|each)\s+(?:day|morning|evening))\b"
)
_WEEKLY = re.compile(r"\b(?:еженедель\w*|кажд\w*\s+недел\w*|weekly|(?:every|each)\s+week)\b")
_CLOCK = re.compile(r"\b(?:в|at)\s+([01]?\d|2[0-3])(?::([0-5]\d))?\b")
_STOP = set(
    "я мы ты вы вам тебе тебя вас ваш твой для про при уже есть это этот эта все так как что чтобы "
    "буду будем будет будут каждый каждое каждую день утро вечер завтра сегодня теперь готово хорошо "
    "отлично успешно конечно точно мск москве времени час часов минуты минут csv pdf xlsx "
    "the this that your you our now already have has been will every each daily weekly morning evening "
    "tomorrow about for with and reminder notification schedule alert sending delivery".split()
)
_IGNORED_PREFIXES = (
    "напомин",
    "уведом",
    "распис",
    "созда",
    "создан",
    "настро",
    "добав",
    "постав",
    "установ",
    "сохран",
    "заплан",
    "перен",
    "измен",
    "обнов",
    "отмен",
    "удал",
    "ежеднев",
    "еженед",
    "ежемес",
    "кажд",
    "присыл",
    "отправ",
    "рассыл",
    "достав",
    "напомн",
    "сообщ",
    "актив",
    "действ",
    "существ",
    "created",
    "scheduled",
    "resched",
    "updated",
    "changed",
    "cancel",
    "removed",
    "deleted",
    "notify",
    "remind",
)


def _keywords(text: str) -> set[str]:
    words = re.findall(r"[a-zа-яё]{3,}", text.lower())
    result = set()
    for word in words:
        if word in _STOP or word.startswith(_IGNORED_PREFIXES):
            continue
        if re.search(r"[а-яё]", word):
            word = re.sub(
                r"(?:ами|ями|ого|ему|ах|ях|ов|ев|ой|ий|ый|ая|яя|ое|ее|а|я|ы|и|у|ю|е|о)$", "", word
            )
        if len(word) >= 3:
            result.add(word[:5])
    return result


def _negated(text: str, match: re.Match[str]) -> bool:
    before, after = text[max(0, match.start() - 60) : match.start()], text[match.end() :]
    return bool(
        re.search(r"(?:\bне|\bnot|\bnever|n't)\s+(?:\w+\s+){0,3}$", before)
        or re.match(r"\s+не\s+(?:был|была|было|были)\b", after)
    )


def _claims(text: str):
    # Quoted user examples and code are data, not the assistant's commitments.
    text = re.sub(r"```.*?```|`[^`]*`", " ", text, flags=re.S)
    text = re.sub(r'«[^»]*»|“[^”]*”|"[^"\n]*"|(?<!\w)\x27[^\x27\n]*\x27(?!\w)', " ", text)
    text = re.sub(r"(?m)^\s*>.*$", "", text).lower()
    for sentence in re.split(r"(?<=[.!?;])\s+|\n+", text):
        if sentence.rstrip().endswith("?") or _NONASSERTIVE.search(sentence):
            continue
        for clause in re.split(r",?\s+(?:но|однако|but|however)\s+", sentence):
            # A written timetable is not a background job without delivery context.
            has_schedule = bool(
                _SCHEDULE.search(clause)
                or (_TIMETABLE.search(clause) and _DELIVERY.search(clause))
            )
            target = has_schedule or bool(_TEMPORAL.search(clause) and _DELIVERY.search(clause))
            if has_schedule and _EXISTING.search(clause):
                if not re.search(r"\b(?:нет|не\s+актив|not\s+active|no\s+active)\w*", clause):
                    yield "existing", clause
                continue
            if target:
                action = next(
                    (
                        name
                        for name in ("cancel", "change", "create")
                        if any(not _negated(clause, m) for m in _VERBS[name].finditer(clause))
                    ),
                    None,
                )
                if action:
                    yield action, clause
                    continue
            for match in _FUTURE.finditer(clause):
                if not _negated(clause, match) and (
                    _TEMPORAL.search(clause) or re.search(r"напомн|напомин|remind", match.group())
                ):
                    yield "future", clause
                    break


def _successful(result: Any) -> bool:
    return (
        isinstance(result, dict)
        and "error" not in result
        and result.get("completed") is not False
        and result.get("success") is not False
        and result.get("status") not in {"failed", "rejected", "pending", "started", "unknown"}
        and bool(result.get("id"))
    )


def _matches(claim: str, row: dict, *, require_subject: bool = False) -> bool:
    subject = _keywords(claim)
    evidence = _keywords(
        " ".join(str(row.get(k) or "") for k in ("text", "fixed_text", "instruction"))
    )
    if require_subject and not subject:
        return False
    if require_subject and not subject.intersection(evidence):
        return False
    cadence = 86400 if _DAILY.search(claim) else 604800 if _WEEKLY.search(claim) else None
    if cadence is not None and row.get("interval_seconds") != cadence:
        return False
    clock = _CLOCK.search(claim)
    if clock and row.get("due_at"):
        try:
            due = datetime.fromisoformat(str(row["due_at"]))
            if row.get("timezone"):
                due = due.astimezone(ZoneInfo(row["timezone"]))
            if due.tzinfo is None or (due.hour, due.minute) != (int(clock[1]), int(clock[2] or 0)):
                return False
        except ValueError, TypeError, ZoneInfoNotFoundError:
            return False
    return True


def needs_schedule_repair(
    assistant_text: str,
    tool_results: list[dict],
    active_schedules: list[dict] | None = None,
) -> bool:
    """Flag unsupported explicit claims; tool_results are {name, result} records.

    This intentionally does not infer successful actions from arbitrary prose or
    an unrelated active reminder. Semantic paraphrases can still evade the guard.
    """
    created, changed, cancelled = {}, {}, {}
    for tool in tool_results:
        result = tool.get("result")
        if not isinstance(result, dict) or not _successful(result):
            continue
        identifier = str(result["id"])
        if tool.get("name") == "schedule_create" and result.get("due_at"):
            created[identifier] = result
            cancelled.pop(identifier, None)
        elif tool.get("name") == "schedule_change":
            if result.get("action") == "cancel":
                cancelled[identifier] = result
                created.pop(identifier, None)
                changed.pop(identifier, None)
            elif result.get("action") == "reschedule":
                changed[identifier] = result
                cancelled.pop(identifier, None)
    active = [
        row
        for row in active_schedules or []
        if isinstance(row, dict)
        and row.get("id")
        and row.get("state") == "active"
        and str(row["id"]) not in cancelled
    ]
    for kind, claim in _claims(assistant_text):
        if kind == "cancel":
            evidence = list(cancelled.values())
        elif kind == "change":
            evidence = list(changed.values())
        else:
            evidence = list(created.values())
            if kind in {"future", "existing"}:
                evidence += [row for row in active if _matches(claim, row, require_subject=True)]
        if not any(_matches(claim, row) for row in evidence):
            return True
    return False
