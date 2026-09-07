import pytest

from cronos.action_confirmation import UNCONFIRMED_SCHEDULE_TEXT, needs_schedule_repair


def created(**overrides):
    return {
        "name": "schedule_create",
        "result": {
            "id": "schedule-cucumbers",
            "due_at": "2026-09-08T10:00:00+03:00",
            "timezone": "Europe/Moscow",
            "text": "Цены на огурцы",
            "interval_seconds": 86400,
            **overrides,
        },
    }


@pytest.mark.parametrize(
    "answer",
    [
        "Настроил ежедневное уведомление о ценах на огурцы.",
        "Создал расписание отправки отчётов.",
        "I created a report delivery schedule.",
        "Буду присылать каждое утро CSV с ценами на огурцы в 10:00 МСК.",
        "Напоминание создано.",
        "Перенёс напоминание на завтра.",
        "Напомню завтра о встрече.",
        "I've created a daily reminder.",
        "I'll send cucumber prices every morning.",
        "The reminder was rescheduled.",
        "Не волнуйся, я создал напоминание.",
        "Напоминание не создано, но буду присылать каждое утро.",
    ],
)
def test_explicit_unsupported_claims_need_repair(answer):
    assert needs_schedule_repair(answer, [])


@pytest.mark.parametrize(
    "answer",
    [
        "Я не создал напоминание.",
        "Напоминание пока не было создано.",
        "Напоминание создано не было.",
        "Не буду присылать каждое утро.",
        "Могу настроить напоминание, если скажешь время.",
        "Если подтвердите, буду присылать каждое утро.",
        "Хочешь, буду присылать каждое утро?",
        "Напоминание создано?",
        "Цены меняются каждый день; для сравнения нужно учитывать сезон.",
        "Создал расписание тренировок. Вот таблица по дням.",
        "Обновил ежедневное расписание. Вот готовый план.",
        "I created a daily schedule. Here is the table.",
        "Ты написал: «Буду присылать каждое утро». Это только цитата.",
        "> Напоминание создано.\nЭто цитата пользователя.",
        'Пример ответа: "Напоминание создано".',
        "`Напоминание создано` — пример строки.",
        "I haven't created a reminder.",
        "If you agree, I'll send prices every morning.",
        "You wrote: I'll send prices every morning.",
        UNCONFIRMED_SCHEDULE_TEXT,
    ],
)
def test_negations_offers_quotes_and_explanations_do_not_trigger(answer):
    assert not needs_schedule_repair(answer, [])


def test_original_failed_tool_and_later_consent_do_not_prove_a_schedule_exists():
    failed = {
        "name": "schedule_create",
        "result": {"error": "proactivity permission required", "completed": False},
    }
    consent = {"name": "preferences_update", "result": {"proactivity": True}}
    assert needs_schedule_repair("Настроил ежедневное уведомление.", [failed, consent])
    assert needs_schedule_repair("Буду присылать каждое утро.", [])


def test_successful_creation_supports_matching_confirmation_and_future_delivery():
    assert not needs_schedule_repair(
        "Напоминание создано. Буду присылать цены на огурцы каждый день в 10:00.", [created()]
    )


def test_successful_original_action_is_not_rejected_for_conversational_preface():
    assert not needs_schedule_repair(
        "Отлично! Раз ты разрешил писать первым, я настроил ежедневное уведомление.",
        [created(text="🔍 Ищу актуальные цены на огурцы")],
    )


@pytest.mark.parametrize("overrides", [{"error": "failed"}, {"completed": False}, {"id": None}])
def test_failure_with_success_looking_fields_is_not_evidence(overrides):
    assert needs_schedule_repair("Напоминание создано.", [created(**overrides)])


def test_unrelated_tools_or_unrelated_schedule_do_not_support_new_creation():
    active = {**created()["result"], "state": "active", "text": "Полить цветы"}
    assert needs_schedule_repair("Настроил ежедневное уведомление.", [], [active])
    assert needs_schedule_repair("Буду присылать цены на огурцы каждый день.", [], [active])
    assert needs_schedule_repair(
        "Напоминание создано.", [{"name": "memory_write", "result": created()["result"]}]
    )


def test_active_schedule_can_support_existing_claim_but_not_a_fresh_change():
    active = {**created()["result"], "state": "active"}
    assert not needs_schedule_repair(
        "Уже есть ежедневное напоминание о ценах на огурцы.", [], [active]
    )
    assert not needs_schedule_repair(
        "Буду присылать цены на огурцы каждый день в 10:00.", [], [active]
    )
    assert needs_schedule_repair("Перенёс напоминание о ценах на огурцы.", [], [active])
    assert needs_schedule_repair("Буду присылать цены на огурцы каждый день в 11:00.", [], [active])
    assert needs_schedule_repair(
        "Буду присылать цены на огурцы каждый день.", [], [{**active, "interval_seconds": None}]
    )


def test_change_needs_matching_action_and_later_cancel_invalidates_creation():
    cancel = {"name": "schedule_change", "result": {"id": "schedule-cucumbers", "action": "cancel"}}
    reschedule = {
        "name": "schedule_change",
        "result": {"id": "schedule-cucumbers", "action": "reschedule"},
    }
    assert needs_schedule_repair("Перенёс напоминание.", [created(), cancel])
    assert not needs_schedule_repair("Перенёс напоминание.", [reschedule])
    assert not needs_schedule_repair("Напоминание отменено.", [cancel])
    assert needs_schedule_repair("Напоминание создано.", [created(), cancel])
