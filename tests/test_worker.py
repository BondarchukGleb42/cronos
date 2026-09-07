import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from cronos.agent import CancelledRun
from cronos.providers import ProviderError
from cronos.telegram import new_chat_keyboard
from cronos.worker import Worker


@pytest.fixture
def worker_case():
    worker = Worker.__new__(Worker)
    conversation = {"id": uuid4(), "user_id": -920005, "chat_id": -920005, "thread_id": 3}
    event = {"id": uuid4()}
    run = {"id": uuid4(), "fence": 2, "memory_revision": 0, "user_id": -920005}
    worker.store = SimpleNamespace(
        start_run=AsyncMock(return_value=run),
        enqueue_for_run=AsyncMock(return_value=1),
        ensure_user=AsyncMock(return_value={"memory_revision": 0}),
        add_message=AsyncMock(),
        queue_topic_title=AsyncMock(),
        finish_run=AsyncMock(),
        run_metrics=AsyncMock(),
    )
    worker.transport = SimpleNamespace(draft=AsyncMock())
    worker.agent = SimpleNamespace(run=AsyncMock(return_value="Готовый ответ"))
    return SimpleNamespace(worker=worker, event=event, conversation=conversation, run=run)


async def test_completed_answer_has_durable_final_message_after_ephemeral_preview(worker_case):
    case = worker_case
    await case.worker.answer(case.event, case.conversation, "Вопрос")
    case.worker.transport.draft.assert_awaited_once_with(
        -920005, 3, "Сейчас разберусь…", case.run["id"].int % 2_000_000_000 + 1
    )
    args = case.worker.store.enqueue_for_run.call_args.args
    assert args[2] == {
        "text": "Готовый ответ",
        "format": "rich",
        "reply_markup": new_chat_keyboard(),
    }
    assert args[3] == f"answer:{case.event['id']}"
    case.worker.store.finish_run.assert_awaited_once_with(case.run["id"], "done", fence=2)


async def test_unexpected_agent_error_is_failed_and_propagates_for_event_retry(worker_case):
    case = worker_case
    case.worker.agent.run.side_effect = RuntimeError("unexpected response")
    with pytest.raises(RuntimeError, match="unexpected response"):
        await case.worker.answer(case.event, case.conversation, "Вопрос")
    case.worker.store.enqueue_for_run.assert_not_awaited()
    case.worker.store.finish_run.assert_awaited_once_with(case.run["id"], "failed", fence=2)


async def test_process_interruption_is_not_reported_as_success(worker_case):
    case = worker_case
    case.worker.agent.run.side_effect = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await case.worker.answer(case.event, case.conversation, "Вопрос")
    case.worker.store.enqueue_for_run.assert_not_awaited()
    case.worker.store.finish_run.assert_awaited_once_with(case.run["id"], "interrupted", fence=2)


async def test_user_cancellation_prevents_final_answer_and_saves_cancelled_status(worker_case):
    case = worker_case
    case.worker.agent.run.side_effect = CancelledRun("stopped")
    await case.worker.answer(case.event, case.conversation, "Вопрос")
    case.worker.store.enqueue_for_run.assert_not_awaited()
    case.worker.store.add_message.assert_not_awaited()
    case.worker.store.finish_run.assert_awaited_once_with(case.run["id"], "cancelled", fence=2)


async def test_cancelled_delivery_does_not_save_a_completed_answer(worker_case):
    case = worker_case
    case.worker.store.enqueue_for_run.return_value = None
    status = await case.worker.answer(case.event, case.conversation, "Вопрос")
    assert status == "cancelled"
    case.worker.store.add_message.assert_not_awaited()
    case.worker.store.finish_run.assert_awaited_once_with(case.run["id"], "cancelled", fence=2)


async def test_provider_failure_keeps_user_error_in_durable_outbox(worker_case):
    case = worker_case
    case.worker.agent.run.side_effect = ProviderError("unavailable")
    await case.worker.answer(case.event, case.conversation, "Вопрос")
    payload = case.worker.store.enqueue_for_run.call_args.args[2]
    assert "Попробуй ещё раз" in payload["text"]
    case.worker.store.finish_run.assert_awaited_once_with(case.run["id"], "failed", fence=2)


async def test_proactive_failure_preserves_reminder_fallback_and_schedule_guards(worker_case):
    case = worker_case
    schedule = {"id": uuid4(), "revision": 4, "fixed_text": "Пора сделать перерыв"}
    case.worker.agent.run.side_effect = ProviderError("unavailable")
    await case.worker.answer(
        case.event, case.conversation, "Напоминание", proactive=True, schedule=schedule
    )
    case.worker.transport.draft.assert_not_awaited()
    assert case.worker.store.enqueue_for_run.call_args.args[2] == {
        "text": "Пора сделать перерыв",
        "schedule_id": str(schedule["id"]),
        "schedule_revision": 4,
    }


async def test_context_save_failure_does_not_mark_entire_event_successful(worker_case):
    case = worker_case
    case.worker.store.add_message.side_effect = RuntimeError("database unavailable")
    with pytest.raises(RuntimeError, match="database unavailable"):
        await case.worker.answer(case.event, case.conversation, "Вопрос")
    # The final message already exists; event replay must use its durable dedupe key.
    assert case.worker.store.enqueue_for_run.await_count == 1
    case.worker.store.finish_run.assert_awaited_once_with(case.run["id"], "failed", fence=2)


async def test_explicit_scheduled_report_uses_tools_mode_without_interactive_side_effects(
    worker_case,
):
    case = worker_case
    schedule = {"id": uuid4(), "revision": 1, "fixed_text": "Цены на огурцы"}
    status = await case.worker.answer(
        case.event, case.conversation, "Собери свежий CSV", schedule=schedule
    )
    assert status == "done"
    case.worker.agent.run.assert_awaited_once_with(
        case.run, case.conversation, "Собери свежий CSV", proactive=False, scheduled=True
    )
    case.worker.transport.draft.assert_not_awaited()
    case.worker.store.add_message.assert_not_awaited()
    case.worker.store.queue_topic_title.assert_not_awaited()
    payload = case.worker.store.enqueue_for_run.call_args.args[2]
    assert "reply_markup" not in payload
    assert payload["schedule_id"] == str(schedule["id"])


@pytest.mark.parametrize("attempt", [1, 2, 3])
async def test_scheduled_provider_failure_retries_then_reports_failure(worker_case, attempt):
    case = worker_case
    case.event["attempts"] = attempt
    schedule = {"id": uuid4(), "revision": 1, "fixed_text": "🔍 Ищу цены…"}
    case.worker.agent.run.side_effect = ProviderError("unavailable")
    with pytest.raises(ProviderError):
        await case.worker.answer(case.event, case.conversation, "Собери CSV", schedule=schedule)
    if attempt < 3:
        case.worker.store.enqueue_for_run.assert_not_awaited()
    else:
        payload = case.worker.store.enqueue_for_run.call_args.args[2]
        assert "Не удалось завершить" in payload["text"]
        assert "Следующий запуск" not in payload["text"]
        assert payload["text"] != schedule["fixed_text"]
        assert payload["schedule_revision"] == 1
    case.worker.store.finish_run.assert_awaited_once_with(case.run["id"], "failed", fence=2)


@pytest.fixture
def timer_case(worker_case):
    case = worker_case
    schedule = {
        "id": uuid4(),
        "revision": 1,
        "state": "active",
        "dynamic": True,
        "proactive": False,
        "user_id": case.conversation["user_id"],
        "chat_id": case.conversation["chat_id"],
        "thread_id": case.conversation["thread_id"],
        "instruction": "Найди свежие цены на огурцы и отправь CSV",
    }
    case.event.update(
        kind="timer",
        attempts=1,
        payload={"schedule_id": str(schedule["id"]), "revision": 1, "occurrence_id": str(uuid4())},
    )

    @asynccontextmanager
    async def user_lock(_):
        yield

    case.worker.store.user_lock = user_lock
    case.worker.store.get_schedule = AsyncMock(return_value=schedule)
    case.worker.store.occurrence_active = AsyncMock(return_value=True)
    case.worker.store.preferences = AsyncMock(return_value={"proactivity": False})
    case.worker.store.conversation = AsyncMock(return_value=case.conversation)
    case.worker.store.mark_occurrence = AsyncMock()
    case.worker.answer = AsyncMock(return_value="done")
    case.schedule = schedule
    return case


async def test_timer_runs_explicit_report_without_general_proactivity_consent(timer_case):
    case = timer_case
    await case.worker.timer(case.event)
    assert case.worker.answer.call_args.kwargs == {"proactive": False, "schedule": case.schedule}
    case.worker.store.mark_occurrence.assert_awaited_once_with(
        case.event["payload"]["occurrence_id"]
    )


async def test_completed_occurrence_replay_does_not_regenerate_report(timer_case):
    case = timer_case
    case.worker.store.occurrence_active.return_value = False
    await case.worker.timer(case.event)
    case.worker.answer.assert_not_awaited()


@pytest.mark.parametrize("attempt", [1, 3])
async def test_timer_generation_failure_is_never_marked_done(timer_case, attempt):
    case = timer_case
    case.event["attempts"] = attempt
    case.worker.answer.side_effect = ProviderError("unavailable")
    with pytest.raises(ProviderError):
        await case.worker.timer(case.event)
    if attempt == 1:
        case.worker.store.mark_occurrence.assert_not_awaited()
    else:
        case.worker.store.mark_occurrence.assert_awaited_once_with(
            case.event["payload"]["occurrence_id"], "failed"
        )
