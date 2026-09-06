import asyncio
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
