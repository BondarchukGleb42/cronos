import asyncio
import threading
from contextlib import asynccontextmanager
from copy import deepcopy
from uuid import uuid4

import httpx
import pytest

from cronos.privacy import (
    CHAT_CONFIRMATION,
    FULL_CONFIRMATION,
    confirmation_keyboard,
    confirmation_scope,
    confirmation_text,
    perform_erasure,
)


def request(scope="all"):
    return {
        "id": str(uuid4()),
        "user_id": 42,
        "chat_id": 42,
        "thread_id": 77,
        "scope": scope,
        "state": "ready",
        "job": {
            "delete_user_files": scope == "all",
            "topic_ids": [77, 88],
            "general_message_ids": [101, 102],
            "general_history_limited": True,
        },
    }


@pytest.mark.parametrize(
    ("text", "scope"),
    [
        (FULL_CONFIRMATION, "all"),
        ("  Подтверждаю полную очистку.  ", "all"),
        (CHAT_CONFIRMATION, "chat"),
        ("ПОДТВЕРЖДАЮ УДАЛЕНИЕ ЧАТА!", "chat"),
    ],
)
def test_only_complete_confirmation_phrase_is_accepted(text, scope):
    assert confirmation_scope(text) == scope


@pytest.mark.parametrize(
    "text",
    [
        "/clearall",
        "Удали всё",
        "Очисти память и файлы",
        "Удалить чат",
        "Да",
        "не подтверждаю полную очистку",
        "подтверждаю полную очистку не сейчас",
        "если я подтверждаю полную очистку, что произойдёт?",
        "подтверждаю полную очистку?",
        "«подтверждаю полную очистку»",
        '"подтверждаю удаление чата"',
        "`подтверждаю полную очистку`",
        "> подтверждаю удаление чата",
        "я написал: подтверждаю полную очистку",
        "подтверждаю удаление чата и всей памяти",
        "подтверждаю полную очистку\nпожалуйста",
    ],
)
def test_requests_negations_quotes_and_substrings_are_not_confirmation(text):
    assert confirmation_scope(text) is None


def test_confirmation_ui_explains_scope_without_claiming_completed_erasure():
    full, chat, general = request(), request("chat"), request("chat")
    general["thread_id"] = 0
    full_text, chat_text, general_text = map(confirmation_text, (full, chat, general))
    assert "Полная очистка удалит" in full_text
    assert "Тариф, баланс и учтённые расходы сохранятся" in full_text
    assert "Общая память, библиотека файлов, другие чаты и тариф сохранятся" in chat_text
    assert "общий чат останется" in general_text
    for value, text in ((full, full_text), (chat, chat_text), (general, general_text)):
        assert "Отменить удаление нельзя" in text and "15 минут" in text
        assert confirmation_scope(text) is None
        keyboard = confirmation_keyboard(value)["inline_keyboard"]
        assert len(keyboard) == 2
        token = value["id"].replace("-", "")
        assert keyboard[0][0]["callback_data"] == f"privacy:confirm:{token}"
        assert keyboard[1][0]["callback_data"] == f"privacy:cancel:{token}"


class FakeStore:
    def __init__(self, descriptor, log):
        self.descriptor = descriptor
        self.log = log
        self.purges = 0
        self.finished = []
        self.begin_error = self.finish_error = None

    @asynccontextmanager
    async def user_lock(self, user_id, *, purpose="work"):
        assert user_id == 42
        self.log.append(f"lock:{purpose}")
        try:
            yield
        finally:
            self.log.append(f"unlock:{purpose}")

    async def begin_privacy_erasure(self, request_id, event_id):
        self.log.append("begin")
        if self.begin_error:
            raise self.begin_error
        if self.descriptor is None:
            return None
        assert request_id == self.descriptor["id"] and event_id
        if self.descriptor["state"] == "ready":
            self.purges += 1
            self.descriptor["state"] = "erasing"
        return deepcopy(self.descriptor)

    async def finish_privacy_erasure(self, request_id, result):
        self.log.append("finish")
        if self.finish_error:
            raise self.finish_error
        self.finished.append(result)
        self.descriptor["state"] = "done"
        return True


class FakeArtifacts:
    def __init__(self, log):
        self.log, self.error = log, None

    def erase_user(self, user_id):
        assert user_id == 42
        self.log.append("files")
        if self.error:
            raise self.error
        return {"removed": False}  # Already absent is a successful erasure too.


class FakeTransport:
    def __init__(self, log):
        self.log = log
        self.topic_error = self.messages_error = None
        self.message_result = {"cleared": 2, "failed": 0, "unavailable_ids": []}

    async def delete_topic(self, chat_id, thread_id):
        assert chat_id == 42
        self.log.append(f"topic:{thread_id}")
        if self.topic_error:
            raise self.topic_error
        return True

    async def delete_messages(self, chat_id, message_ids):
        assert chat_id == 42 and message_ids == [101, 102]
        self.log.append("messages")
        if self.messages_error:
            raise self.messages_error
        return self.message_result


@pytest.fixture
def service():
    log = []
    descriptor = request()
    event = {
        "id": uuid4(),
        "attempts": 1,
        "payload": {"user_id": 42, "request_id": descriptor["id"]},
    }
    return FakeStore(descriptor, log), FakeTransport(log), FakeArtifacts(log), event, log


async def test_erasure_holds_both_locks_and_finishes_after_files_and_telegram(service):
    store, transport, artifacts, event, log = service
    await perform_erasure(store, transport, artifacts, event)
    assert log == [
        "lock:work",
        "lock:delivery",
        "begin",
        "files",
        "topic:77",
        "topic:88",
        "messages",
        "finish",
        "unlock:delivery",
        "unlock:work",
    ]
    assert store.finished == [
        {
            "database_erased": True,
            "files_erased": True,
            "files_failed": False,
            "topics_cleared": 2,
            "topics_failed": 0,
            "messages_cleared": 2,
            "messages_failed": 0,
            "general_history_limited": True,
            "billing_preserved": True,
        }
    ]


async def test_chat_erasure_preserves_file_library(service):
    store, transport, artifacts, event, log = service
    store.descriptor["job"]["delete_user_files"] = False
    await perform_erasure(store, transport, artifacts, event)
    assert "files" not in log
    assert store.finished[0]["files_erased"] is False
    assert store.finished[0]["files_failed"] is False


async def test_stale_event_performs_no_file_remote_or_finish_operation(service):
    store, transport, artifacts, event, log = service
    store.descriptor = None
    await perform_erasure(store, transport, artifacts, event)
    assert log == ["lock:work", "lock:delivery", "begin", "unlock:delivery", "unlock:work"]


@pytest.mark.parametrize("phase", ["files", "topics", "messages"])
async def test_first_two_failures_retry_same_database_job_then_finish_honestly(service, phase):
    store, transport, artifacts, event, _ = service
    error = (
        OSError("private path must not enter result")
        if phase == "files"
        else httpx.ProxyError("proxy details must not enter result")
    )
    if phase == "files":
        artifacts.error = error
    elif phase == "topics":
        transport.topic_error = error
    else:
        transport.messages_error = error
    for attempt in (1, 2):
        with pytest.raises(type(error)) as caught:
            await perform_erasure(store, transport, artifacts, {**event, "attempts": attempt})
        assert caught.value is error
        assert not store.finished
        assert store.descriptor["state"] == "erasing"
    await perform_erasure(store, transport, artifacts, {**event, "attempts": 3})
    assert store.purges == 1 and store.descriptor["state"] == "done"
    result = store.finished[0]
    assert result["files_erased"] is (phase != "files")
    assert result["files_failed"] is (phase == "files")
    assert result["topics_failed"] == (2 if phase == "topics" else 0)
    assert result["messages_failed"] == (2 if phase == "messages" else 0)
    assert all(isinstance(value, (bool, int)) for value in result.values())


async def test_third_file_failure_still_attempts_remote_cleanup_and_finishes(service):
    store, transport, artifacts, event, log = service
    artifacts.error = PermissionError("file inaccessible")
    transport.topic_error = httpx.ConnectError("connection unavailable")
    transport.message_result = {"cleared": 1, "failed": 1, "unavailable_ids": [102]}
    await perform_erasure(store, transport, artifacts, {**event, "attempts": 3})
    assert "messages" in log and "finish" in log
    result = store.finished[0]
    assert result["files_failed"] and not result["files_erased"]
    assert result["topics_failed"] == 2
    assert result["messages_cleared"] == result["messages_failed"] == 1
    assert "unavailable_ids" not in result


@pytest.mark.parametrize("phase", ["begin", "finish"])
async def test_database_failures_are_never_reclassified_as_remote_partial_success(service, phase):
    store, transport, artifacts, event, _ = service
    error = RuntimeError("database failure")
    setattr(store, phase + "_error", error)
    with pytest.raises(RuntimeError) as caught:
        await perform_erasure(store, transport, artifacts, {**event, "attempts": 3})
    assert caught.value is error
    assert not store.finished


async def test_cancelled_file_cleanup_joins_thread_before_releasing_service_locks(service):
    store, transport, artifacts, event, log = service
    loop = asyncio.get_running_loop()
    started = asyncio.Event()
    finish = threading.Event()

    def erase(user_id):
        loop.call_soon_threadsafe(started.set)
        assert finish.wait(timeout=5), "test did not release file operation"
        log.append("files-finished")
        return {"removed": True}

    artifacts.erase_user = erase
    task = asyncio.create_task(perform_erasure(store, transport, artifacts, event))
    try:
        async with asyncio.timeout(5):
            await started.wait()
            task.cancel()
            cancellation_turn = asyncio.Event()
            loop.call_soon(cancellation_turn.set)
            await cancellation_turn.wait()
            assert not task.done() and "unlock:delivery" not in log
            finish.set()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert log[-3:] == ["files-finished", "unlock:delivery", "unlock:work"]
            assert "messages" not in log and not store.finished
    finally:
        finish.set()
        await asyncio.gather(task, return_exceptions=True)
