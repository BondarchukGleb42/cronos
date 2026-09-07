import asyncio
from contextlib import asynccontextmanager
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest

from cronos.agent import CancelledRun
from cronos.privacy import confirmation_keyboard
from cronos.settings import Settings
from cronos.telegram import TelegramTransport, navigation_keyboard
from cronos.worker import Worker

USER_ID = 920072
THREAD_ID = 77


def photo_message(message_id, file_id, caption=None):
    return {
        "message_id": message_id,
        "message_thread_id": THREAD_ID,
        "chat": {"id": USER_ID, "type": "private"},
        "from": {"id": USER_ID, "is_bot": False},
        "photo": [{"file_id": file_id + "-thumbnail"}, {"file_id": file_id}],
        **({"caption": caption} if caption else {}),
    }


@pytest.fixture
def incoming_case():
    first = photo_message(101, "photo-one", "Сравни эти два изображения")
    second = photo_message(102, "photo-two")
    event = {
        "id": uuid4(),
        "kind": "telegram",
        "user_id": USER_ID,
        "payload": {"update_id": 201, "message": first, "media_group_messages": [first, second]},
    }
    conversation = {"id": uuid4(), "user_id": USER_ID, "chat_id": USER_ID, "thread_id": THREAD_ID}
    artifacts = {
        "photo-one": {"id": str(uuid4()), "filename": "one.jpg", "mime": "image/jpeg"},
        "photo-two": {"id": str(uuid4()), "filename": "two.jpg", "mime": "image/jpeg"},
        "sheet": {"id": str(uuid4()), "filename": "report.csv", "mime": "text/csv"},
    }
    state = {"locked": False}
    operations = {}

    @asynccontextmanager
    async def user_lock(user_id):
        assert user_id == USER_ID and not state["locked"]
        state["locked"] = True
        try:
            yield
        finally:
            state["locked"] = False

    async def current(event_id):
        assert state["locked"] and event_id == event["id"]
        return deepcopy(event)

    async def execute(sql, op_id, user_id, artifact):
        assert state["locked"] and user_id == USER_ID
        operations.setdefault(op_id, deepcopy(artifact))

    conn = SimpleNamespace(execute=AsyncMock(side_effect=execute))

    @asynccontextmanager
    async def connection():
        assert state["locked"]
        yield conn

    async def download(file_path, destination):
        assert state["locked"]
        destination.write(file_path.encode())

    def ingest(user_id, filename, data):
        assert state["locked"] and user_id == USER_ID
        return deepcopy(artifacts[data.decode()])

    worker = Worker.__new__(Worker)
    worker.store = SimpleNamespace(
        user_lock=user_lock,
        event_current=AsyncMock(side_effect=current),
        conversation=AsyncMock(return_value=conversation),
        operation=AsyncMock(side_effect=lambda key: operations.get(key)),
        save_artifact=AsyncMock(),
        connection=connection,
        enqueue=AsyncMock(),
        add_message=AsyncMock(),
        queue_home=AsyncMock(),
    )
    worker.transport = SimpleNamespace(
        bot=SimpleNamespace(
            get_file=AsyncMock(side_effect=lambda file_id: SimpleNamespace(file_path=file_id)),
            download_file=AsyncMock(side_effect=download),
        )
    )
    worker.agent = SimpleNamespace(artifacts=SimpleNamespace(ingest=Mock(side_effect=ingest)))
    worker.answer = AsyncMock()
    return SimpleNamespace(
        worker=worker,
        event=event,
        conversation=conversation,
        artifacts=artifacts,
        state=state,
        operations=operations,
        conn=conn,
    )


async def test_two_photo_album_has_one_answer_with_both_reference_images(incoming_case):
    case = incoming_case
    await case.worker.telegram(case.event)
    case.worker.answer.assert_awaited_once()
    actual_event, conversation, prompt = case.worker.answer.await_args.args
    assert actual_event == case.event and conversation == case.conversation
    assert prompt[0]["text"].startswith("Сравни эти два изображения")
    assert prompt[1:] == [
        {"type": "image_ref", "artifact_id": case.artifacts[name]["id"]}
        for name in ("photo-one", "photo-two")
    ]
    assert [call.args[0] for call in case.worker.transport.bot.get_file.await_args_list] == [
        "photo-one",
        "photo-two",
    ]
    assert case.worker.store.save_artifact.await_count == 2
    assert len(case.operations) == 2
    case.worker.store.queue_home.assert_awaited_once_with(USER_ID)
    case.worker.store.enqueue.assert_not_awaited()


async def test_message_with_photo_and_document_preserves_both_attachments(incoming_case):
    case = incoming_case
    payload = case.event["payload"]
    payload.pop("media_group_messages")
    payload["message"]["document"] = {"file_id": "sheet", "file_name": "report.csv"}
    await case.worker.telegram(case.event)
    case.worker.answer.assert_awaited_once()
    prompt = case.worker.answer.await_args.args[2]
    assert case.artifacts["sheet"]["id"] in prompt[0]["text"]
    assert case.artifacts["photo-one"]["id"] in prompt[0]["text"]
    assert prompt[1:] == [{"type": "image_ref", "artifact_id": case.artifacts["photo-one"]["id"]}]
    assert case.worker.store.save_artifact.await_count == 2
    assert [call.args[0] for call in case.worker.transport.bot.get_file.await_args_list] == [
        "sheet",
        "photo-one",
    ]


async def test_failed_album_download_never_generates_from_partial_reference_set(incoming_case):
    case = incoming_case
    case.worker.transport.bot.get_file.side_effect = [
        SimpleNamespace(file_path="photo-one"),
        TimeoutError("unavailable"),
    ]
    await case.worker.telegram(case.event)
    case.worker.answer.assert_not_awaited()
    case.worker.store.queue_home.assert_not_awaited()
    case.worker.store.add_message.assert_not_awaited()
    case.worker.store.save_artifact.assert_awaited_once()
    case.worker.store.enqueue.assert_awaited_once()
    assert "Не удалось прочитать" in case.worker.store.enqueue.await_args.args[3]["text"]


async def test_replayed_album_reuses_saved_attachments_without_redownloading(incoming_case):
    case = incoming_case
    await case.worker.telegram(case.event)
    await case.worker.telegram(case.event)
    assert case.worker.transport.bot.get_file.await_count == 2
    assert case.worker.agent.artifacts.ingest.call_count == 2
    assert case.worker.store.save_artifact.await_count == 2
    assert (
        case.worker.answer.await_args_list[0].args[2]
        == case.worker.answer.await_args_list[1].args[2]
    )


async def test_late_album_continuation_saves_full_context_without_second_generation(incoming_case):
    case = incoming_case
    case.event["payload"]["media_group_continuation"] = True
    await case.worker.telegram(case.event)
    case.worker.answer.assert_not_awaited()
    case.worker.store.queue_home.assert_not_awaited()
    case.worker.store.add_message.assert_awaited_once()
    user_id, conversation_id, role, prompt, key = case.worker.store.add_message.await_args.args
    assert (user_id, conversation_id, role, key) == (
        USER_ID,
        case.conversation["id"],
        "user",
        f"user:{case.event['id']}",
    )
    assert len([item for item in prompt if item["type"] == "image_ref"]) == 2
    payload = case.worker.store.enqueue.await_args.args[3]
    assert "Остальные вложения альбома" in payload["text"]
    assert payload["reply_markup"] == navigation_keyboard()


@pytest.mark.parametrize("field,value", [("message_thread_id", 88), ("from", {"id": 55})])
async def test_album_cannot_mix_other_users_or_topics(incoming_case, field, value):
    case = incoming_case
    case.event["payload"]["media_group_messages"][1][field] = value
    with pytest.raises(ValueError, match="one user and conversation"):
        await case.worker.telegram(case.event)
    case.worker.transport.bot.get_file.assert_not_awaited()
    case.worker.answer.assert_not_awaited()


@pytest.mark.parametrize("label", ["➕ Новый чат", "🗑 Удалить чат"])
async def test_bottom_navigation_text_routes_without_calling_agent(incoming_case, label):
    case = incoming_case
    message = case.event["payload"]["message"]
    message.pop("photo")
    message.pop("caption")
    message["text"] = label
    case.event["payload"].pop("media_group_messages")
    case.worker.new_chat = AsyncMock()
    request = {"id": uuid4(), "scope": "chat", "thread_id": THREAD_ID, "title": "Работа"}
    case.worker.store.requests_prepare = AsyncMock(return_value=request)
    case.worker.store.confirm_privacy_request = AsyncMock()
    case.worker.store.begin_privacy_erasure = AsyncMock()
    await case.worker.telegram(case.event)
    if label == "➕ Новый чат":
        case.worker.new_chat.assert_awaited_once_with(case.event, case.conversation)
        case.worker.store.requests_prepare.assert_not_awaited()
    else:
        case.worker.store.requests_prepare.assert_awaited_once_with(
            USER_ID,
            case.conversation,
            "chat",
            source_key=f"privacy-request:{case.event['id']}",
        )
        assert case.worker.store.enqueue.await_args.args[3][
            "reply_markup"
        ] == confirmation_keyboard(request)
        case.worker.store.confirm_privacy_request.assert_not_awaited()
        case.worker.store.begin_privacy_erasure.assert_not_awaited()
    case.worker.answer.assert_not_awaited()
    case.worker.transport.bot.get_file.assert_not_awaited()


@pytest.fixture
def answer_case():
    worker = Worker.__new__(Worker)
    conversation = {"id": uuid4(), "user_id": USER_ID, "chat_id": USER_ID, "thread_id": THREAD_ID}
    event = {"id": uuid4()}
    run = {"id": uuid4(), "fence": 2, "memory_revision": 0, "user_id": USER_ID}
    worker.store = SimpleNamespace(
        start_run=AsyncMock(return_value=run),
        enqueue_for_run=AsyncMock(return_value=1),
        ensure_user=AsyncMock(return_value={"memory_revision": 0}),
        add_message=AsyncMock(),
        queue_topic_title=AsyncMock(),
        privacy_request_for_run=AsyncMock(return_value=None),
        finish_run=AsyncMock(),
        run_metrics=AsyncMock(),
        run_active=AsyncMock(return_value=True),
        get_artifact=AsyncMock(
            side_effect=lambda user, artifact_id: {"path": f"/tmp/{artifact_id}.png"}
        ),
    )
    worker.transport = TelegramTransport(
        Settings(
            database_url="postgresql://unused",
            telegram_bot_token="123456:TEST_TOKEN",
            telegram_proxy="socks5://proxy.example:1080",
        )
    )
    worker.transport.bot = SimpleNamespace(send_message_draft=AsyncMock(return_value=True))
    worker.agent = SimpleNamespace(run=AsyncMock(return_value="Готово"))
    return SimpleNamespace(worker=worker, event=event, conversation=conversation, run=run)


async def test_generated_images_and_final_text_form_one_outbox_with_image_history(answer_case):
    case = answer_case
    image_ids = [str(uuid4()), str(uuid4())]
    case.worker.agent.run.return_value = {"text": "Два варианта", "image_artifact_ids": image_ids}
    assert await case.worker.answer(case.event, case.conversation, "Нарисуй два варианта") == "done"
    case.worker.store.enqueue_for_run.assert_awaited_once_with(
        case.run,
        case.conversation,
        {
            "image_paths": [f"/tmp/{image_id}.png" for image_id in image_ids],
            "caption": "Два варианта",
            "format": "rich",
            "reply_markup": navigation_keyboard(),
        },
        f"answer:{case.event['id']}",
    )
    assert all(call.args[0] == USER_ID for call in case.worker.store.get_artifact.await_args_list)
    assistant = case.worker.store.add_message.await_args_list[1].args
    assert assistant[2] == "assistant"
    assert assistant[3] == [
        {"type": "text", "text": "Два варианта"},
        *[{"type": "image_ref", "artifact_id": image_id} for image_id in image_ids],
    ]
    assert not case.worker.transport._drafts


async def test_worker_generated_phonix_caption_reaches_transport_with_bold_entity(answer_case):
    case = answer_case
    image_id = str(uuid4())
    case.worker.agent.run.return_value = {
        "text": "Логотип **Phonix**",
        "image_artifact_ids": [image_id],
    }
    case.worker.transport.bot.send_photo = AsyncMock(return_value=SimpleNamespace(message_id=51))
    case.worker.transport.bot.send_message = AsyncMock()
    await case.worker.answer(case.event, case.conversation, "Нарисуй логотип Phonix")
    payload = case.worker.store.enqueue_for_run.await_args.args[2]
    assert payload["format"] == "rich"
    assert await case.worker.transport.send(USER_ID, THREAD_ID, payload) == [51]
    sent = case.worker.transport.bot.send_photo.await_args.kwargs
    assert sent["caption"] == "Логотип Phonix"
    assert [(entity.type, entity.offset, entity.length) for entity in sent["caption_entities"]] == [
        ("bold", 8, 6),
    ]
    case.worker.transport.bot.send_message.assert_not_awaited()


async def test_thinking_refresh_stops_before_final_enqueue_and_keeps_streamed_text(
    answer_case, monkeypatch
):
    case = answer_case
    order, refreshed, heartbeat_stopped = [], asyncio.Event(), asyncio.Event()
    real_sleep = asyncio.sleep

    async def fast_sleep(seconds):
        assert seconds == 4
        await real_sleep(0)

    monkeypatch.setattr("cronos.worker.asyncio.sleep", fast_sleep)
    preview = case.worker.refresh_preview

    async def tracked_preview(*args):
        try:
            await preview(*args)
        finally:
            heartbeat_stopped.set()

    case.worker.refresh_preview = tracked_preview

    async def send_draft(**kwargs):
        order.append(("draft", kwargs["text"]))
        if order.count(("draft", "Начало ответа")) >= 2:
            refreshed.set()

    case.worker.transport.bot.send_message_draft.side_effect = send_draft

    async def generate(*args, **kwargs):
        assert order == [("draft", "Думаю...")]
        draft_id = case.run["id"].int % 2_000_000_000 + 1
        await case.worker.transport.draft(USER_ID, THREAD_ID, "Начало ответа", draft_id)
        await asyncio.wait_for(refreshed.wait(), timeout=1)
        return "Окончательный ответ"

    async def enqueue(*args):
        assert heartbeat_stopped.is_set()
        order.append(("final", args[2]["text"]))
        assert "inline_keyboard" not in args[2]["reply_markup"]
        return 1

    case.worker.agent.run.side_effect = generate
    case.worker.store.enqueue_for_run.side_effect = enqueue
    assert await case.worker.answer(case.event, case.conversation, "Вопрос") == "done"
    assert order[-1] == ("final", "Окончательный ответ")
    assert all(text == "Начало ответа" for kind, text in order[1:-1] if kind == "draft")
    assert not case.worker.transport._drafts


@pytest.mark.parametrize(
    "error,expected",
    [(CancelledRun("stop"), "cancelled"), (asyncio.CancelledError(), "interrupted")],
)
async def test_cancelled_generation_cleans_preview_and_never_enqueues_final(
    answer_case, error, expected
):
    case = answer_case
    case.worker.agent.run.side_effect = error
    if isinstance(error, asyncio.CancelledError):
        with pytest.raises(asyncio.CancelledError):
            await case.worker.answer(case.event, case.conversation, "Вопрос")
    else:
        assert await case.worker.answer(case.event, case.conversation, "Вопрос") == expected
    case.worker.store.enqueue_for_run.assert_not_awaited()
    assert not case.worker.transport._drafts
    case.worker.store.finish_run.assert_awaited_once_with(case.run["id"], expected, fence=2)
