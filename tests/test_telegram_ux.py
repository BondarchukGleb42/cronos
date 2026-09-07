import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.types import FSInputFile, ReplyKeyboardMarkup

from cronos.settings import Settings
from cronos.telegram import PartialDeliveryError, TelegramTransport, navigation_keyboard


@pytest.fixture
def transport():
    value = TelegramTransport(
        Settings(
            database_url="postgresql://unused",
            telegram_bot_token="123456:TEST_TOKEN",
            telegram_proxy="socks5://proxy.example:1080",
        )
    )
    value.bot = SimpleNamespace(
        send_message=AsyncMock(return_value=SimpleNamespace(message_id=11)),
        send_rich_message=AsyncMock(return_value=SimpleNamespace(message_id=12)),
        send_document=AsyncMock(return_value=SimpleNamespace(message_id=13)),
        send_photo=AsyncMock(return_value=SimpleNamespace(message_id=14)),
        send_message_draft=AsyncMock(return_value=True),
        edit_message_text=AsyncMock(),
        pin_chat_message=AsyncMock(return_value=True),
    )
    return value


@pytest.mark.parametrize(
    "content,expected",
    [
        ({"text": "Вот изображение <b>без HTML</b>"}, "Вот изображение <b>без HTML</b>"),
        ({"caption": "Готово"}, "Готово"),
        ({"text": "Готово", "caption": "Готово"}, "Готово"),
        ({"text": "Ответ", "caption": "Источник"}, "Ответ\n\nИсточник"),
        ({"caption": "😀" * 512}, "😀" * 512),
    ],
)
async def test_image_and_answer_are_one_photo_message(transport, content, expected):
    assert await transport.send(
        42, 77, {"image_path": "/tmp/result.png", "format": "rich", **content}
    ) == [14]
    call = transport.bot.send_photo.await_args.kwargs
    assert isinstance(call["photo"], FSInputFile)
    assert call["caption"] == expected
    assert call["parse_mode"] is None
    assert (call["chat_id"], call["message_thread_id"]) == (42, 77)
    transport.bot.send_message.assert_not_called()
    transport.bot.send_rich_message.assert_not_called()


async def test_multiple_images_caption_overflow_and_retry_preserve_every_part(transport):
    caption = "Картинка 😀\n" * 800
    transport.bot.send_photo.side_effect = [SimpleNamespace(message_id=51), TimeoutError()]
    payload = {
        "image_paths": ["/tmp/one.png", "/tmp/two.png"],
        "caption": caption,
        "reply_markup": navigation_keyboard(),
        "pin": True,
    }
    with pytest.raises(PartialDeliveryError) as caught:
        await transport.send(42, 77, payload)
    first = transport.bot.send_photo.await_args_list[0].kwargs
    assert len(first["caption"].encode("utf-16-le")) // 2 <= 1024
    assert caught.value.sent_ids == [51]
    remaining = caught.value.remaining_payload
    assert remaining["pin"] is True
    assert remaining["_telegram_parts"][0] == {
        "kind": "image",
        "path": "/tmp/two.png",
        "caption": None,
    }
    transport.bot.pin_chat_message.assert_not_called()
    transport.bot.send_photo.reset_mock(side_effect=True)
    transport.bot.send_photo.return_value = SimpleNamespace(message_id=52)
    sent = await transport.send(42, 77, remaining)
    assert sent[0] == 52
    assert transport.bot.send_photo.await_count == 1
    assert str(transport.bot.send_photo.await_args.kwargs["photo"].path) == "/tmp/two.png"
    texts = [call.kwargs["text"] for call in transport.bot.send_message.await_args_list]
    assert first["caption"] + "".join(texts) == caption
    final = transport.bot.send_message.await_args.kwargs
    assert isinstance(final["reply_markup"], ReplyKeyboardMarkup)
    assert final["message_thread_id"] == 77
    transport.bot.pin_chat_message.assert_awaited_once_with(
        chat_id=42, message_id=sent[-1], disable_notification=True
    )


async def test_multiple_images_keep_only_first_caption_and_last_keyboard(transport):
    await transport.send(
        42,
        77,
        {
            "image_path": "/tmp/one.png",
            "image_paths": ["/tmp/one.png", "/tmp/two.png"],
            "caption": "Два варианта",
            "reply_markup": navigation_keyboard(),
        },
    )
    first, second = [call.kwargs for call in transport.bot.send_photo.await_args_list]
    assert first["caption"] == "Два варианта" and first["reply_markup"] is None
    assert second["caption"] is None
    assert isinstance(second["reply_markup"], ReplyKeyboardMarkup)
    transport.bot.send_message.assert_not_called()


async def test_reply_panel_validates_in_real_aiogram_methods_through_same_session(monkeypatch):
    seen = []

    async def request(session, bot, method, **kwargs):
        seen.append((session, method))
        return SimpleNamespace(message_id=1)

    monkeypatch.setattr(AiohttpSession, "make_request", request)
    value = TelegramTransport(
        Settings(
            database_url="postgresql://unused",
            telegram_bot_token="123456:TEST_TOKEN",
            telegram_proxy="socks5://proxy.example:1080",
        )
    )
    session = value.bot.session
    try:
        for payload in (
            {"text": "Ответ"},
            {"text": "**Ответ**", "format": "rich"},
            {"document_path": "/tmp/report.csv"},
            {"image_path": "/tmp/result.png"},
        ):
            await value.send(42, 77, {**payload, "reply_markup": navigation_keyboard()})
        assert [method.__api_method__ for _, method in seen] == [
            "sendMessage",
            "sendRichMessage",
            "sendDocument",
            "sendPhoto",
        ]
        for actual, method in seen:
            assert actual is session
            assert method.message_thread_id == 77
            assert isinstance(method.reply_markup, ReplyKeyboardMarkup)
            assert method.reply_markup.is_persistent is False
    finally:
        await value.close()


async def test_reply_keyboard_is_not_passed_to_edit_api_or_sent_as_extra_message(transport):
    with pytest.raises(ValueError, match="inline keyboards only"):
        await transport.send(
            42,
            77,
            {
                "text": "Панель",
                "edit_message_id": 11,
                "reply_markup": navigation_keyboard(),
            },
        )
    transport.bot.edit_message_text.assert_not_called()
    transport.bot.send_message.assert_not_called()


@pytest.mark.parametrize("initial", ["", " \n\t", "Думаю..."])
async def test_initial_thinking_then_refresh_keeps_latest_text_until_cleanup(transport, initial):
    await transport.draft(42, 77, initial, 9)
    assert transport.bot.send_message_draft.await_args.kwargs["text"] == "Думаю..."
    await transport.draft(42, 77, "Начало ответа", 9)
    await transport.draft(42, 77, initial, 9)
    await transport.refresh_draft(42, 77, 9)
    call = transport.bot.send_message_draft.await_args.kwargs
    assert call["text"] == "Начало ответа"
    assert call["can_stop"] is True
    count = transport.bot.send_message_draft.await_count
    await transport.refresh_draft(42, 78, 9)
    await transport.refresh_draft(43, 77, 9)
    await transport.refresh_draft(42, 77, 10)
    transport.forget_draft(42, 77, 9)
    await transport.refresh_draft(42, 77, 9)
    assert transport.bot.send_message_draft.await_count == count


async def test_refresh_does_not_revert_concurrent_streaming_text(transport):
    entered, release = asyncio.Event(), asyncio.Event()

    async def stalled(**kwargs):
        entered.set()
        await release.wait()
        return True

    await transport.draft(42, 77, "", 9)
    transport.bot.send_message_draft.side_effect = stalled
    heartbeat = asyncio.create_task(transport.refresh_draft(42, 77, 9))
    await entered.wait()
    update = asyncio.create_task(transport.draft(42, 77, "Новый текст", 9))
    release.set()
    await asyncio.gather(heartbeat, update)
    assert transport.bot.send_message_draft.await_args.kwargs["text"] == "Новый текст"


async def test_preview_cache_is_bounded_and_evicted_drafts_do_not_restart(transport):
    for draft_id in range(1, 140):
        await transport.draft(42, 77, "Думаю...", draft_id)
    assert len(transport._drafts) == 128
    count = transport.bot.send_message_draft.await_count
    await transport.refresh_draft(42, 77, 1)
    assert transport.bot.send_message_draft.await_count == count


async def test_stalled_preview_times_out_but_latest_text_remains_refreshable(
    transport, monkeypatch
):
    monkeypatch.setattr("cronos.telegram._DRAFT_REQUEST_TIMEOUT", 0.01)
    waiting = asyncio.Event()

    async def stalled(**kwargs):
        await waiting.wait()

    transport.bot.send_message_draft.side_effect = stalled
    await asyncio.wait_for(transport.draft(42, 77, "Начало ответа", 9), timeout=1)
    transport.bot.send_message_draft.side_effect = None
    await transport.refresh_draft(42, 77, 9)
    assert transport.bot.send_message_draft.await_args.kwargs["text"] == "Начало ответа"
    transport.bot.send_message.assert_not_called()
