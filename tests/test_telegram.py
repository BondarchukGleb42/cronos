import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.exceptions import TelegramBadRequest, TelegramRetryAfter
from aiogram.methods import EditForumTopic, SendRichMessage
from aiogram.types import FSInputFile, Update
from aiohttp import TCPConnector
from aiohttp_socks import ProxyConnectionError as AiohttpProxyConnectionError
from aiohttp_socks import ProxyConnector
from python_socks import ProxyConnectionError

from cronos.gateway import poll_once
from cronos.settings import Settings
from cronos.telegram import (
    PartialDeliveryError,
    TelegramTransport,
    chat_navigation_keyboard,
    new_chat_keyboard,
    rich_message,
    split_text,
    upgrade_keyboard,
)


@pytest.fixture
def transport():
    settings = Settings(
        database_url="postgresql://unused",
        telegram_bot_token="123456:TEST_TOKEN",
        telegram_proxy="socks5://127.0.0.1:1080",
    )
    result = TelegramTransport(settings)
    result.bot = SimpleNamespace(
        send_message=AsyncMock(return_value=SimpleNamespace(message_id=11)),
        send_rich_message=AsyncMock(return_value=SimpleNamespace(message_id=12)),
        send_document=AsyncMock(return_value=SimpleNamespace(message_id=13)),
        send_photo=AsyncMock(return_value=SimpleNamespace(message_id=14)),
        send_message_draft=AsyncMock(),
        get_me=AsyncMock(
            return_value=SimpleNamespace(
                username="cronos_ait_bot",
                has_topics_enabled=True,
                allows_users_to_create_topics=False,
            )
        ),
        create_forum_topic=AsyncMock(
            return_value=SimpleNamespace(model_dump=lambda **kwargs: {"message_thread_id": 77})
        ),
        edit_forum_topic=AsyncMock(return_value=True),
    )
    return result


def test_unicode_chunks_preserve_every_character():
    text = ("Текст 😀 <b>данные</b>\n" * 1000) + "конец"
    chunks = split_text(text)
    assert "".join(chunks) == text
    assert all(len(chunk.encode("utf-16-le")) // 2 <= 3900 for chunk in chunks)


def test_native_table_and_literal_html():
    message = rich_message(
        "# Сводка\n\n| Поле | Значение |\n| :--- | ---: |\n| **Итого** | 42 |\n\n<script>bad()</script> [bad](javascript:alert)"
    )
    data = message.model_dump(exclude_none=True)
    assert data.get("html") is None
    table = data["blocks"][1]
    assert table["type"] == "table"
    assert table["cells"][1][1]["align"] == "right"
    assert table["cells"][1][0]["text"] == {"type": "bold", "text": "Итого"}
    assert "javascript:alert" in str(data["blocks"][-1]["text"])
    assert "'type': 'url'" not in str(data)


async def test_rich_rejection_falls_back_without_interpreting_html(transport):
    text = "<b>literal</b> " + "😀" * 2500
    transport.bot.send_rich_message.side_effect = TelegramBadRequest(
        method=SendRichMessage(chat_id=1, rich_message=rich_message("x")),
        message="Bad Request: can't parse rich message",
    )
    await transport.send(1, 9, {"text": text, "format": "rich", "reply_markup": upgrade_keyboard()})
    calls = transport.bot.send_message.call_args_list
    assert "".join(call.kwargs["text"] for call in calls) == text
    assert all(call.kwargs["parse_mode"] is None for call in calls)
    assert all(call.kwargs["message_thread_id"] == 9 for call in calls)
    assert calls[0].kwargs["reply_markup"] is None
    assert calls[-1].kwargs["reply_markup"].inline_keyboard[0][0].callback_data == "plan:START"


async def test_rate_limit_is_not_retried_as_plain_text(transport):
    transport.bot.send_rich_message.side_effect = TelegramRetryAfter(
        method=SendRichMessage(chat_id=1, rich_message=rich_message("x")),
        message="Flood control",
        retry_after=5,
    )
    with pytest.raises(TelegramRetryAfter):
        await transport.send(1, None, {"text": "x", "format": "rich"})
    transport.bot.send_message.assert_not_called()


async def test_file_uses_filesystem_upload_and_preserves_caption_overflow(transport, tmp_path):
    path = tmp_path / "result.pdf"
    path.write_bytes(b"%PDF-test")
    caption = "Я" * 1200
    ids = await transport.send(1, None, {"document_path": str(path), "caption": caption})
    call = transport.bot.send_document.call_args
    assert isinstance(call.kwargs["document"], FSInputFile)
    assert call.kwargs["parse_mode"] is None
    assert call.kwargs["caption"] + transport.bot.send_message.call_args.kwargs["text"] == caption
    assert ids == [13, 11]


async def test_poll_does_not_ack_batch_after_failed_database_commit():
    update = Update.model_validate(
        {
            "update_id": 41,
            "message": {
                "message_id": 3,
                "date": 1,
                "chat": {"id": 7, "type": "private"},
                "from": {"id": 7, "is_bot": False, "first_name": "Test"},
                "text": "hi",
            },
        }
    )
    store = SimpleNamespace(
        acquire_poll_lease=AsyncMock(return_value=True),
        get_poll_offset=AsyncMock(return_value=41),
        ingest_updates=AsyncMock(side_effect=RuntimeError("db unavailable")),
    )
    bot = SimpleNamespace(get_updates=AsyncMock(return_value=[update]))
    with pytest.raises(RuntimeError):
        await poll_once(store, bot, "owner")
    store.ingest_updates.side_effect = None
    await poll_once(store, bot, "owner")
    assert [call.kwargs["offset"] for call in bot.get_updates.call_args_list] == [41, 41]
    raw, cursor = store.ingest_updates.call_args.args
    assert cursor == 42
    assert raw[0]["message"]["from"]["id"] == 7


async def test_poller_discards_result_if_lease_was_lost():
    store = SimpleNamespace(
        acquire_poll_lease=AsyncMock(side_effect=[True, False]),
        get_poll_offset=AsyncMock(return_value=5),
        ingest_updates=AsyncMock(),
    )
    bot = SimpleNamespace(get_updates=AsyncMock(return_value=[Update(update_id=5)]))
    assert await poll_once(store, bot, "owner") is False
    store.ingest_updates.assert_not_called()


async def test_retry_sends_only_remaining_file_after_partial_delivery(transport, tmp_path):
    path = tmp_path / "result.pdf"
    path.write_bytes(b"%PDF-test")
    transport.bot.send_document.side_effect = TimeoutError()
    with pytest.raises(PartialDeliveryError) as caught:
        await transport.send(1, 8, {"text": "Готово", "document_path": str(path)})
    partial = caught.value
    assert partial.sent_ids == [11]
    assert isinstance(partial.cause, TimeoutError)
    transport.bot.send_document.side_effect = None
    assert await transport.send(1, 8, partial.remaining_payload) == [13]
    assert transport.bot.send_message.await_count == 1


async def test_draft_is_best_effort_and_never_sends_a_permanent_fallback(transport):
    transport.bot.send_message_draft.side_effect = TimeoutError()
    await transport.draft(1, 8, "ищу", 23)
    assert transport.bot.send_message_draft.call_args.kwargs["can_stop"] is True
    transport.bot.send_message.assert_not_called()


@pytest.mark.parametrize("proxy", [None, "", "   "])
def test_missing_proxy_fails_closed_before_bot_creation(proxy, monkeypatch):
    bot = Mock()
    monkeypatch.setattr("cronos.telegram.Bot", bot)
    with pytest.raises(ValueError, match="TELEGRAM_PROXY is required"):
        TelegramTransport(Settings(database_url="postgresql://unused", telegram_proxy=proxy))
    bot.assert_not_called()


def test_invalid_proxy_error_does_not_expose_credentials():
    password = "private-proxy-password"
    with pytest.raises(ValueError) as caught:
        TelegramTransport(
            Settings(
                database_url="postgresql://unused",
                telegram_bot_token="123456:TEST_TOKEN",
                telegram_proxy=f"invalid://user:{password}@host:80",
            )
        )
    assert password not in str(caught.value)
    assert "host" not in str(caught.value)


async def test_all_telegram_operations_share_the_configured_proxy_session(monkeypatch):
    seen, downloads = [], []

    async def request(session, bot, method, **kwargs):
        seen.append((session, method.__api_method__))
        if method.__api_method__ == "getMe":
            return SimpleNamespace(
                username="cronos_ait_bot",
                has_topics_enabled=True,
                allows_users_to_create_topics=True,
            )
        if method.__api_method__ == "getUpdates":
            return []
        if method.__api_method__ == "createForumTopic":
            return SimpleNamespace(model_dump=lambda **kwargs: {"message_thread_id": 77})
        if method.__api_method__ == "editForumTopic":
            return True
        return SimpleNamespace(message_id=11)

    async def stream(session, url, **kwargs):
        downloads.append(session)
        yield b"document bytes"

    monkeypatch.setattr(AiohttpSession, "make_request", request)
    monkeypatch.setattr(AiohttpSession, "stream_content", stream)
    value = TelegramTransport(
        Settings(
            database_url="postgresql://unused",
            telegram_bot_token="123456:TEST_TOKEN",
            telegram_proxy="socks5://proxy.example:1080",
        )
    )
    try:
        session = value.bot.session
        assert isinstance(session, AiohttpSession)
        assert session.proxy == "socks5://proxy.example:1080"
        assert session._connector_init["rdns"] is True
        assert (await value.topic_capabilities()).has_topics_enabled is True
        await value.bot.get_updates()
        await value.bot.get_file("file-id")
        await value.bot.download_file("documents/file.pdf")
        await value.send(1, None, {"text": "hello"})
        await value.send(1, None, {"text": "table", "format": "rich"})
        await value.draft(1, None, "thinking", 5)
        assert await value.create_topic(1, "topic") == {"message_thread_id": 77}
        assert await value.edit_topic(1, 77, "renamed") is True
        await value.bot.answer_callback_query("callback-id")
        assert {method for _, method in seen} == {
            "getMe",
            "getUpdates",
            "getFile",
            "sendMessage",
            "sendRichMessage",
            "sendMessageDraft",
            "createForumTopic",
            "editForumTopic",
            "answerCallbackQuery",
        }
        assert all(actual is session for actual, _ in seen)
        assert downloads == [session]
    finally:
        await value.close()


async def test_proxy_connection_failure_never_tries_a_direct_connector(monkeypatch):
    proxy_connect = AsyncMock(side_effect=ProxyConnectionError("proxy unavailable"))
    direct_connect = AsyncMock(side_effect=AssertionError("direct connection is forbidden"))
    monkeypatch.setattr(ProxyConnector, "_connect_via_proxy", proxy_connect)
    monkeypatch.setattr(TCPConnector, "_wrap_create_connection", direct_connect)
    value = TelegramTransport(
        Settings(
            database_url="postgresql://unused",
            telegram_bot_token="123456:TEST_TOKEN",
            telegram_proxy="socks5://127.0.0.1:1080",
        )
    )
    try:
        with pytest.raises(AiohttpProxyConnectionError):
            await value.send(1, None, {"text": "hello", "format": "rich"})
        proxy_connect.assert_awaited_once()
        direct_connect.assert_not_called()
    finally:
        await value.close()


async def test_proxy_failure_on_draft_does_not_create_permanent_or_direct_fallback(transport):
    transport.bot.send_message_draft.side_effect = AiohttpProxyConnectionError("proxy unavailable")
    await transport.draft(1, 8, "ищу", 23)
    transport.bot.send_message.assert_not_called()


async def test_topic_capabilities_cache_coalesces_reads_and_distinguishes_user_permission(
    transport,
):
    results = await asyncio.gather(*(transport.topic_capabilities() for _ in range(5)))
    transport.bot.get_me.assert_awaited_once()
    assert all(result is results[0] for result in results)
    assert results[0].has_topics_enabled is True
    assert results[0].allows_users_to_create_topics is False
    assert results[0].username == "cronos_ait_bot"
    # Disallowing users to create topics in BotFather does not disable bot creation.
    assert await transport.create_topic(1, "Работа") == {"message_thread_id": 77}


async def test_topic_capabilities_refresh_and_expiration_notice_botfather_changes(
    transport, monkeypatch
):
    clock = Mock(return_value=10.0)
    monkeypatch.setattr("cronos.telegram.time", SimpleNamespace(monotonic=clock))
    assert (await transport.topic_capabilities()).has_topics_enabled is True
    transport.bot.get_me.return_value = SimpleNamespace(username="cronos_ait_bot")
    assert (await transport.topic_capabilities()).has_topics_enabled is True
    clock.return_value = 71.0
    expired = await transport.topic_capabilities()
    assert expired.has_topics_enabled is False
    assert expired.allows_users_to_create_topics is False
    transport.bot.get_me.return_value.has_topics_enabled = True
    assert (await transport.topic_capabilities(force_refresh=True)).has_topics_enabled is True
    assert transport.bot.get_me.await_count == 3


async def test_topic_capabilities_failure_is_not_cached_as_disabled(transport):
    transport.bot.get_me.side_effect = AiohttpProxyConnectionError("proxy unavailable")
    with pytest.raises(AiohttpProxyConnectionError):
        await transport.topic_capabilities()
    transport.bot.get_me.side_effect = None
    assert (await transport.topic_capabilities()).has_topics_enabled is True
    assert transport.bot.get_me.await_count == 2


async def test_edit_topic_preserves_destination_and_unicode_title(transport):
    name = "  Работа\n" + "😀" * 60
    assert await transport.edit_topic(42, 77, name) is True
    call = transport.bot.edit_forum_topic.call_args.kwargs
    assert call["chat_id"] == 42
    assert call["message_thread_id"] == 77
    assert call["name"].startswith("Работа 😀")
    assert "\n" not in call["name"]
    assert len(call["name"].encode("utf-8")) <= 128
    await transport.create_topic(42, name)
    assert transport.bot.create_forum_topic.call_args.kwargs["name"] == call["name"]


async def test_edit_topic_retry_is_idempotent_but_other_api_errors_propagate(transport):
    method = EditForumTopic(chat_id=42, message_thread_id=77, name="Работа")
    transport.bot.edit_forum_topic.side_effect = TelegramBadRequest(
        method=method, message="Bad Request: TOPIC_NOT_MODIFIED"
    )
    assert await transport.edit_topic(42, 77, "Работа") is True
    transport.bot.edit_forum_topic.side_effect = TelegramBadRequest(
        method=method, message="Bad Request: TOPIC_ID_INVALID"
    )
    with pytest.raises(TelegramBadRequest):
        await transport.edit_topic(42, 77, "Работа")


@pytest.mark.parametrize("thread_id", [0, -1])
async def test_edit_topic_rejects_invalid_thread_before_api_call(transport, thread_id):
    with pytest.raises(ValueError, match="positive"):
        await transport.edit_topic(42, thread_id, "Работа")
    transport.bot.edit_forum_topic.assert_not_called()


def test_chat_keyboard_is_a_collapsible_text_panel():
    assert new_chat_keyboard() == {
        "keyboard": [
            [{"text": "➕ Новый чат"}, {"text": "🗑 Удалить чат"}],
            [{"text": "🪐 Главное меню"}],
        ],
        "resize_keyboard": True,
        "is_persistent": False,
        "one_time_keyboard": False,
    }
    keyboard = chat_navigation_keyboard("@cronos_ait_bot")
    assert keyboard == new_chat_keyboard()
    assert chat_navigation_keyboard() == new_chat_keyboard()
    # No unverified private-topic URL or simulated topic-switch callback.
    assert "chat:switch" not in str(keyboard)
    assert "thread=" not in str(keyboard)


@pytest.mark.parametrize("username", ["x", "https://evil.example", "cronos_bot?start=inject"])
def test_chat_keyboard_omits_invalid_username_links(username):
    assert chat_navigation_keyboard(username) == new_chat_keyboard()
