import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.exceptions import TelegramBadRequest, TelegramNetworkError, TelegramRetryAfter
from aiogram.methods import EditMessageText, PinChatMessage

from cronos.settings import Settings
from cronos.telegram import PartialDeliveryError, TelegramTransport, new_chat_keyboard


def settings():
    return Settings(
        database_url="postgresql://unused",
        telegram_bot_token="123456:TEST_TOKEN",
        telegram_proxy="socks5://proxy.example:1080",
    )


@pytest.fixture
def transport():
    value = TelegramTransport(settings())
    value.bot = SimpleNamespace(
        edit_message_text=AsyncMock(return_value=SimpleNamespace(message_id=55)),
        send_message=AsyncMock(return_value=SimpleNamespace(message_id=101)),
        send_rich_message=AsyncMock(),
        pin_chat_message=AsyncMock(return_value=True),
    )
    return value


def panel(**overrides):
    return {
        "text": "🪐 Cronos\n<b>Literal user text</b>",
        "reply_markup": {"inline_keyboard": [[{"text": "Чаты", "callback_data": "home:chats"}]]},
        "pin": True,
        **overrides,
    }


async def test_existing_panel_edits_plain_text_and_returns_same_id(transport):
    payload = panel(edit_message_id=55, format="rich")
    assert await transport.send(42, 77, payload) == [55]
    edit = transport.bot.edit_message_text.await_args.kwargs
    assert (edit["chat_id"], edit["message_id"]) == (42, 55)
    assert edit["text"] == payload["text"] and edit["parse_mode"] is None
    assert edit["reply_markup"].inline_keyboard[0][0].callback_data == "home:chats"
    transport.bot.send_message.assert_not_called()
    transport.bot.send_rich_message.assert_not_called()
    transport.bot.pin_chat_message.assert_awaited_once_with(
        chat_id=42, message_id=55, disable_notification=True
    )


@pytest.mark.parametrize(
    "description",
    [
        "Bad Request: message is not modified",
        "Bad Request: message is not modified: specified new message content and reply markup are exactly the same as a current content and reply markup of the message",
    ],
)
async def test_unchanged_panel_is_an_acknowledged_edit_retry(transport, description):
    transport.bot.edit_message_text.side_effect = TelegramBadRequest(
        method=EditMessageText(chat_id=42, message_id=55, text="panel"), message=description
    )
    assert await transport.send(42, 77, panel(edit_message_id=55)) == [55]
    transport.bot.send_message.assert_not_called()
    transport.bot.pin_chat_message.assert_awaited_once_with(
        chat_id=42, message_id=55, disable_notification=True
    )


@pytest.mark.parametrize("description", ["message to edit not found", "message can't be edited"])
async def test_uneditable_panel_recreates_only_in_requested_home_topic(transport, description):
    transport.bot.edit_message_text.side_effect = TelegramBadRequest(
        method=EditMessageText(chat_id=42, message_id=55, text="panel"),
        message=f"Bad Request: {description}",
    )
    payload = panel(edit_message_id=55, format="rich")
    assert await transport.send(42, 77, payload) == [101]
    sent = transport.bot.send_message.await_args.kwargs
    assert (sent["chat_id"], sent["message_thread_id"]) == (42, 77)
    assert sent["text"] == payload["text"] and sent["parse_mode"] is None
    assert sent["reply_markup"].inline_keyboard[0][0].callback_data == "home:chats"
    transport.bot.send_rich_message.assert_not_called()
    transport.bot.pin_chat_message.assert_awaited_once_with(
        chat_id=42, message_id=101, disable_notification=True
    )
    assert payload["format"] == "rich"  # Durable caller payload is not mutated.


@pytest.mark.parametrize("failure_kind", ["network", "rate_limit", "unknown", "false_match"])
async def test_uncertain_edit_never_resends_or_moves_panel(transport, failure_kind):
    method = EditMessageText(chat_id=42, message_id=55, text="panel")
    if failure_kind == "network":
        error = TelegramNetworkError(method=method, message="connection unavailable")
    elif failure_kind == "rate_limit":
        error = TelegramRetryAfter(method=method, message="rate limited", retry_after=5)
    else:
        error = TelegramBadRequest(
            method=method,
            message=(
                "Bad Request: message to edit not found with an unexpected suffix"
                if failure_kind == "false_match"
                else "Bad Request: chat not found"
            ),
        )
    transport.bot.edit_message_text.side_effect = error
    with pytest.raises(type(error)) as caught:
        await transport.send(42, 77, panel(edit_message_id=55))
    assert caught.value is error
    transport.bot.send_message.assert_not_called()
    transport.bot.pin_chat_message.assert_not_called()


@pytest.mark.parametrize("editing", [False, True])
@pytest.mark.parametrize("failure_kind", ["network", "rate_limit", "bad_request"])
async def test_pin_failure_does_not_hide_delivery_or_log_secret_details(
    transport, caplog, editing, failure_kind
):
    method = PinChatMessage(chat_id=42, message_id=55)
    secret = "socks5://private:secret@proxy.example"
    if failure_kind == "network":
        error = TelegramNetworkError(method=method, message=secret)
    elif failure_kind == "rate_limit":
        error = TelegramRetryAfter(method=method, message=secret, retry_after=5)
    else:
        error = TelegramBadRequest(method=method, message=secret)
    transport.bot.pin_chat_message.side_effect = error
    with caplog.at_level(logging.INFO, logger="cronos.telegram"):
        assert await transport.send(
            42, 77, panel(**({"edit_message_id": 55} if editing else {}))
        ) == ([55] if editing else [101])
    assert transport.bot.send_message.await_count == (0 if editing else 1)
    assert transport.bot.edit_message_text.await_count == (1 if editing else 0)
    assert transport.bot.pin_chat_message.await_count == 1
    assert secret not in caplog.text and "secret" not in caplog.text
    assert type(error).__name__ in caplog.text


async def test_false_pin_acknowledgement_keeps_successful_delivery(transport):
    transport.bot.pin_chat_message.return_value = False
    assert await transport.send(42, 77, panel()) == [101]
    transport.bot.send_message.assert_awaited_once()


async def test_partial_panel_send_preserves_pin_until_last_remaining_part_succeeds(transport):
    transport.bot.send_message.side_effect = [SimpleNamespace(message_id=11), TimeoutError()]
    with pytest.raises(PartialDeliveryError) as caught:
        await transport.send(42, 77, panel(text="x" * 8000))
    error = caught.value
    assert error.sent_ids == [11]
    assert error.remaining_payload["pin"] is True
    assert "edit_message_id" not in error.remaining_payload
    transport.bot.pin_chat_message.assert_not_called()
    transport.bot.send_message.side_effect = [
        SimpleNamespace(message_id=12),
        SimpleNamespace(message_id=13),
    ]
    assert await transport.send(42, 77, error.remaining_payload) == [12, 13]
    transport.bot.pin_chat_message.assert_awaited_once_with(
        chat_id=42, message_id=13, disable_notification=True
    )
    calls = transport.bot.send_message.await_args_list
    assert all(call.kwargs["message_thread_id"] == 77 for call in calls)
    assert calls[-1].kwargs["reply_markup"].inline_keyboard[0][0].callback_data == "home:chats"


async def test_home_operations_use_the_existing_proxy_session(monkeypatch):
    seen = []

    async def request(session, bot, method, **kwargs):
        seen.append((session, method.__api_method__))
        return True if method.__api_method__ == "pinChatMessage" else SimpleNamespace(message_id=55)

    monkeypatch.setattr(AiohttpSession, "make_request", request)
    value = TelegramTransport(settings())
    try:
        session = value.bot.session
        assert session.proxy == "socks5://proxy.example:1080"
        assert session._connector_init["rdns"] is True
        assert await value.send(42, 77, panel(edit_message_id=55)) == [55]
        assert await value.send(42, 77, panel()) == [55]
        assert [method for _, method in seen] == [
            "editMessageText",
            "pinChatMessage",
            "sendMessage",
            "pinChatMessage",
        ]
        assert all(actual is session for actual, _ in seen)
    finally:
        await value.close()


def test_home_button_joins_existing_chat_navigation():
    assert new_chat_keyboard()["keyboard"][-1] == [{"text": "🪐 Главное меню"}]
