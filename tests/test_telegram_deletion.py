from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.exceptions import TelegramBadRequest, TelegramNetworkError, TelegramRetryAfter
from aiogram.methods import DeleteForumTopic, DeleteMessage, DeleteMessages

from cronos.settings import Settings
from cronos.telegram import TelegramTransport, new_chat_keyboard


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
        delete_forum_topic=AsyncMock(return_value=True),
        delete_messages=AsyncMock(return_value=True),
        delete_message=AsyncMock(return_value=True),
    )
    return value


@pytest.mark.parametrize("thread_id", [0, 1, -1])
async def test_general_topic_is_never_submitted_for_deletion(transport, thread_id):
    with pytest.raises(ValueError, match="General"):
        await transport.delete_topic(42, thread_id)
    transport.bot.delete_forum_topic.assert_not_called()


@pytest.mark.parametrize(
    "description",
    [
        "Bad Request: TOPIC_NOT_FOUND",
        "Bad Request: message thread not found",
        "Bad Request: Forum topic not found",
    ],
)
async def test_deleted_topic_retry_acknowledges_only_exact_absence(transport, description):
    transport.bot.delete_forum_topic.side_effect = TelegramBadRequest(
        method=DeleteForumTopic(chat_id=42, message_thread_id=77), message=description
    )
    assert await transport.delete_topic(42, 77) is True
    transport.bot.delete_forum_topic.assert_awaited_once_with(chat_id=42, message_thread_id=77)


@pytest.mark.parametrize(
    "description",
    [
        "Bad Request: TOPIC_ID_INVALID",
        "Bad Request: CHAT_ADMIN_REQUIRED",
        "Bad Request: chat not found",
        "Bad Request: TOPIC_NOT_FOUND and another unexpected error",
    ],
)
async def test_topic_unknown_bad_request_is_not_reported_as_deleted(transport, description):
    failure = TelegramBadRequest(
        method=DeleteForumTopic(chat_id=42, message_thread_id=77), message=description
    )
    transport.bot.delete_forum_topic.side_effect = failure
    with pytest.raises(TelegramBadRequest) as caught:
        await transport.delete_topic(42, 77)
    assert caught.value is failure


async def test_deletion_methods_keep_the_same_proxy_session(monkeypatch):
    seen = []

    async def request(session, bot, method, **kwargs):
        seen.append((session, method))
        return True

    monkeypatch.setattr(AiohttpSession, "make_request", request)
    value = TelegramTransport(settings())
    try:
        session = value.bot.session
        assert session.proxy == "socks5://proxy.example:1080"
        assert session._connector_init["rdns"] is True
        assert await value.delete_topic(42, 77) is True
        assert await value.delete_messages(42, [11, 12]) == {
            "cleared": 2,
            "failed": 0,
            "unavailable_ids": [],
        }
        assert all(actual is session for actual, _ in seen)
        assert [method.__api_method__ for _, method in seen] == [
            "deleteForumTopic",
            "deleteMessages",
        ]
        assert all(method.chat_id == 42 for _, method in seen)
        assert seen[0][1].message_thread_id == 77
    finally:
        await value.close()


async def test_bulk_deletion_bounds_requests_and_deduplicates_ids(transport):
    ids = list(range(1, 206))
    result = await transport.delete_messages(42, [*ids, 2, 205])
    assert result == {"cleared": 205, "failed": 0, "unavailable_ids": []}
    calls = transport.bot.delete_messages.await_args_list
    assert [len(call.kwargs["message_ids"]) for call in calls] == [100, 100, 5]
    assert [item for call in calls for item in call.kwargs["message_ids"]] == ids
    assert all(call.kwargs["chat_id"] == 42 for call in calls)
    transport.bot.delete_message.assert_not_called()


async def test_mixed_batch_isolates_undeletable_messages_and_accepts_absent_ids(transport):
    transport.bot.delete_messages.side_effect = TelegramBadRequest(
        method=DeleteMessages(chat_id=42, message_ids=[11, 12, 13]),
        message="Bad Request: message can't be deleted",
    )

    async def individual(chat_id, message_id):
        if message_id == 12:
            raise TelegramBadRequest(
                method=DeleteMessage(chat_id=chat_id, message_id=message_id),
                message="Bad Request: message can't be deleted",
            )
        if message_id == 13:
            raise TelegramBadRequest(
                method=DeleteMessage(chat_id=chat_id, message_id=message_id),
                message="Bad Request: message to delete not found",
            )
        return True

    transport.bot.delete_message.side_effect = individual
    assert await transport.delete_messages(42, [11, 12, 13]) == {
        "cleared": 2,
        "failed": 1,
        "unavailable_ids": [12],
    }
    assert [call.kwargs["message_id"] for call in transport.bot.delete_message.await_args_list] == [
        11,
        12,
        13,
    ]


@pytest.mark.parametrize("phase", ["topic", "bulk", "individual"])
@pytest.mark.parametrize("failure_kind", ["network", "rate_limit", "unknown_bad_request"])
async def test_transient_and_unknown_errors_escape_for_durable_retry(
    transport, phase, failure_kind
):
    method = DeleteMessage(chat_id=42, message_id=11)
    if failure_kind == "network":
        failure = TelegramNetworkError(method=method, message="connection unavailable")
    elif failure_kind == "rate_limit":
        failure = TelegramRetryAfter(method=method, message="rate limited", retry_after=5)
    else:
        failure = TelegramBadRequest(method=method, message="Bad Request: unexpected failure")
    if phase == "topic":
        transport.bot.delete_forum_topic.side_effect = failure
        operation = transport.delete_topic(42, 77)
    else:
        if phase == "bulk":
            transport.bot.delete_messages.side_effect = failure
        else:
            transport.bot.delete_messages.side_effect = TelegramBadRequest(
                method=method, message="Bad Request: message can't be deleted"
            )
            transport.bot.delete_message.side_effect = failure
        operation = transport.delete_messages(42, [11, 12])
    with pytest.raises(type(failure)) as caught:
        await operation
    assert caught.value is failure
    if phase == "bulk":
        transport.bot.delete_message.assert_not_called()
    if phase == "individual":
        assert transport.bot.delete_message.await_count == 1


async def test_empty_and_invalid_id_lists_do_not_issue_requests(transport):
    assert await transport.delete_messages(42, []) == {
        "cleared": 0,
        "failed": 0,
        "unavailable_ids": [],
    }
    for invalid in (0, -1, True, "11"):
        with pytest.raises(ValueError, match="Positive"):
            await transport.delete_messages(42, [11, invalid])
    transport.bot.delete_messages.assert_not_called()
    transport.bot.delete_message.assert_not_called()


async def test_false_acknowledgement_is_not_reported_as_cleared(transport):
    transport.bot.delete_messages.return_value = False
    with pytest.raises(RuntimeError, match="acknowledge"):
        await transport.delete_messages(42, [11])


def test_chat_keyboard_exposes_explicit_current_chat_deletion():
    callbacks = [
        button["callback_data"] for row in new_chat_keyboard()["inline_keyboard"] for button in row
    ]
    assert callbacks == ["chat:new", "chat:delete", "home:main"]
