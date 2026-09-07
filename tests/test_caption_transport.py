import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from aiogram.exceptions import TelegramBadRequest, TelegramRetryAfter
from aiogram.methods import SendDocument, SendPhoto
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
        send_photo=AsyncMock(return_value=SimpleNamespace(message_id=10)),
        send_document=AsyncMock(return_value=SimpleNamespace(message_id=20)),
        send_message=AsyncMock(return_value=SimpleNamespace(message_id=30)),
        send_rich_message=AsyncMock(return_value=SimpleNamespace(message_id=40)),
    )
    return value


def units(text):
    return len(text.encode("utf-16-le")) // 2


def entity_text(text, entity):
    raw = text.encode("utf-16-le")
    return raw[entity.offset * 2 : (entity.offset + entity.length) * 2].decode("utf-16-le")


async def test_phonix_markdown_is_one_photo_with_clean_bold_caption(transport):
    assert await transport.send(
        42,
        77,
        {
            "image_path": "/tmp/logo.png",
            "caption": "Логотип **Phonix**",
            "format": "rich",
            "reply_markup": navigation_keyboard(),
        },
    ) == [10]
    sent = transport.bot.send_photo.await_args.kwargs
    assert sent["caption"] == "Логотип Phonix"
    assert sent["parse_mode"] is None
    assert isinstance(sent["photo"], FSInputFile)
    assert isinstance(sent["reply_markup"], ReplyKeyboardMarkup)
    assert (sent["chat_id"], sent["message_thread_id"]) == (42, 77)
    assert [(entity.type, entity.offset, entity.length) for entity in sent["caption_entities"]] == [
        ("bold", 8, 6),
    ]
    transport.bot.send_message.assert_not_awaited()
    transport.bot.send_rich_message.assert_not_awaited()


async def test_caption_entities_use_utf16_offsets_and_keep_link_target(transport):
    await transport.send(
        42,
        77,
        {
            "image_path": "/tmp/logo.png",
            "caption": "🪐 **Phonix** — [сайт](https://example.com/a?q=1)",
            "format": "rich",
        },
    )
    sent = transport.bot.send_photo.await_args.kwargs
    assert sent["caption"] == "🪐 Phonix — сайт"
    bold = next(entity for entity in sent["caption_entities"] if entity.type == "bold")
    link = next(entity for entity in sent["caption_entities"] if entity.type == "text_link")
    assert (bold.offset, bold.length) == (3, 6)
    assert entity_text(sent["caption"], bold) == "Phonix"
    assert entity_text(sent["caption"], link) == "сайт"
    assert link.url == "https://example.com/a?q=1"
    assert link.offset == units("🪐 Phonix — ")


@pytest.mark.parametrize(
    "field,method", [("image_path", "send_photo"), ("document_path", "send_document")]
)
async def test_unmarked_filename_caption_stays_literal(transport, field, method):
    caption = "design_**Phonix**_v2.png"
    await transport.send(42, 77, {field: "/tmp/file.bin", "caption": caption})
    sent = getattr(transport.bot, method).await_args.kwargs
    assert sent["caption"] == caption
    assert not sent.get("caption_entities")
    assert sent["parse_mode"] is None


async def test_formatted_caption_overflow_keeps_text_and_rebased_entities(transport):
    bold_content = "Б" * 1400
    expected = "До " + bold_content + " после сайт"
    await transport.send(
        42,
        77,
        {
            "image_path": "/tmp/logo.png",
            "caption": f"До **{bold_content}** после [сайт](https://example.com)",
            "format": "rich",
        },
    )
    first = transport.bot.send_photo.await_args.kwargs
    chunks = [(first["caption"], first.get("caption_entities") or [])]
    chunks.extend(
        (call.kwargs["text"], call.kwargs.get("entities") or [])
        for call in transport.bot.send_message.await_args_list
    )
    assert len(chunks) >= 2 and units(chunks[0][0]) <= 1024
    assert "".join(text for text, _ in chunks) == expected
    bold_segments = []
    links = []
    for text, entities in chunks:
        for entity in entities:
            assert 0 <= entity.offset < entity.offset + entity.length <= units(text)
            if entity.type == "bold":
                bold_segments.append(entity_text(text, entity))
            elif entity.type == "text_link":
                links.append((entity_text(text, entity), entity.url))
    assert "".join(bold_segments) == bold_content
    assert links == [("сайт", "https://example.com")]
    assert all(
        call.kwargs["parse_mode"] is None for call in transport.bot.send_message.await_args_list
    )
    transport.bot.send_rich_message.assert_not_awaited()


async def test_caption_retry_serializes_entities_and_does_not_resend_photo(transport):
    caption = "**" + "Я" * 1600 + "**"
    transport.bot.send_message.side_effect = TimeoutError()
    with pytest.raises(PartialDeliveryError) as caught:
        await transport.send(
            42,
            77,
            {
                "image_paths": ["/tmp/logo.png"],
                "caption": caption,
                "format": "rich",
                "reply_markup": navigation_keyboard(),
            },
        )
    assert caught.value.sent_ids == [10]
    # Simulate the outbox JSON round trip, not an in-memory pydantic object continuation.
    remaining = json.loads(json.dumps(caught.value.remaining_payload))
    assert all(part["kind"] != "image" for part in remaining["_telegram_parts"])
    first = transport.bot.send_photo.await_args.kwargs
    transport.bot.send_message.reset_mock(side_effect=True)
    transport.bot.send_message.return_value = SimpleNamespace(message_id=31)
    assert await transport.send(42, 77, remaining) == [31]
    transport.bot.send_photo.assert_awaited_once()
    final = transport.bot.send_message.await_args.kwargs
    assert first["caption"] + final["text"] == "Я" * 1600
    entities = final["entities"]
    assert len(entities) == 1 and entities[0].type == "bold"
    assert entities[0].offset == 0 and entities[0].length == units(final["text"])
    assert isinstance(final["reply_markup"], ReplyKeyboardMarkup)
    assert final["message_thread_id"] == 77


async def test_rich_document_caption_uses_entities_without_separate_text_message(transport):
    assert await transport.send(
        42,
        77,
        {
            "document_path": "/tmp/report.pdf",
            "caption": "Отчёт **Phonix**",
            "format": "rich",
        },
    ) == [20]
    sent = transport.bot.send_document.await_args.kwargs
    assert sent["caption"] == "Отчёт Phonix"
    assert isinstance(sent["document"], FSInputFile)
    assert sent["parse_mode"] is None
    assert [(entity.type, entity.offset, entity.length) for entity in sent["caption_entities"]] == [
        ("bold", 6, 6),
    ]
    transport.bot.send_message.assert_not_awaited()


@pytest.mark.parametrize(
    "field,method", [("image_path", "send_photo"), ("document_path", "send_document")]
)
async def test_definite_entity_rejection_retries_same_clean_caption_without_entities(
    transport, field, method
):
    api_method = (
        SendPhoto(chat_id=42, photo="file-id")
        if field == "image_path"
        else SendDocument(chat_id=42, document="file-id")
    )
    sender = getattr(transport.bot, method)
    sender.side_effect = [
        TelegramBadRequest(method=api_method, message="Bad Request: can't parse entities"),
        SimpleNamespace(message_id=51),
    ]
    assert await transport.send(
        42,
        77,
        {
            field: "/tmp/result.bin",
            "caption": "Логотип **Phonix**",
            "format": "rich",
        },
    ) == [51]
    first, second = [call.kwargs for call in sender.await_args_list]
    assert first["caption"] == second["caption"] == "Логотип Phonix"
    assert first["caption_entities"] and not second.get("caption_entities")
    assert first["parse_mode"] is second["parse_mode"] is None
    assert first["message_thread_id"] == second["message_thread_id"] == 77
    transport.bot.send_message.assert_not_awaited()


@pytest.mark.parametrize("failure", ["timeout", "unrelated-bad-request", "rate-limit"])
async def test_caption_does_not_retry_media_for_ambiguous_or_unrelated_failures(transport, failure):
    api_method = SendPhoto(chat_id=42, photo="file-id")
    error = {
        "timeout": TimeoutError(),
        "unrelated-bad-request": TelegramBadRequest(
            method=api_method, message="Bad Request: chat not found"
        ),
        "rate-limit": TelegramRetryAfter(method=api_method, message="Retry later", retry_after=5),
    }[failure]
    transport.bot.send_photo.side_effect = error
    with pytest.raises(type(error)):
        await transport.send(
            42,
            77,
            {
                "image_path": "/tmp/logo.png",
                "caption": "**Phonix**",
                "format": "rich",
            },
        )
    transport.bot.send_photo.assert_awaited_once()
    transport.bot.send_message.assert_not_awaited()
