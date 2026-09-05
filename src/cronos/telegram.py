"""Telegram delivery and a deliberately small, safe Markdown renderer."""

import logging
import re
from typing import Any, TypedDict
from urllib.parse import urlsplit

from aiogram import Bot
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.exceptions import TelegramAPIError, TelegramBadRequest
from aiogram.types import FSInputFile, InlineKeyboardMarkup, InputRichMessage
from aiohttp_socks import ProxyConnectionError, ProxyError, ProxyTimeoutError

from cronos.settings import Settings

logger = logging.getLogger(__name__)
_INLINE = re.compile(r"(`[^`\n]+`|\*\*[^*\n]+\*\*|\|\|[^|\n]+\|\||\[[^\]\n]+\]\([^\s)]+\))")


class _Destination(TypedDict):
    chat_id: int
    message_thread_id: int | None


class PartialDeliveryError(Exception):
    """Known successful parts plus a JSON-safe continuation for the sender outbox."""

    def __init__(self, sent_ids: list[int], remaining_payload: dict, cause: Exception):
        super().__init__(
            f"Partial Telegram delivery: {len(sent_ids)} parts sent; {type(cause).__name__}"
        )
        self.sent_ids = sent_ids
        self.remaining_payload = remaining_payload
        self.cause = cause


def upgrade_keyboard() -> dict:
    return {
        "inline_keyboard": [
            [{"text": f"{plan} · тест, без оплаты", "callback_data": f"plan:{plan}"}]
            for plan in ("START", "PREMIUM", "PRO")
        ]
    }


def split_text(text: str, limit: int = 3900) -> list[str]:
    """Split without losing whitespace or cutting a non-BMP character in half."""
    if limit < 2:
        raise ValueError("Text chunk limit must be at least 2")
    chunks = []
    start = 0
    while start < len(text):
        units = 0
        end = start
        boundary = start
        while end < len(text):
            width = 2 if ord(text[end]) > 0xFFFF else 1
            if units + width > limit:
                break
            units += width
            end += 1
            if text[end - 1] in " \n\t":
                boundary = end
        if end < len(text) and boundary > start + (end - start) // 2:
            end = boundary
        chunks.append(text[start:end])
        start = end
    return chunks


def _inline(text: str) -> str | list | dict:
    """Create typed inline entities; HTML is always literal text."""
    parts = []
    pos = 0
    for match in _INLINE.finditer(text):
        if match.start() > pos:
            parts.append(text[pos : match.start()])
        token = match.group()
        if token.startswith("**"):
            parts.append({"type": "bold", "text": token[2:-2]})
        elif token.startswith("||"):
            parts.append({"type": "spoiler", "text": token[2:-2]})
        elif token.startswith("`"):
            parts.append({"type": "code", "text": token[1:-1]})
        else:
            label, url = token[1:-1].split("](", 1)
            try:
                parsed = urlsplit(url)
                allowed = parsed.scheme in {"http", "https"} and bool(parsed.hostname)
            except ValueError:
                allowed = False
            parts.append({"type": "url", "text": label, "url": url} if allowed else token)
        pos = match.end()
    if pos < len(text):
        parts.append(text[pos:])
    return parts if len(parts) > 1 else (parts[0] if parts else text)


def _cells(line: str) -> list[str]:
    return [
        cell.strip().replace(r"\|", "|") for cell in re.split(r"(?<!\\)\|", line.strip().strip("|"))
    ]


def rich_message(text: str) -> InputRichMessage:
    """Support tables, headings, code, quotes and safe inline formatting."""
    lines = text.splitlines()
    blocks: list[dict[str, Any]] = []
    paragraph = []
    budget = 0

    def flush():
        if paragraph:
            blocks.append({"type": "paragraph", "text": _inline("\n".join(paragraph))})
            paragraph.clear()

    i = 0
    while i < len(lines):
        line = lines[i]
        if line.startswith("```"):
            flush()
            language = line[3:].strip()
            code = []
            i += 1
            while i < len(lines) and not lines[i].startswith("```"):
                code.append(lines[i])
                i += 1
            blocks.append({"type": "pre", "text": "\n".join(code), "language": language or None})
            i += 1
            continue
        if "|" in line and i + 1 < len(lines):
            headers, separators = _cells(line), _cells(lines[i + 1])
            if (
                1 < len(headers) <= 20
                and len(headers) == len(separators)
                and all(re.fullmatch(r":?-{3,}:?", x) for x in separators)
            ):
                flush()
                align = [
                    "center"
                    if x.startswith(":") and x.endswith(":")
                    else "right"
                    if x.endswith(":")
                    else "left"
                    for x in separators
                ]
                rows = [headers]
                i += 2
                while i < len(lines) and "|" in lines[i] and len(_cells(lines[i])) == len(headers):
                    rows.append(_cells(lines[i]))
                    i += 1
                blocks.append(
                    {
                        "type": "table",
                        "is_bordered": True,
                        "is_striped": True,
                        "cells": [
                            [
                                {
                                    "text": _inline(value),
                                    "align": align[col],
                                    "valign": "top",
                                    "is_header": row == 0,
                                }
                                for col, value in enumerate(values)
                            ]
                            for row, values in enumerate(rows)
                        ],
                    }
                )
                budget += len(rows)
                continue
        heading = re.match(r"^(#{1,6})\s+(.+)$", line)
        if heading:
            flush()
            blocks.append({"type": "heading", "size": len(heading[1]), "text": _inline(heading[2])})
        elif line.startswith("> "):
            flush()
            blocks.append(
                {
                    "type": "blockquote",
                    "blocks": [
                        {"type": "paragraph", "text": _inline(line[2:])},
                    ],
                }
            )
            budget += 1
        elif not line.strip():
            flush()
        else:
            paragraph.append(line)
        i += 1
    flush()
    if not blocks or len(blocks) + budget > 450:
        blocks = [{"type": "paragraph", "text": text}]
    return InputRichMessage.model_validate({"blocks": blocks})


def _format_error(exc: TelegramBadRequest) -> bool:
    message = exc.message.lower()
    return any(
        fragment in message
        for fragment in (
            "can't parse",
            "cannot parse",
            "rich_message",
            "rich message",
            "rich text",
            "entity",
            "entities",
            "unsupported",
            "method not found",
            "too many blocks",
        )
    )


class TelegramTransport:
    def __init__(self, settings: Settings):
        proxy = (
            settings.telegram_proxy.get_secret_value().strip() if settings.telegram_proxy else ""
        )
        if not proxy:
            raise ValueError("TELEGRAM_PROXY is required for Telegram connections")
        # No global parse_mode: neither model HTML nor fallback text is interpreted.
        # This one session also carries polling, downloads, drafts and callbacks.
        # A missing/failed proxy must never silently create a direct connection.
        try:
            session = AiohttpSession(proxy=proxy)
        except ValueError, TypeError, RuntimeError:
            raise ValueError("TELEGRAM_PROXY could not be configured") from None
        self.bot = Bot(settings.telegram_bot_token.get_secret_value(), session=session)

    async def close(self):
        await self.bot.session.close()

    async def send(self, chat_id: int, thread_id: int | None, payload: dict) -> list[int]:
        destination: _Destination = {"chat_id": chat_id, "message_thread_id": thread_id or None}
        parts: list[dict[str, Any]] | None = payload.get("_telegram_parts")
        if parts is None:
            rich = payload.get("format") == "rich"
            parts = [
                {"kind": "rich" if rich else "text", "text": chunk}
                for chunk in split_text(str(payload.get("text") or ""), 12000 if rich else 3900)
            ]
            caption_parts = split_text(str(payload.get("caption") or ""), 1000)
            caption = caption_parts[0] if caption_parts else None
            for field, kind in (("document_path", "document"), ("image_path", "image")):
                if payload.get(field):
                    parts.append({"kind": kind, "path": payload[field], "caption": caption})
            parts.extend({"kind": "text", "text": text} for text in caption_parts[1:])
            if parts and payload.get("reply_markup"):
                parts[-1]["reply_markup"] = payload["reply_markup"]
        else:
            # Do not mutate a persisted payload while expanding a rejected rich part.
            parts = [dict(part) for part in parts]
        if not parts:
            raise ValueError("Telegram payload must contain text or a file")
        sent = []
        index = 0
        while index < len(parts):
            part = parts[index]
            try:
                markup = (
                    InlineKeyboardMarkup.model_validate(part["reply_markup"])
                    if part.get("reply_markup")
                    else None
                )
                if part["kind"] == "rich":
                    try:
                        message = await self.bot.send_rich_message(
                            **destination,
                            rich_message=rich_message(part["text"]),
                            reply_markup=markup,
                        )
                    except TelegramBadRequest as exc:
                        if not _format_error(exc):
                            raise
                        replacements = [
                            {"kind": "text", "text": text} for text in split_text(part["text"])
                        ]
                        if replacements and part.get("reply_markup"):
                            replacements[-1]["reply_markup"] = part["reply_markup"]
                        parts[index : index + 1] = replacements
                        logger.info("Rich message rejected; using literal text")
                        continue
                elif part["kind"] == "text":
                    message = await self.bot.send_message(
                        **destination,
                        text=part["text"],
                        parse_mode=None,
                        reply_markup=markup,
                    )
                elif part["kind"] == "document":
                    message = await self.bot.send_document(
                        **destination,
                        document=FSInputFile(part["path"]),
                        caption=part.get("caption"),
                        parse_mode=None,
                        reply_markup=markup,
                    )
                elif part["kind"] == "image":
                    message = await self.bot.send_photo(
                        **destination,
                        photo=FSInputFile(part["path"]),
                        caption=part.get("caption"),
                        parse_mode=None,
                        reply_markup=markup,
                    )
                else:
                    raise ValueError("Unsupported Telegram delivery part")
            except Exception as exc:
                if sent:
                    raise PartialDeliveryError(
                        sent, {"_telegram_parts": parts[index:]}, exc
                    ) from exc
                raise
            sent.append(message.message_id)
            index += 1
        return sent

    async def draft(self, chat_id: int, thread_id: int | None, text: str, draft_id: int):
        try:
            await self.bot.send_message_draft(
                chat_id=chat_id,
                message_thread_id=thread_id or None,
                draft_id=draft_id or 1,
                text=(split_text(text) or [""])[0],
                parse_mode=None,
                can_stop=True,
            )
        except (
            TelegramAPIError,
            TimeoutError,
            OSError,
            ProxyConnectionError,
            ProxyError,
            ProxyTimeoutError,
        ):
            # A lost ephemeral preview must not lose the durable final answer.
            logger.debug("Telegram draft unavailable")

    async def create_topic(self, chat_id: int, name: str) -> dict:
        topic = await self.bot.create_forum_topic(
            chat_id=chat_id, name=name.strip()[:128] or "Новый чат"
        )
        return topic.model_dump(mode="json", by_alias=True, exclude_none=True)
