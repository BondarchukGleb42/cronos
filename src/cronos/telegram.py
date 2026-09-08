"""Telegram delivery and a deliberately small, safe Markdown renderer."""

import asyncio
import logging
import re
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any, TypedDict
from urllib.parse import urlsplit

from aiogram import Bot
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.exceptions import TelegramAPIError, TelegramBadRequest
from aiogram.types import (
    FSInputFile,
    InlineKeyboardMarkup,
    InputRichMessage,
    MessageEntity,
    ReplyKeyboardMarkup,
)
from aiohttp_socks import ProxyConnectionError, ProxyError, ProxyTimeoutError

from cronos.advanced_controls import MODEL_BUTTON, REASONING_BUTTON
from cronos.captions import caption_chunks
from cronos.settings import Settings

logger = logging.getLogger(__name__)
_INLINE = re.compile(r"(`[^`\n]+`|\*\*[^*\n]+\*\*|\|\|[^|\n]+\|\||\[[^\]\n]+\]\([^\s)]+\))")
_TOPIC_CAPABILITIES_TTL = 60.0
_DRAFT_CACHE_LIMIT = 128
_DRAFT_REQUEST_TIMEOUT = 3.0
_THINKING_TEXT = "Думаю..."


@dataclass
class _DraftPreview:
    text: str = _THINKING_TEXT
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


@dataclass(frozen=True)
class TopicCapabilities:
    has_topics_enabled: bool
    allows_users_to_create_topics: bool
    username: str | None


class _Destination(TypedDict):
    chat_id: int
    message_thread_id: int | None


class MessageDeletionResult(TypedDict):
    cleared: int
    failed: int
    unavailable_ids: list[int]


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


def navigation_keyboard() -> dict:
    """A collapsible input panel; its buttons arrive as ordinary text messages."""
    return {
        "keyboard": [
            [{"text": MODEL_BUTTON}, {"text": REASONING_BUTTON}],
            [{"text": "🗑 Удалить чат"}],
        ],
        "resize_keyboard": True,
        "is_persistent": False,
        "one_time_keyboard": False,
    }


def new_chat_keyboard() -> dict:
    """Compatibility name for the shared bottom navigation panel."""
    return navigation_keyboard()


def chat_navigation_keyboard(bot_username: str | None = None) -> dict:
    """Topic selection stays in Telegram's native list, with no invented jump URL."""
    return navigation_keyboard()


def _reply_markup(value: dict | None) -> InlineKeyboardMarkup | ReplyKeyboardMarkup | None:
    if not value:
        return None
    if "keyboard" in value:
        if "inline_keyboard" in value:
            raise ValueError("A Telegram message cannot have both keyboard types")
        return ReplyKeyboardMarkup.model_validate(value)
    return InlineKeyboardMarkup.model_validate(value)


def _topic_name(name: str) -> str:
    # MTProto additionally limits the title to 128 UTF-8 bytes.
    return (
        " ".join(name.split()).encode("utf-8")[:128].decode("utf-8", errors="ignore") or "Новый чат"
    )


def _bad_request_description(error: TelegramBadRequest) -> str:
    return error.message.strip().casefold().removeprefix("bad request: ")


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
        self._topic_capabilities: TopicCapabilities | None = None
        self._topic_capabilities_checked_at = 0.0
        self._topic_capabilities_lock = asyncio.Lock()
        self._drafts: OrderedDict[tuple[int, int, int], _DraftPreview] = OrderedDict()

    async def close(self):
        self._drafts.clear()
        await self.bot.session.close()

    async def topic_capabilities(self, force_refresh: bool = False) -> TopicCapabilities:
        """Cache successful getMe results briefly so BotFather changes can take effect.

        User-side topic creation is independent of the bot's ability to create
        topics. API failures propagate instead of masquerading as disabled mode.
        """
        async with self._topic_capabilities_lock:
            if (
                not force_refresh
                and self._topic_capabilities is not None
                and time.monotonic() - self._topic_capabilities_checked_at < _TOPIC_CAPABILITIES_TTL
            ):
                return self._topic_capabilities
            me = await self.bot.get_me()
            capabilities = TopicCapabilities(
                has_topics_enabled=getattr(me, "has_topics_enabled", None) is True,
                allows_users_to_create_topics=getattr(me, "allows_users_to_create_topics", None)
                is True,
                username=getattr(me, "username", None),
            )
            self._topic_capabilities = capabilities
            self._topic_capabilities_checked_at = time.monotonic()
            return capabilities

    async def send(self, chat_id: int, thread_id: int | None, payload: dict) -> list[int]:
        destination: _Destination = {"chat_id": chat_id, "message_thread_id": thread_id or None}
        parts: list[dict[str, Any]] | None = payload.get("_telegram_parts")
        edit_message_id = payload.get("edit_message_id")
        if parts is None and edit_message_id is not None:
            if type(edit_message_id) is not int or edit_message_id <= 0:
                raise ValueError("A positive Telegram edit message ID is required")
            if any(
                payload.get(key)
                for key in ("document_path", "image_path", "image_paths", "caption")
            ):
                raise ValueError("An editable panel must contain only text and its keyboard")
            if await self._edit_panel(chat_id, edit_message_id, payload):
                if payload.get("pin"):
                    await self.pin_message(chat_id, edit_message_id)
                return [edit_message_id]
            # An absent/uneditable panel is recreated only in the supplied destination.
            # Network failures never reach this fallback: the edit may have succeeded.
            payload = {**payload, "format": "text"}
        if parts is None:
            rich = payload.get("format") == "rich"
            rich_caption = rich or payload.get("caption_format") == "rich"
            caption_parts: list[dict[str, Any]]
            text = str(payload.get("text") or "")
            caption_text = str(payload.get("caption") or "")
            image_paths = payload.get("image_paths") or []
            if not isinstance(image_paths, list) or any(
                not isinstance(path, str) or not path for path in image_paths
            ):
                raise ValueError("image_paths must be a list of nonempty file paths")
            images = list(
                dict.fromkeys(
                    ([payload["image_path"]] if payload.get("image_path") else []) + image_paths
                )
            )
            if images and not payload.get("document_path"):
                # The answer belongs to the photo, not to a separate preceding bubble.
                if text and caption_text and text != caption_text:
                    caption_text = text + "\n\n" + caption_text
                else:
                    caption_text = text or caption_text
                caption_parts = (
                    caption_chunks(caption_text)
                    if rich_caption
                    else [
                        {"text": chunk, "entities": []} for chunk in split_text(caption_text, 1024)
                    ]
                )
                parts = [
                    {
                        "kind": "image",
                        "path": path,
                        "caption": caption_parts[0]["text"]
                        if index == 0 and caption_parts
                        else None,
                    }
                    for index, path in enumerate(images)
                ]
            else:
                parts = [
                    {"kind": "rich" if rich else "text", "text": chunk}
                    for chunk in split_text(text, 12000 if rich else 3900)
                ]
                caption_parts = (
                    caption_chunks(caption_text)
                    if rich_caption
                    else [
                        {"text": chunk, "entities": []} for chunk in split_text(caption_text, 1024)
                    ]
                )
                caption = caption_parts[0]["text"] if caption_parts else None
                if payload.get("document_path"):
                    parts.append(
                        {"kind": "document", "path": payload["document_path"], "caption": caption}
                    )
                parts.extend(
                    {"kind": "image", "path": path, "caption": caption if index == 0 else None}
                    for index, path in enumerate(images)
                )
            # Do not truncate model output if it exceeds Telegram's caption limit.
            if caption_parts and caption_parts[0]["entities"]:
                for part in parts:
                    if part["kind"] in {"image", "document"} and part.get("caption"):
                        part["caption_entities"] = caption_parts[0]["entities"]
            if rich_caption:
                parts.extend({"kind": "text", **chunk} for chunk in caption_parts[1:])
            else:
                parts.extend(
                    {"kind": "text", "text": chunk}
                    for chunk in split_text("".join(part["text"] for part in caption_parts[1:]))
                )
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
                markup = _reply_markup(part.get("reply_markup"))
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
                        **(
                            {
                                "entities": [
                                    MessageEntity.model_validate(entity)
                                    for entity in part["entities"]
                                ]
                            }
                            if part.get("entities")
                            else {}
                        ),
                    )
                elif part["kind"] in {"document", "image"}:
                    method = (
                        self.bot.send_document
                        if part["kind"] == "document"
                        else self.bot.send_photo
                    )
                    media: dict[str, Any] = {
                        "document" if part["kind"] == "document" else "photo": FSInputFile(
                            part["path"]
                        )
                    }
                    caption_options = (
                        {
                            "caption_entities": [
                                MessageEntity.model_validate(entity)
                                for entity in part["caption_entities"]
                            ]
                        }
                        if part.get("caption_entities")
                        else {}
                    )
                    try:
                        message = await method(
                            **destination,
                            **media,
                            caption=part.get("caption"),
                            parse_mode=None,
                            reply_markup=markup,
                            **caption_options,
                        )
                    except TelegramBadRequest as exc:
                        if not caption_options or not _format_error(exc):
                            raise
                        # A definite formatting rejection is safe to retry. Keep
                        # the rendered readable caption, without exposing Markdown.
                        message = await method(
                            **destination,
                            **media,
                            caption=part.get("caption"),
                            parse_mode=None,
                            reply_markup=markup,
                        )
                else:
                    raise ValueError("Unsupported Telegram delivery part")
            except Exception as exc:
                if sent:
                    remainder: dict[str, Any] = {"_telegram_parts": parts[index:]}
                    if payload.get("pin"):
                        remainder["pin"] = True
                    raise PartialDeliveryError(sent, remainder, exc) from exc
                raise
            sent.append(message.message_id)
            index += 1
        if payload.get("pin"):
            await self.pin_message(chat_id, sent[-1])
        return sent

    async def _edit_panel(self, chat_id: int, message_id: int, payload: dict) -> bool:
        text = str(payload.get("text") or "")
        if not text:
            raise ValueError("An editable panel must contain text")
        markup = _reply_markup(payload.get("reply_markup"))
        if isinstance(markup, ReplyKeyboardMarkup):
            raise ValueError(
                "Editable panels accept inline keyboards only; send bottom navigation separately"
            )
        try:
            result = await self.bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=text,
                parse_mode=None,
                reply_markup=markup,
            )
        except TelegramBadRequest as exc:
            description = _bad_request_description(exc)
            if description in {
                "message_not_modified",
                "message is not modified",
                "message is not modified: specified new message content and reply markup are exactly the same as a current content and reply markup of the message",
            }:
                return True
            if description in {"message to edit not found", "message can't be edited"}:
                return False
            raise
        if not result:
            raise RuntimeError("Telegram did not acknowledge panel update")
        return True

    async def pin_message(self, chat_id: int, message_id: int) -> bool:
        """Pin best-effort without converting an acknowledged delivery into a retry."""
        try:
            async with asyncio.timeout(5):
                return bool(
                    await self.bot.pin_chat_message(
                        chat_id=chat_id, message_id=message_id, disable_notification=True
                    )
                )
        except Exception as exc:
            # Exception messages may contain a proxy URL or other sensitive details.
            logger.info("Telegram pin unavailable: %s", type(exc).__name__)
            return False

    async def draft(self, chat_id: int, thread_id: int | None, text: str, draft_id: int):
        key = (chat_id, thread_id or 0, draft_id or 1)
        preview = self._drafts.get(key)
        if preview is None:
            preview = _DraftPreview()
            self._drafts[key] = preview
            while len(self._drafts) > _DRAFT_CACHE_LIMIT:
                self._drafts.popitem(last=False)
        self._drafts.move_to_end(key)
        async with preview.lock:
            if self._drafts.get(key) is not preview:
                return
            # A late initial heartbeat must not replace already streamed text.
            if text.strip() and (text != _THINKING_TEXT or preview.text == _THINKING_TEXT):
                preview.text = split_text(text)[0]
            await self._send_draft(key, preview.text)

    async def refresh_draft(self, chat_id: int, thread_id: int | None, draft_id: int):
        key = (chat_id, thread_id or 0, draft_id or 1)
        preview = self._drafts.get(key)
        if preview is None:
            return
        async with preview.lock:
            if self._drafts.get(key) is preview:
                self._drafts.move_to_end(key)
                await self._send_draft(key, preview.text)

    def forget_draft(self, chat_id: int, thread_id: int | None, draft_id: int) -> None:
        self._drafts.pop((chat_id, thread_id or 0, draft_id or 1), None)

    async def _send_draft(self, key: tuple[int, int, int], text: str):
        chat_id, thread_id, draft_id = key
        try:
            async with asyncio.timeout(_DRAFT_REQUEST_TIMEOUT):
                await self.bot.send_message_draft(
                    chat_id=chat_id,
                    message_thread_id=thread_id or None,
                    draft_id=draft_id,
                    text=text,
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
        topic = await self.bot.create_forum_topic(chat_id=chat_id, name=_topic_name(name))
        return topic.model_dump(mode="json", by_alias=True, exclude_none=True)

    async def edit_topic(self, chat_id: int, thread_id: int, name: str) -> bool:
        if thread_id <= 0:
            raise ValueError("A positive Telegram topic ID is required")
        try:
            return await self.bot.edit_forum_topic(
                chat_id=chat_id, message_thread_id=thread_id, name=_topic_name(name)
            )
        except TelegramBadRequest as exc:
            # Renaming a topic to its current name is an already-applied retry.
            if "TOPIC_NOT_MODIFIED" in exc.message.upper():
                return True
            raise

    async def delete_topic(self, chat_id: int, thread_id: int) -> bool:
        """Delete a private topic and its entire history; General cannot be deleted."""
        if thread_id <= 1:
            raise ValueError("The General topic cannot be deleted; a topic ID above 1 is required")
        try:
            return await self.bot.delete_forum_topic(chat_id=chat_id, message_thread_id=thread_id)
        except TelegramBadRequest as exc:
            # Do not mistake permission failures or TOPIC_ID_INVALID for an applied retry.
            if _bad_request_description(exc) in {
                "topic_not_found",
                "message thread not found",
                "forum topic not found",
            }:
                return True
            raise

    async def delete_messages(self, chat_id: int, message_ids: list[int]) -> MessageDeletionResult:
        """Clear known IDs subject to Telegram's age/service-message restrictions.

        ``cleared`` counts acknowledged deletion OR absence, because deleteMessages
        silently skips missing IDs. ``unavailable_ids`` contains only known IDs that
        Telegram explicitly refused to delete. Transient or unknown failures raise;
        callers may safely retry the same IDs without restoring cleared messages.
        """
        if any(type(message_id) is not int or message_id <= 0 for message_id in message_ids):
            raise ValueError("Positive Telegram message IDs are required")
        unique_ids = list(dict.fromkeys(message_ids))
        result: MessageDeletionResult = {"cleared": 0, "failed": 0, "unavailable_ids": []}
        missing_errors = {"message to delete not found"}
        undeletable_errors = {"message can't be deleted", "messages can't be deleted"}
        for start in range(0, len(unique_ids), 100):
            chunk = unique_ids[start : start + 100]
            try:
                acknowledged = await self.bot.delete_messages(chat_id=chat_id, message_ids=chunk)
            except TelegramBadRequest as exc:
                if _bad_request_description(exc) not in missing_errors | undeletable_errors:
                    raise
            else:
                if not acknowledged:
                    raise RuntimeError("Telegram did not acknowledge message deletion")
                result["cleared"] += len(chunk)
                continue
            # A mixed-age batch or topic creation service message may reject a batch.
            # Isolate those IDs without hiding proxy failures, rate limits or new errors.
            for message_id in chunk:
                try:
                    acknowledged = await self.bot.delete_message(
                        chat_id=chat_id, message_id=message_id
                    )
                except TelegramBadRequest as exc:
                    description = _bad_request_description(exc)
                    if description in undeletable_errors:
                        result["failed"] += 1
                        result["unavailable_ids"].append(message_id)
                        continue
                    if description not in missing_errors:
                        raise
                else:
                    if not acknowledged:
                        raise RuntimeError("Telegram did not acknowledge message deletion")
                result["cleared"] += 1
        return result
