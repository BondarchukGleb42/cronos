"""User-created Telegram conversations, with durable creation intent."""

import hashlib
import json
import unicodedata

from aiogram.exceptions import TelegramBadRequest, TelegramNetworkError, TelegramServerError
from aiohttp_socks import ProxyConnectionError, ProxyError, ProxyTimeoutError

from cronos.telegram import new_chat_keyboard

TOPICS_UNAVAILABLE = (
    "Темы ещё не включены в настройках Cronos в Telegram. "
    "После включения появится общий список чатов, как в группе с темами. "
    "Пока можно продолжать этот разговор."
)


async def welcome_topic(store, user_id, chat_id, thread_id, *, title_auto=True):
    text = "Новый чат готов. Напиши вопрос. "
    if title_auto:
        text += "Название появится после моего ответа. "
    text += "Между чатами можно переключаться в списке тем Telegram."
    await store.enqueue(
        user_id,
        chat_id,
        thread_id,
        {
            "text": text,
            "reply_markup": new_chat_keyboard(),
        },
        f"topic-welcome:{chat_id}:{thread_id}",
    )


def _unknown_creation() -> dict:
    return {
        "status": "unknown",
        "may_have_completed": True,
        "automatic_retry_allowed": False,
        "error": "Telegram не подтвердил создание. Новый чат мог уже появиться в списке тем. "
        "Не повторяй создание автоматически: сначала проверь список; "
        "повторная попытка возможна только по новой просьбе пользователя.",
    }


async def create_chat(
    store,
    transport,
    user_id,
    chat_id,
    operation_id,
    name="Новый чат",
    *,
    creation_scope: str | None = None,
):
    """Retry DB/delivery work, but never blindly repeat an ambiguous Telegram create."""
    if creation_scope is not None:
        # A model retry uses a new tool-call ID. Share the durable intent for the
        # same requested name within its run, not across new user requests.
        normalized_name = unicodedata.normalize("NFC", " ".join(name.split()))
        normalized_name = normalized_name.encode("utf-8")[:128].decode("utf-8", errors="ignore")
        normalized_name = (normalized_name or "Новый чат").casefold()
        identity = json.dumps([user_id, chat_id, creation_scope, normalized_name])
        operation_id = "topic-scope:" + hashlib.sha256(identity.encode("utf-8")).hexdigest()
    result = await store.operation(operation_id)
    if result is not None and "error" in result:
        return result
    if result is None:
        capabilities = await transport.topic_capabilities()
        if not capabilities.has_topics_enabled:
            return {"error": TOPICS_UNAVAILABLE}
        async with store.connection() as conn:
            inserted = await conn.fetchval(
                """INSERT INTO operations(id,user_id,kind,status)
                VALUES($1,$2,'topic_intent','started') ON CONFLICT DO NOTHING RETURNING id""",
                operation_id + ":intent",
                user_id,
            )
        if not inserted:
            return _unknown_creation()
        try:
            topic = await transport.create_topic(chat_id, name)
        except TelegramBadRequest:
            # A rejected request has not created a topic. A new click can retry safely.
            result = {
                "status": "rejected",
                "may_have_completed": False,
                "automatic_retry_allowed": False,
                "error": "Не удалось создать тему. Проверь, включены ли темы у Cronos, "
                "и попробуй снова.",
            }
            await store.save_operation(operation_id, user_id, None, "topic_create", result)
            return result
        except (
            TelegramNetworkError,
            TelegramServerError,
            TimeoutError,
            OSError,
            ProxyConnectionError,
            ProxyError,
            ProxyTimeoutError,
        ):
            result = _unknown_creation()
            await store.save_operation(operation_id, user_id, None, "topic_create", result)
            return result
        result = {"thread_id": topic["message_thread_id"], "name": topic.get("name", name)}
        await store.save_operation(operation_id, user_id, None, "topic_create", result)
    conversation = await store.conversation(user_id, chat_id, result["thread_id"], result["name"])
    await welcome_topic(store, user_id, chat_id, result["thread_id"])
    return {"id": str(conversation["id"]), **result}
