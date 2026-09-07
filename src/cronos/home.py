"""A durable home topic; callers hold the user's work lock."""

from aiogram.exceptions import TelegramBadRequest

from cronos.home_dashboard import personal_main_panel
from cronos.topics import create_chat

HOME_TITLE = "🪐 Cronos"


class HomeUnavailable(Exception):
    pass


async def ensure_home(store, transport, user_id, chat_id, source_key, *, verify=False):
    """Return (conversation, created), without blindly retrying an unknown create."""
    home = await store.get_home(user_id)
    if home and verify:
        try:
            await transport.edit_topic(chat_id, home["thread_id"], HOME_TITLE)
        except TelegramBadRequest as error:
            reason = error.message.strip().casefold().removeprefix("bad request: ")
            if reason not in {
                "topic_not_found",
                "message thread not found",
                "forum topic not found",
            }:
                raise
            await store.reset_home(user_id, home["id"], source_key=f"home-missing:{source_key}")
            home = None
    if home:
        # A previous attempt may have stored the topic and failed before enqueue.
        await welcome_home(store, home)
        return home, False
    capabilities = await transport.topic_capabilities()
    if not capabilities.has_topics_enabled:
        return await store.conversation(user_id, chat_id, 0, HOME_TITLE), False
    result = await create_chat(
        store,
        transport,
        user_id,
        chat_id,
        await store.home_creation_key(user_id),
        HOME_TITLE,
        show_welcome=False,
    )
    if result.get("error"):
        raise HomeUnavailable(result["error"])
    home = await store.set_home(user_id, result["id"])
    await welcome_home(store, home)
    return home, True


async def welcome_home(store, home):
    await store.enqueue(
        home["user_id"],
        home["chat_id"],
        home["thread_id"],
        {**await personal_main_panel(store, home["user_id"]), "pin": True},
        f"home-welcome:{home['id']}",
    )


def unavailable_panel(message):
    return {
        "text": message + "\n\nПроверь список тем Telegram перед повторной попыткой.",
        "reply_markup": {
            "inline_keyboard": [
                [{"text": "Повторить создание 🪐 Cronos", "callback_data": "home:retry"}]
            ]
        },
    }
