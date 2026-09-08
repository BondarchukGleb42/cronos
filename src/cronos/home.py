"""A durable home topic; callers hold the user's work lock."""

from aiogram.exceptions import TelegramBadRequest

from cronos.home_dashboard import personal_main_panel
from cronos.topics import create_chat

HOME_TITLE = "🪐 Cronos"


class HomeUnavailable(Exception):
    pass


def is_missing_topic_error(error: Exception) -> bool:
    """Only an unambiguous Telegram response can authorize home recovery."""
    return isinstance(error, TelegramBadRequest) and error.message.strip().casefold().removeprefix(
        "bad request: "
    ) in {"topic_not_found", "message thread not found", "forum topic not found"}


async def ensure_home(store, transport, user_id, chat_id, source_key):
    """Return the durable home without using a Telegram mutation as a read probe.

    Bot API has no read-only topic lookup. Re-sending its name with editForumTopic
    can produce a visible service message, so opening a panel must not rename it.
    A missing topic is recovered only after an actual delivery failure;
    neither opening the menu nor an uncertain create authorizes a new topic.
    """
    home = await store.get_home(user_id)
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
