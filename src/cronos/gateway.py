"""Durable polling ingress. No model calls or outgoing conversation messages."""

import asyncio
import logging
import time
from contextlib import asynccontextmanager, suppress
from uuid import uuid4

import uvicorn
from aiogram import Bot
from aiogram.types import BotCommand, BotCommandScopeAllPrivateChats
from fastapi import FastAPI
from fastapi.responses import JSONResponse

from cronos.logging import configure_logging
from cronos.settings import Settings, get_settings
from cronos.storage import Store
from cronos.telegram import TelegramTransport

logger = logging.getLogger(__name__)
ALLOWED_UPDATES = [
    "message",
    "edited_message",
    "callback_query",
    "my_chat_member",
    "stopped_message_generation",
]


async def poll_once(store: Store, bot: Bot, owner: str) -> bool:
    if not await store.acquire_poll_lease(owner):
        return False
    offset = await store.get_poll_offset()
    updates = await bot.get_updates(
        offset=offset,
        limit=100,
        timeout=20,
        request_timeout=30,
        allowed_updates=ALLOWED_UPDATES,
    )
    # Renew after the network await; an instance that lost leadership cannot ingest.
    if not await store.acquire_poll_lease(owner):
        return False
    if updates:
        raw = [
            update.model_dump(mode="json", by_alias=True, exclude_none=True) for update in updates
        ]
        accepted = await store.ingest_updates(
            raw, max(update.update_id for update in updates) + 1, owner=owner
        )
        if accepted is False:
            return False
    # Only the NEXT getUpdates confirms this batch, using the committed DB cursor.
    return True


async def poll_forever(store: Store, bot: Bot, owner: str, on_success=None):
    try:
        while True:
            try:
                if not await poll_once(store, bot, owner):
                    await asyncio.sleep(3)
                elif on_success:
                    on_success()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                # Telegram/network exceptions can contain request data; log only the class.
                logger.warning("Telegram ingress retry: %s", type(exc).__name__)
                await asyncio.sleep(3)
    finally:
        with suppress(Exception):
            await store.release_poll_lease(owner)


def create_app(settings: Settings | None = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        config = settings or get_settings()
        configure_logging(config.log_level)
        store = Store(config)
        transport = TelegramTransport(config)
        try:
            await store.open()
            try:
                await transport.bot.set_my_commands(
                    [
                        BotCommand(command=command, description=description)
                        for command, description in (
                            ("new", "Новый чат"),
                            ("chats", "Мои чаты"),
                            ("stop", "Остановить ответ"),
                            ("plans", "Тестовые тарифы"),
                            ("start", "Начать общение"),
                        )
                    ],
                    scope=BotCommandScopeAllPrivateChats(),
                    request_timeout=10,
                )
            except Exception as exc:
                logger.warning("Telegram commands setup retry on restart: %s", type(exc).__name__)
            app.state.store = store
            app.state.last_poll_success = 0.0
            task = asyncio.create_task(
                poll_forever(
                    store,
                    transport.bot,
                    str(uuid4()),
                    lambda: setattr(app.state, "last_poll_success", time.monotonic()),
                )
            )
            app.state.poll_task = task
            try:
                yield
            finally:
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task
        finally:
            await transport.close()
            await store.close()

    application = FastAPI(title="Cronos gateway", lifespan=lifespan)

    @application.get("/healthz")
    async def health():
        return {"status": "ok"}

    @application.get("/readyz")
    async def ready():
        try:
            healthy = (
                not application.state.poll_task.done()
                and application.state.last_poll_success > 0
                and time.monotonic() - application.state.last_poll_success < 90
                and await application.state.store.ready()
            )
        except Exception:
            healthy = False
        return JSONResponse(
            {"status": "ready" if healthy else "unavailable"}, status_code=200 if healthy else 503
        )

    return application


app = create_app()

if __name__ == "__main__":
    settings = get_settings()
    uvicorn.run(app, host="0.0.0.0", port=settings.port, log_level=settings.log_level.lower())
