import asyncio
import io
import logging
import os
import resource
import time
from contextlib import suppress
from pathlib import Path
from uuid import UUID, uuid4

import aio_pika

from cronos.advanced_controls import ADVANCED_ENTRYPOINTS, advanced_callback, advanced_panel
from cronos.agent import Agent, CancelledRun, setup_checkpoints
from cronos.file_tasks import file_task
from cronos.home import HomeUnavailable, ensure_home, unavailable_panel
from cronos.home_dashboard import (
    continue_home_project,
    hide_home_project,
    home_overview,
    home_project_panel,
    personal_main_panel,
)
from cronos.home_views import (
    chats_panel,
    guide_panel,
    memory_panel,
    plans_panel,
    projects_panel,
    settings_panel,
    tasks_panel,
)
from cronos.lifecycle import cancel_tasks, close_all, run_until_stopped
from cronos.logging import configure_logging
from cronos.privacy import (
    confirmation_keyboard,
    confirmation_scope,
    confirmation_text,
    perform_erasure,
)
from cronos.providers import Provider, ProviderError
from cronos.settings import get_settings
from cronos.storage import STOP_COMMANDS, Store, message_command
from cronos.telegram import TelegramTransport, navigation_keyboard, split_text
from cronos.topics import create_chat, welcome_topic

log = logging.getLogger(__name__)


def navigation_command(text):
    return {
        "➕ Новый чат": "/new",
        "🗑 Удалить чат": "/delete",
        "🪐 Главное меню": "/menu",
    }.get(text.strip(), text)


class Worker:
    def __init__(self, settings):
        self.settings = settings
        self.store = Store(settings)
        self.provider = Provider(settings)
        self.transport = TelegramTransport(settings)
        self.agent = Agent(settings, self.store, self.provider, self.transport)
        self.owner = f"worker:{uuid4()}"
        self.semaphore = asyncio.Semaphore(settings.worker_concurrency)
        self.healthy = Path("/tmp/worker.healthy")

    async def heartbeat(self, event):
        while await self.store.heartbeat(event["id"], self.owner):  # noqa: ASYNC110 -- lease renewal
            await asyncio.sleep(30)

    async def process(self, event_id=None):
        async with self.semaphore:
            event = await self.store.claim_event(self.owner, event_id)
            if not event:
                return
            heartbeat = asyncio.create_task(self.heartbeat(event))
            try:
                if event["kind"] == "timer":
                    await self.timer(event)
                elif event["kind"] == "privacy":
                    await perform_erasure(self.store, self.transport, self.agent.artifacts, event)
                elif event["kind"] == "home_init":
                    await self.home_init(event)
                elif event["kind"] in {"topic_title", "topic_title_reset"}:
                    await self.topic_title(event)
                else:
                    await self.telegram(event)
                await self.store.finish_event(event["id"], self.owner)
            except Exception as error:
                log.exception("Event failed: id=%s type=%s", event["id"], type(error).__name__)
                await self.store.finish_event(event["id"], self.owner, type(error).__name__)
            finally:
                heartbeat.cancel()
                with suppress(asyncio.CancelledError):
                    await heartbeat

    async def telegram(self, event):
        update = event["payload"]
        callback = update.get("callback_query")
        message = update.get("message") or (callback or {}).get("message")
        if not message:
            return
        if message.get("chat", {}).get("type") != "private":
            return
        sender = (callback or message).get("from", {})
        created = message.get("forum_topic_created")
        edited = message.get("forum_topic_edited")
        if created or edited:
            # Service updates describe this user's private chat even when the actor is our bot.
            user_id = message["chat"]["id"]
            thread_id = message.get("message_thread_id", 0)
            async with self.store.user_lock(user_id):
                current = await self.store.event_current(event["id"])
                if not current:
                    return
                message = current["payload"].get("message", {})
                created = message.get("forum_topic_created")
                edited = message.get("forum_topic_edited")
                sender = message.get("from", {})
                if created:
                    topic_conversation = await self.store.sync_topic_title(
                        user_id,
                        user_id,
                        thread_id,
                        created.get("name", "Новый чат"),
                        manual=not sender.get("is_bot")
                        and not created.get("is_name_implicit", False),
                        created=True,
                    )
                    if topic_conversation and not topic_conversation.get("is_home"):
                        await welcome_topic(
                            self.store,
                            user_id,
                            user_id,
                            thread_id,
                            title_auto=topic_conversation["title_auto"],
                        )
                elif edited and "name" in edited and not sender.get("is_bot"):
                    topic_conversation = await self.store.sync_topic_title(
                        user_id,
                        user_id,
                        thread_id,
                        edited["name"],
                        manual=True,
                        update_id=message.get("message_id", 0),
                    )
                    # A title job may have completed while this service update was queued.
                    # Restore the explicit user choice in Telegram as well as in our metadata.
                    if topic_conversation:
                        await self.transport.edit_topic(
                            user_id, thread_id, topic_conversation["title"]
                        )
            return
        user_id = sender.get("id")
        if (
            not user_id
            or user_id != message["chat"]["id"]
            or sender.get("is_bot")
            or message.get("chat", {}).get("type") != "private"
        ):
            return
        chat_id, thread_id = message["chat"]["id"], message.get("message_thread_id", 0)
        async with self.store.user_lock(user_id):
            current = await self.store.event_current(event["id"])
            if not current:
                return
            update = current["payload"]
            event = current
            callback = update.get("callback_query")
            message = update.get("message") or (callback or {}).get("message")
            if not message:
                return
            text = message.get("text") or message.get("caption") or ""
            text = navigation_command(text)
            if await self.privacy_control(event, user_id, chat_id, thread_id, text, callback):
                return
            if await self.advanced_control(event, user_id, chat_id, thread_id, text, callback):
                return
            if await self.home_control(event, user_id, chat_id, thread_id, text, callback):
                return
            conversation = await self.store.conversation(user_id, chat_id, thread_id)
            if callback:
                data = callback.get("data", "")
                if data == "chat:delete":
                    with suppress(Exception):
                        await self.transport.bot.answer_callback_query(callback["id"])
                    await self.request_deletion(event, conversation, "chat")
                    return
                if data == "chat:new":
                    with suppress(Exception):
                        await self.transport.bot.answer_callback_query(
                            callback["id"], text="Создаю новый чат…"
                        )
                    await self.new_chat(event, conversation)
                    return
                return
            command = message_command(text)
            if command in {"/delete", "/clearall"}:
                await self.request_deletion(
                    event, conversation, "all" if command == "/clearall" else "chat"
                )
                return
            if command == "/new":
                await self.new_chat(event, conversation)
                return
            if command in STOP_COMMANDS:
                await self.store.enqueue(
                    user_id,
                    chat_id,
                    thread_id,
                    {"text": "Остановил текущую задачу. Уже выполненные действия сохранены."},
                    f"stop:{event['id']}",
                )
                return
            if message.get("voice") or message.get("audio"):
                await self.store.enqueue(
                    user_id,
                    chat_id,
                    thread_id,
                    {
                        "text": "Голосовые появятся чуть позже. Пока напиши текстом или пришли документ."
                    },
                    f"voice:{event['id']}",
                )
                return
            attachments = []
            media_messages = update.get("media_group_messages") or [message]
            if any(
                item.get("chat", {}).get("id") != chat_id
                or item.get("from", {}).get("id") != user_id
                or item.get("message_thread_id", 0) != thread_id
                for item in media_messages
            ):
                raise ValueError("Album messages must belong to one user and conversation")
            if update.get("media_group_messages"):
                text = "\n\n".join(
                    item.get("caption") or item.get("text") or ""
                    for item in media_messages
                    if item.get("caption") or item.get("text")
                )
            for item in media_messages:
                document = item.get("document")
                if document:
                    attachments.append(
                        (document["file_id"], document.get("file_name", "document.bin"))
                    )
                if item.get("photo"):
                    attachments.append((item["photo"][-1]["file_id"], "photo.jpg"))
            image_refs, attachment_failed = [], False
            for file_id, filename in attachments:
                op = f"attachment:{event['id']}:{file_id}"
                artifact = await self.store.operation(op)
                if not artifact:
                    try:
                        metadata = await self.transport.bot.get_file(file_id)
                        if not metadata.file_path:
                            raise ValueError("Telegram не вернул файл")
                        output = io.BytesIO()
                        await self.transport.bot.download_file(
                            metadata.file_path, destination=output
                        )
                        artifact = await file_task(
                            self.agent.artifacts.ingest, user_id, filename, output.getvalue()
                        )
                        await self.store.save_artifact(user_id, artifact)
                        # Save only metadata; large extraction lives once in artifacts.
                        artifact = {key: artifact[key] for key in ("id", "filename", "mime")}
                        async with self.store.connection() as conn:
                            await conn.execute(
                                "INSERT INTO operations(id,user_id,kind,result,status) VALUES($1,$2,'attachment',$3,'done') ON CONFLICT DO NOTHING",
                                op,
                                user_id,
                                artifact,
                            )
                    except Exception as error:
                        attachment_failed = True
                        log.warning("Attachment failed: %s", type(error).__name__)
                        await self.store.enqueue(
                            user_id,
                            chat_id,
                            thread_id,
                            {
                                "text": f"Не удалось прочитать {filename}. Пришли PDF, XLSX, CSV, DOCX, текст или изображение. Если файл большой, раздели его: Telegram ограничивает скачивание ботом."
                            },
                            op + ":error",
                        )
                        continue
                text += f"\n[Пользователь приложил файл: {artifact['filename']}, artifact_id={artifact['id']}, mime={artifact['mime']}]"
                if artifact["mime"].startswith("image/"):
                    image_refs.append({"type": "image_ref", "artifact_id": str(artifact["id"])})
            # Generating from only a fraction of the supplied references changes the task.
            if attachment_failed:
                return
            if not text:
                return
            prompt = [{"type": "text", "text": text}, *image_refs] if image_refs else text
            if update.get("media_group_continuation"):
                await self.store.add_message(
                    user_id, conversation["id"], "user", prompt, f"user:{event['id']}"
                )
                await self.store.enqueue(
                    user_id,
                    chat_id,
                    thread_id,
                    {
                        "text": "Остальные вложения альбома пришли позже начала ответа. Теперь весь набор сохранён в этом чате. Напиши, как продолжить — учту все изображения.",
                        "reply_markup": navigation_keyboard(),
                    },
                    f"album-continuation:{event['id']}",
                )
                return
            await self.store.queue_home(user_id)
            await self.answer(event, conversation, prompt)

    async def home_init(self, event):
        user_id = event["payload"]["user_id"]
        async with self.store.user_lock(user_id):
            current = await self.store.event_current(event["id"])
            if not current:
                return
            key = await self.store.home_creation_key(user_id)
            if not key.endswith(":" + str(current["payload"].get("generation"))):
                return
            try:
                home, created = await ensure_home(
                    self.store, self.transport, user_id, user_id, str(event["id"])
                )
                if not created and not home.get("is_home"):
                    await self.store.enqueue(
                        user_id,
                        user_id,
                        0,
                        {**await personal_main_panel(self.store, user_id), "pin": True},
                        f"home-fallback:{event['id']}",
                    )
            except HomeUnavailable as error:
                await self.store.enqueue(
                    user_id, user_id, 0, unavailable_panel(str(error)), f"home-error:{event['id']}"
                )

    async def advanced_control(self, event, user_id, chat_id, thread_id, text, callback):
        page = ADVANCED_ENTRYPOINTS.get(text.strip()) if not callback else None
        values = None
        data = (callback or {}).get("data") or ""
        if data.startswith("advanced:"):
            parsed = advanced_callback(data)
            if parsed is None:
                with suppress(Exception):
                    await self.transport.bot.answer_callback_query(
                        callback["id"], text="Открой выбор из нижней панели."
                    )
                return True
            page, values = parsed
            with suppress(Exception):
                await self.transport.bot.answer_callback_query(callback["id"])
        if page is None:
            return False
        if values is not None:
            await self.store.set_model_preferences(
                user_id, values, f"callback:{callback['id']}:model-preference"
            )
        preferences = await self.store.preferences(user_id)
        panel = advanced_panel(page, preferences, self.settings)
        if callback:
            message_id = (callback.get("message") or {}).get("message_id")
            if isinstance(message_id, int) and not isinstance(message_id, bool) and message_id > 0:
                panel["edit_message_id"] = message_id
        order = event.get("update_id") or event["payload"].get("update_id") or 0
        await self.store.enqueue_home_panel(
            user_id,
            chat_id,
            thread_id,
            panel,
            f"home-panel:advanced:{event['id']}",
            order,
        )
        return True

    async def home_control(self, event, user_id, chat_id, thread_id, text, callback):
        command_pages = {
            "/start": "main",
            "/menu": "main",
            "/help": "guide",
            "/tasks": "tasks",
            "/chats": "chats",
            "/plans": "plans",
            "/upgrade": "plans",
            "/memory": "memory",
            "/projects": "projects",
            "/settings": "settings",
        }
        data = (callback or {}).get("data") or ""
        page = command_pages.get(message_command(text)) if not callback else None
        if not callback and text.strip().casefold() in {"меню", "главное меню"}:
            page = "main"
        number, action, value = 0, None, None
        if data.startswith("home:"):
            parts = data.split(":")
            page = parts[1]
            if page in {"chats", "tasks", "memory", "projects"}:
                if len(parts) != 3 or not parts[2].isdigit() or len(parts[2]) > 6:
                    page = None
                else:
                    number = int(parts[2])
            elif page in {"project", "hide", "continue", "result"} and len(parts) == 3:
                try:
                    value = str(UUID(parts[2]))
                except ValueError:
                    page = None
                else:
                    action = page
                    if page in {"hide", "result"}:
                        page = "main"
            elif page == "proactivity" and len(parts) == 3 and parts[2] in {"on", "off"}:
                action, value, page = "proactivity", parts[2] == "on", "settings"
            elif page in {"retry", "clearall", "show"} and len(parts) == 2:
                action, page = page, "main"
            elif page not in {"main", "guide", "plans", "settings"} or len(parts) != 2:
                page = None
            if page is None:
                with suppress(Exception):
                    await self.transport.bot.answer_callback_query(
                        callback["id"], text="Открой актуальное меню командой /menu"
                    )
                return True
        elif data.startswith("plan:") and data[5:] in {"FREE", "START", "PREMIUM", "PRO"}:
            action, value, page = "plan", data[5:], "plans"
        if page is None:
            return False
        if callback:
            with suppress(Exception):
                await self.transport.bot.answer_callback_query(callback["id"])
        if action == "retry" and not await self.store.get_home(user_id):
            await self.store.reset_home(user_id, source_key=f"home-retry:{event['id']}")
        try:
            home, created = await ensure_home(
                self.store,
                self.transport,
                user_id,
                chat_id,
                str(event["id"]),
            )
        except HomeUnavailable as error:
            await self.store.enqueue(
                user_id,
                chat_id,
                thread_id,
                unavailable_panel(str(error)),
                f"home-error:{event['id']}",
            )
            return True
        if action == "clearall":
            await self.request_deletion(event, home, "all")
            return True
        if action == "proactivity":
            await self.store.set_proactivity(
                user_id, bool(value), f"callback:{callback['id']}:proactivity"
            )
        if action == "plan":
            assert isinstance(value, str)
            balance = await self.store.change_plan(user_id, value, f"callback:{callback['id']}")
        else:
            balance = None
        if action == "hide":
            await hide_home_project(self.store, user_id, value)
        elif action == "show":
            await self.store.preferences(
                user_id, {"home_hidden_projects": {}, "home_suggestions": True}
            )
        elif action == "result":
            overview = await home_overview(self.store, user_id)
            if value in {row["id"] for row in overview["results"]}:
                artifact = await self.store.get_artifact(user_id, value)
                field = "photo_path" if artifact["mime"].startswith("image/") else "document_path"
                await self.store.enqueue(
                    user_id,
                    chat_id,
                    home["thread_id"],
                    {field: artifact["path"], "caption": artifact["filename"]},
                    f"home-result:{event['id']}",
                )
        if page == "main":
            panel = await personal_main_panel(self.store, user_id)
        elif page == "project":
            panel = await home_project_panel(self.store, user_id, value)
        elif page == "continue":
            panel = await continue_home_project(
                self.store, self.transport, user_id, chat_id, value, event["id"]
            )
        elif page == "projects":
            panel = projects_panel(await self.store.list_projects(user_id, limit=100), number)
        elif page == "guide":
            panel = guide_panel()
        elif page == "chats":
            panel = chats_panel(await self.store.list_conversations(user_id), number)
        elif page == "tasks":
            panel = tasks_panel(await self.store.list_schedules(user_id), number)
        elif page == "plans":
            panel = plans_panel(balance or await self.store.balance(user_id))
            if action == "plan":
                notice = (
                    f"Переход на {balance['pending_plan']} запланирован на следующий период."
                    if balance and balance.get("pending_plan")
                    else "Тариф обновлён — без оплаты."
                )
                panel["text"] = notice + "\n\n" + panel["text"]
        elif page == "settings":
            panel = settings_panel(await self.store.preferences(user_id))
        else:
            panel = memory_panel(await self.store.memories(user_id), number)
        if callback and thread_id == home["thread_id"]:
            message_id = (callback.get("message") or {}).get("message_id")
            if isinstance(message_id, int) and message_id > 0:
                panel["edit_message_id"] = message_id
        if not home.get("is_home"):
            panel["pin"] = True
        if not (created and page == "main"):
            order = event.get("update_id") or event["payload"].get("update_id") or 0
            await self.store.enqueue_home_panel(
                user_id, chat_id, home["thread_id"], panel, f"home-panel:{event['id']}", order
            )
        if thread_id != home["thread_id"]:
            await self.store.enqueue(
                user_id,
                chat_id,
                thread_id,
                {
                    "text": "Меню открыто в чате 🪐 Cronos. Выбери его в списке чатов Telegram.",
                    "reply_markup": navigation_keyboard(),
                },
                f"home-location:{event['id']}",
            )
        return True

    async def request_deletion(self, event, conversation, scope):
        request = await self.store.requests_prepare(
            conversation["user_id"],
            conversation,
            scope,
            source_key=f"privacy-request:{event['id']}",
        )
        await self.store.enqueue(
            conversation["user_id"],
            conversation["chat_id"],
            conversation["thread_id"],
            {"text": confirmation_text(request), "reply_markup": confirmation_keyboard(request)},
            f"privacy-confirm:{request['id']}",
        )

    async def privacy_control(self, event, user_id, chat_id, thread_id, text, callback):
        data = (callback or {}).get("data", "")
        result_text = None
        if data.startswith(("privacy:confirm:", "privacy:cancel:")):
            action, token = data.split(":", 2)[1:]
            try:
                if action == "confirm":
                    request = await self.store.confirm_privacy_request(user_id, token, thread_id)
                    result_text = (
                        "Очистка началась" if request else "Подтверждение устарело или недоступно"
                    )
                else:
                    cancelled = await self.store.cancel_privacy_request(user_id, token, thread_id)
                    result_text = (
                        "Удаление отменено" if cancelled else "Нет ожидающего подтверждения"
                    )
            except ValueError:
                result_text = "Подтверждение недоступно"
            with suppress(Exception):
                await self.transport.bot.answer_callback_query(callback["id"], text=result_text)
            return True
        if callback:
            return False
        scope = confirmation_scope(text)
        if scope:
            request = await self.store.pending_privacy_request(user_id, thread_id, scope)
            if request and await self.store.confirm_privacy_request(
                user_id, request["id"], thread_id
            ):
                return True
            result_text = "Нет действующего подтверждения. Используй /clearall для полной очистки или /delete для удаления чата."
        elif text.strip().casefold() in {"отмена", "/cancel"}:
            request = await self.store.pending_privacy_request(user_id, thread_id)
            if not request:
                return False
            await self.store.cancel_privacy_request(user_id, request["id"], thread_id)
            result_text = "Удаление отменено."
        if result_text:
            await self.store.enqueue(
                user_id, chat_id, thread_id, {"text": result_text}, f"privacy-control:{event['id']}"
            )
            return True
        return False

    async def new_chat(self, event, conversation):
        result = await create_chat(
            self.store,
            self.transport,
            conversation["user_id"],
            conversation["chat_id"],
            f"new-chat:{event['id']}",
        )
        text = result.get("error") or "Создал новый чат. Открой его в списке тем Telegram."
        await self.store.enqueue(
            conversation["user_id"],
            conversation["chat_id"],
            conversation["thread_id"],
            {
                "text": text,
                "reply_markup": navigation_keyboard(),
            },
            f"new-chat-result:{event['id']}",
        )

    async def topic_title(self, event):
        data = event["payload"]
        user_id = data["user_id"]
        async with self.store.user_lock(user_id):
            if event["kind"] == "topic_title_reset":
                from cronos.storage import uid

                async with self.store.connection(user_id) as conn:
                    row = await conn.fetchrow(
                        "SELECT * FROM conversations WHERE user_id=$1 AND id=$2",
                        user_id,
                        uid(data["conversation_id"]),
                    )
                if (
                    row
                    and not row.get("is_home")
                    and row["revision"] == data["revision"]
                    and row["title_auto"]
                    and row["title"] == "Новый чат"
                    and row["thread_id"] > 0
                ):
                    await self.transport.edit_topic(row["chat_id"], row["thread_id"], row["title"])
                return
            context = await self.store.topic_title_context(user_id, data["conversation_id"])
            if context is None or context["conversation"]["revision"] != data["revision"]:
                return
            conversation = context["conversation"]
            run = await self.store.start_run(event["id"], user_id, conversation["id"])
            status = "failed"
            try:
                op = f"{run['id']}:topic_title"
                result = await self.store.operation(op)
                if result is None:
                    result = await self.agent.paid(
                        run,
                        op,
                        lambda: self.provider.topic_title(
                            context["messages"], conversation["title"]
                        ),
                        proactive=True,
                    )
                    await self.store.save_operation(op, user_id, run["id"], "topic_title", result)
                await self.agent.active(run)
                title = result["message"]["content"]
                if not await self.transport.edit_topic(
                    conversation["chat_id"], conversation["thread_id"], title
                ):
                    raise RuntimeError("Telegram topic rename was not confirmed")
                await self.store.set_topic_title(
                    user_id,
                    conversation["id"],
                    title,
                    context["message_count"],
                    conversation["revision"],
                )
                status = "done"
            except CancelledRun:
                status = "cancelled"
            except asyncio.CancelledError:
                status = "interrupted"
                raise
            finally:
                await self.store.finish_run(run["id"], status, fence=run["fence"])

    async def answer(
        self, event, conversation, prompt, *, proactive=False, schedule=None, initiative=None
    ):
        run = await self.store.start_run(event["id"], conversation["user_id"], conversation["id"])
        started, cpu = time.monotonic(), time.process_time()
        status = "failed"
        interactive = not proactive and schedule is None
        draft_id = run["id"].int % 2_000_000_000 + 1
        draft_heartbeat = None
        try:
            if interactive:
                await self.transport.draft(
                    conversation["chat_id"],
                    conversation["thread_id"],
                    "Думаю...",
                    draft_id,
                )
                if hasattr(self.transport, "refresh_draft"):
                    draft_heartbeat = asyncio.create_task(
                        self.refresh_preview(run, conversation, draft_id)
                    )
            agent_options = {"proactive": proactive, "scheduled": schedule is not None}
            if initiative is not None:
                if not initiative.get("available") or not proactive or schedule is None:
                    raise CancelledRun("Prepared initiative policy is unavailable")
                agent_options["initiative"] = initiative
            result = await self.agent.run(run, conversation, prompt, **agent_options)
            if draft_heartbeat:
                await cancel_tasks([draft_heartbeat])
                draft_heartbeat = None
            answer = result["text"] if isinstance(result, dict) else result
            image_ids = result.get("image_artifact_ids", []) if isinstance(result, dict) else []
            payload = {"text": answer, "format": "rich"}
            if initiative is not None:
                decision = result.get("initiative_decision") if isinstance(result, dict) else None
                if not isinstance(decision, dict) or not isinstance(
                    decision.get("should_send"), bool
                ):
                    raise CancelledRun("Prepared initiative has no durable send decision")
                if not decision["should_send"]:
                    status = "done"
                    return status
                guard = decision.get("delivery_guard")
                if (
                    not isinstance(guard, dict)
                    or guard.get("initiative_id") != initiative["id"]
                    or not await self.store.validate_initiative_delivery(
                        conversation["user_id"], guard
                    )
                ):
                    raise CancelledRun("Prepared initiative decision is no longer valid")
                if not isinstance(answer, str) or not answer.strip() or image_ids:
                    raise CancelledRun("Prepared initiative result is invalid")
                prepared_ids = result.get("prepared_artifact_ids", [])
                if not isinstance(prepared_ids, list) or any(
                    not isinstance(value, str) for value in prepared_ids
                ):
                    raise CancelledRun("Prepared initiative file references are invalid")
                # One outbox and final acknowledgement cover every prepared file. A partial
                # Telegram send retries only the persisted tail of these same parts.
                parts = [{"kind": "rich", "text": part} for part in split_text(answer, 12000)]
                for artifact_id in dict.fromkeys(prepared_ids):
                    artifact = await self.store.get_artifact(conversation["user_id"], artifact_id)
                    parts.append(
                        {
                            "kind": "document",
                            "path": artifact["path"],
                            "caption": artifact["filename"],
                        }
                    )
                payload = {
                    "_telegram_parts": parts,
                    "initiative_artifact_ids": list(dict.fromkeys(prepared_ids)),
                    **guard,
                }
            if image_ids:
                images = [
                    await self.store.get_artifact(conversation["user_id"], artifact_id)
                    for artifact_id in image_ids
                ]
                payload = {
                    "image_paths": [image["path"] for image in images],
                    "caption": answer,
                    "format": "rich",
                }
            privacy_request = (
                await self.store.privacy_request_for_run(run["id"]) if interactive else None
            )
            if interactive:
                payload["reply_markup"] = (
                    confirmation_keyboard(privacy_request)
                    if privacy_request
                    else navigation_keyboard()
                )
            if schedule:
                payload["schedule_id"] = str(schedule["id"])
                payload["schedule_revision"] = schedule["revision"]
            delivery = await self.store.enqueue_for_run(
                run, conversation, payload, f"answer:{event['id']}"
            )
            if delivery is None:
                raise CancelledRun("Run or scheduled delivery cancelled")
            if interactive and not privacy_request:
                # Forgotten facts must not reappear through a saved current turn.
                latest = await self.store.ensure_user(conversation["user_id"])
                if latest["memory_revision"] == run["memory_revision"]:
                    await self.store.add_message(
                        conversation["user_id"],
                        conversation["id"],
                        "user",
                        prompt,
                        f"user:{event['id']}",
                    )
                    await self.store.add_message(
                        conversation["user_id"],
                        conversation["id"],
                        "assistant",
                        [
                            {"type": "text", "text": answer},
                            *[
                                {"type": "image_ref", "artifact_id": image_id}
                                for image_id in image_ids
                            ],
                        ]
                        if image_ids
                        else answer,
                        f"assistant:{event['id']}",
                    )
                else:
                    await self.store.add_message(
                        conversation["user_id"],
                        conversation["id"],
                        "assistant",
                        answer,
                        f"assistant:{event['id']}",
                    )
                await self.store.queue_topic_title(conversation["user_id"], conversation["id"])
            status = "done"
        except asyncio.CancelledError:
            status = "interrupted"
            raise
        except CancelledRun:
            status = "cancelled"
        except ProviderError:
            status = "failed"
            if initiative is not None:
                # Retry optional preparation without a generic fallback or unfinished file.
                raise
            if schedule and not proactive:
                # Keep the durable timer pending for its bounded event retry. A report
                # fallback must not masquerade as successful task execution.
                if event.get("attempts", 1) >= 3:
                    failure_text = "Не удалось завершить задание по расписанию после трёх попыток."
                    if schedule.get("interval_seconds") is not None:
                        failure_text += " Следующий запуск остаётся по расписанию."
                    await self.store.enqueue_for_run(
                        run,
                        conversation,
                        {
                            "text": failure_text,
                            "schedule_id": str(schedule["id"]),
                            "schedule_revision": schedule["revision"],
                        },
                        f"schedule-failed:{event['id']}",
                    )
                raise
            text = (
                schedule["fixed_text"]
                if schedule
                else "Сейчас модель не ответила. Попробуй ещё раз чуть позже — сохранённые данные и выполненные действия на месте."
            )
            payload = {"text": text}
            if schedule:
                payload.update(
                    schedule_id=str(schedule["id"]), schedule_revision=schedule["revision"]
                )
            await self.store.enqueue_for_run(run, conversation, payload, f"answer:{event['id']}")
        finally:
            if draft_heartbeat:
                await cancel_tasks([draft_heartbeat])
            if interactive and hasattr(self.transport, "forget_draft"):
                self.transport.forget_draft(
                    conversation["chat_id"], conversation["thread_id"], draft_id
                )
            await self.store.finish_run(run["id"], status, fence=run["fence"])
            rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            await self.store.run_metrics(
                run["id"],
                conversation["user_id"],
                int((time.monotonic() - started) * 1000),
                time.process_time() - cpu,
                rss if os.uname().sysname == "Darwin" else rss * 1024,
            )
        return status

    async def refresh_preview(self, run, conversation, draft_id):
        while True:
            await asyncio.sleep(4)
            if not await self.store.run_active(run["id"], run["fence"]):
                return
            await self.transport.refresh_draft(
                conversation["chat_id"], conversation["thread_id"], draft_id
            )

    async def timer(self, event):
        data = event["payload"]
        schedule = await self.store.get_schedule(data["schedule_id"])
        if (
            not schedule
            or schedule["state"] == "cancelled"
            or schedule["revision"] != data["revision"]
        ):
            await self.store.mark_occurrence(data["occurrence_id"], "cancelled")
            return
        async with self.store.user_lock(schedule["user_id"]):
            if not await self.store.occurrence_active(data["occurrence_id"]):
                return
            prefs = await self.store.preferences(schedule["user_id"])
            if schedule["proactive"] and not prefs.get("proactivity"):
                await self.store.mark_occurrence(data["occurrence_id"], "cancelled")
                return
            initiative = None
            if schedule.get("initiative_id"):
                initiative = await self.store.get_initiative_for_schedule(
                    schedule["user_id"], schedule["id"]
                )
                if not initiative or not initiative.get("available"):
                    await self.store.mark_occurrence(data["occurrence_id"], "cancelled")
                    return
            conversation = await self.store.conversation(
                schedule["user_id"], schedule["chat_id"], schedule["thread_id"]
            )
            if schedule["dynamic"]:
                try:
                    options = {"initiative": initiative} if initiative is not None else {}
                    status = await self.answer(
                        event,
                        conversation,
                        "Наступило время существующего задания. Выполни его сейчас, "
                        "используя нужные инструменты; не создавай это расписание повторно. "
                        "Задание: " + schedule["instruction"],
                        proactive=schedule["proactive"],
                        schedule=schedule,
                        **options,
                    )
                except ProviderError:
                    if event.get("attempts", 1) >= 3:
                        await self.store.mark_occurrence(data["occurrence_id"], "failed")
                    raise
                if status == "cancelled":
                    await self.store.mark_occurrence(data["occurrence_id"], "cancelled")
                    return
            else:
                await self.store.enqueue(
                    schedule["user_id"],
                    schedule["chat_id"],
                    schedule["thread_id"],
                    {
                        "text": schedule["fixed_text"],
                        "schedule_id": str(schedule["id"]),
                        "schedule_revision": schedule["revision"],
                    },
                    f"answer:{event['id']}",
                )
            await self.store.mark_occurrence(data["occurrence_id"])

    async def health(self):
        while True:
            if await self.store.ready():
                self.healthy.touch()
            await asyncio.sleep(15)

    async def recovery(self):
        while True:
            try:
                await self.process()
                if await self.store.ready():
                    self.healthy.touch()
            except Exception:
                log.exception("Recovery tick failed")
            await asyncio.sleep(2)

    async def consume(self):
        while True:
            try:
                connection = await aio_pika.connect_robust(
                    self.settings.rabbitmq_url.get_secret_value()
                )
                async with connection:
                    channel = await connection.channel()
                    await channel.set_qos(prefetch_count=1)
                    queue = await channel.declare_queue("cronos.events", durable=True)
                    async with queue.iterator() as iterator:
                        async for message in iterator:
                            async with message.process(requeue=True):
                                await self.process(message.body.decode())
            except Exception:
                log.warning("RabbitMQ unavailable; durable SQL recovery remains active")
                await asyncio.sleep(5)

    async def run(self):
        tasks = []
        try:
            await self.store.open()
            await setup_checkpoints(self.settings)
            # Migrations run while the previous release may still be polling.
            # Only a worker that understands home_init can release this backfill.
            await self.store.activate_home_bootstrap()
            tasks = [
                asyncio.create_task(self.recovery()),
                asyncio.create_task(self.consume()),
                asyncio.create_task(self.health()),
            ]
            await asyncio.gather(*tasks)
        finally:
            await cancel_tasks(tasks)
            await close_all(self.provider.close, self.transport.close, self.store.close)


def main():
    settings = get_settings()
    configure_logging(settings.log_level)
    asyncio.run(run_until_stopped(Worker(settings).run()))


if __name__ == "__main__":
    main()
