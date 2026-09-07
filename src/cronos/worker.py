import asyncio
import io
import logging
import os
import resource
import time
from contextlib import suppress
from pathlib import Path
from uuid import uuid4

import aio_pika

from cronos.agent import Agent, CancelledRun, setup_checkpoints
from cronos.file_tasks import file_task
from cronos.home import HomeUnavailable, ensure_home, unavailable_panel
from cronos.home_views import (
    chats_panel,
    guide_panel,
    main_panel,
    memory_panel,
    plans_panel,
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
from cronos.telegram import TelegramTransport, new_chat_keyboard
from cronos.topics import create_chat, welcome_topic

log = logging.getLogger(__name__)


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
            callback = update.get("callback_query")
            message = update.get("message") or (callback or {}).get("message")
            if not message:
                return
            text = message.get("text") or message.get("caption") or ""
            if await self.privacy_control(event, user_id, chat_id, thread_id, text, callback):
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
            text = message.get("text") or message.get("caption") or ""
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
            document = message.get("document")
            if document:
                attachments.append((document["file_id"], document.get("file_name", "document.bin")))
            if message.get("photo"):
                attachments.append((message["photo"][-1]["file_id"], "photo.jpg"))
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
            if not text:
                return
            await self.store.queue_home(user_id)
            await self.answer(event, conversation, text)

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
                        {**main_panel(), "pin": True},
                        f"home-fallback:{event['id']}",
                    )
            except HomeUnavailable as error:
                await self.store.enqueue(
                    user_id, user_id, 0, unavailable_panel(str(error)), f"home-error:{event['id']}"
                )

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
            if page in {"chats", "tasks", "memory"}:
                if len(parts) != 3 or not parts[2].isdigit() or len(parts[2]) > 6:
                    page = None
                else:
                    number = int(parts[2])
            elif page == "proactivity" and len(parts) == 3 and parts[2] in {"on", "off"}:
                action, value, page = "proactivity", parts[2] == "on", "settings"
            elif page in {"retry", "clearall"} and len(parts) == 2:
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
                verify=page == "main",
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
        if page == "main":
            panel = main_panel()
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
                {"text": "Меню открыто в чате 🪐 Cronos. Выбери его в списке чатов Telegram."},
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
                "reply_markup": main_panel()["reply_markup"]
                if conversation.get("is_home")
                else new_chat_keyboard(),
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

    async def answer(self, event, conversation, prompt, *, proactive=False, schedule=None):
        run = await self.store.start_run(event["id"], conversation["user_id"], conversation["id"])
        started, cpu = time.monotonic(), time.process_time()
        status = "failed"
        interactive = not proactive and schedule is None
        try:
            if interactive:
                await self.transport.draft(
                    conversation["chat_id"],
                    conversation["thread_id"],
                    "Сейчас разберусь…",
                    run["id"].int % 2_000_000_000 + 1,
                )
            answer = await self.agent.run(
                run, conversation, prompt, proactive=proactive, scheduled=schedule is not None
            )
            payload = {"text": answer, "format": "rich"}
            privacy_request = (
                await self.store.privacy_request_for_run(run["id"]) if interactive else None
            )
            if interactive:
                payload["reply_markup"] = (
                    confirmation_keyboard(privacy_request)
                    if privacy_request
                    else main_panel()["reply_markup"]
                    if conversation.get("is_home")
                    else new_chat_keyboard()
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
                        answer,
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
            conversation = await self.store.conversation(
                schedule["user_id"], schedule["chat_id"], schedule["thread_id"]
            )
            if schedule["dynamic"]:
                try:
                    status = await self.answer(
                        event,
                        conversation,
                        "Наступило время существующего задания. Выполни его сейчас, "
                        "используя нужные инструменты; не создавай это расписание повторно. "
                        "Задание: " + schedule["instruction"],
                        proactive=schedule["proactive"],
                        schedule=schedule,
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
