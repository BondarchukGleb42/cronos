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
from cronos.lifecycle import cancel_tasks, close_all, run_until_stopped
from cronos.logging import configure_logging
from cronos.providers import Provider, ProviderError
from cronos.settings import get_settings
from cronos.storage import STOP_COMMANDS, Store, message_command
from cronos.telegram import TelegramTransport, upgrade_keyboard

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
        sender = (callback or message).get("from", {})
        user_id = sender.get("id")
        if not user_id or sender.get("is_bot") or message.get("chat", {}).get("type") != "private":
            return
        chat_id, thread_id = message["chat"]["id"], message.get("message_thread_id", 0)
        async with self.store.user_lock(user_id):
            conversation = await self.store.conversation(user_id, chat_id, thread_id)
            if callback:
                data = callback.get("data", "")
                if data.startswith("plan:"):
                    balance = await self.store.change_plan(
                        user_id, data.split(":", 1)[1], f"callback:{callback['id']}"
                    )
                    await self.store.enqueue(
                        user_id,
                        chat_id,
                        thread_id,
                        {
                            "text": f"Готово! Тариф {balance['plan']}, доступно {balance['tokens_remaining']:,.0f} токенов. Это тестовое повышение — деньги не списывались."
                        },
                        f"callback:{callback['id']}",
                    )
                with suppress(Exception):
                    await self.transport.bot.answer_callback_query(
                        callback["id"], text="Готово — без оплаты"
                    )
                return
            text = message.get("text") or message.get("caption") or ""
            command = message_command(text)
            if command in STOP_COMMANDS:
                await self.store.enqueue(
                    user_id,
                    chat_id,
                    thread_id,
                    {"text": "Остановил текущую задачу. Уже выполненные действия сохранены."},
                    f"stop:{event['id']}",
                )
                return
            if command in {"/upgrade", "/plans"}:
                await self.store.enqueue(
                    user_id,
                    chat_id,
                    thread_id,
                    {
                        "text": "Выбери тестовый тариф — без оплаты.",
                        "reply_markup": upgrade_keyboard(),
                    },
                    f"plans:{event['id']}",
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
                        artifact = await asyncio.to_thread(
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
            if command == "/start":
                text = "Привет! Мы впервые общаемся. Коротко познакомься и помоги мне понять, с чего начать."
            if not text:
                return
            await self.answer(event, conversation, text)

    async def answer(self, event, conversation, prompt, *, proactive=False, schedule=None):
        run = await self.store.start_run(event["id"], conversation["user_id"], conversation["id"])
        started, cpu = time.monotonic(), time.process_time()
        status = "failed"
        try:
            if not proactive:
                await self.transport.draft(
                    conversation["chat_id"],
                    conversation["thread_id"],
                    "Сейчас разберусь…",
                    run["id"].int % 2_000_000_000 + 1,
                )
            answer = await self.agent.run(run, conversation, prompt, proactive=proactive)
            payload = {"text": answer, "format": "rich"}
            if schedule:
                payload["schedule_id"] = str(schedule["id"])
                payload["schedule_revision"] = schedule["revision"]
            await self.store.enqueue_for_run(run, conversation, payload, f"answer:{event['id']}")
            if not proactive:
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
            status = "done"
        except asyncio.CancelledError:
            status = "interrupted"
            raise
        except CancelledRun:
            status = "cancelled"
        except ProviderError:
            status = "failed"
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
            prefs = await self.store.preferences(schedule["user_id"])
            if schedule["proactive"] and not prefs.get("proactivity"):
                await self.store.mark_occurrence(data["occurrence_id"], "cancelled")
                return
            conversation = await self.store.conversation(
                schedule["user_id"], schedule["chat_id"], schedule["thread_id"]
            )
            if schedule["dynamic"]:
                await self.answer(
                    event,
                    conversation,
                    "Наступило время согласованного обращения. Его цель: "
                    + schedule["instruction"],
                    proactive=True,
                    schedule=schedule,
                )
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
