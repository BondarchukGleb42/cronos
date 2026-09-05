import asyncio
import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4
from zoneinfo import ZoneInfo

import aio_pika
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError, TelegramRetryAfter
from redis.asyncio import Redis

from cronos.billing import Reconciler
from cronos.lifecycle import cancel_tasks, close_all, run_until_stopped
from cronos.logging import configure_logging
from cronos.settings import get_settings
from cronos.storage import Store, uid
from cronos.telegram import PartialDeliveryError, TelegramTransport

log = logging.getLogger(__name__)


def initiative_delay(prefs, now, sent_today):
    """Return the next acceptable local time; exact user reminders bypass this policy."""
    local = now.astimezone(ZoneInfo(prefs.get("timezone", "UTC")))
    start, end = prefs.get("quiet_start", 22), prefs.get("quiet_end", 9)
    quiet = start <= local.hour < end if start < end else local.hour >= start or local.hour < end
    if start == end:
        quiet = False
    if sent_today >= prefs.get("initiative_limit", 2):
        return (local + timedelta(days=1)).replace(hour=end, minute=0, second=0, microsecond=0)
    if quiet:
        target = local.replace(hour=end, minute=0, second=0, microsecond=0)
        if target <= local:
            target += timedelta(days=1)
        return target
    return None


class Coordinator:
    def __init__(self, settings):
        self.settings = settings
        self.store = Store(settings)
        self.transport = TelegramTransport(settings)
        self.reconciler = Reconciler(settings, self.store)
        self.redis = Redis.from_url(
            settings.redis_url.get_secret_value(), socket_connect_timeout=2, socket_timeout=2
        )
        self.owner = f"coordinator:{uuid4()}"
        self.connection = None
        self.channel = None
        self.healthy = Path("/tmp/coordinator.healthy")

    async def publish(self):
        try:
            if self.channel is None or self.channel.is_closed:
                self.connection = await aio_pika.connect_robust(
                    self.settings.rabbitmq_url.get_secret_value(), timeout=3
                )
                self.channel = await self.connection.channel(publisher_confirms=True)
                await self.channel.declare_queue("cronos.events", durable=True)
            for event_id in await self.store.pending_events():
                await self.channel.default_exchange.publish(
                    aio_pika.Message(
                        event_id.encode(),
                        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                        message_id=event_id,
                    ),
                    routing_key="cronos.events",
                    timeout=3,
                )
                await self.store.notified(event_id)
        except Exception:
            log.warning("RabbitMQ publish delayed; events remain in PostgreSQL")

    async def delivery(self):
        row = await self.store.next_delivery(self.owner)
        if not row:
            return False
        payload = row["payload"]
        try:
            if payload.get("schedule_id"):
                schedule = await self.store.get_schedule(payload["schedule_id"])
                if (
                    not schedule
                    or schedule["state"] == "cancelled"
                    or schedule["revision"] != payload.get("schedule_revision")
                ):
                    await self.store.delivery_result(
                        row["id"], self.owner, error="Cancelled schedule", permanent=True
                    )
                    return True
                if schedule["proactive"]:
                    prefs = await self.store.preferences(row["user_id"])
                    if not prefs.get("proactivity"):
                        await self.store.delivery_result(
                            row["id"], self.owner, error="Proactivity disabled", permanent=True
                        )
                        return True
                    now = datetime.now(UTC)
                    local_start = now.astimezone(ZoneInfo(prefs.get("timezone", "UTC"))).replace(
                        hour=0, minute=0, second=0, microsecond=0
                    )
                    async with self.store.connection() as conn:
                        count = await conn.fetchval(
                            "SELECT count(*) FROM outbox o JOIN schedules s ON o.payload->>'schedule_id'=s.id::text WHERE o.user_id=$1 AND o.state='sent' AND s.proactive AND o.sent_at>=$2",
                            row["user_id"],
                            local_start,
                        )
                    postpone = initiative_delay(prefs, now, count)
                    if postpone:
                        await self.store.delivery_result(
                            row["id"],
                            self.owner,
                            error="Waiting for agreed initiative window",
                            retry_after=max(1, int((postpone - now).total_seconds())),
                        )
                        return True
            # Ephemeral rate coordination; SQL outbox remains authoritative during Redis failure.
            try:
                allowed = await self.redis.set(
                    f"cronos:delivery:{row['chat_id']}", "1", nx=True, ex=1
                )
            except Exception:
                allowed = True
            if not allowed:
                await self.store.delivery_result(
                    row["id"], self.owner, error="Chat rate limit", retry_after=1
                )
                return True
            ids = await self.transport.send(row["chat_id"], row["thread_id"], payload)
            await self.store.delivery_result(
                row["id"], self.owner, ids=(row.get("telegram_ids") or []) + ids
            )
            if payload.get("schedule_id"):
                async with self.store.connection() as conn:
                    await conn.execute(
                        "UPDATE schedules SET last_sent_at=now() WHERE id=$1",
                        uid(payload["schedule_id"]),
                    )
        except PartialDeliveryError as error:
            # Persist acknowledged parts before retrying only the remainder.
            remainder = {**error.remaining_payload}
            for key in ("schedule_id", "schedule_revision"):
                if key in payload:
                    remainder[key] = payload[key]
            ids = (row.get("telegram_ids") or []) + error.sent_ids
            permanent = isinstance(
                error.cause, (TelegramForbiddenError, TelegramBadRequest, FileNotFoundError)
            )
            delay = error.cause.retry_after if isinstance(error.cause, TelegramRetryAfter) else 5
            async with self.store.connection() as conn:
                await conn.execute(
                    "UPDATE outbox SET payload=$3,telegram_ids=$4,state=$5,lease_until=NULL,next_attempt_at=now()+$6*interval '1 second',error=$7 WHERE id=$1 AND owner=$2",
                    row["id"],
                    self.owner,
                    remainder,
                    ids,
                    "failed" if permanent else "pending",
                    delay,
                    type(error.cause).__name__,
                )
        except TelegramRetryAfter as error:
            await self.store.delivery_result(
                row["id"], self.owner, error="Telegram rate limit", retry_after=error.retry_after
            )
        except (TelegramForbiddenError, TelegramBadRequest, FileNotFoundError) as error:
            await self.store.delivery_result(
                row["id"], self.owner, error=type(error).__name__, permanent=True
            )
        except Exception as error:
            # Telegram has no idempotency key: an ambiguous successful send may duplicate after retry.
            log.warning("Delivery uncertain: id=%s type=%s", row["id"], type(error).__name__)
            await self.store.delivery_result(
                row["id"],
                self.owner,
                error=type(error).__name__,
                retry_after=min(300, 2 ** min(row["attempts"], 8)),
                permanent=row["attempts"] >= 8,
            )
        return True

    async def reconcile(self):
        while True:
            try:
                await self.reconciler.tick()
            except Exception as error:
                log.warning("Usage reconciliation delayed: %s", type(error).__name__)
            await asyncio.sleep(60)

    async def health(self):
        while True:
            if await self.store.ready():
                self.healthy.touch()
            await asyncio.sleep(15)

    async def run(self):
        tasks = []
        try:
            await self.store.open()
            tasks = [asyncio.create_task(self.health()), asyncio.create_task(self.reconcile())]
            while True:
                try:
                    await self.store.schedule_due()
                    await self.publish()
                    for _ in range(10):
                        if not await self.delivery():
                            break
                    self.healthy.touch()
                except Exception:
                    log.exception("Coordinator tick failed")
                await asyncio.sleep(1)
        finally:
            await cancel_tasks(tasks)
            closers = [self.reconciler.close]
            if self.connection:
                closers.append(self.connection.close)
            await close_all(*closers, self.redis.aclose, self.transport.close, self.store.close)


def main():
    settings = get_settings()
    configure_logging(settings.log_level)
    asyncio.run(run_until_stopped(Coordinator(settings).run()))


if __name__ == "__main__":
    main()
