import asyncio
import hashlib
import json
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

import asyncpg
from dateutil.relativedelta import relativedelta

from cronos.settings import Settings

PLANS = {"FREE": 25_000_000, "START": 700_000_000, "PREMIUM": 1_700_000_000, "PRO": 3_700_000_000}
STOP_COMMANDS = {"стоп", "остановись", "отмена", "stop", "/stop"}


def message_command(text: str) -> str:
    parts = text.strip().casefold().split(maxsplit=1)
    command = parts[0].rstrip(",.!?:;") if parts else ""
    return command.split("@", 1)[0] if command.startswith("/") else command


def uid(value):
    return value if isinstance(value, UUID) else UUID(str(value))


class Store:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.pool: asyncpg.Pool | None = None
        self._user_lock_slots = asyncio.Semaphore(4)

    async def open(self):
        async def init(conn):
            for name in ("json", "jsonb"):
                await conn.set_type_codec(
                    name, encoder=json.dumps, decoder=json.loads, schema="pg_catalog"
                )

        self.pool = await asyncpg.create_pool(
            self.settings.database_url.get_secret_value(),
            min_size=1,
            max_size=8,
            init=init,
        )

    async def close(self):
        if self.pool:
            await self.pool.close()

    @asynccontextmanager
    async def user_lock(self, user_id: int):
        """Serialize a user's topics across processes without exhausting the pool."""
        if self.pool is None:
            raise RuntimeError("Database is not connected")
        key = int.from_bytes(
            hashlib.blake2b(f"cronos:user:{user_id}".encode(), digest_size=8).digest(),
            signed=True,
        )
        async with self._user_lock_slots:
            connection = None
            while connection is None:
                candidate = await self.pool.acquire()
                locked = False
                try:
                    locked = await candidate.fetchval(
                        "SELECT pg_try_advisory_lock($1::bigint)", key
                    )
                    if locked:
                        connection = candidate
                finally:
                    if not locked:
                        await self.pool.release(candidate)
                if connection is None:
                    await asyncio.sleep(0.05)
            try:
                yield
            finally:
                try:
                    await connection.execute("SELECT pg_advisory_unlock($1::bigint)", key)
                finally:
                    await self.pool.release(connection)

    @asynccontextmanager
    async def connection(self, user_id: int | None = None):
        if self.pool is None:
            raise RuntimeError("Database is not connected")
        async with self.pool.acquire() as conn, conn.transaction():
            if user_id is not None:
                await conn.execute("SELECT set_config('app.user_id',$1,true)", str(user_id))
            yield conn

    async def ready(self):
        async with self.connection() as conn:
            return await conn.fetchval("SELECT 1") == 1

    async def get_poll_offset(self):
        async with self.connection() as conn:
            row = await conn.fetchval("SELECT value FROM runtime_state WHERE key='telegram_offset'")
            return int((row or {}).get("offset", 0))

    async def acquire_poll_lease(self, owner: str):
        async with self.connection() as conn:
            return bool(
                await conn.fetchval(
                    """INSERT INTO service_leases(key,owner,expires_at)
                VALUES('poller',$1,now()+interval '60 seconds') ON CONFLICT(key) DO UPDATE
                SET owner=$1,expires_at=now()+interval '60 seconds'
                WHERE service_leases.owner=$1 OR service_leases.expires_at<now() RETURNING owner""",
                    owner,
                )
            )

    async def release_poll_lease(self, owner: str):
        async with self.connection() as conn:
            await conn.execute("DELETE FROM service_leases WHERE key='poller' AND owner=$1", owner)

    async def ingest_updates(self, updates: list[dict], next_offset: int, owner: str | None = None):
        async with self.connection() as conn:
            if owner is not None:
                lease = await conn.fetchrow(
                    "SELECT owner,expires_at>clock_timestamp() AS active FROM service_leases WHERE key='poller' FOR UPDATE"
                )
                if not lease or lease["owner"] != owner or not lease["active"]:
                    return False
            for update in updates:
                inserted = await conn.fetchval(
                    """INSERT INTO events(id,update_id,payload) VALUES($1,$2,$3)
                    ON CONFLICT(update_id) DO NOTHING RETURNING id""",
                    uuid4(),
                    update["update_id"],
                    update,
                )
                if not inserted:
                    continue
                msg = update.get("message", {})
                sender = msg.get("from", {})
                if (
                    msg.get("chat", {}).get("type") == "private"
                    and sender.get("id")
                    and not sender.get("is_bot")
                    and message_command(msg.get("text") or msg.get("caption") or "")
                    in STOP_COMMANDS
                ):
                    await conn.execute(
                        "UPDATE runs SET cancel_requested=true WHERE user_id=$1 AND status='running'",
                        sender["id"],
                    )
                stopped = update.get("stopped_message_generation")
                if stopped and stopped.get("chat", {}).get("type") == "private":
                    chat = stopped.get("chat", {})
                    await conn.execute("SELECT set_config('app.user_id',$1,true)", str(chat["id"]))
                    candidates = await conn.fetch(
                        """SELECT r.id FROM runs r JOIN conversations c ON c.id=r.conversation_id
                        WHERE r.user_id=$1 AND c.chat_id=$1 AND c.thread_id=$2
                        AND r.status='running' FOR UPDATE OF r""",
                        chat["id"],
                        stopped.get("message_thread_id") or 0,
                    )
                    for run in candidates:
                        if run["id"].int % 2_000_000_000 + 1 == stopped.get("draft_id"):
                            await conn.execute(
                                "UPDATE runs SET cancel_requested=true WHERE id=$1", run["id"]
                            )
            await conn.execute(
                """INSERT INTO runtime_state(key,value) VALUES('telegram_offset',$1)
                ON CONFLICT(key) DO UPDATE SET value=jsonb_build_object('offset',
                GREATEST((runtime_state.value->>'offset')::bigint,(EXCLUDED.value->>'offset')::bigint)),
                updated_at=now()""",
                {"offset": next_offset},
            )
            return True

    async def ensure_user(self, user_id: int):
        async with self.connection(user_id) as conn:
            await conn.execute(
                "INSERT INTO users(user_id,preferences) VALUES($1,$2) ON CONFLICT DO NOTHING",
                user_id,
                {
                    "timezone": self.settings.default_timezone,
                    "proactivity": False,
                    "tone": "доброжелательно, по делу",
                    "initiative_limit": 2,
                },
            )
            row = await conn.fetchrow("SELECT * FROM users WHERE user_id=$1 FOR UPDATE", user_id)
            now = datetime.now(UTC)
            if row["period_end"] <= now:
                plan = row["pending_plan"] or row["plan"]
                anchor = row["billing_anchor"] or row["period_start"]
                if plan == "FREE":
                    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
                    end = start + timedelta(days=1)
                    anchor = None
                else:
                    months = (now.year - anchor.year) * 12 + now.month - anchor.month
                    start = anchor + relativedelta(months=months)
                    if start > now:
                        months -= 1
                        start = anchor + relativedelta(months=months)
                    end = anchor + relativedelta(months=months + 1)
                await conn.execute(
                    """UPDATE users SET balance_micro=$2,entitlement_micro=$2,
                    period_start=$3,period_end=$4,plan=$5,pending_plan=NULL,billing_anchor=$6
                    WHERE user_id=$1""",
                    user_id,
                    PLANS[plan],
                    start,
                    end,
                    plan,
                    anchor,
                )
            return dict(await conn.fetchrow("SELECT * FROM users WHERE user_id=$1", user_id))

    async def preferences(self, user_id: int, values: dict | None = None):
        await self.ensure_user(user_id)
        async with self.connection(user_id) as conn:
            if values:
                await conn.execute(
                    "UPDATE users SET preferences=preferences||$2::jsonb WHERE user_id=$1",
                    user_id,
                    values,
                )
                if values.get("proactivity") is False:
                    await conn.execute(
                        "UPDATE schedules SET state='cancelled',revision=revision+1 WHERE user_id=$1 AND proactive",
                        user_id,
                    )
            return await conn.fetchval("SELECT preferences FROM users WHERE user_id=$1", user_id)

    async def conversation(self, user_id: int, chat_id: int, thread_id: int | None, title=""):
        await self.ensure_user(user_id)
        async with self.connection(user_id) as conn:
            await conn.execute(
                """INSERT INTO conversations(id,user_id,chat_id,thread_id,title) VALUES($1,$2,$3,$4,$5)
                ON CONFLICT(user_id,chat_id,thread_id) DO NOTHING""",
                uuid4(),
                user_id,
                chat_id,
                thread_id or 0,
                title,
            )
            return dict(
                await conn.fetchrow(
                    "SELECT * FROM conversations WHERE user_id=$1 AND chat_id=$2 AND thread_id=$3",
                    user_id,
                    chat_id,
                    thread_id or 0,
                )
            )

    async def list_conversations(self, user_id: int):
        async with self.connection(user_id) as conn:
            return [
                dict(x)
                for x in await conn.fetch(
                    "SELECT id,title,chat_id,thread_id FROM conversations WHERE user_id=$1 ORDER BY created_at DESC",
                    user_id,
                )
            ]

    async def _topic_title_state(self, conn, user_id: int, conversation_id):
        conversation = await conn.fetchrow(
            "SELECT * FROM conversations WHERE user_id=$1 AND id=$2 FOR UPDATE",
            user_id,
            uid(conversation_id),
        )
        if not conversation or not conversation["title_auto"] or conversation["thread_id"] <= 0:
            return None
        counts = await conn.fetchrow(
            """SELECT count(*) AS total, bool_or(role='user') AS has_user,
            bool_or(role='assistant') AS has_assistant FROM messages
            WHERE user_id=$1 AND conversation_id=$2 AND NOT excluded
            AND role IN ('user','assistant')""",
            user_id,
            conversation["id"],
        )
        count = counts["total"]
        if count < 2 or not counts["has_user"] or not counts["has_assistant"]:
            return None
        milestone = max(2, count // 10 * 10)
        previous = conversation["title_message_count"]
        if previous >= milestone:
            return None
        return {"conversation": dict(conversation), "message_count": count, "threshold": milestone}

    async def queue_topic_title(self, user_id: int, conversation_id) -> bool:
        async with self.connection(user_id) as conn:
            context = await self._topic_title_state(conn, user_id, conversation_id)
            if context is None:
                return False
            conversation = context["conversation"]
            dedupe = (
                f"cronos:topic_title:{user_id}:{conversation['id']}:"
                f"{conversation['revision']}:{context['threshold']}"
            )
            return bool(
                await conn.fetchval(
                    """INSERT INTO events(id,kind,payload) VALUES($1,'topic_title',$2)
                    ON CONFLICT(id) DO NOTHING RETURNING id""",
                    uuid5(NAMESPACE_URL, dedupe),
                    {
                        "user_id": user_id,
                        "conversation_id": str(conversation["id"]),
                        "revision": conversation["revision"],
                        "threshold": context["threshold"],
                    },
                )
            )

    async def topic_title_context(self, user_id: int, conversation_id) -> dict | None:
        async with self.connection(user_id) as conn:
            context = await self._topic_title_state(conn, user_id, conversation_id)
            if context is None:
                return None
            rows = await conn.fetch(
                """SELECT role,content FROM messages
                WHERE user_id=$1 AND conversation_id=$2 AND NOT excluded
                AND role IN ('user','assistant') ORDER BY id DESC LIMIT 12""",
                user_id,
                context["conversation"]["id"],
            )
            return {
                "conversation": context["conversation"],
                "messages": [dict(row) for row in reversed(rows)],
                "message_count": context["message_count"],
            }

    async def set_topic_title(
        self, user_id: int, conversation_id, title: str, message_count: int, revision: int
    ) -> bool:
        if message_count < 2 or not title.strip():
            return False
        async with self.connection(user_id) as conn:
            return bool(
                await conn.fetchval(
                    """UPDATE conversations SET title=$3,title_message_count=$4
                    WHERE user_id=$1 AND id=$2 AND title_auto AND thread_id>0 AND revision=$5
                    AND title_message_count<GREATEST(2,($4::integer/10)*10)
                    RETURNING id""",
                    user_id,
                    uid(conversation_id),
                    title.strip(),
                    message_count,
                    revision,
                )
            )

    async def sync_topic_title(
        self,
        user_id: int,
        chat_id: int,
        thread_id: int,
        title: str,
        manual: bool = False,
        created: bool = False,
        update_id: int = 0,
    ) -> dict | None:
        """Sync service events; update_id is the chat's monotonic service message_id.

        Creation only initializes missing topics. Older manual events return None;
        repeating the latest event returns its row for a safe Telegram API retry.
        """
        if thread_id <= 0:
            return None
        await self.ensure_user(user_id)
        async with self.connection(user_id) as conn:
            row = await conn.fetchrow(
                """INSERT INTO conversations
                (id,user_id,chat_id,thread_id,title,title_auto,revision,title_update_id)
                VALUES($1,$2,$3,$4,$5,NOT $6::boolean,CASE WHEN $6 THEN 1 ELSE 0 END,
                    CASE WHEN $6 AND $7::bigint>0 THEN $7 ELSE 0 END)
                ON CONFLICT(user_id,chat_id,thread_id) DO NOTHING RETURNING *""",
                uuid4(),
                user_id,
                chat_id,
                thread_id,
                title,
                manual,
                update_id,
            )
            if row is not None:
                return dict(row)
            row = await conn.fetchrow(
                """SELECT * FROM conversations WHERE user_id=$1 AND chat_id=$2
                AND thread_id=$3 FOR UPDATE""",
                user_id,
                chat_id,
                thread_id,
            )
            if created:
                return dict(row)
            if manual and update_id > 0:
                if update_id < row["title_update_id"]:
                    return None
                if update_id == row["title_update_id"]:
                    return dict(row)
            row = await conn.fetchrow(
                """UPDATE conversations SET
                title=CASE WHEN $4 OR title_auto THEN $3 ELSE title END,
                title_auto=CASE WHEN $4 THEN false ELSE title_auto END,
                revision=revision+CASE WHEN $4 AND
                    (title_auto OR title IS DISTINCT FROM $3) THEN 1 ELSE 0 END,
                title_update_id=CASE WHEN $4 AND $5::bigint>0 THEN $5 ELSE title_update_id END
                WHERE user_id=$1 AND id=$2 RETURNING *""",
                user_id,
                row["id"],
                title,
                manual,
                update_id,
            )
            return dict(row)

    async def history(self, user_id: int, conversation_id, limit: int = 30):
        async with self.connection(user_id) as conn:
            rows = await conn.fetch(
                "SELECT role,content FROM messages WHERE user_id=$1 AND conversation_id=$2 AND NOT excluded ORDER BY id DESC LIMIT $3",
                user_id,
                uid(conversation_id),
                limit,
            )
            return [{"role": x["role"], "content": x["content"]} for x in reversed(rows)]

    async def add_message(
        self, user_id: int, conversation_id, role: str, content, source_key: str | None = None
    ):
        async with self.connection(user_id) as conn:
            await conn.execute(
                "INSERT INTO messages(user_id,conversation_id,role,content,source_key) VALUES($1,$2,$3,$4,$5) ON CONFLICT(source_key) DO NOTHING",
                user_id,
                uid(conversation_id),
                role,
                content,
                source_key,
            )

    async def memories(self, user_id: int):
        async with self.connection(user_id) as conn:
            return [
                dict(x)
                for x in await conn.fetch(
                    "SELECT id,content,category FROM memory WHERE user_id=$1 ORDER BY created_at DESC LIMIT 100",
                    user_id,
                )
            ]

    async def remember(self, user_id: int, content: str, category="preference", source=""):
        async with self.connection(user_id) as conn:
            row = await conn.fetchrow(
                "INSERT INTO memory(id,user_id,content,category,source) VALUES($1,$2,$3,$4,$5) ON CONFLICT(user_id,content) DO UPDATE SET category=EXCLUDED.category RETURNING id,content",
                uuid4(),
                user_id,
                content,
                category,
                source,
            )
            return {"id": str(row["id"]), "content": row["content"]}

    async def forget(self, user_id: int, query: str, current_run=None):
        async with self.connection(user_id) as conn:
            deleted = await conn.fetch(
                "DELETE FROM memory WHERE user_id=$1 AND (content ILIKE $2 OR id::text=$3) RETURNING content",
                user_id,
                "%" + query + "%",
                query,
            )
            # Rotating all active context prevents facts returning through a checkpoint or paraphrase.
            await conn.execute("UPDATE messages SET excluded=true WHERE user_id=$1", user_id)
            conversations = await conn.fetch(
                """UPDATE conversations SET revision=revision+1,title_message_count=0,
                title=CASE WHEN title_auto AND thread_id>0 THEN 'Новый чат' ELSE title END
                WHERE user_id=$1 RETURNING id,revision,thread_id,title_auto""",
                user_id,
            )
            for conversation in conversations:
                if not conversation["title_auto"] or conversation["thread_id"] <= 0:
                    continue
                dedupe = (
                    f"cronos:topic_title_reset:{user_id}:{conversation['id']}:"
                    f"{conversation['revision']}"
                )
                await conn.execute(
                    """INSERT INTO events(id,kind,payload) VALUES($1,'topic_title_reset',$2)
                    ON CONFLICT(id) DO NOTHING""",
                    uuid5(NAMESPACE_URL, dedupe),
                    {
                        "user_id": user_id,
                        "conversation_id": str(conversation["id"]),
                        "revision": conversation["revision"],
                    },
                )
            await conn.execute(
                "UPDATE users SET memory_revision=memory_revision+1 WHERE user_id=$1", user_id
            )
            await conn.execute(
                """UPDATE runs SET cancel_requested=true WHERE user_id=$1
                AND status='running' AND ($2::uuid IS NULL OR id<>$2)""",
                user_id,
                uid(current_run["id"] if isinstance(current_run, dict) else current_run)
                if current_run
                else None,
            )
            return {"forgotten": [x["content"] for x in deleted], "context_reset": True}

    async def claim_event(self, owner: str, event_id=None):
        async with self.connection() as conn:
            await conn.execute(
                """UPDATE events SET state='failed',lease_until=NULL,owner=NULL,
                error='Event attempt limit reached' WHERE attempts>=3
                AND (state='pending' OR (state='processing' AND lease_until<now()))
                AND ($1::uuid IS NULL OR id=$1)""",
                uid(event_id) if event_id else None,
            )
            row = await conn.fetchrow(
                """WITH picked AS (SELECT id FROM events
                WHERE (state='pending' OR (state='processing' AND lease_until<now())) AND available_at<=now()
                AND attempts<3
                AND ($2::uuid IS NULL OR id=$2) ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 1)
                UPDATE events SET state='processing',owner=$1,lease_until=now()+interval '180 seconds',attempts=attempts+1
                FROM picked WHERE events.id=picked.id RETURNING events.*""",
                owner,
                uid(event_id) if event_id else None,
            )
            return dict(row) if row else None

    async def heartbeat(self, event_id, owner):
        async with self.connection() as conn:
            return bool(
                await conn.fetchval(
                    "UPDATE events SET lease_until=now()+interval '180 seconds' WHERE id=$1 AND owner=$2 AND state='processing' RETURNING id",
                    uid(event_id),
                    owner,
                )
            )

    async def finish_event(self, event_id, owner, error: str | None = None):
        async with self.connection() as conn:
            await conn.execute(
                """UPDATE events SET state=CASE WHEN $3::text IS NULL THEN 'done'
                WHEN attempts>=3 THEN 'failed' ELSE 'pending' END,error=$3,
                lease_until=NULL,available_at=now()+interval '15 seconds' WHERE id=$1 AND owner=$2 AND state='processing'""",
                uid(event_id),
                owner,
                error,
            )

    async def pending_events(self):
        async with self.connection() as conn:
            rows = await conn.fetch(
                "SELECT id FROM events WHERE state='pending' AND available_at<=now() AND (notified_at IS NULL OR notified_at<now()-interval '30 seconds') ORDER BY created_at LIMIT 50"
            )
            return [str(x["id"]) for x in rows]

    async def notified(self, event_id):
        async with self.connection() as conn:
            await conn.execute("UPDATE events SET notified_at=now() WHERE id=$1", uid(event_id))

    async def enqueue(
        self, user_id: int, chat_id: int, thread_id: int | None, payload: dict, dedupe_key: str
    ):
        async with self.connection() as conn:
            return await conn.fetchval(
                "INSERT INTO outbox(user_id,chat_id,thread_id,payload,dedupe_key) VALUES($1,$2,$3,$4,$5) ON CONFLICT(dedupe_key) DO UPDATE SET dedupe_key=EXCLUDED.dedupe_key RETURNING id",
                user_id,
                chat_id,
                thread_id or 0,
                payload,
                dedupe_key,
            )

    async def enqueue_for_run(self, run: dict, conversation: dict, payload: dict, dedupe: str):
        user_id = run["user_id"]
        async with self.connection(user_id) as conn:
            active = await conn.fetchrow(
                """SELECT id FROM runs WHERE id=$1 AND user_id=$2
                AND status='running' AND fence=$3 AND NOT cancel_requested FOR SHARE""",
                uid(run["id"]),
                user_id,
                run["fence"],
            )
            if not active:
                return None
            target = await conn.fetchrow(
                """SELECT id,chat_id,thread_id FROM conversations
                WHERE id=$1 AND user_id=$2""",
                uid(conversation["id"]),
                user_id,
            )
            if not target:
                return None
            return await conn.fetchval(
                """INSERT INTO outbox(user_id,chat_id,thread_id,payload,dedupe_key)
                VALUES($1,$2,$3,$4,$5) ON CONFLICT(dedupe_key) DO UPDATE
                SET dedupe_key=EXCLUDED.dedupe_key WHERE outbox.user_id=EXCLUDED.user_id RETURNING id""",
                user_id,
                target["chat_id"],
                target["thread_id"],
                payload,
                dedupe,
            )

    async def next_delivery(self, owner: str):
        async with self.connection() as conn:
            row = await conn.fetchrow(
                """WITH picked AS (SELECT id FROM outbox WHERE
                (state='pending' OR (state='sending' AND lease_until<now())) AND next_attempt_at<=now()
                ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1)
                UPDATE outbox SET state='sending',owner=$1,lease_until=now()+interval '180 seconds',attempts=attempts+1
                FROM picked WHERE outbox.id=picked.id RETURNING outbox.*""",
                owner,
            )
            return dict(row) if row else None

    async def delivery_result(
        self,
        delivery_id: int,
        owner: str,
        ids: list[int] | None = None,
        error=None,
        retry_after=5,
        permanent=False,
    ):
        async with self.connection() as conn:
            await conn.execute(
                """UPDATE outbox SET state=$3,telegram_ids=COALESCE($4,telegram_ids),error=$5,
                sent_at=CASE WHEN $4::jsonb IS NOT NULL THEN now() ELSE sent_at END,
                next_attempt_at=now()+$6*interval '1 second',lease_until=NULL WHERE id=$1 AND owner=$2""",
                delivery_id,
                owner,
                "sent" if ids is not None else ("failed" if permanent else "pending"),
                ids,
                error,
                retry_after,
            )

    async def start_run(self, event_id, user_id, conversation_id):
        user = await self.ensure_user(user_id)
        async with self.connection() as conn:
            row = await conn.fetchrow(
                """INSERT INTO runs(id,event_id,user_id,conversation_id,memory_revision) VALUES($1,$2,$3,$4,$5)
                ON CONFLICT(event_id) DO UPDATE SET status='running',fence=runs.fence+1 RETURNING *""",
                uuid4(),
                uid(event_id),
                user_id,
                uid(conversation_id),
                user["memory_revision"],
            )
            return dict(row)

    async def run_active(self, run_id, fence):
        async with self.connection() as conn:
            return bool(
                await conn.fetchval(
                    "SELECT status='running' AND NOT cancel_requested AND fence=$2 FROM runs WHERE id=$1",
                    uid(run_id),
                    fence,
                )
            )

    async def finish_run(self, run_id, status="done", fence=None):
        async with self.connection() as conn:
            return bool(
                await conn.fetchval(
                    """UPDATE runs SET status=$2,finished_at=now()
                WHERE id=$1 AND ($3::bigint IS NULL OR fence=$3) AND status='running' RETURNING id""",
                    uid(run_id),
                    status,
                    fence,
                )
            )

    async def operation(self, operation_id: str):
        async with self.connection() as conn:
            return await conn.fetchval(
                "SELECT result FROM operations WHERE id=$1 AND status='done'", operation_id
            )

    async def save_operation(self, operation_id: str, user_id, run_id, kind, result):
        async with self.connection() as conn:
            await conn.execute(
                "INSERT INTO operations(id,user_id,run_id,kind,result,status) VALUES($1,$2,$3,$4,$5,'done') ON CONFLICT(id) DO NOTHING",
                operation_id,
                user_id,
                uid(run_id),
                kind,
                result,
            )

    async def reserve(self, user_id: int, operation_id: str, amount_micro: int):
        await self.ensure_user(user_id)
        async with self.connection(user_id) as conn:
            user = await conn.fetchrow("SELECT * FROM users WHERE user_id=$1 FOR UPDATE", user_id)
            if (
                not self.settings.alpha_soft_limits
                and user["balance_micro"] + user["topup_micro"] - user["reserved_micro"]
                < amount_micro
            ):
                raise ValueError("Недостаточно токенов: можно повысить тариф без оплаты.")
            inserted = await conn.fetchval(
                "INSERT INTO reservations(id,user_id,amount_micro) VALUES($1,$2,$3) ON CONFLICT DO NOTHING RETURNING id",
                operation_id,
                user_id,
                amount_micro,
            )
            if inserted:
                await conn.execute(
                    "UPDATE users SET reserved_micro=reserved_micro+$2 WHERE user_id=$1",
                    user_id,
                    amount_micro,
                )

    async def record_usage(self, user_id: int, run_id, operation_id: str, usage: dict):
        cost = usage.get("cost_rub")
        if cost is None:
            # Keep the reservation for reconciliation instead of claiming an unknown call was free.
            async with self.connection(user_id) as conn:
                await conn.execute(
                    "INSERT INTO usage(operation_id,user_id,run_id,model,raw) VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING",
                    operation_id,
                    user_id,
                    uid(run_id) if run_id else None,
                    usage.get("model") or "unknown",
                    {**usage, "reconciliation": "pending"},
                )
            return
        cost_micro = int(Decimal(str(cost)) * 1_000_000)
        if cost_micro < 0:
            raise ValueError("Стоимость не может быть отрицательной")
        async with self.connection(user_id) as conn:
            user = await conn.fetchrow("SELECT * FROM users WHERE user_id=$1 FOR UPDATE", user_id)
            reservation = await conn.fetchrow(
                "SELECT * FROM reservations WHERE id=$1 AND user_id=$2 FOR UPDATE",
                operation_id,
                user_id,
            )
            charged = min(cost_micro, reservation["amount_micro"]) if reservation else cost_micro
            if reservation and reservation["status"] == "released":
                charged = 0
                usage = {
                    **usage,
                    "reconciliation": "unresolved_alpha",
                    "late_receipt": True,
                    "charged_to": "project",
                }
            inserted = await conn.fetchval(
                """INSERT INTO ledger(operation_id,user_id,kind,amount_micro,description)
                VALUES($1,$2,'usage',$3,$4) ON CONFLICT DO NOTHING RETURNING id""",
                operation_id,
                user_id,
                -charged,
                usage.get("model") or "unknown",
            )
            await conn.execute(
                """INSERT INTO usage(operation_id,user_id,run_id,model,prompt_tokens,completion_tokens,cost_micro,raw)
                VALUES($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT(operation_id) DO UPDATE
                SET cost_micro=EXCLUDED.cost_micro,raw=EXCLUDED.raw,prompt_tokens=EXCLUDED.prompt_tokens,
                completion_tokens=EXCLUDED.completion_tokens,model=EXCLUDED.model""",
                operation_id,
                user_id,
                uid(run_id) if run_id else None,
                usage.get("model") or "unknown",
                usage.get("prompt_tokens", 0),
                usage.get("completion_tokens", 0),
                cost_micro,
                usage,
            )
            if inserted:
                period_debit = min(charged, max(0, user["balance_micro"]))
                topup_debit = min(charged - period_debit, max(0, user["topup_micro"]))
                # Soft-limit debt belongs to the period bucket, never a negative top-up.
                period_debit += charged - period_debit - topup_debit
                await conn.execute(
                    """UPDATE users SET balance_micro=balance_micro-$2,
                    topup_micro=topup_micro-$3 WHERE user_id=$1""",
                    user_id,
                    period_debit,
                    topup_debit,
                )
            if reservation and reservation["status"] == "reserved":
                await conn.execute(
                    "UPDATE reservations SET status='settled' WHERE id=$1", operation_id
                )
                await conn.execute(
                    "UPDATE users SET reserved_micro=GREATEST(0,reserved_micro-$2) WHERE user_id=$1",
                    user_id,
                    reservation["amount_micro"],
                )

    async def balance(self, user_id: int):
        user = await self.ensure_user(user_id)
        async with self.connection(user_id) as conn:
            spent = await conn.fetchval(
                "SELECT COALESCE(sum(-amount_micro),0) FROM ledger WHERE user_id=$1 AND kind='usage'",
                user_id,
            )
            pending = await conn.fetchrow(
                """SELECT count(*) AS operations,COALESCE(sum(amount_micro),0) AS amount_micro
                FROM reservations WHERE user_id=$1 AND status='reserved'""",
                user_id,
            )
            unresolved = await conn.fetchval(
                "SELECT count(*) FROM usage WHERE user_id=$1 AND raw->>'reconciliation'='unresolved_alpha'",
                user_id,
            )
        return {
            "plan": user["plan"],
            "tokens_remaining": (user["balance_micro"] + user["topup_micro"]) / 1000,
            "tokens_spent": int(spent) / 1000,
            "tokens_reserved": user["reserved_micro"] / 1000,
            "period_end": user["period_end"].isoformat(),
            "soft_limits": self.settings.alpha_soft_limits,
            "plan_tokens": PLANS[user["plan"]] / 1000,
            "pending_plan": user["pending_plan"],
            "pending_cost_operations": pending["operations"],
            "tokens_pending_cost": int(pending["amount_micro"]) / 1000,
            "unresolved_cost_operations": unresolved,
        }

    async def pending_reservations(self, *, limit=100, user_id=None):
        async with self.connection() as conn:
            return [
                dict(row)
                for row in await conn.fetch(
                    """SELECT id,user_id,amount_micro,created_at FROM reservations
                WHERE status='reserved' AND created_at<now()-interval '2 minutes'
                AND ($1::bigint IS NULL OR user_id=$1) ORDER BY created_at LIMIT $2""",
                    user_id,
                    limit,
                )
            ]

    async def reconciliation_usage(self, user_id, operation_id):
        async with self.connection(user_id) as conn:
            row = await conn.fetchrow(
                "SELECT * FROM usage WHERE operation_id=$1 AND user_id=$2", operation_id, user_id
            )
            return dict(row) if row else None

    async def release_unresolved_reservation(self, user_id, operation_id):
        async with self.connection(user_id) as conn:
            await conn.fetchval("SELECT user_id FROM users WHERE user_id=$1 FOR UPDATE", user_id)
            row = await conn.fetchrow(
                """UPDATE reservations SET status='released'
                WHERE id=$1 AND user_id=$2 AND status='reserved'
                AND created_at<now()-interval '24 hours' RETURNING amount_micro""",
                operation_id,
                user_id,
            )
            if not row:
                return False
            await conn.execute(
                "UPDATE users SET reserved_micro=GREATEST(0,reserved_micro-$2) WHERE user_id=$1",
                user_id,
                row["amount_micro"],
            )
            await conn.execute(
                """INSERT INTO usage(operation_id,user_id,model,raw) VALUES($1,$2,'unknown',$3)
                ON CONFLICT(operation_id) DO UPDATE SET raw=usage.raw||EXCLUDED.raw""",
                operation_id,
                user_id,
                {"reconciliation": "unresolved_alpha", "charged_to": "project"},
            )
            return True

    async def change_plan(self, user_id: int, plan: str, operation_id: str):
        plan = plan.upper()
        if plan not in PLANS:
            raise ValueError("Неизвестный тариф")
        await self.ensure_user(user_id)
        async with self.connection(user_id) as conn:
            user = await conn.fetchrow("SELECT * FROM users WHERE user_id=$1 FOR UPDATE", user_id)
            existing = await conn.fetchval(
                "SELECT id FROM ledger WHERE operation_id=$1", operation_id
            )
            if not existing:
                if user["plan"] == "FREE" and plan != "FREE":
                    now = datetime.now(UTC)
                    amount = PLANS[plan]
                    await conn.execute(
                        "UPDATE users SET plan=$2,balance_micro=$3,entitlement_micro=$3,period_start=$4,period_end=$5,billing_anchor=$4,pending_plan=NULL WHERE user_id=$1",
                        user_id,
                        plan,
                        amount,
                        now,
                        now + relativedelta(months=1),
                    )
                else:
                    amount = max(0, PLANS[plan] - user["entitlement_micro"])
                    if PLANS[plan] < user["entitlement_micro"]:
                        await conn.execute(
                            "UPDATE users SET pending_plan=$2 WHERE user_id=$1", user_id, plan
                        )
                    else:
                        await conn.execute(
                            "UPDATE users SET plan=$2,balance_micro=balance_micro+$3,entitlement_micro=GREATEST(entitlement_micro,$4),pending_plan=NULL WHERE user_id=$1",
                            user_id,
                            plan,
                            amount,
                            PLANS[plan],
                        )
                await conn.execute(
                    "INSERT INTO ledger(operation_id,user_id,kind,amount_micro,description) VALUES($1,$2,'test_upgrade',$3,$4)",
                    operation_id,
                    user_id,
                    amount,
                    plan,
                )
        return await self.balance(user_id)

    async def top_up(self, user_id: int, tokens: int, operation_id: str):
        if tokens <= 0 or tokens > 1_000_000_000:
            raise ValueError("Укажите положительное количество токенов")
        await self.ensure_user(user_id)
        async with self.connection(user_id) as conn:
            await conn.fetchval("SELECT user_id FROM users WHERE user_id=$1 FOR UPDATE", user_id)
            added = await conn.fetchval(
                "INSERT INTO ledger(operation_id,user_id,kind,amount_micro,description) VALUES($1,$2,'test_topup',$3,'Mock payment') ON CONFLICT DO NOTHING RETURNING id",
                operation_id,
                user_id,
                tokens * 1000,
            )
            if added:
                await conn.execute(
                    "UPDATE users SET topup_micro=topup_micro+$2 WHERE user_id=$1",
                    user_id,
                    tokens * 1000,
                )
        return await self.balance(user_id)

    async def schedule(
        self,
        user_id,
        conversation: dict,
        due_at: datetime,
        text: str,
        instruction: str = "",
        dynamic=False,
        proactive=False,
        interval_seconds=None,
        source_key=None,
    ):
        if due_at.tzinfo is None:
            raise ValueError("У времени должен быть часовой пояс")
        prefs = await self.preferences(user_id)
        if proactive and not prefs.get("proactivity"):
            raise ValueError("Сначала согласуй с пользователем возможность писать первым")
        if interval_seconds is not None and interval_seconds < 60:
            raise ValueError("Слишком частое повторение")
        async with self.connection(user_id) as conn:
            row = await conn.fetchrow(
                """INSERT INTO schedules(id,user_id,conversation_id,chat_id,thread_id,due_at,
                timezone,interval_seconds,instruction,fixed_text,dynamic,proactive,source_key)
                VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
                ON CONFLICT(source_key) DO UPDATE SET source_key=EXCLUDED.source_key RETURNING *""",
                uuid4(),
                user_id,
                conversation["id"],
                conversation["chat_id"],
                conversation["thread_id"],
                due_at,
                prefs.get("timezone", "UTC"),
                interval_seconds,
                instruction or text,
                text,
                dynamic,
                proactive,
                source_key,
            )
            return {
                "id": str(row["id"]),
                "due_at": row["due_at"].isoformat(),
                "text": row["fixed_text"],
                "dynamic": dynamic,
            }

    async def list_schedules(self, user_id):
        async with self.connection() as conn:
            rows = await conn.fetch(
                "SELECT id,due_at,fixed_text,instruction,dynamic,proactive,state FROM schedules WHERE user_id=$1 AND state='active' ORDER BY due_at",
                user_id,
            )
            return [
                {**dict(x), "id": str(x["id"]), "due_at": x["due_at"].isoformat()} for x in rows
            ]

    async def change_schedule(self, user_id, schedule_id, action, due_at=None):
        async with self.connection() as conn:
            if action == "cancel":
                changed = await conn.fetchval(
                    "UPDATE schedules SET state='cancelled',revision=revision+1 WHERE id=$1 AND user_id=$2 RETURNING id",
                    uid(schedule_id),
                    user_id,
                )
            elif action == "reschedule" and due_at:
                changed = await conn.fetchval(
                    "UPDATE schedules SET state='active',revision=revision+1,due_at=$3 WHERE id=$1 AND user_id=$2 RETURNING id",
                    uid(schedule_id),
                    user_id,
                    due_at,
                )
            else:
                raise ValueError("Укажите cancel или reschedule с новым временем")
            if not changed:
                raise ValueError("Напоминание не найдено")
            return {"id": str(changed), "action": action}

    async def schedule_due(self, user_id: int | None = None):
        async with self.connection() as conn:
            rows = await conn.fetch(
                """SELECT * FROM schedules WHERE state='active' AND due_at<=now()
                AND ($1::bigint IS NULL OR user_id=$1) ORDER BY due_at FOR UPDATE SKIP LOCKED LIMIT 20""",
                user_id,
            )
            for row in rows:
                event_id, occurrence_id = uuid4(), uuid4()
                inserted = await conn.fetchval(
                    "INSERT INTO occurrences(id,schedule_id,revision,scheduled_at,event_id) VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING RETURNING id",
                    occurrence_id,
                    row["id"],
                    row["revision"],
                    row["due_at"],
                    event_id,
                )
                if inserted:
                    await conn.execute(
                        "INSERT INTO events(id,kind,payload) VALUES($1,'timer',$2)",
                        event_id,
                        {
                            "schedule_id": str(row["id"]),
                            "revision": row["revision"],
                            "occurrence_id": str(occurrence_id),
                        },
                    )
                if row["interval_seconds"]:
                    now = datetime.now(UTC)
                    steps = max(
                        1, int((now - row["due_at"]).total_seconds() // row["interval_seconds"]) + 1
                    )
                    await conn.execute(
                        "UPDATE schedules SET due_at=due_at+$2*interval '1 second' WHERE id=$1",
                        row["id"],
                        steps * row["interval_seconds"],
                    )
                else:
                    await conn.execute(
                        "UPDATE schedules SET state='triggered' WHERE id=$1", row["id"]
                    )
            return len(rows)

    async def get_schedule(self, schedule_id):
        async with self.connection() as conn:
            row = await conn.fetchrow("SELECT * FROM schedules WHERE id=$1", uid(schedule_id))
            return dict(row) if row else None

    async def occurrence_active(self, occurrence_id):
        async with self.connection() as conn:
            return bool(
                await conn.fetchval(
                    """SELECT o.state='pending' AND s.state<>'cancelled'
                AND o.revision=s.revision FROM occurrences o JOIN schedules s ON s.id=o.schedule_id
                WHERE o.id=$1""",
                    uid(occurrence_id),
                )
            )

    async def mark_occurrence(self, occurrence_id, state="done"):
        async with self.connection() as conn:
            await conn.execute(
                "UPDATE occurrences SET state=$2 WHERE id=$1", uid(occurrence_id), state
            )

    async def save_artifact(self, user_id, artifact: dict):
        async with self.connection(user_id) as conn:
            await conn.execute(
                """INSERT INTO artifacts(id,user_id,filename,mime,path,extracted,size_bytes,checksum)
                VALUES($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT(id) DO NOTHING""",
                uid(artifact["id"]),
                user_id,
                artifact["filename"],
                artifact["mime"],
                artifact["path"],
                artifact.get("extracted", {}),
                artifact.get("size_bytes", 0),
                artifact.get("checksum"),
            )

    async def get_artifact(self, user_id, artifact_id):
        async with self.connection(user_id) as conn:
            row = await conn.fetchrow(
                "SELECT * FROM artifacts WHERE user_id=$1 AND id=$2", user_id, uid(artifact_id)
            )
            if not row:
                raise ValueError("Файл не найден")
            return dict(row)

    async def list_artifacts(self, user_id):
        async with self.connection(user_id) as conn:
            rows = await conn.fetch(
                "SELECT id,filename,mime,size_bytes FROM artifacts WHERE user_id=$1 ORDER BY created_at DESC LIMIT 30",
                user_id,
            )
            return [{**dict(x), "id": str(x["id"])} for x in rows]

    async def run_metrics(self, run_id, user_id, duration_ms, cpu_seconds, peak_rss_bytes):
        async with self.connection() as conn:
            await conn.execute(
                "INSERT INTO run_metrics(run_id,user_id,duration_ms,cpu_seconds,peak_rss_bytes) VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING",
                uid(run_id),
                user_id,
                duration_ms,
                cpu_seconds,
                peak_rss_bytes,
            )


async def migrate(settings: Settings):
    url = settings.admin_database_url or settings.database_url
    conn = await asyncpg.connect(url.get_secret_value())
    try:
        async with conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock(903125)")
            await conn.execute(Path(__file__).with_name("schema.sql").read_text())
    finally:
        await conn.close()


if __name__ == "__main__":
    import asyncio

    from cronos.settings import get_settings

    asyncio.run(migrate(get_settings()))
