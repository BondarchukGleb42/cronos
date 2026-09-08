import asyncio
import hashlib
import json
import re
from contextlib import AsyncExitStack, asynccontextmanager
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

import asyncpg
from dateutil.relativedelta import relativedelta

from cronos.initiative import (
    InitiativeStoreMixin,
    invalidate_initiative_context,
    mark_initiative_sent,
    validate_initiative_payload,
)
from cronos.library import LibraryStoreMixin
from cronos.memory import MemoryStoreMixin
from cronos.memory_privacy import invalidate_memory_context
from cronos.model_preferences import normalize_model_choice
from cronos.proactivity import APPLY_DEFAULT_SQL
from cronos.projects import ProjectsStoreMixin
from cronos.recipes import RecipesStoreMixin
from cronos.settings import Settings
from cronos.versions import VersionsStoreMixin
from cronos.workflows import WorkflowsStoreMixin

PLANS = {"FREE": 25_000_000, "START": 700_000_000, "PREMIUM": 1_700_000_000, "PRO": 3_700_000_000}
HOME_TITLE = "🪐 Cronos"
MEDIA_GROUP_DEBOUNCE_SECONDS = 2
STOP_COMMANDS = {"стоп", "остановись", "отмена", "stop", "/stop"}


def message_command(text: str) -> str:
    parts = text.strip().casefold().split(maxsplit=1)
    command = parts[0].rstrip(",.!?:;") if parts else ""
    return command.split("@", 1)[0] if command.startswith("/") else command


def uid(value):
    return value if isinstance(value, UUID) else UUID(str(value))


def sanitized_usage(value: dict) -> dict:
    """Keep accounting/receipt fields, never arbitrary provider response content."""
    keys = {
        "prompt_tokens",
        "completion_tokens",
        "cost_rub",
        "model",
        "provider",
        "request_id",
        "token_counts_known",
        "reconciliation",
        "charged_to",
        "late_receipt",
    }
    return {
        key: item
        for key, item in value.items()
        if key in keys and isinstance(item, (str, int, float, bool, type(None)))
    }


def telegram_message(update: dict) -> dict:
    for key in ("message", "edited_message", "stopped_message_generation", "my_chat_member"):
        if isinstance(update.get(key), dict):
            return update[key]
    return (update.get("callback_query") or {}).get("message") or {}


def telegram_event_messages(update: dict) -> list[dict]:
    messages = update.get("media_group_messages")
    if isinstance(messages, list) and messages:
        return [message for message in messages if isinstance(message, dict)]
    message = telegram_message(update)
    return [message] if message else []


def telegram_owner(update: dict) -> int | None:
    message = telegram_message(update)
    chat = message.get("chat") or {}
    if chat.get("type") == "private" and isinstance(chat.get("id"), int):
        return chat["id"]
    sender = (update.get("callback_query") or {}).get("from") or message.get("from") or {}
    return sender.get("id") if isinstance(sender.get("id"), int) else None


def privacy_event_id(request_id) -> UUID:
    return uuid5(NAMESPACE_URL, f"cronos:privacy:{uid(request_id)}")


class Store(
    ProjectsStoreMixin,
    MemoryStoreMixin,
    LibraryStoreMixin,
    VersionsStoreMixin,
    WorkflowsStoreMixin,
    RecipesStoreMixin,
    InitiativeStoreMixin,
):
    def __init__(self, settings: Settings):
        self.settings = settings
        self.pool: asyncpg.Pool | None = None
        self._user_lock_slots = asyncio.Semaphore(3)
        self._delivery_lock_slots = asyncio.Semaphore(3)

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
    async def user_lock(self, user_id: int, purpose: str = "work"):
        """Serialize a user's topics across processes without exhausting the pool."""
        if self.pool is None:
            raise RuntimeError("Database is not connected")
        if purpose not in {"work", "delivery"}:
            raise ValueError("Unknown user lock purpose")
        namespace = "user" if purpose == "work" else "delivery"
        key = int.from_bytes(
            hashlib.blake2b(f"cronos:{namespace}:{user_id}".encode(), digest_size=8).digest(),
            signed=True,
        )
        slots = self._user_lock_slots if purpose == "work" else self._delivery_lock_slots
        async with slots:
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
            for original in updates:
                update, privacy_confirmed = await self._ingress_privacy(conn, original)
                if update is None:
                    await conn.execute(
                        "INSERT INTO events(id,update_id,payload,state) VALUES($1,$2,'{}','done') ON CONFLICT(update_id) DO NOTHING",
                        uuid4(),
                        original["update_id"],
                    )
                    continue
                if self._media_group_key(update) is not None:
                    inserted = await self._ingest_media_group(conn, update)
                else:
                    album_key = await self._pending_media_group_for_text(conn, update)
                    if album_key is not None:
                        inserted = await self._ingest_media_group(
                            conn, update, key=album_key, join_text=True
                        )
                        if inserted is not False:
                            # Plain album instructions cannot be control commands.
                            continue
                    inserted = await conn.fetchval(
                        """INSERT INTO events(id,update_id,payload) VALUES($1,$2,$3)
                        ON CONFLICT(update_id) DO NOTHING RETURNING id""",
                        uuid4(),
                        update["update_id"],
                        update,
                    )
                if not inserted:
                    continue
                if privacy_confirmed:
                    await conn.execute(
                        "UPDATE runs SET cancel_requested=true WHERE user_id=$1 AND status='running'",
                        telegram_owner(update),
                    )
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

    @staticmethod
    def _media_group_key(update):
        message = update.get("message") or {}
        group_id = message.get("media_group_id")
        chat = message.get("chat") or {}
        owner = telegram_owner(update)
        if (
            not isinstance(group_id, str)
            or not group_id
            or owner is None
            or not isinstance(chat.get("id"), int)
            or not isinstance(message.get("message_id"), int)
        ):
            return None
        scope = [owner, chat["id"], message.get("message_thread_id") or 0, group_id]
        return hashlib.sha256(json.dumps(scope, separators=(",", ":")).encode()).hexdigest()

    async def _pending_media_group_for_text(self, conn, update):
        message = update.get("message") or {}
        text = message.get("text")
        if (
            not isinstance(text, str)
            or not text.strip()
            or (message.get("from") or {}).get("is_bot")
        ):
            return None
        command = message_command(text)
        if (
            command.startswith("/")
            or command in STOP_COMMANDS
            or text.strip().casefold().rstrip(".! ")
            in {
                "подтверждаю полную очистку",
                "подтверждаю удаление чата",
                "➕ новый чат",
                "🗑 удалить чат",
                "🪐 главное меню",
                "🤖 модель",
                "🧠 режим рассуждения",
            }
        ):
            return None
        owner = telegram_owner(update)
        chat_id = (message.get("chat") or {}).get("id")
        if owner is None or not isinstance(chat_id, int):
            return None
        return await conn.fetchval(
            """SELECT media_group_key FROM events WHERE media_group_key IS NOT NULL
            AND state='pending' AND attempts=0 AND available_at>clock_timestamp()
            AND payload->>'media_group_continuation' IS DISTINCT FROM 'true'
            AND payload#>>'{message,chat,id}'=$1
            AND COALESCE(payload#>>'{message,message_thread_id}','0')=$2
            AND ((payload#>>'{message,chat,type}'='private' AND payload#>>'{message,chat,id}'=$3)
                 OR payload#>>'{message,from,id}'=$3)
            ORDER BY created_at DESC,id DESC LIMIT 1""",
            str(chat_id),
            str(message.get("message_thread_id") or 0),
            str(owner),
        )

    async def _ingest_media_group(self, conn, update, *, key=None, join_text=False):
        """False means a text no longer qualifies; None means an existing update receipt."""
        key = key or self._media_group_key(update)
        message = update["message"]
        chat = message["chat"]
        lock_key = int.from_bytes(
            hashlib.blake2b(f"cronos:media-group:{key}".encode(), digest_size=8).digest(),
            signed=True,
        )
        await conn.execute("SELECT pg_advisory_xact_lock($1::bigint)", lock_key)
        previous = await conn.fetchrow(
            """SELECT *,available_at>clock_timestamp() AS collecting_window FROM events
            WHERE media_group_key=$1 ORDER BY created_at DESC,id DESC LIMIT 1 FOR UPDATE""",
            key,
        )
        collecting = (
            previous is not None and previous["state"] == "pending" and previous["attempts"] == 0
        )
        if join_text and (
            not collecting
            or not previous["collecting_window"]
            or previous["payload"].get("media_group_continuation")
        ):
            return False
        receipt_message = {
            "message_id": message["message_id"],
            "date": message.get("date"),
            "message_thread_id": message.get("message_thread_id") or 0,
            "chat": {"id": chat["id"], "type": chat.get("type")},
            "from": {"id": (message.get("from") or {}).get("id")},
        }
        receipt = await conn.fetchval(
            """INSERT INTO events(id,update_id,payload,state) VALUES($1,$2,$3,'done')
            ON CONFLICT(update_id) DO NOTHING RETURNING id""",
            uuid4(),
            update["update_id"],
            {
                "update_id": update["update_id"],
                "message": receipt_message,
                "media_group_receipt": True,
            },
        )
        if receipt is None:
            return None
        messages = telegram_event_messages(previous["payload"]) if previous else []
        if any(item.get("message_id") == message["message_id"] for item in messages):
            return receipt
        messages = sorted([*messages, message], key=lambda item: item["message_id"])
        primary = next((item for item in messages if (item.get("caption") or "").strip()), None)
        if primary is None:
            primary = next(
                (item for item in messages if (item.get("text") or "").strip()), messages[0]
            )
        payload = dict(previous["payload"] if collecting else update)
        payload["message"] = primary
        payload["media_group_messages"] = messages
        if previous and not collecting:
            # Do not mutate a run's input after it may have made paid requests.
            payload["media_group_continuation"] = True
        if collecting:
            await conn.execute(
                """UPDATE events SET payload=$2,available_at=clock_timestamp()+$3*interval '1 second',
                notified_at=NULL WHERE id=$1""",
                previous["id"],
                payload,
                MEDIA_GROUP_DEBOUNCE_SECONDS,
            )
        else:
            await conn.execute(
                """INSERT INTO events(id,payload,media_group_key,available_at)
                VALUES($1,$2,$3,clock_timestamp()+$4*interval '1 second')""",
                uuid4(),
                payload,
                key,
                MEDIA_GROUP_DEBOUNCE_SECONDS,
            )
        return receipt

    async def _ingress_privacy(self, conn, update):
        user_id = telegram_owner(update)
        if user_id is None:
            return update, False
        await conn.execute("SELECT set_config('app.user_id',$1,true)", str(user_id))
        user = await conn.fetchrow(
            "SELECT content_reset_at FROM users WHERE user_id=$1 FOR SHARE", user_id
        )
        if not user:
            return update, False
        message = telegram_message(update)
        thread = message.get("message_thread_id") or 0
        chat_id = (message.get("chat") or {}).get("id")
        text = (message.get("text") or "").strip().casefold().rstrip(".! ")
        callback = update.get("callback_query") or {}
        control, confirms = None, False
        data = callback.get("data") or ""
        if data.startswith(("privacy:confirm:", "privacy:cancel:")):
            try:
                token = uid(data.split(":", 2)[2])
            except ValueError, TypeError:
                token = None
            if token is not None and (callback.get("from") or {}).get("id") == user_id:
                control = await conn.fetchrow(
                    """SELECT * FROM privacy_requests WHERE id=$1 AND user_id=$2
                    AND origin_thread_id=$3 AND expires_at>now()
                    AND state IN ('pending','ready','erasing')""",
                    token,
                    user_id,
                    thread,
                )
                confirms = bool(control and data.startswith("privacy:confirm:"))
        else:
            scope = {"подтверждаю полную очистку": "all", "подтверждаю удаление чата": "chat"}.get(
                text
            )
            if scope and not (message.get("from") or {}).get("is_bot"):
                control = await conn.fetchrow(
                    """SELECT * FROM privacy_requests WHERE user_id=$1 AND origin_thread_id=$2
                    AND scope=$3 AND state IN ('pending','ready','erasing') AND expires_at>now()
                    ORDER BY created_at DESC LIMIT 1""",
                    user_id,
                    thread,
                    scope,
                )
                confirms = bool(control)
        erasing = await conn.fetchval(
            "SELECT 1 FROM privacy_requests WHERE user_id=$1 AND state='erasing'", user_id
        )
        if erasing and not control and message_command(text) != "/clearall":
            return None, False
        tombstones = await conn.fetch(
            "SELECT thread_id,deleted_at FROM deleted_topics WHERE user_id=$1 AND chat_id=$2",
            user_id,
            chat_id,
        )
        deleted = {row["thread_id"]: row["deleted_at"] for row in tombstones}
        cutoff = user["content_reset_at"]
        topic_cutoff = deleted.get(thread)
        if topic_cutoff and thread > 1 and not control:
            return None, False
        if topic_cutoff and (cutoff is None or topic_cutoff > cutoff):
            cutoff = topic_cutoff
        timestamp = message.get("date")
        if (
            cutoff
            and isinstance(timestamp, (int, float))
            and timestamp <= int(cutoff.timestamp())
            and not control
        ):
            return None, False
        if erasing or (control and (cutoff or deleted)):
            # Cleanup controls need IDs and a nonce, never profile names or old content.
            minimal_message = {
                "message_id": message.get("message_id"),
                "date": message.get("date"),
                "chat": {"id": chat_id, "type": "private"},
                "message_thread_id": thread,
                "from": {"id": user_id, "is_bot": False},
            }
            if callback:
                return {
                    "update_id": update["update_id"],
                    "callback_query": {
                        "id": callback.get("id"),
                        "from": {"id": user_id, "is_bot": False},
                        "data": data,
                        "message": minimal_message,
                    },
                }, confirms
            minimal_message["text"] = "/clearall" if message_command(text) == "/clearall" else text
            return {"update_id": update["update_id"], "message": minimal_message}, confirms
        if cutoff or deleted:
            # A new update must not bring back an old quoted/replied-to message.
            update = json.loads(json.dumps(update))
            message = telegram_message(update)
            for key in ("reply_to_message", "external_reply", "quote", "forward_origin"):
                message.pop(key, None)
            if update.get("callback_query"):
                for key in ("text", "caption", "entities", "caption_entities", "reply_markup"):
                    message.pop(key, None)
        return update, confirms

    @staticmethod
    def _privacy_descriptor(row, title=""):
        if not row:
            return None
        result = dict(row)
        for key in ("id", "conversation_id", "run_id"):
            if result.get(key) is not None:
                result[key] = str(result[key])
        result["thread_id"] = result["target_thread_id"]
        result["title"] = title
        result["expires_at"] = result["expires_at"].isoformat()
        return result

    async def requests_prepare(
        self, user_id, conversation, scope, target_conversation_id=None, *, source_key, run_id=None
    ):
        if scope not in {"all", "chat"}:
            raise ValueError("Неизвестная область удаления")
        async with self.connection(user_id) as conn:
            await conn.fetchval("SELECT user_id FROM users WHERE user_id=$1 FOR UPDATE", user_id)
            origin = await conn.fetchrow(
                "SELECT * FROM conversations WHERE user_id=$1 AND id=$2",
                user_id,
                uid(conversation["id"]),
            )
            if not origin:
                raise ValueError("Чат не найден")
            existing = await conn.fetchrow(
                "SELECT * FROM privacy_requests WHERE source_key=$1 AND user_id=$2",
                source_key,
                user_id,
            )
            if existing:
                target = await conn.fetchrow(
                    "SELECT title FROM conversations WHERE user_id=$1 AND id=$2",
                    user_id,
                    existing["conversation_id"],
                )
                return self._privacy_descriptor(existing, target["title"] if target else "")
            if scope == "all":
                recovering = await conn.fetchrow(
                    "SELECT * FROM privacy_requests WHERE user_id=$1 AND state='erasing' FOR UPDATE",
                    user_id,
                )
                if recovering:
                    recovering = await conn.fetchrow(
                        """UPDATE privacy_requests SET expires_at=now()+interval '15 minutes',
                        origin_thread_id=$2 WHERE id=$1 RETURNING *""",
                        recovering["id"],
                        origin["thread_id"],
                    )
                    return self._privacy_descriptor(recovering)
            target = await conn.fetchrow(
                "SELECT * FROM conversations WHERE user_id=$1 AND id=$2",
                user_id,
                uid(target_conversation_id) if target_conversation_id else origin["id"],
            )
            if not target or target["chat_id"] != origin["chat_id"]:
                raise ValueError("Чат не найден")
            if run_id and not await conn.fetchval(
                "SELECT 1 FROM runs WHERE id=$1 AND user_id=$2", uid(run_id), user_id
            ):
                raise ValueError("Запрос не найден")
            await conn.execute(
                """UPDATE privacy_requests SET state='cancelled' WHERE user_id=$1
                AND origin_thread_id=$2 AND state='pending'""",
                user_id,
                origin["thread_id"],
            )
            row = await conn.fetchrow(
                """INSERT INTO privacy_requests(id,user_id,scope,conversation_id,chat_id,
                target_thread_id,origin_thread_id,run_id,source_key)
                VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING *""",
                uuid4(),
                user_id,
                scope,
                target["id"] if scope == "chat" else None,
                origin["chat_id"],
                target["thread_id"],
                origin["thread_id"],
                uid(run_id) if run_id else None,
                source_key,
            )
            return self._privacy_descriptor(row, target["title"])

    async def get_privacy_request(self, user_id, request_id):
        async with self.connection(user_id) as conn:
            row = await conn.fetchrow(
                """SELECT p.*,c.title AS live_title FROM privacy_requests p
                LEFT JOIN conversations c ON c.id=p.conversation_id AND c.user_id=p.user_id
                WHERE p.id=$1 AND p.user_id=$2""",
                uid(request_id),
                user_id,
            )
            result = self._privacy_descriptor(row, row["live_title"] or "" if row else "")
            if result:
                result.pop("live_title", None)
            return result

    async def pending_privacy_request(self, user_id, origin_thread_id, scope=None):
        async with self.connection(user_id) as conn:
            row = await conn.fetchrow(
                """SELECT p.*,c.title AS live_title FROM privacy_requests p
                LEFT JOIN conversations c ON c.id=p.conversation_id AND c.user_id=p.user_id
                WHERE p.user_id=$1 AND p.origin_thread_id=$2
                AND (p.state='pending' OR ($3::text IS NOT NULL AND p.state IN ('ready','erasing')))
                AND p.expires_at>now() AND ($3::text IS NULL OR p.scope=$3)
                ORDER BY p.created_at DESC LIMIT 1""",
                user_id,
                origin_thread_id or 0,
                scope,
            )
            result = self._privacy_descriptor(row, row["live_title"] or "" if row else "")
            if result:
                result.pop("live_title", None)
            return result

    async def privacy_request_for_run(self, run_id):
        async with self.connection() as conn:
            row = await conn.fetchrow(
                """SELECT * FROM privacy_requests WHERE run_id=$1 AND state='pending'
                AND expires_at>now() ORDER BY created_at DESC LIMIT 1""",
                uid(run_id),
            )
            return self._privacy_descriptor(row)

    async def confirm_privacy_request(self, user_id, request_id, origin_thread_id):
        async with self.connection(user_id) as conn:
            await conn.fetchval("SELECT user_id FROM users WHERE user_id=$1 FOR UPDATE", user_id)
            row = await conn.fetchrow(
                """SELECT * FROM privacy_requests WHERE id=$1 AND user_id=$2
                FOR UPDATE""",
                uid(request_id),
                user_id,
            )
            if not row or row["state"] == "cancelled":
                return None
            if row["state"] == "done":
                return self._privacy_descriptor(row)
            if row["origin_thread_id"] != (origin_thread_id or 0) or row[
                "expires_at"
            ] <= datetime.now(UTC):
                return None
            row = await conn.fetchrow(
                """UPDATE privacy_requests SET state=CASE WHEN state='pending' THEN 'ready'
                ELSE state END WHERE id=$1 RETURNING *""",
                row["id"],
            )
            await conn.execute(
                """INSERT INTO events(id,kind,payload) VALUES($1,'privacy',$2)
                ON CONFLICT(id) DO UPDATE SET state='pending',attempts=0,owner=NULL,
                lease_until=NULL,available_at=now(),notified_at=NULL,error=NULL
                WHERE events.state='failed'""",
                privacy_event_id(row["id"]),
                {"user_id": user_id, "request_id": str(row["id"])},
            )
            return self._privacy_descriptor(row)

    async def cancel_privacy_request(self, user_id, request_id, origin_thread_id):
        async with self.connection(user_id) as conn:
            return bool(
                await conn.fetchval(
                    """UPDATE privacy_requests SET state='cancelled' WHERE id=$1 AND user_id=$2
                AND origin_thread_id=$3 AND state='pending' RETURNING id""",
                    uid(request_id),
                    user_id,
                    origin_thread_id or 0,
                )
            )

    async def event_current(self, event_id):
        async with self.connection() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM events WHERE id=$1 AND payload<>'{}'::jsonb AND state IN ('pending','processing')",
                uid(event_id),
            )
            if not row:
                return None
            user_id = row["payload"].get("user_id") or telegram_owner(row["payload"])
            if row["kind"] != "privacy" and user_id is not None:
                if await conn.fetchval(
                    "SELECT 1 FROM privacy_requests WHERE user_id=$1 AND state='erasing'", user_id
                ):
                    if telegram_owner(row["payload"]) is None:
                        return None
                    payload, _ = await self._ingress_privacy(conn, row["payload"])
                    if payload is None:
                        return None
                    return {**dict(row), "payload": payload}
            return dict(row)

    async def claimed_delivery(self, delivery_id, owner):
        async with self.connection() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM outbox WHERE id=$1 AND owner=$2 AND state='sending'",
                delivery_id,
                owner,
            )
            return dict(row) if row else None

    async def begin_privacy_erasure(self, request_id, event_id):
        async with self.connection() as conn:
            # Root holds both advisory locks. The row lock also fences ingress.
            owner = await conn.fetchval(
                "SELECT user_id FROM privacy_requests WHERE id=$1", uid(request_id)
            )
            if owner is None:
                return None
            await conn.execute("SELECT set_config('app.user_id',$1,true)", str(owner))
            await conn.fetchval("SELECT user_id FROM users WHERE user_id=$1 FOR UPDATE", owner)
            request = await conn.fetchrow(
                "SELECT * FROM privacy_requests WHERE id=$1 FOR UPDATE", uid(request_id)
            )
            event = await conn.fetchrow(
                "SELECT * FROM events WHERE id=$1 FOR UPDATE", uid(event_id)
            )
            if (
                not event
                or event["kind"] != "privacy"
                or event["payload"].get("user_id") != owner
                or event["payload"].get("request_id") != str(request["id"])
                or uid(event_id) != privacy_event_id(request["id"])
            ):
                return None
            if request["state"] == "erasing":
                return self._privacy_descriptor(request)
            if request["state"] != "ready":
                return None
            full = request["scope"] == "all"
            conversations = await conn.fetch(
                """SELECT * FROM conversations WHERE user_id=$1 AND ($2 OR id=$3
                OR ($4::bigint IN (0,1) AND chat_id=$5 AND thread_id IN (0,1))) FOR UPDATE""",
                owner,
                full,
                request["conversation_id"],
                request["target_thread_id"],
                request["chat_id"],
            )
            if full or any(row["is_home"] for row in conversations):
                await conn.execute(
                    "UPDATE users SET home_generation=gen_random_uuid() WHERE user_id=$1", owner
                )
            conversation_ids = [row["id"] for row in conversations]
            runs = await conn.fetch(
                """SELECT * FROM runs WHERE user_id=$1
                AND ($2 OR conversation_id=ANY($3::uuid[]) OR id=$4) FOR UPDATE""",
                owner,
                full,
                conversation_ids,
                request["run_id"],
            )
            run_ids = [row["id"] for row in runs]
            event_ids = {row["event_id"] for row in runs if row["event_id"]}
            # Commands prepare without a run, but their source event contains the intent.
            for match in re.findall(
                r"[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}", request["source_key"]
            ):
                source = await conn.fetchval("SELECT payload FROM events WHERE id=$1", uid(match))
                if source and (telegram_owner(source) == owner or source.get("user_id") == owner):
                    event_ids.add(uid(match))
            schedules = await conn.fetch(
                "SELECT id FROM schedules WHERE user_id=$1 AND ($2 OR conversation_id=ANY($3::uuid[]))",
                owner,
                full,
                conversation_ids,
            )
            schedule_ids = [row["id"] for row in schedules]
            event_ids.update(
                row["event_id"]
                for row in await conn.fetch(
                    "SELECT event_id FROM occurrences WHERE schedule_id=ANY($1::uuid[])",
                    schedule_ids,
                )
            )
            candidates = await conn.fetch(
                """SELECT * FROM events WHERE id=ANY($1::uuid[]) OR payload->>'user_id'=$2
                OR payload#>>'{message,chat,id}'=$2 OR payload#>>'{edited_message,chat,id}'=$2
                OR payload#>>'{message,from,id}'=$2 OR payload#>>'{edited_message,from,id}'=$2
                OR payload#>>'{my_chat_member,from,id}'=$2
                OR payload#>>'{callback_query,from,id}'=$2 OR payload#>>'{callback_query,message,chat,id}'=$2
                OR payload#>>'{my_chat_member,chat,id}'=$2 OR payload#>>'{stopped_message_generation,chat,id}'=$2""",
                list(event_ids),
                str(owner),
            )
            topic_ids = {row["thread_id"] for row in conversations if row["thread_id"] > 1}
            general_ids = set()
            general_deadlines = {}
            now_seconds = datetime.now(UTC).timestamp()
            # Telegram permits <48h; leave five minutes for retries and API latency.
            deletion_window = 48 * 3600 - 300
            if full:
                topic_ids.update(
                    row["thread_id"]
                    for row in await conn.fetch(
                        "SELECT thread_id FROM deleted_topics WHERE user_id=$1 AND chat_id=$2 AND thread_id>1",
                        owner,
                        request["chat_id"],
                    )
                )
                for previous in await conn.fetch(
                    "SELECT job FROM privacy_requests WHERE user_id=$1 AND chat_id=$2 AND state='done'",
                    owner,
                    request["chat_id"],
                ):
                    for message_id in previous["job"].get("general_message_ids", []):
                        deadline = (
                            previous["job"]
                            .get("general_message_deadlines", {})
                            .get(str(message_id), 0)
                        )
                        if deadline > now_seconds:
                            general_ids.add(message_id)
                            general_deadlines[str(message_id)] = deadline
            request_tokens = [str(request["id"]), request["id"].hex]
            for candidate in candidates:
                payload = candidate["payload"]
                message = telegram_message(payload)
                thread = message.get("message_thread_id") or 0
                targeted = (
                    full
                    or candidate["id"] in event_ids
                    or any(token in json.dumps(payload) for token in request_tokens)
                    or (
                        message
                        and (message.get("chat") or {}).get("id") == request["chat_id"]
                        and (
                            thread == request["target_thread_id"]
                            or thread in {0, 1}
                            and request["target_thread_id"] in {0, 1}
                        )
                    )
                    or payload.get("conversation_id") in {str(x) for x in conversation_ids}
                )
                if not targeted or candidate["id"] == uid(event_id):
                    continue
                event_ids.add(candidate["id"])
                for item in telegram_event_messages(payload):
                    if (item.get("chat") or {}).get("id") != request["chat_id"]:
                        continue
                    item_thread = item.get("message_thread_id") or 0
                    if item_thread > 1 and full:
                        topic_ids.add(item_thread)
                    message_date = item.get("date")
                    if (
                        item_thread in {0, 1}
                        and isinstance(item.get("message_id"), int)
                        and isinstance(message_date, (int, float))
                        and message_date + deletion_window > now_seconds
                    ):
                        general_ids.add(item["message_id"])
                        general_deadlines[str(item["message_id"])] = message_date + deletion_window
            request_tokens = [str(request["id"]), request["id"].hex]
            linked = [str(value) for value in event_ids | set(run_ids)] + request_tokens
            deliveries = await conn.fetch("SELECT * FROM outbox WHERE user_id=$1 FOR UPDATE", owner)
            delivery_ids = []
            for delivery in deliveries:
                targeted = (
                    full
                    or (
                        delivery["chat_id"] == request["chat_id"]
                        and (
                            delivery["thread_id"] == request["target_thread_id"]
                            or delivery["thread_id"] in {0, 1}
                            and request["target_thread_id"] in {0, 1}
                        )
                    )
                    or any(
                        key in delivery["dedupe_key"] or key in json.dumps(delivery["payload"])
                        for key in linked
                    )
                )
                if not targeted:
                    continue
                delivery_ids.append(delivery["id"])
                if delivery["chat_id"] == request["chat_id"]:
                    if delivery["thread_id"] > 1 and full:
                        topic_ids.add(delivery["thread_id"])
                    delivery_date = delivery["sent_at"] or delivery["created_at"]
                    deadline = delivery_date.timestamp() + deletion_window
                    if delivery["thread_id"] in {0, 1} and deadline > now_seconds:
                        for message_id in delivery["telegram_ids"] or []:
                            if isinstance(message_id, int):
                                general_ids.add(message_id)
                                general_deadlines[str(message_id)] = deadline
            await conn.execute("DELETE FROM outbox WHERE id=ANY($1::bigint[])", delivery_ids)
            operations = await conn.fetch(
                "SELECT id,run_id FROM operations WHERE user_id=$1", owner
            )
            operation_ids = [
                row["id"]
                for row in operations
                if full or row["run_id"] in run_ids or any(key in row["id"] for key in linked)
            ]
            await conn.execute(
                "DELETE FROM operations WHERE user_id=$1 AND id=ANY($2::text[])",
                owner,
                operation_ids,
            )
            for table in ("checkpoint_writes", "checkpoint_blobs", "checkpoints"):
                if await conn.fetchval("SELECT to_regclass($1)", f"langgraph.{table}"):
                    await conn.execute(
                        f"DELETE FROM langgraph.{table} WHERE thread_id=ANY($1::text[])",
                        [str(value) for value in run_ids],
                    )
            await conn.execute(
                "DELETE FROM run_metrics WHERE user_id=$1 AND ($2 OR run_id=ANY($3::uuid[]))",
                owner,
                full,
                run_ids,
            )
            await conn.execute(
                "DELETE FROM runs WHERE user_id=$1 AND id=ANY($2::uuid[])", owner, run_ids
            )
            await conn.execute(
                "DELETE FROM occurrences WHERE schedule_id=ANY($1::uuid[])", schedule_ids
            )
            await conn.execute(
                "DELETE FROM schedules WHERE user_id=$1 AND id=ANY($2::uuid[])", owner, schedule_ids
            )
            await conn.execute(
                "DELETE FROM messages WHERE user_id=$1 AND conversation_id=ANY($2::uuid[])",
                owner,
                conversation_ids,
            )
            await conn.execute(
                "DELETE FROM conversations WHERE user_id=$1 AND id=ANY($2::uuid[])",
                owner,
                conversation_ids,
            )
            event_ids.discard(uid(event_id))
            # Telegram update tombstones contain no personal payload and prevent replay.
            await conn.execute(
                """UPDATE events SET payload='{}',state='done',error=NULL,owner=NULL,
                               lease_until=NULL WHERE id=ANY($1::uuid[]) AND update_id IS NOT NULL""",
                list(event_ids),
            )
            await conn.execute(
                "DELETE FROM events WHERE id=ANY($1::uuid[]) AND update_id IS NULL", list(event_ids)
            )
            usage_rows = await conn.fetch(
                "SELECT operation_id,raw FROM usage WHERE user_id=$1 AND ($2 OR run_id=ANY($3::uuid[]))",
                owner,
                full,
                run_ids,
            )
            for usage in usage_rows:
                await conn.execute(
                    "UPDATE usage SET raw=$2,run_id=NULL WHERE operation_id=$1 AND user_id=$3",
                    usage["operation_id"],
                    sanitized_usage(usage["raw"]),
                    owner,
                )
            await conn.execute(
                "UPDATE ledger SET description='Account transaction' WHERE user_id=$1", owner
            )
            if full:
                await conn.execute("DELETE FROM projects WHERE user_id=$1", owner)
                await conn.execute("DELETE FROM recipes WHERE user_id=$1", owner)
                await conn.execute("DELETE FROM memory WHERE user_id=$1", owner)
                await conn.execute("DELETE FROM artifacts WHERE user_id=$1", owner)
                await conn.execute("DELETE FROM deleted_topics WHERE user_id=$1", owner)
                await conn.execute(
                    """UPDATE users SET preferences=$2,proactivity_default_applied=true,
                                   memory_revision=memory_revision+1,
                                   content_reset_at=clock_timestamp() WHERE user_id=$1""",
                    owner,
                    {
                        "timezone": self.settings.default_timezone,
                        "proactivity": True,
                        "tone": "доброжелательно, по делу",
                        "initiative_limit": 2,
                    },
                )
                await conn.execute(
                    "DELETE FROM privacy_requests WHERE user_id=$1 AND id<>$2", owner, request["id"]
                )
            else:
                await conn.execute(
                    """DELETE FROM privacy_requests WHERE user_id=$1 AND id<>$2
                                   AND (conversation_id=$3 OR run_id=ANY($4::uuid[]))""",
                    owner,
                    request["id"],
                    request["conversation_id"],
                    run_ids,
                )
            removed_threads = (
                ({0, 1} if request["target_thread_id"] in {0, 1} else {request["target_thread_id"]})
                if not full
                else set()
            )
            for thread in topic_ids | removed_threads:
                await conn.execute(
                    """INSERT INTO deleted_topics(user_id,chat_id,thread_id) VALUES($1,$2,$3)
                                   ON CONFLICT(user_id,chat_id,thread_id) DO UPDATE SET deleted_at=clock_timestamp()""",
                    owner,
                    request["chat_id"],
                    thread,
                )
            job = {
                "delete_user_files": full,
                "general_message_ids": sorted(general_ids),
                "general_message_deadlines": general_deadlines,
                "topic_ids": sorted(topic_ids),
                "general_history_limited": full or request["target_thread_id"] in {0, 1},
                "privacy_event_id": str(event_id),
                "progress": {},
            }
            request = await conn.fetchrow(
                """UPDATE privacy_requests SET state='erasing',job=$2,
                                          run_id=NULL,conversation_id=NULL,source_key=$3
                                          WHERE id=$1 RETURNING *""",
                request["id"],
                job,
                f"privacy-erasure:{request['id']}",
            )
            return self._privacy_descriptor(request)

    async def finish_privacy_erasure(self, request_id, result):
        async with self.connection() as conn:
            owner = await conn.fetchval(
                "SELECT user_id FROM privacy_requests WHERE id=$1", uid(request_id)
            )
            if owner is None:
                return False
            await conn.execute("SELECT set_config('app.user_id',$1,true)", str(owner))
            await conn.fetchval("SELECT user_id FROM users WHERE user_id=$1 FOR UPDATE", owner)
            request = await conn.fetchrow(
                "SELECT * FROM privacy_requests WHERE id=$1 FOR UPDATE", uid(request_id)
            )
            if request["state"] == "done":
                return True
            if request["state"] != "erasing":
                return False
            failed_files = bool(result.get("files_failed")) or (
                request["job"].get("delete_user_files") and not result.get("files_erased")
            )
            topics_failed = int(result.get("topics_failed") or 0)
            messages_failed = int(result.get("messages_failed") or 0)
            if request["scope"] == "all":
                text = (
                    "Данные Cronos очищены."
                    if not failed_files
                    else "История, память и настройки Cronos очищены. Не удалось удалить файлы. Для повторной попытки используй /clearall."
                )
            else:
                text = "Чат удалён из Cronos. Общая память и библиотека файлов сохранены."
            text += " Тариф, баланс и учтённые расходы сохранены."
            if topics_failed or messages_failed:
                text += f" В Telegram не удалось удалить тем: {topics_failed}; сообщений: {messages_failed}."
            if result.get("general_history_limited") or request["job"].get(
                "general_history_limited"
            ):
                text += " Telegram может не разрешить боту удалить сообщения общего чата старше 48 часов; их можно очистить вручную."
            if request["scope"] == "all":
                await conn.execute("DELETE FROM outbox WHERE user_id=$1", owner)
                await conn.execute("DELETE FROM conversations WHERE user_id=$1", owner)
                await conn.execute(
                    """UPDATE events SET payload='{}',state='done',owner=NULL,error=NULL,lease_until=NULL
                    WHERE kind='telegram' AND (payload#>>'{message,chat,id}'=$1
                    OR payload#>>'{edited_message,chat,id}'=$1 OR payload#>>'{callback_query,from,id}'=$1)""",
                    str(owner),
                )
            await conn.execute(
                """INSERT INTO outbox(user_id,chat_id,thread_id,payload,dedupe_key)
                               VALUES($1,$2,0,$3,$4) ON CONFLICT(dedupe_key) DO NOTHING""",
                owner,
                request["chat_id"],
                {"text": text},
                f"privacy-done:{request['id']}",
            )
            await conn.execute(
                """UPDATE events SET state='done',payload='{}',owner=NULL,error=NULL,lease_until=NULL
                               WHERE id=$1""",
                privacy_event_id(request["id"]),
            )
            remaining = {}
            if messages_failed and request["job"].get("general_message_ids"):
                remaining["general_message_ids"] = request["job"]["general_message_ids"]
                remaining["general_message_deadlines"] = request["job"].get(
                    "general_message_deadlines", {}
                )
            await conn.execute(
                """UPDATE privacy_requests SET state='done',job=$2,run_id=NULL,
                               conversation_id=NULL,target_thread_id=0,origin_thread_id=0 WHERE id=$1""",
                request["id"],
                remaining,
            )
            if request["scope"] == "all":
                await conn.execute(
                    "UPDATE users SET content_reset_at=clock_timestamp() WHERE user_id=$1", owner
                )
            return True

    async def ensure_user(self, user_id: int):
        async with self.connection(user_id) as conn:
            await conn.execute(
                """INSERT INTO users(user_id,preferences,proactivity_default_applied)
                VALUES($1,$2,true) ON CONFLICT DO NOTHING""",
                user_id,
                {
                    "timezone": self.settings.default_timezone,
                    "proactivity": True,
                    "tone": "доброжелательно, по делу",
                    "initiative_limit": 2,
                },
            )
            row = await conn.fetchrow("SELECT * FROM users WHERE user_id=$1 FOR UPDATE", user_id)
            if not row["proactivity_default_applied"]:
                await conn.execute(APPLY_DEFAULT_SQL, user_id)
                row = await conn.fetchrow("SELECT * FROM users WHERE user_id=$1", user_id)
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
        if values:
            for name in ("proactivity", "home_suggestions"):
                if name in values and not isinstance(values[name], bool):
                    raise ValueError(f"{name} must be a boolean")
        await self.ensure_user(user_id)
        async with AsyncExitStack() as stack:
            if values:
                await stack.enter_async_context(self.user_lock(user_id, purpose="delivery"))
            conn = await stack.enter_async_context(self.connection(user_id))
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

    async def set_model_preferences(self, user_id: int, values: dict, source_key: str) -> dict:
        """Apply a dialogue control once; a late replay never restores an older choice."""
        if not isinstance(values, dict) or not values or set(values) - {"model", "reasoning"}:
            raise ValueError("Неизвестная настройка диалога")
        if not isinstance(source_key, str) or not source_key.strip():
            raise ValueError("Не указан источник изменения настройки")
        result = dict(values)
        if "model" in result:
            result["model"] = normalize_model_choice(result["model"])
        if "reasoning" in result and not isinstance(result["reasoning"], bool):
            raise ValueError("Режим рассуждения должен быть включён или выключен")
        await self.ensure_user(user_id)
        async with self.user_lock(user_id, purpose="delivery"), self.connection(user_id) as conn:
            await conn.fetchval("SELECT user_id FROM users WHERE user_id=$1 FOR UPDATE", user_id)
            existing = await conn.fetchrow("SELECT * FROM operations WHERE id=$1", source_key)
            if existing:
                if (
                    existing["user_id"] != user_id
                    or existing["kind"] != "model_preference"
                    or not isinstance(existing["result"], dict)
                    or not existing["result"]
                    or set(existing["result"]) - {"model", "reasoning"}
                ):
                    raise ValueError("Операция настройки недоступна")
                return existing["result"]
            if await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM privacy_requests WHERE user_id=$1 AND state='erasing')",
                user_id,
            ):
                raise ValueError("Сначала дождись завершения очистки данных")
            await conn.execute(
                "UPDATE users SET preferences=preferences||$2::jsonb WHERE user_id=$1",
                user_id,
                result,
            )
            await conn.execute(
                """INSERT INTO operations(id,user_id,kind,result,status)
                VALUES($1,$2,'model_preference',$3,'done')""",
                source_key,
                user_id,
                result,
            )
            return result

    async def set_proactivity(self, user_id: int, enabled: bool, source_key: str) -> dict:
        """Apply one callback once; return its original minimal preference change on replay."""
        if not isinstance(enabled, bool):
            raise ValueError("Инициативность должна быть включена или выключена")
        await self.ensure_user(user_id)
        async with self.user_lock(user_id, purpose="delivery"), self.connection(user_id) as conn:
            await conn.fetchval("SELECT user_id FROM users WHERE user_id=$1 FOR UPDATE", user_id)
            existing = await conn.fetchrow("SELECT * FROM operations WHERE id=$1", source_key)
            if existing:
                if (
                    existing["user_id"] != user_id
                    or existing["kind"] != "home_preference"
                    or not isinstance(existing["result"], dict)
                    or not isinstance(existing["result"].get("proactivity"), bool)
                ):
                    raise ValueError("Операция настройки недоступна")
                return existing["result"]
            result = {"proactivity": enabled}
            await conn.execute(
                "UPDATE users SET preferences=preferences||$2::jsonb WHERE user_id=$1",
                user_id,
                result,
            )
            if not enabled:
                await conn.execute(
                    """UPDATE schedules SET state='cancelled',revision=revision+1
                    WHERE user_id=$1 AND proactive AND state<>'cancelled'""",
                    user_id,
                )
            await conn.execute(
                """INSERT INTO operations(id,user_id,kind,result,status)
                VALUES($1,$2,'home_preference',$3,'done')""",
                source_key,
                user_id,
                result,
            )
            return result

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

    async def activate_home_bootstrap(self) -> int:
        """Only a home-aware worker activates migration jobs after replacing its predecessor."""
        async with self.connection() as conn:
            return await conn.fetchval(
                """WITH activated AS (
                    UPDATE events SET state='pending',available_at=now(),notified_at=NULL
                    WHERE kind='home_init' AND state='awaiting_home_worker' RETURNING id
                ) SELECT count(*) FROM activated"""
            )

    async def queue_home(self, user_id: int) -> bool:
        """Queue home only for a live interaction; caller already ensured the account."""
        async with self.connection(user_id) as conn:
            return bool(
                await conn.fetchval(
                    """INSERT INTO events(id,kind,payload)
                SELECT md5('cronos:home-init:' || u.user_id::text || ':' || u.home_generation::text)::uuid,
                       'home_init',jsonb_build_object('user_id',u.user_id,'generation',u.home_generation::text)
                FROM users u WHERE u.user_id=$1
                  AND NOT EXISTS(SELECT 1 FROM conversations c WHERE c.user_id=u.user_id AND c.is_home)
                  AND NOT EXISTS(SELECT 1 FROM privacy_requests p WHERE p.user_id=u.user_id AND p.state='erasing')
                ON CONFLICT(id) DO NOTHING RETURNING id""",
                    user_id,
                )
            )

    async def get_home(self, user_id: int) -> dict | None:
        async with self.connection(user_id) as conn:
            row = await conn.fetchrow(
                "SELECT * FROM conversations WHERE user_id=$1 AND is_home", user_id
            )
            return dict(row) if row else None

    async def recover_missing_home_delivery(
        self,
        delivery_id: int,
        owner: str,
        *,
        telegram_ids: list[int] | None = None,
        remaining_payload: dict | None = None,
    ) -> bool:
        """Queue recovery after exact TOPIC_NOT_FOUND, under the delivery lock.

        The worker owns remote creation under its work lock. This transaction
        only retires the still-current missing home and records one durable job;
        it must never acquire the work lock in the reverse order.
        """
        async with self.connection() as conn:
            user_id = await conn.fetchval(
                "SELECT user_id FROM outbox WHERE id=$1 AND owner=$2 AND state='sending'",
                delivery_id,
                owner,
            )
            if user_id is None:
                return False
            await conn.execute("SELECT set_config('app.user_id',$1,true)", str(user_id))
            # Match the ingress/privacy lock order before locking the outbox.
            if not await conn.fetchval(
                "SELECT user_id FROM users WHERE user_id=$1 FOR UPDATE", user_id
            ):
                return False
            delivery = await conn.fetchrow(
                "SELECT * FROM outbox WHERE id=$1 AND owner=$2 AND state='sending' FOR UPDATE",
                delivery_id,
                owner,
            )
            if not delivery or delivery["thread_id"] <= 1 or delivery["chat_id"] != user_id:
                return False
            if await conn.fetchval(
                "SELECT 1 FROM privacy_requests WHERE user_id=$1 AND state='erasing'", user_id
            ):
                return False
            home = await conn.fetchrow(
                """SELECT id FROM conversations WHERE user_id=$1 AND chat_id=$2
                AND thread_id=$3 AND is_home FOR UPDATE""",
                user_id,
                delivery["chat_id"],
                delivery["thread_id"],
            )
            if home is None:
                return False
            await conn.execute(
                """UPDATE outbox SET state='failed',lease_until=NULL,error='Home topic not found',
                telegram_ids=COALESCE($3,telegram_ids),payload=COALESCE($4,payload)
                WHERE id=$1 AND owner=$2""",
                delivery_id,
                owner,
                telegram_ids,
                remaining_payload,
            )
            # Panels prepared before the reset must not overwrite the new home.
            await conn.execute(
                """UPDATE outbox SET state='cancelled',owner=NULL,lease_until=NULL,
                error='Home topic replaced' WHERE user_id=$1 AND chat_id=$2 AND thread_id=$3
                AND state IN ('pending','sending')
                AND (dedupe_key LIKE 'home-panel:%' OR dedupe_key LIKE 'home-welcome:%')""",
                user_id,
                delivery["chat_id"],
                delivery["thread_id"],
            )
            await conn.execute(
                "UPDATE conversations SET is_home=false,revision=revision+1 WHERE id=$1",
                home["id"],
            )
            generation = await conn.fetchval(
                "UPDATE users SET home_generation=gen_random_uuid() WHERE user_id=$1 RETURNING home_generation",
                user_id,
            )
            await conn.execute(
                """INSERT INTO events(id,kind,payload)
                VALUES(md5('cronos:home-init:' || $1::bigint::text || ':' || $2::text)::uuid,
                       'home_init',jsonb_build_object('user_id',$1::bigint,'generation',$2::text))
                ON CONFLICT(id) DO NOTHING""",
                user_id,
                str(generation),
            )
            return True

    async def home_creation_key(self, user_id: int) -> str:
        async with self.connection(user_id) as conn:
            generation = await conn.fetchval(
                "SELECT home_generation FROM users WHERE user_id=$1", user_id
            )
        if generation is None:
            generation = (await self.ensure_user(user_id))["home_generation"]
        return f"home-create:{user_id}:{generation}"

    async def set_home(self, user_id: int, conversation_id) -> dict:
        """Promote one owned conversation; caller holds the user's work lock."""
        async with self.connection(user_id) as conn:
            await conn.fetchval("SELECT user_id FROM users WHERE user_id=$1 FOR UPDATE", user_id)
            conversation = await conn.fetchrow(
                "SELECT * FROM conversations WHERE user_id=$1 AND id=$2 FOR UPDATE",
                user_id,
                uid(conversation_id),
            )
            if not conversation:
                raise ValueError("Чат не найден")
            existing = await conn.fetchval(
                "SELECT id FROM conversations WHERE user_id=$1 AND is_home", user_id
            )
            if existing and existing != conversation["id"]:
                raise ValueError("Домашняя тема уже существует")
            row = await conn.fetchrow(
                """UPDATE conversations SET is_home=true,title=$3,title_auto=false,
                title_message_count=0,revision=revision+CASE
                WHEN NOT is_home OR title_auto OR title IS DISTINCT FROM $3 THEN 1 ELSE 0 END
                WHERE user_id=$1 AND id=$2 RETURNING *""",
                user_id,
                conversation["id"],
                HOME_TITLE,
            )
            return dict(row)

    async def reset_home(
        self, user_id: int, conversation_id=None, *, source_key: str | None = None
    ) -> str:
        """Explicit recovery only: retain history and rotate the durable creation key."""
        async with self.connection(user_id) as conn:
            if not await conn.fetchval(
                "SELECT user_id FROM users WHERE user_id=$1 FOR UPDATE", user_id
            ):
                raise ValueError("Пользователь не найден")
            if source_key is not None:
                existing = await conn.fetchrow("SELECT * FROM operations WHERE id=$1", source_key)
                if existing:
                    if (
                        existing["user_id"] != user_id
                        or existing["kind"] != "home_reset"
                        or not isinstance(existing["result"], dict)
                        or not isinstance(existing["result"].get("key"), str)
                    ):
                        raise ValueError("Операция восстановления недоступна")
                    return existing["result"]["key"]
            if conversation_id is not None:
                demoted = await conn.fetchval(
                    """UPDATE conversations SET is_home=false,revision=revision+1
                    WHERE user_id=$1 AND id=$2 AND is_home RETURNING id""",
                    user_id,
                    uid(conversation_id),
                )
                if demoted is None:
                    raise ValueError("Домашняя тема не найдена")
            generation = await conn.fetchval(
                "UPDATE users SET home_generation=gen_random_uuid() WHERE user_id=$1 RETURNING home_generation",
                user_id,
            )
            key = f"home-create:{user_id}:{generation}"
            if source_key is not None:
                await conn.execute(
                    """INSERT INTO operations(id,user_id,kind,result,status)
                    VALUES($1,$2,'home_reset',$3,'done')""",
                    source_key,
                    user_id,
                    {"key": key},
                )
            return key

    async def list_conversations(self, user_id: int):
        async with self.connection(user_id) as conn:
            return [
                dict(x)
                for x in await conn.fetch(
                    "SELECT id,title,chat_id,thread_id,is_home FROM conversations WHERE user_id=$1 ORDER BY created_at DESC",
                    user_id,
                )
            ]

    async def _topic_title_state(self, conn, user_id: int, conversation_id):
        conversation = await conn.fetchrow(
            "SELECT * FROM conversations WHERE user_id=$1 AND id=$2 FOR UPDATE",
            user_id,
            uid(conversation_id),
        )
        if (
            not conversation
            or conversation["is_home"]
            or not conversation["title_auto"]
            or conversation["thread_id"] <= 0
        ):
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
                    WHERE user_id=$1 AND id=$2 AND title_auto AND NOT is_home AND thread_id>0 AND revision=$5
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
            if row["is_home"]:
                # Native manual renames are repaired by the caller using this canonical row.
                row = await conn.fetchrow(
                    """UPDATE conversations SET title=$3,title_auto=false,
                    title_update_id=CASE WHEN $4 AND $5::bigint>0 THEN $5 ELSE title_update_id END
                    WHERE user_id=$1 AND id=$2 RETURNING *""",
                    user_id,
                    row["id"],
                    HOME_TITLE,
                    manual,
                    update_id,
                )
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
        return await self.query_memories(user_id, all_scopes=True)

    async def remember(self, user_id: int, content: str, category="preference", source=""):
        return await self.write_memory(
            user_id, content, category, source, source_key=f"legacy-memory:{uuid4()}"
        )

    async def forget(
        self, user_id: int, query: str, current_run=None, *, source_key=None, run_fence=None
    ):
        if not isinstance(query, str) or not query.strip():
            raise ValueError("Укажи факт, который нужно забыть")
        if isinstance(current_run, dict):
            run_fence = current_run.get("fence", run_fence)
            current_run = current_run["id"]
        async with self.user_lock(user_id, purpose="delivery"), self.connection(user_id) as conn:
            await conn.fetchval("SELECT user_id FROM users WHERE user_id=$1 FOR UPDATE", user_id)
            if source_key:
                replay = await conn.fetchrow(
                    "SELECT result FROM operations WHERE user_id=$1 AND id=$2 AND kind='memory_forget' AND status='done'",
                    user_id,
                    source_key,
                )
                if replay:
                    return replay["result"]
            if current_run is not None and run_fence is not None:
                active = await conn.fetchval(
                    "SELECT id FROM runs WHERE user_id=$1 AND id=$2 AND fence=$3 AND status='running' AND NOT cancel_requested FOR SHARE",
                    user_id,
                    uid(current_run),
                    run_fence,
                )
                if not active:
                    raise ValueError("Запрос отменён или уже заменён новым запуском")
            matches = []
            needle = query.casefold()
            async for fact in conn.cursor(
                "SELECT id,content FROM memory WHERE user_id=$1", user_id
            ):
                if needle in fact["content"].casefold() or str(fact["id"]) == query:
                    matches.append(fact["id"])
            deleted = await conn.fetch(
                "DELETE FROM memory WHERE user_id=$1 AND id=ANY($2::uuid[]) RETURNING content",
                user_id,
                matches,
            )
            await invalidate_memory_context(conn, user_id)
            await invalidate_initiative_context(conn, user_id)
            # Rotating all active context prevents facts returning through a checkpoint or paraphrase.
            await conn.execute("UPDATE messages SET excluded=true WHERE user_id=$1", user_id)
            conversations = await conn.fetch(
                """UPDATE conversations SET revision=revision+1,title_message_count=0,
                title=CASE WHEN is_home THEN '🪐 Cronos' WHEN title_auto AND thread_id>0 THEN 'Новый чат' ELSE title END,
                title_auto=CASE WHEN is_home THEN false ELSE title_auto END
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
            result = {"forgotten": [x["content"] for x in deleted], "context_reset": True}
            if source_key:
                result = {"forgotten_count": len(deleted), "context_reset": True}
                await conn.execute(
                    "INSERT INTO operations(id,user_id,run_id,kind,result,status) VALUES($1,$2,$3,'memory_forget',$4,'done') ON CONFLICT(id) DO NOTHING",
                    source_key,
                    user_id,
                    uid(current_run) if current_run else None,
                    result,
                )
            return result

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
            await conn.execute(
                "UPDATE events SET notified_at=now() WHERE id=$1 AND state='pending' AND available_at<=now()",
                uid(event_id),
            )

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

    async def enqueue_home_panel(
        self,
        user_id: int,
        chat_id: int,
        thread_id: int | None,
        payload: dict,
        dedupe_key: str,
        order: int,
    ) -> int | None:
        """Serialize panel edits with delivery; old callbacks cannot replace a newer panel."""
        if not dedupe_key.startswith("home-panel:"):
            raise ValueError("Неизвестный ключ домашней панели")
        if not isinstance(order, int) or isinstance(order, bool):
            raise ValueError("Некорректный порядок панели")
        async with self.user_lock(user_id, purpose="delivery"), self.connection(user_id) as conn:
            existing = await conn.fetchrow(
                "SELECT id,user_id FROM outbox WHERE dedupe_key=$1", dedupe_key
            )
            if existing:
                if existing["user_id"] != user_id:
                    raise ValueError("Панель недоступна")
                return existing["id"]
            edit_id = payload.get("edit_message_id")
            if edit_id is not None:
                if not isinstance(edit_id, int) or isinstance(edit_id, bool) or edit_id <= 0:
                    raise ValueError("Некорректное сообщение панели")
                latest = await conn.fetchval(
                    """SELECT max((payload->>'home_panel_order')::bigint) FROM outbox
                    WHERE user_id=$1 AND chat_id=$2 AND dedupe_key LIKE 'home-panel:%'
                    AND payload->>'edit_message_id'=$3""",
                    user_id,
                    chat_id,
                    str(edit_id),
                )
                if latest is not None and latest > order:
                    return None
                await conn.execute(
                    """UPDATE outbox SET state='cancelled',owner=NULL,lease_until=NULL,
                    error='Superseded home panel' WHERE user_id=$1 AND chat_id=$2
                    AND dedupe_key LIKE 'home-panel:%' AND payload->>'edit_message_id'=$3
                    AND state IN ('pending','sending')""",
                    user_id,
                    chat_id,
                    str(edit_id),
                )
            return await conn.fetchval(
                """INSERT INTO outbox(user_id,chat_id,thread_id,payload,dedupe_key)
                VALUES($1,$2,$3,$4,$5) RETURNING id""",
                user_id,
                chat_id,
                thread_id or 0,
                {**payload, "home_panel_order": order},
                dedupe_key,
            )

    async def _active_timer_schedule(self, conn, event_payload, user_id, conversation_id):
        if not isinstance(event_payload, dict):
            return None
        revision = event_payload.get("revision")
        if not isinstance(revision, int) or isinstance(revision, bool):
            return None
        try:
            schedule_id = uid(event_payload.get("schedule_id"))
        except TypeError, ValueError:
            return None
        return await conn.fetchrow(
            """SELECT id,revision,chat_id,thread_id,initiative_id FROM schedules
            WHERE id=$1 AND user_id=$2 AND revision=$3 AND state<>'cancelled'
            AND conversation_id=$4 FOR SHARE""",
            schedule_id,
            user_id,
            revision,
            conversation_id,
        )

    async def enqueue_for_run(self, run: dict, conversation: dict, payload: dict, dedupe: str):
        user_id = run["user_id"]
        async with self.connection(user_id) as conn:
            active = await conn.fetchrow(
                """SELECT r.id,r.conversation_id,e.kind AS event_kind,e.payload AS event_payload
                FROM runs r LEFT JOIN events e ON e.id=r.event_id
                WHERE r.id=$1 AND r.user_id=$2
                AND r.status='running' AND r.fence=$3 AND NOT r.cancel_requested
                FOR SHARE OF r""",
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
            if active["event_kind"] == "timer":
                if active["conversation_id"] != target["id"]:
                    return None
                schedule = await self._active_timer_schedule(
                    conn,
                    active["event_payload"],
                    user_id,
                    target["id"],
                )
                if (
                    not schedule
                    or schedule["chat_id"] != target["chat_id"]
                    or schedule["thread_id"] != target["thread_id"]
                ):
                    return None
                payload = {
                    **payload,
                    "schedule_id": str(schedule["id"]),
                    "schedule_revision": schedule["revision"],
                }
                if schedule["initiative_id"] and (
                    payload.get("initiative_id") != str(schedule["initiative_id"])
                    or not await validate_initiative_payload(conn, user_id, payload)
                ):
                    return None
            elif payload.get("initiative_id"):
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
            acknowledged = await conn.fetchrow(
                """UPDATE outbox SET state=$3,telegram_ids=COALESCE($4,telegram_ids),error=$5,
                sent_at=CASE WHEN $4::jsonb IS NOT NULL THEN now() ELSE sent_at END,
                next_attempt_at=now()+$6*interval '1 second',lease_until=NULL WHERE id=$1 AND owner=$2
                RETURNING user_id,payload""",
                delivery_id,
                owner,
                "sent" if ids is not None else ("failed" if permanent else "pending"),
                ids,
                error,
                retry_after,
            )
            if ids is not None and acknowledged and acknowledged["payload"].get("initiative_id"):
                await conn.execute(
                    "SELECT set_config('app.user_id',$1,true)", str(acknowledged["user_id"])
                )
                await mark_initiative_sent(conn, acknowledged["payload"])

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
            run = await conn.fetchrow(
                """SELECT r.user_id,r.conversation_id,e.kind AS event_kind,e.payload AS event_payload
                FROM runs r LEFT JOIN events e ON e.id=r.event_id
                WHERE r.id=$1 AND r.status='running' AND NOT r.cancel_requested AND r.fence=$2""",
                uid(run_id),
                fence,
            )
            if not run:
                return False
            if run["event_kind"] == "timer":
                return bool(
                    await self._active_timer_schedule(
                        conn, run["event_payload"], run["user_id"], run["conversation_id"]
                    )
                )
            return True

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
                uid(run_id) if run_id is not None else None,
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
        usage = sanitized_usage(usage)
        cost = usage.get("cost_rub")
        if cost is None:
            # Keep the reservation for reconciliation instead of claiming an unknown call was free.
            async with self.connection(user_id) as conn:
                await conn.fetchval(
                    "SELECT user_id FROM users WHERE user_id=$1 FOR UPDATE", user_id
                )
                if run_id and not await conn.fetchval(
                    "SELECT 1 FROM runs WHERE id=$1 AND user_id=$2", uid(run_id), user_id
                ):
                    run_id = None
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
            if run_id and not await conn.fetchval(
                "SELECT 1 FROM runs WHERE id=$1 AND user_id=$2", uid(run_id), user_id
            ):
                run_id = None
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
                "dynamic": row["dynamic"],
                "proactive": row["proactive"],
                "interval_seconds": row["interval_seconds"],
                "timezone": row["timezone"],
            }

    async def list_schedules(self, user_id):
        async with self.connection() as conn:
            rows = await conn.fetch(
                "SELECT id,due_at,fixed_text,instruction,dynamic,proactive,interval_seconds,timezone,state,initiative_id FROM schedules WHERE user_id=$1 AND state='active' ORDER BY due_at",
                user_id,
            )
            return [
                {**dict(x), "id": str(x["id"]), "due_at": x["due_at"].isoformat()} for x in rows
            ]

    async def change_schedule(self, user_id, schedule_id, action, due_at=None):
        async with self.user_lock(user_id, purpose="delivery"), self.connection() as conn:
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
            await conn.execute(Path(__file__).with_name("projects.sql").read_text())
            await conn.execute(Path(__file__).with_name("memory_privacy.sql").read_text())
            await conn.execute(Path(__file__).with_name("memory.sql").read_text())
            await conn.execute(Path(__file__).with_name("library.sql").read_text())
            await conn.execute(Path(__file__).with_name("versions.sql").read_text())
            await conn.execute(Path(__file__).with_name("workflows.sql").read_text())
            await conn.execute(Path(__file__).with_name("recipes.sql").read_text())
            await conn.execute(Path(__file__).with_name("initiative.sql").read_text())
            await conn.execute(APPLY_DEFAULT_SQL, None)
    finally:
        await conn.close()


if __name__ == "__main__":
    import asyncio

    from cronos.settings import get_settings

    asyncio.run(migrate(get_settings()))
