"""Opt-in real-model, real-PostgreSQL evaluation; never send Telegram messages.

Requires an already migrated, local test database and provider settings in the
environment. Output contains checks and accounting only, never conversations.
"""

import argparse
import asyncio
import json
import logging
import os
import re
from decimal import Decimal
from tempfile import TemporaryDirectory
from time import monotonic
from typing import Any
from urllib.parse import urlsplit
from uuid import uuid4

from cronos.agent import Agent
from cronos.artifacts import ArtifactManager
from cronos.providers import Provider, ProviderError
from cronos.settings import Settings
from cronos.storage import Store

USER = -990071
PROMPTS = (
    "Сохрани отдельный проект подготовки к экзамену по математике и свяжи с этим чатом. "
    "Цель — подготовиться к экзамену. Ограничение: могу заниматься не более двух раз в неделю. "
    "Пока не планируй расписание, не ищи материалы и не создавай напоминания.",
    "Обнови наш проект подготовки к экзамену: мы решили сначала решать практические задачи, "
    "а теорию разбирать по возникающим вопросам. Следующий шаг — решить пять задач "
    "по квадратным уравнениям. Прежнее ограничение по занятиям сохрани. "
    "Ничего не отправляй по расписанию.",
    "Где мы остановились в подготовке к экзамену? Коротко назови ограничение, принятое "
    "решение и следующий шаг. Сейчас только напомни состояние проекта, ничего не меняй.",
)


class SmokeRefused(ValueError):
    pass


def validate_environment(user_id: int, environment=None) -> str:
    environment = os.environ if environment is None else environment
    if environment.get("CRONOS_PERSONAL_SMOKE") != "1":
        raise SmokeRefused("opt_in_required")
    if isinstance(user_id, bool) or not isinstance(user_id, int) or user_id >= 0:
        raise SmokeRefused("negative_synthetic_user_required")
    try:
        url = urlsplit(environment.get("DATABASE_URL", ""))
        database = url.path.removeprefix("/")
        allowed = (
            url.scheme in {"postgres", "postgresql"}
            and url.hostname in {"localhost", "127.0.0.1", "::1"}
            and (database.endswith("_test") or database.startswith("test_"))
            and "/" not in database
            and not url.query
            and not url.fragment
        )
    except ValueError:
        allowed = False
    if not allowed:
        raise SmokeRefused("local_test_database_required")
    return database


def two_sessions(text: str) -> bool:
    text = text.casefold()
    return (
        bool(re.search(r"\b(?:2|дв[ауеы]\w*|дважды)\b", text))
        and "недел" in text
        and any(stem in text for stem in ("занят", "раз", "урок", "сесси"))
    )


def practice_first(text: str) -> bool:
    text = text.casefold()
    return any(stem in text for stem in ("задач", "практик")) and any(
        stem in text for stem in ("сначала", "начн", "начать", "перв", "приоритет")
    )


def next_step_present(text: str) -> bool:
    text = text.casefold()
    return bool(re.search(r"\b(?:5|пят\w*)\b", text)) and "квадрат" in text and "уравнен" in text


class SmokeStore(Store):
    def __init__(self, settings, user_id):
        super().__init__(settings)
        self.user_id = user_id
        self.deliveries = []

    async def enqueue_for_run(self, run, conversation, payload, dedupe):
        if run["user_id"] != self.user_id or self.user_id >= 0:
            raise SmokeRefused("unexpected_delivery_owner")
        self.deliveries.append({"payload_keys": sorted(payload), "dedupe": dedupe})
        return -1

    async def enqueue(self, user_id, chat_id, thread_id, payload, dedupe_key):
        if user_id != self.user_id or self.user_id >= 0:
            raise SmokeRefused("unexpected_delivery_owner")
        self.deliveries.append({"payload_keys": sorted(payload), "dedupe": dedupe_key})
        return -1


class SmokeArtifacts(ArtifactManager):
    def __init__(self, root, user_id):
        super().__init__(root)
        self.user_id = user_id

    def _user_dir(self, user_id):
        if user_id != self.user_id or user_id >= 0:
            raise SmokeRefused("unexpected_artifact_owner")
        return super()._user_dir(-user_id)


class MeteredProvider(Provider):
    calls = 0
    http_attempts = 0

    async def _completion(self, payload, models):
        self.calls += 1
        try:
            result = await super()._completion(payload, models)
        except ProviderError as error:
            self.http_attempts += len(error.attempts)
            raise
        self.http_attempts += len(result.get("attempts", []))
        return result


async def cleanup(store, user_id, event_ids):
    """Only called after this process atomically created the synthetic owner."""
    if user_id >= 0:
        raise SmokeRefused("positive_cleanup_refused")
    async with store.connection(user_id) as conn:
        runs = await conn.fetch("SELECT id,event_id FROM runs WHERE user_id=$1", user_id)
        thread_ids = [str(row["id"]) for row in runs]
        for table in ("checkpoint_writes", "checkpoint_blobs", "checkpoints"):
            await conn.execute(
                f"DELETE FROM langgraph.{table} WHERE thread_id=ANY($1::text[])", thread_ids
            )
        await conn.execute(
            "DELETE FROM occurrences WHERE schedule_id IN (SELECT id FROM schedules WHERE user_id=$1)",
            user_id,
        )
        for table in (
            "project_changes",
            "project_artifacts",
            "project_conversations",
            "projects",
            "outbox",
            "operations",
            "run_metrics",
            "reservations",
            "usage",
            "ledger",
            "privacy_requests",
            "deleted_topics",
            "artifacts",
            "memory",
            "messages",
            "schedules",
            "runs",
            "conversations",
            "users",
        ):
            await conn.execute(f"DELETE FROM {table} WHERE user_id=$1", user_id)
        await conn.execute("DELETE FROM events WHERE id=ANY($1::uuid[])", event_ids)


async def project_snapshot(store, user_id, conversation_id):
    async with store.connection(user_id) as conn:
        projects = await conn.fetch("SELECT * FROM projects WHERE user_id=$1", user_id)
        linked = await conn.fetchval(
            "SELECT project_id FROM project_conversations WHERE user_id=$1 AND conversation_id=$2",
            user_id,
            conversation_id,
        )
        changes = await conn.fetch(
            "SELECT project_id,kind,source_key,conversation_id,run_id,patch FROM project_changes WHERE user_id=$1 ORDER BY created_at,id",
            user_id,
        )
    return projects, linked, changes


async def evaluate(user_id: int) -> dict:
    report: dict[str, Any] = {
        "scenario": "projects_multiturn",
        "success": False,
        "calls": 0,
        "http_attempts": 0,
        "cost_rub": "0",
        "unknown_cost_operations": 0,
        "failed_checks": [],
        "steps": [],
        "cleanup_complete": False,
    }
    started, store, provider, created = monotonic(), None, None, False
    event_ids = []

    def check(name, passed):
        if not passed and name not in report["failed_checks"]:
            report["failed_checks"].append(name)
        return bool(passed)

    try:
        database = validate_environment(user_id)
        with TemporaryDirectory(prefix="cronos-personal-smoke-") as scratch:
            settings = Settings().model_copy(
                update={
                    "artifacts_dir": scratch,
                    "max_output_tokens": 1024,
                    "max_model_steps": 6,
                }
            )
            settings = settings.model_copy(
                update={"max_run_cost_rub": min(settings.max_run_cost_rub, 3)}
            )
            if not settings.alltokens_api_key.get_secret_value():
                raise SmokeRefused("provider_key_required")
            store = SmokeStore(settings, user_id)
            await store.open()
            async with store.connection(user_id) as conn:
                if await conn.fetchval("SELECT current_database()") != database:
                    raise SmokeRefused("database_identity_mismatch")
                if not await conn.fetchval(
                    "SELECT to_regclass('public.projects') IS NOT NULL AND to_regclass('langgraph.checkpoints') IS NOT NULL"
                ):
                    raise SmokeRefused("migrated_test_database_required")
                claimed = await conn.fetchval(
                    "INSERT INTO users(user_id) VALUES($1) ON CONFLICT DO NOTHING RETURNING user_id",
                    user_id,
                )
                if claimed is None:
                    raise SmokeRefused("synthetic_user_already_exists")
            created = True
            provider = MeteredProvider(settings)
            agent = Agent(settings, store, provider, None)
            agent.artifacts = SmokeArtifacts(scratch, user_id)
            conversation = await store.conversation(user_id, user_id, 0, "Synthetic project QA")
            first_project_id, first_revision, second_revision = None, None, None
            for index, prompt in enumerate(PROMPTS, 1):
                phase = f"turn{index}"
                calls_before = provider.calls
                event_id = uuid4()
                event_ids.append(event_id)
                async with store.connection() as conn:
                    await conn.execute(
                        "INSERT INTO events(id,kind,payload,state) VALUES($1,'personal_smoke',$2,'processing')",
                        event_id,
                        {"user_id": user_id, "turn": index},
                    )
                async with store.user_lock(user_id):
                    run = await store.start_run(event_id, user_id, conversation["id"])
                    try:
                        async with asyncio.timeout(180):
                            result = await agent.run(run, conversation, prompt)
                        answer = result.get("text", "") if isinstance(result, dict) else result
                        await store.add_message(
                            user_id, conversation["id"], "user", prompt, f"user:{event_id}"
                        )
                        await store.add_message(
                            user_id,
                            conversation["id"],
                            "assistant",
                            answer,
                            f"assistant:{event_id}",
                        )
                        await store.finish_run(run["id"], fence=run["fence"])
                    except BaseException:
                        await store.finish_run(run["id"], status="failed", fence=run["fence"])
                        raise
                async with store.connection() as conn:
                    await conn.execute("UPDATE events SET state='done' WHERE id=$1", event_id)
                projects, linked, changes = await project_snapshot(
                    store, user_id, conversation["id"]
                )
                checks = {"one_project": check(f"{phase}.one_project", len(projects) == 1)}
                if len(projects) == 1:
                    project = projects[0]
                    state = project["state"]
                    checks["conversation_link"] = check(
                        f"{phase}.conversation_link", linked == project["id"]
                    )
                    checks["constraint"] = check(
                        f"{phase}.two_weekly_sessions",
                        two_sessions(" ".join(state.get("constraints", []))),
                    )
                    if index == 1:
                        first_project_id, first_revision = project["id"], project["revision"]
                        subject = (project["name"] + " " + project["goal"]).casefold()
                        checks["goal"] = check(
                            f"{phase}.exam_goal", "экзам" in subject and "математ" in subject
                        )
                    else:
                        checks["same_project"] = check(
                            f"{phase}.same_project", project["id"] == first_project_id
                        )
                        checks["decision"] = check(
                            f"{phase}.practice_first",
                            practice_first(" ".join(state.get("decisions", []))),
                        )
                        checks["next_step"] = check(
                            f"{phase}.five_quadratic_problems",
                            next_step_present(state.get("next_step", "")),
                        )
                    if index == 2:
                        second_revision = project["revision"]
                        checks["revision_changed"] = check(
                            f"{phase}.revision_changed",
                            first_revision is not None and second_revision > first_revision,
                        )
                    if index in (1, 2):
                        kind = "create" if index == 1 else "update"
                        checks["source"] = check(
                            f"{phase}.persisted_source",
                            any(
                                change["kind"] == kind
                                and change["run_id"] == run["id"]
                                and change["conversation_id"] == conversation["id"]
                                and change["source_key"]
                                and change["patch"]
                                for change in changes
                            ),
                        )
                    else:
                        checks["read_only"] = check(
                            f"{phase}.no_unrequested_update", project["revision"] == second_revision
                        )
                check(f"{phase}.nonempty_answer", isinstance(answer, str) and bool(answer.strip()))
                if index == 3:
                    checks["answer_recap"] = check(
                        f"{phase}.answer_recap",
                        two_sessions(answer)
                        and practice_first(answer)
                        and next_step_present(answer),
                    )
                report["steps"].append(
                    {"turn": index, "calls": provider.calls - calls_before, "checks": checks}
                )
            async with store.connection(user_id) as conn:
                history_count = await conn.fetchval(
                    "SELECT count(*) FROM messages WHERE user_id=$1 AND conversation_id=$2",
                    user_id,
                    conversation["id"],
                )
                checkpoint_count = await conn.fetchval(
                    "SELECT count(*) FROM langgraph.checkpoints WHERE thread_id IN (SELECT id::text FROM public.runs WHERE user_id=$1)",
                    user_id,
                )
                schedules = await conn.fetchval(
                    "SELECT count(*) FROM schedules WHERE user_id=$1", user_id
                )
                outbox = await conn.fetchval(
                    "SELECT count(*) FROM outbox WHERE user_id=$1", user_id
                )
            check("three_turn_history", history_count == 6)
            check("real_pg_checkpoints", checkpoint_count > 0)
            check("no_schedules", schedules == 0)
            check("no_outbox_rows", outbox == 0)
            check("no_unrequested_deliveries", not store.deliveries)
    except SmokeRefused as error:
        check(str(error), False)
    except Exception as error:
        check("runtime_error", False)
        report["error_type"] = type(error).__name__
    finally:
        if provider:
            report["calls"], report["http_attempts"] = provider.calls, provider.http_attempts
        if created and store:
            try:
                async with store.connection(user_id) as conn:
                    usage = await conn.fetch(
                        "SELECT cost_micro,raw FROM usage WHERE user_id=$1", user_id
                    )
                    charged_micro = await conn.fetchval(
                        "SELECT COALESCE(sum(-amount_micro),0) FROM ledger WHERE user_id=$1 AND kind='usage'",
                        user_id,
                    )
                known = [
                    Decimal(str(row["raw"]["cost_rub"]))
                    for row in usage
                    if row["raw"].get("cost_rub") is not None
                ]
                report["cost_rub"] = str(sum(known, Decimal(0)))
                report["unknown_cost_operations"] = len(usage) - len(known)
                report["charged_cost_rub"] = str(Decimal(charged_micro) / 1_000_000)
                report["usage_operations"] = len(usage)
                check("cost_known", report["unknown_cost_operations"] == 0)
            except Exception:
                check("usage_read_failed", False)
            try:
                await cleanup(store, user_id, event_ids)
                report["cleanup_complete"] = True
            except Exception:
                check("synthetic_cleanup_failed", False)
        if provider:
            try:
                await provider.close()
            except Exception:
                check("provider_close_failed", False)
        if store:
            try:
                await store.close()
            except Exception:
                check("store_close_failed", False)
    report["elapsed_seconds"] = round(monotonic() - started, 2)
    report["success"] = not report["failed_checks"]
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--user-id", type=int, default=USER)
    args = parser.parse_args()
    # Library diagnostics must not print provider request/response content.
    for name in ("httpx", "httpcore", "openai", "langgraph"):
        logging.getLogger(name).setLevel(logging.CRITICAL)
    report = asyncio.run(evaluate(args.user_id))
    print(json.dumps(report, ensure_ascii=False), flush=True)
    raise SystemExit(0 if report["success"] else 1)


if __name__ == "__main__":
    main()
