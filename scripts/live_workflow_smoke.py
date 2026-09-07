"""Opt-in, synthetic real-model workflow/recipe QA against a local test database.

Run through the authorized provider environment. No Telegram transport is used;
reports contain checks/accounting only and cleanup is limited to this run's owner.
"""

import argparse
import asyncio
import json
import logging
from decimal import Decimal
from tempfile import TemporaryDirectory
from time import monotonic
from typing import Any
from uuid import uuid4

if __package__:
    from .live_personal_smoke import (
        MeteredProvider,
        SmokeArtifacts,
        SmokeRefused,
        SmokeStore,
        cleanup,
        validate_environment,
    )
else:
    from live_personal_smoke import (
        MeteredProvider,
        SmokeArtifacts,
        SmokeRefused,
        SmokeStore,
        cleanup,
        validate_environment,
    )

from cronos.agent import Agent
from cronos.settings import Settings

USER = -990081
PROMPTS = (
    "Сохрани проект «Домашние тренировки» и свяжи с этим чатом. Цель — регулярно "
    "двигаться дома, два занятия в неделю по 20 минут, оборудование — коврик. "
    "Начни в проекте структурированный личный план тренировок: два занятия, "
    "первое называется «Разминка и ходьба», второе — «Мобилизация». "
    "Используй шаблон тренировок и сохрани сам план, а не только текст ответа. "
    "Напоминания, поиск и изображения не нужны.",
    "Первое занятие «Разминка и ходьба» я выполнил: 20 минут, усилие 7 из 10. "
    "Обратная связь: слишком утомительно. Запиши это как выполненное занятие, "
    "сохрани обратную связь и обнови план: следующее занятие «Мобилизация» "
    "сократи до 10 минут. Второе занятие я ещё не выполнял. "
    "Обнови следующий шаг проекта, не создавай напоминаний.",
    "Сохрани мой многоразовый сценарий «Цена товара CSV». У него два обязательных "
    "входа: product типа string (название товара), price типа number (цена). "
    "Единственный шаг — создать CSV-файл с колонками product и price и ровно "
    "одной строкой из этих входных значений. Требования: сохранить название "
    "без перевода и числовую цену без изменения. Сейчас только сохрани сценарий, "
    "не запускай и файл не создавай.",
    "Запусти сохранённый сценарий «Цена товара CSV»: product = Яблоко. "
    "Цену пока не указал; спроси её, не подставляй догадку.",
    "Цена 120. Продолжи тот же ожидающий сценарий: создай CSV и заверши "
    "его применение после успешного создания файла.",
)


class BoundedProvider(MeteredProvider):
    async def search(self, query):
        raise SmokeRefused("search_disabled_in_smoke")

    async def generate_image(self, *args, **kwargs):
        raise SmokeRefused("image_generation_disabled_in_smoke")


def csv_matches(extracted):
    tables = extracted.get("tables", [])
    if len(tables) != 1:
        return False
    table = tables[0]
    if table.get("columns") != ["product", "price"]:
        return False
    rows = table.get("rows", [])
    if len(rows) != 1 or len(rows[0]) != 2 or rows[0][0] != "Яблоко":
        return False
    try:
        return Decimal(str(rows[0][1])) == Decimal(120)
    except ValueError, ArithmeticError:
        return False


async def evaluate(user_id=USER):
    report: dict[str, Any] = {
        "scenario": "workflow_recipe_multiturn",
        "success": False,
        "checks": {},
        "steps": [],
        "failed_checks": [],
        "calls": 0,
        "cost_rub": "0",
        "cleanup_complete": False,
    }
    started, store, provider, created = monotonic(), None, None, False
    event_ids = []

    def check(name, passed):
        report["checks"][name] = bool(passed)
        if not passed and name not in report["failed_checks"]:
            report["failed_checks"].append(name)
        return bool(passed)

    try:
        database = validate_environment(user_id)
        if user_id != USER:
            raise SmokeRefused("dedicated_workflow_owner_required")
        with TemporaryDirectory(prefix="cronos-workflow-smoke-") as scratch:
            settings = Settings().model_copy(
                update={
                    "artifacts_dir": scratch,
                    "max_output_tokens": 1600,
                    "max_model_steps": 6,
                    "max_run_cost_rub": 3,
                }
            )
            if not settings.alltokens_api_key.get_secret_value():
                raise SmokeRefused("provider_key_required")
            store = SmokeStore(settings, user_id)
            await store.open()
            async with store.connection(user_id) as conn:
                if await conn.fetchval("SELECT current_database()") != database:
                    raise SmokeRefused("database_identity_mismatch")
                if not await conn.fetchval(
                    "SELECT to_regclass('public.workflows') IS NOT NULL "
                    "AND to_regclass('public.recipe_applications') IS NOT NULL "
                    "AND to_regclass('langgraph.checkpoints') IS NOT NULL"
                ):
                    raise SmokeRefused("migrated_test_database_required")
                claimed = await conn.fetchval(
                    "INSERT INTO users(user_id) VALUES($1) ON CONFLICT DO NOTHING RETURNING user_id",
                    user_id,
                )
                if claimed is None:
                    raise SmokeRefused("synthetic_user_already_exists")
            created = True
            provider = BoundedProvider(settings)
            agent = Agent(settings, store, provider, None)
            agent.artifacts = SmokeArtifacts(scratch, user_id)
            conversation = await store.conversation(user_id, user_id, 0, "Synthetic workflow QA")
            for index, prompt in enumerate(PROMPTS, 1):
                event_id = uuid4()
                event_ids.append(event_id)
                calls_before = provider.calls
                async with store.connection() as conn:
                    await conn.execute(
                        "INSERT INTO events(id,kind,payload,state) VALUES($1,'workflow_smoke',$2,'processing')",
                        event_id,
                        {"user_id": user_id, "turn": index},
                    )
                async with store.user_lock(user_id):
                    run = await store.start_run(event_id, user_id, conversation["id"])
                    try:
                        async with asyncio.timeout(240):
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
                async with store.connection(user_id) as conn:
                    await conn.execute("UPDATE events SET state='done' WHERE id=$1", event_id)
                    workflow = await conn.fetchrow(
                        "SELECT * FROM workflows WHERE user_id=$1", user_id
                    )
                    artifacts = await conn.fetch(
                        "SELECT * FROM artifacts WHERE user_id=$1", user_id
                    )
                    operations = await conn.fetch(
                        "SELECT kind,result FROM operations WHERE user_id=$1 AND run_id=$2",
                        user_id,
                        run["id"],
                    )
                    run_cost = await conn.fetchval(
                        "SELECT coalesce(sum(cost_micro),0) FROM usage WHERE user_id=$1 AND run_id=$2",
                        user_id,
                        run["id"],
                    )
                    if index == 1:
                        project = await store.get_project(
                            user_id, conversation_id=conversation["id"]
                        )
                        check(
                            "training_project_linked",
                            bool(
                                project
                                and workflow
                                and project["id"] == str(workflow["project_id"])
                            ),
                        )
                        check(
                            "typed_training_parameters",
                            bool(
                                workflow
                                and workflow["kind"] == "training"
                                and workflow["parameters"]["sessions_per_week"] == 2
                                and workflow["parameters"]["available_minutes"] == 20
                            ),
                        )
                        check(
                            "two_planned_sessions",
                            bool(workflow and len(workflow["plan"]["sessions"]) == 2),
                        )
                    if index == 2:
                        observations = await conn.fetch(
                            "SELECT observation,run_id FROM workflow_observations WHERE user_id=$1",
                            user_id,
                        )
                        plans = await conn.fetch(
                            "SELECT revision,run_id FROM workflow_plans WHERE user_id=$1 ORDER BY revision",
                            user_id,
                        )
                        check(
                            "actual_completion_and_feedback",
                            len(observations) == 1
                            and observations[0]["run_id"] == run["id"]
                            and observations[0]["observation"]["completed"] is True
                            and observations[0]["observation"]["minutes"] == 20
                            and observations[0]["observation"]["effort"] == 7
                            and bool(observations[0]["observation"].get("feedback")),
                        )
                        check(
                            "revised_plan_persisted",
                            len(plans) >= 2
                            and plans[-1]["run_id"] == run["id"]
                            and bool(
                                workflow
                                and any(
                                    session["minutes"] == 10
                                    for session in workflow["plan"]["sessions"]
                                )
                            ),
                        )
                    if index == 3:
                        definitions = await conn.fetch(
                            "SELECT definition FROM recipe_versions WHERE user_id=$1", user_id
                        )
                        definition = definitions[0]["definition"] if len(definitions) == 1 else {}
                        check(
                            "recipe_definition_saved",
                            bool(definition)
                            and {
                                item["name"]: item["type"] for item in definition.get("inputs", [])
                            }
                            == {"product": "string", "price": "number"}
                            and [step["tool"] for step in definition.get("steps", [])]
                            == ["file_create"],
                        )
                        check("save_has_no_file_effect", not artifacts)
                    if index == 4:
                        pending = await conn.fetch(
                            "SELECT missing_inputs FROM recipe_applications WHERE user_id=$1 AND status='awaiting_input'",
                            user_id,
                        )
                        check(
                            "missing_price_waits",
                            len(pending) == 1 and pending[0]["missing_inputs"] == ["price"],
                        )
                        check("waiting_has_no_file_effect", not artifacts and not store.deliveries)
                    if index == 5:
                        applications = await conn.fetch(
                            "SELECT id,inputs FROM recipe_applications WHERE user_id=$1 AND status='completed'",
                            user_id,
                        )
                        receipts = await conn.fetch(
                            "SELECT artifact_id FROM recipe_application_receipts WHERE user_id=$1 AND tool='file_create'",
                            user_id,
                        )
                        check(
                            "recipe_completed",
                            len(applications) == 1
                            and applications[0]["inputs"] == {"product": "Яблоко", "price": 120},
                        )
                        check(
                            "owned_file_completion_receipt",
                            len(artifacts) == 1
                            and len(receipts) == 1
                            and receipts[0]["artifact_id"] == artifacts[0]["id"],
                        )
                        check(
                            "actual_csv_readback",
                            len(artifacts) == 1
                            and csv_matches(agent.artifacts.read(user_id, artifacts[0]["path"])),
                        )
                        check("delivery_collected_not_sent", bool(store.deliveries))
                check(
                    f"turn{index}.nonempty_answer", isinstance(answer, str) and bool(answer.strip())
                )
                check(f"turn{index}.budget", run_cost <= 3_000_000)
                report["steps"].append(
                    {
                        "turn": index,
                        "calls": provider.calls - calls_before,
                        "cost_rub": str(Decimal(run_cost) / 1_000_000),
                        "tool_errors": [
                            {"tool": item["kind"], "error": str(item["result"]["error"])[:200]}
                            for item in operations
                            if isinstance(item["result"], dict) and item["result"].get("error")
                        ],
                    }
                )
            async with store.connection(user_id) as conn:
                check(
                    "no_schedules",
                    await conn.fetchval("SELECT count(*) FROM schedules WHERE user_id=$1", user_id)
                    == 0,
                )
                check(
                    "no_outbox_rows",
                    await conn.fetchval("SELECT count(*) FROM outbox WHERE user_id=$1", user_id)
                    == 0,
                )
                check(
                    "five_turn_history",
                    await conn.fetchval("SELECT count(*) FROM messages WHERE user_id=$1", user_id)
                    == 10,
                )
                check(
                    "real_pg_checkpoints",
                    await conn.fetchval(
                        "SELECT count(*) FROM langgraph.checkpoints WHERE thread_id IN (SELECT id::text FROM public.runs WHERE user_id=$1)",
                        user_id,
                    )
                    > 0,
                )
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
                    usage = await conn.fetch("SELECT raw FROM usage WHERE user_id=$1", user_id)
                known = [
                    Decimal(str(row["raw"]["cost_rub"]))
                    for row in usage
                    if row["raw"].get("cost_rub") is not None
                ]
                report["cost_rub"] = str(sum(known, Decimal(0)))
                report["unknown_cost_operations"] = len(usage) - len(known)
                check("cost_known", len(usage) == len(known))
            except Exception:
                check("usage_read_failed", False)
            try:
                await cleanup(store, user_id, event_ids)
                async with store.connection(user_id) as conn:
                    report["cleanup_complete"] = not await conn.fetchval(
                        "SELECT EXISTS(SELECT 1 FROM users WHERE user_id=$1)", user_id
                    )
                check("synthetic_cleanup", report["cleanup_complete"])
            except Exception:
                check("synthetic_cleanup_failed", False)
        if provider:
            await provider.close()
        if store:
            await store.close()
    report["elapsed_seconds"] = round(monotonic() - started, 2)
    report["success"] = not report["failed_checks"]
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--user-id", type=int, default=USER)
    args = parser.parse_args()
    for name in ("httpx", "httpcore", "openai", "langgraph"):
        logging.getLogger(name).setLevel(logging.CRITICAL)
    report = asyncio.run(evaluate(args.user_id))
    print(json.dumps(report, ensure_ascii=False), flush=True)
    raise SystemExit(0 if report["success"] else 1)


if __name__ == "__main__":
    main()
