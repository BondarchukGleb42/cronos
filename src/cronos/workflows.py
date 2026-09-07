"""Explicit personal project cycles: a plan, observations and a revised plan."""

from contextlib import AbstractAsyncContextManager
from datetime import UTC, datetime
from decimal import Decimal
from typing import Annotated, Literal
from uuid import uuid4

import asyncpg
from pydantic import BaseModel, ConfigDict, Field, FiniteFloat, StringConstraints

from cronos.projects import (
    _change,
    _origin,
    _owner_lock,
    _project_row,
    _source_key,
    _uid,
    serialize_project_delivery,
)

Text = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]
PositiveInt = Annotated[int, Field(gt=0)]
NonnegativeInt = Annotated[int, Field(ge=0)]
Quantity = Annotated[FiniteFloat, Field(ge=0)]
Scale = Annotated[int, Field(ge=0, le=10)]


class Data(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class StockItem(Data):
    name: Text
    quantity: Quantity
    unit: Text


class NutritionParameters(Data):
    household_count: PositiveInt
    cooking_minutes: PositiveInt
    pantry: list[StockItem] = Field(default_factory=list)
    constraints: list[Text] = Field(default_factory=list)


class Meal(Data):
    name: Text
    servings: PositiveInt
    minutes: PositiveInt
    ingredients: list[StockItem] = Field(default_factory=list)


class NutritionPlan(Data):
    meals: list[Meal] = Field(min_length=1)
    shopping_list: list[StockItem] = Field(default_factory=list)
    next_step: Text


class NutritionObservation(Data):
    action: Literal["restock", "consume", "set", "meal"]
    items: list[StockItem] = Field(default_factory=list)
    meal_name: str = ""
    feedback: str = ""


class TrainingParameters(Data):
    sessions_per_week: PositiveInt
    available_minutes: PositiveInt
    equipment: list[Text] = Field(default_factory=list)


class Session(Data):
    id: Text
    name: Text
    minutes: PositiveInt
    exercises: list[Text] = Field(default_factory=list)


class TrainingPlan(Data):
    sessions: list[Session] = Field(min_length=1)
    next_step: Text


class TrainingObservation(Data):
    session_id: Text
    completed: bool
    minutes: NonnegativeInt = 0
    effort: Annotated[int, Field(ge=1, le=10)] | None = None
    feedback: str = ""


class LearningParameters(Data):
    topics: list[Text] = Field(min_length=1)
    minutes_per_session: PositiveInt
    target_success_rate: Annotated[FiniteFloat, Field(ge=0, le=1)] = 0.8
    minimum_evidence: PositiveInt = 3


class Exercise(Data):
    id: Text
    topic: Text
    prompt: Text


class LearningPlan(Data):
    exercises: list[Exercise] = Field(min_length=1)
    next_step: Text


class LearningObservation(Data):
    topic: Text
    evidence: Text
    correct: bool
    mistake: str = ""
    exercise_id: str | None = None


class ContentParameters(Data):
    channels: list[Text] = Field(min_length=1)
    criteria: list[Text] = Field(min_length=1)


class EditorialItem(Data):
    id: Text
    title: Text
    channel: Text


class ContentPlan(Data):
    items: list[EditorialItem] = Field(min_length=1)
    next_step: Text


class ContentObservation(Data):
    item_id: Text
    published: bool
    views: NonnegativeInt = 0
    interactions: NonnegativeInt = 0
    met_criteria: list[Text] = Field(default_factory=list)
    feedback: str = ""


class WellbeingParameters(Data):
    focus: Text
    scale_description: Text = "Самооценка от 0 до 10: больше означает лучшее самочувствие"


class Practice(Data):
    name: Text
    minutes: PositiveInt


class WellbeingPlan(Data):
    practices: list[Practice] = Field(min_length=1)
    next_step: Text


class WellbeingObservation(Data):
    energy: Scale
    mood: Scale
    note: Text
    practices_completed: list[Text] = Field(default_factory=list)


MODELS = {
    "nutrition": (NutritionParameters, NutritionPlan, NutritionObservation),
    "training": (TrainingParameters, TrainingPlan, TrainingObservation),
    "learning": (LearningParameters, LearningPlan, LearningObservation),
    "content": (ContentParameters, ContentPlan, ContentObservation),
    "wellbeing": (WellbeingParameters, WellbeingPlan, WellbeingObservation),
}

_EXAMPLES = {
    "nutrition": {
        "parameters": {
            "household_count": 2,
            "cooking_minutes": 30,
            "pantry": [{"name": "рис", "quantity": 500, "unit": "г"}],
        },
        "plan": {
            "meals": [
                {
                    "name": "Рис",
                    "servings": 2,
                    "minutes": 20,
                    "ingredients": [{"name": "рис", "quantity": 150, "unit": "г"}],
                }
            ],
            "next_step": "Приготовить рис на двоих",
        },
        "observation": {
            "action": "meal",
            "meal_name": "Рис",
            "items": [{"name": "рис", "quantity": 150, "unit": "г"}],
            "feedback": "Порции подошли",
        },
    },
    "training": {
        "parameters": {"sessions_per_week": 2, "available_minutes": 25, "equipment": ["коврик"]},
        "plan": {
            "sessions": [
                {
                    "id": "week1-a",
                    "name": "Мобильность",
                    "minutes": 20,
                    "exercises": ["Знакомая пользователю разминка"],
                }
            ],
            "next_step": "Провести первую короткую тренировку",
        },
        "observation": {
            "session_id": "week1-a",
            "completed": True,
            "minutes": 18,
            "effort": 4,
            "feedback": "Уложился во время",
        },
    },
    "learning": {
        "parameters": {
            "topics": ["дроби"],
            "minutes_per_session": 15,
            "minimum_evidence": 3,
            "target_success_rate": 0.8,
        },
        "plan": {
            "exercises": [{"id": "fractions-1", "topic": "дроби", "prompt": "Вычисли 1/2 + 1/4"}],
            "next_step": "Решить пример и объяснить общий знаменатель",
        },
        "observation": {
            "topic": "дроби",
            "exercise_id": "fractions-1",
            "evidence": "Получилось 3/4, приведены к знаменателю 4",
            "correct": True,
        },
    },
    "content": {
        "parameters": {"channels": ["блог"], "criteria": ["Проверенный пример", "Понятный вывод"]},
        "plan": {
            "items": [{"id": "post-1", "title": "Первый разбор", "channel": "блог"}],
            "next_step": "Написать черновик с примером",
        },
        "observation": {
            "item_id": "post-1",
            "published": True,
            "views": 100,
            "interactions": 8,
            "met_criteria": ["Проверенный пример"],
            "feedback": "Нужен яснее вывод",
        },
    },
    "wellbeing": {
        "parameters": {"focus": "Замечать свой уровень энергии"},
        "plan": {
            "practices": [{"name": "Короткая прогулка", "minutes": 10}],
            "next_step": "Записать самочувствие после прогулки",
        },
        "observation": {
            "energy": 6,
            "mood": 7,
            "note": "После прогулки стало легче сосредоточиться",
            "practices_completed": ["Короткая прогулка"],
        },
    },
}

_INSTRUCTIONS = {
    "nutrition": "План для указанного числа людей и времени готовки. Ингредиенты — на всё блюдо. Pantry хранит явно известные остатки. Observation.action: restock добавляет, consume/meal списывает, set задаёт остаток; meal требует meal_name. Единицы не конвертируются автоматически. Не придумывай калорийность или медицинскую диету.",
    "training": "Короткий план с устойчивыми уникальными session id. Обратная связь — сообщение пользователя о выполнении, минутах и субъективной нагрузке. Повторная запись той же сессии уточняет её итог, не добавляет выполненную сессию. Новая неделя требует новых id. Не диагностируй травмы.",
    "learning": "Каждое наблюдение содержит конкретное evidence, topic и correct; ошибка фиксируется явно. mastery отображается через число попыток и долю верных ответов, при minimum_evidence и target_success_rate. Это проверка имеющихся свидетельств, не вывод о знаниях вне них. Следующий пример хранится в плане.",
    "content": "План содержит редакционные единицы и критерии качества. Пользователь сообщает факт публикации и показатели; никаких внешних публикаций или чтения статистики этот навык не выполняет. Последняя запись единицы обновляет её показатели, вместо суммирования снимков.",
    "wellbeing": "Только добровольный дневник и сообщённые пользователем energy/mood 0..10. Отображай самооценки и простые средние без диагнозов, медицинских оценок, предсказаний или автоматических сообщений.",
}

WORKFLOW_TEMPLATES = {
    kind: {
        "kind": kind,
        "title": {
            "nutrition": "Питание",
            "training": "Тренировки",
            "learning": "Обучение",
            "content": "Контент",
            "wellbeing": "Самочувствие",
        }[kind],
        "instructions": instructions,
        "parameters_schema": MODELS[kind][0].model_json_schema(),
        "plan_schema": MODELS[kind][1].model_json_schema(),
        "observation_schema": MODELS[kind][2].model_json_schema(),
        "example": _EXAMPLES[kind],
    }
    for kind, instructions in _INSTRUCTIONS.items()
}


class WorkflowRevisionConflict(ValueError):
    """Reload the workflow/project before changing a stale plan or observation."""


def _validate(kind: str, slot: int, value: dict) -> dict:
    if not isinstance(kind, str) or kind not in MODELS:
        raise ValueError("Unknown personal workflow template")
    return MODELS[kind][slot].model_validate(value).model_dump(mode="json")


def _revision(value, field="revision") -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise ValueError(f"Current workflow {field} is required")
    return value


def _unique(items, field: str):
    values = [item[field] for item in items]
    if len(values) != len(set(values)):
        raise ValueError(f"Workflow {field} values must be unique")


def _validate_plan(kind, parameters, plan):
    if kind == "nutrition":
        for meal in plan["meals"]:
            if meal["servings"] < parameters["household_count"]:
                raise ValueError("Meal servings must cover the household")
            if meal["minutes"] > parameters["cooking_minutes"]:
                raise ValueError("Meal exceeds the available cooking time")
    elif kind == "training":
        _unique(plan["sessions"], "id")
        if any(
            session["minutes"] > parameters["available_minutes"] for session in plan["sessions"]
        ):
            raise ValueError("Training exceeds the available session time")
    elif kind == "learning":
        _unique(plan["exercises"], "id")
        if any(exercise["topic"] not in parameters["topics"] for exercise in plan["exercises"]):
            raise ValueError("Exercise topic is outside this learning plan")
    elif kind == "content":
        _unique(plan["items"], "id")
        if any(item["channel"] not in parameters["channels"] for item in plan["items"]):
            raise ValueError("Editorial item channel is outside this plan")


def _validate_observation(kind, parameters, plan, observation):
    if kind == "nutrition":
        if observation["action"] not in {"restock", "consume", "set", "meal"}:
            raise ValueError("Nutrition action must be restock, consume, set or meal")
        if observation["action"] == "meal" and not observation["meal_name"].strip():
            raise ValueError("A completed meal requires its name")
        if observation["action"] != "meal" and not observation["items"]:
            raise ValueError("An inventory observation requires items")
    elif kind == "training":
        if observation["session_id"] not in {session["id"] for session in plan["sessions"]}:
            raise ValueError("Observation refers to an unknown planned session")
    elif kind == "learning":
        if observation["topic"] not in parameters["topics"]:
            raise ValueError("Observation topic is outside this learning plan")
        if observation["exercise_id"] is not None and not any(
            exercise["id"] == observation["exercise_id"]
            and exercise["topic"] == observation["topic"]
            for exercise in plan["exercises"]
        ):
            raise ValueError("Observation exercise does not match its topic")
    elif kind == "content":
        if observation["item_id"] not in {item["id"] for item in plan["items"]}:
            raise ValueError("Observation refers to an unknown editorial item")
        if set(observation["met_criteria"]) - set(parameters["criteria"]):
            raise ValueError("Observation contains unknown editorial criteria")


def _progress(kind, parameters, plan, observations, *, strict_last=False):
    progress = {"observations_count": len(observations)}
    if kind == "nutrition":
        stock = {}
        for item in parameters["pantry"]:
            key = (item["name"].casefold(), item["unit"].casefold())
            previous = stock.get(key, {**item, "quantity": Decimal(0)})
            stock[key] = {
                **previous,
                "quantity": previous["quantity"] + Decimal(str(item["quantity"])),
            }
        meals, unknown = 0, set()
        for index, observation in enumerate(observations):
            action = observation["action"]
            meals += action == "meal"
            for item in observation["items"]:
                key = (item["name"].casefold(), item["unit"].casefold())
                previous = stock.get(key, {**item, "quantity": Decimal(0)})
                quantity = Decimal(str(item["quantity"]))
                amount = (
                    quantity
                    if action == "set"
                    else previous["quantity"] + (quantity if action == "restock" else -quantity)
                )
                if action == "set":
                    unknown.discard(key)
                if amount < 0 or (key in unknown and action in {"consume", "meal"}):
                    if strict_last and index == len(observations) - 1:
                        raise ValueError("Consumed inventory exceeds the known amount in that unit")
                    unknown.add(key)
                    amount = max(amount, Decimal(0))
                stock[key] = {**previous, "quantity": amount}
        progress.update(
            pantry=[
                {
                    **stock[key],
                    "quantity": None if key in unknown else float(stock[key]["quantity"]),
                }
                for key in sorted(stock)
            ],
            meals_completed=meals,
            household_count=parameters["household_count"],
            inventory_consistent=not unknown,
        )
    elif kind == "training":
        latest = {row["session_id"]: row for row in observations}
        planned = {session["id"] for session in plan["sessions"]}
        completed = [row for key, row in latest.items() if key in planned and row["completed"]]
        progress.update(
            planned_sessions=len(planned),
            completed_sessions=len(completed),
            completed_minutes=sum(row["minutes"] for row in completed),
            completion_rate=len(completed) / len(planned),
            session_feedback={
                key: row["feedback"] for key, row in latest.items() if key in planned
            },
        )
    elif kind == "learning":
        topics = []
        for topic in parameters["topics"]:
            evidence = [row for row in observations if row["topic"] == topic]
            successes = sum(row["correct"] for row in evidence)
            rate = successes / len(evidence) if evidence else None
            topics.append(
                {
                    "topic": topic,
                    "evidence_count": len(evidence),
                    "correct_count": successes,
                    "success_rate": rate,
                    "meets_target": len(evidence) >= parameters["minimum_evidence"]
                    and rate is not None
                    and rate >= parameters["target_success_rate"],
                    "last_evidence": evidence[-1]["evidence"] if evidence else None,
                    "last_mistake": next(
                        (row["mistake"] for row in reversed(evidence) if not row["correct"]), None
                    ),
                }
            )
        progress["topics"] = topics
    elif kind == "content":
        latest = {row["item_id"]: row for row in observations}
        planned = {item["id"] for item in plan["items"]}
        published = [row for key, row in latest.items() if key in planned and row["published"]]
        views, interactions = (
            sum(row["views"] for row in published),
            sum(row["interactions"] for row in published),
        )
        progress.update(
            planned_items=len(planned),
            published_items=len(published),
            views=views,
            interactions=interactions,
            interaction_rate=interactions / views if views else None,
            criteria_evidence={
                key: row["met_criteria"] for key, row in latest.items() if key in planned
            },
        )
    else:
        progress.update(
            self_reported=True,
            latest_energy=observations[-1]["energy"] if observations else None,
            latest_mood=observations[-1]["mood"] if observations else None,
            mean_energy=sum(row["energy"] for row in observations) / len(observations)
            if observations
            else None,
            mean_mood=sum(row["mood"] for row in observations) / len(observations)
            if observations
            else None,
        )
    return progress


async def _source(conn, user_id, run):
    if run is None:
        return None, None
    if not isinstance(run, dict) or "id" not in run or "fence" not in run:
        raise ValueError("Workflow source run requires id and fence")
    origin, run_id = await _origin(conn, user_id, None, run["id"], run["fence"])
    if not await conn.fetchval(
        """SELECT 1 FROM runs r JOIN users u ON u.user_id=r.user_id
        WHERE r.user_id=$1 AND r.id=$2 AND r.memory_revision=u.memory_revision""",
        user_id,
        run_id,
    ):
        raise ValueError("Workflow source predates a context reset")
    return origin, run_id


async def _current(conn, user_id, project_id):
    return await conn.fetchrow(
        "SELECT * FROM workflows WHERE user_id=$1 AND project_id=$2", user_id, _uid(project_id)
    )


def _needs_context(project, workflow=None):
    return project["context_excluded"] or (
        workflow is not None and workflow["context_revision"] != project["context_reset_revision"]
    )


async def _existing_operation(conn, user_id, source_key, project_id, kind, current):
    receipt = await conn.fetchrow(
        "SELECT project_id,workflow_id,kind FROM workflow_operations WHERE user_id=$1 AND source_key=$2",
        user_id,
        source_key,
    )
    if receipt is None:
        return False
    if receipt["project_id"] != project_id or receipt["kind"] != kind:
        raise ValueError("Workflow source_key was already used for another operation")
    if current is None or current["id"] != receipt["workflow_id"]:
        raise WorkflowRevisionConflict(
            "The previous workflow was reset; an old operation cannot restart it"
        )
    return True


async def _receipt(conn, user_id, source_key, workflow, kind, origin, run_id):
    await conn.execute(
        """INSERT INTO workflow_operations
        (user_id,source_key,project_id,workflow_id,kind,origin_conversation_id,run_id)
        VALUES($1,$2,$3,$4,$5,$6,$7)""",
        user_id,
        source_key,
        workflow["project_id"],
        workflow["id"],
        kind,
        origin,
        run_id,
    )


async def _project_plan(
    conn, user_id, project, expected_revision, summary, next_step, source_key, origin, run_id
):
    revision = await conn.fetchval(
        """UPDATE projects SET state=state || $3::jsonb,revision=revision+1,
        updated_at=clock_timestamp() WHERE user_id=$1 AND id=$2 AND revision=$4
        AND NOT context_excluded RETURNING revision""",
        user_id,
        project["id"],
        {"summary": summary, "next_step": next_step},
        expected_revision,
    )
    if revision is None:
        raise WorkflowRevisionConflict("Project changed; reload its current revision")
    await _change(
        conn,
        user_id,
        project["id"],
        revision,
        "update",
        f"workflow:project:{source_key}",
        origin,
        run_id,
        {"state": {"summary": summary, "next_step": next_step}},
    )
    return revision


async def _plan_snapshot(conn, user_id, workflow, origin, run_id):
    await conn.execute(
        """INSERT INTO workflow_plans
        (id,user_id,project_id,workflow_id,revision,parameters,plan,origin_conversation_id,run_id)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)""",
        uuid4(),
        user_id,
        workflow["project_id"],
        workflow["id"],
        workflow["revision"],
        workflow["parameters"],
        workflow["plan"],
        origin,
        run_id,
    )


async def _observations(conn, user_id, workflow_id):
    return [
        row["observation"]
        for row in await conn.fetch(
            "SELECT observation FROM workflow_observations WHERE user_id=$1 AND workflow_id=$2 ORDER BY revision",
            user_id,
            workflow_id,
        )
    ]


def _entry(row):
    return {
        "id": str(row["id"]),
        "revision": row["revision"],
        "observation": row["observation"],
        "observed_at": row["observed_at"].isoformat(),
    }


async def _detail(conn, user_id, project_id, limit=20, offset=0):
    project = await _project_row(conn, user_id, project_id)
    workflow = await _current(conn, user_id, project_id)
    if _needs_context(project, workflow):
        return {"project_id": str(project_id), "needs_context": True}
    if workflow is None:
        return None
    observations = await _observations(conn, user_id, workflow["id"])
    entries = await conn.fetch(
        """SELECT id,revision,observation,observed_at FROM workflow_observations
        WHERE user_id=$1 AND workflow_id=$2 ORDER BY revision DESC LIMIT $3 OFFSET $4""",
        user_id,
        workflow["id"],
        limit,
        offset,
    )
    return {
        "id": str(workflow["id"]),
        "project_id": str(project_id),
        "kind": workflow["kind"],
        "parameters": workflow["parameters"],
        "plan": workflow["plan"],
        "revision": workflow["revision"],
        "project_revision": project["revision"],
        "progress": _progress(
            workflow["kind"], workflow["parameters"], workflow["plan"], observations
        ),
        "entries": [_entry(row) for row in entries],
        "entries_total": len(observations),
        "limit": limit,
        "offset": offset,
    }


def _args(args, allowed, required):
    if not isinstance(args, dict) or set(args) - allowed or not required <= args.keys():
        raise ValueError("Missing or unknown workflow arguments; inspect workflow_templates")


class WorkflowsStoreMixin:
    def connection(
        self, user_id: int | None = None
    ) -> AbstractAsyncContextManager[asyncpg.Connection]:
        """Implemented by Store as an owner-scoped transaction."""
        raise NotImplementedError

    @serialize_project_delivery
    async def workflow_start(self, user_id: int, project_id, args: dict, source_key: str, run=None):
        _args(
            args,
            {
                "kind",
                "parameters",
                "plan",
                "project_revision",
                "project_summary",
                "project_next_step",
            },
            {"kind", "parameters", "plan", "project_revision"},
        )
        parameters = _validate(args["kind"], 0, args["parameters"])
        plan = _validate(args["kind"], 1, args["plan"])
        _validate_plan(args["kind"], parameters, plan)
        project_revision = _revision(args["project_revision"], "project_revision")
        source_key, project_id = _source_key(source_key), _uid(project_id)
        async with self.connection(user_id) as conn:
            await _owner_lock(conn, user_id)
            origin, run_id = await _source(conn, user_id, run)
            project = await _project_row(conn, user_id, project_id)
            current = await _current(conn, user_id, project_id)
            if project["context_excluded"]:
                raise ValueError(
                    "Restore the project with a new name and goal before starting a workflow"
                )
            if await _existing_operation(conn, user_id, source_key, project_id, "start", current):
                return await _detail(conn, user_id, project_id)
            if current and not _needs_context(project, current):
                raise ValueError("This project already has a workflow; observe or replan it")
            if current:
                await conn.execute(
                    "DELETE FROM workflows WHERE user_id=$1 AND id=$2", user_id, current["id"]
                )
            summary = args.get(
                "project_summary",
                f"{WORKFLOW_TEMPLATES[args['kind']]['title']}: {plan['next_step']}",
            )
            next_step = args.get("project_next_step", plan["next_step"])
            if not isinstance(summary, str) or not isinstance(next_step, str):
                raise ValueError("Project summary and next step must be text")
            await _project_plan(
                conn,
                user_id,
                project,
                project_revision,
                summary,
                next_step,
                source_key,
                origin,
                run_id,
            )
            workflow = await conn.fetchrow(
                """INSERT INTO workflows(id,user_id,project_id,kind,parameters,plan,context_revision)
                VALUES($1,$2,$3,$4,$5,$6,$7) RETURNING *""",
                uuid4(),
                user_id,
                project_id,
                args["kind"],
                parameters,
                plan,
                project["context_reset_revision"],
            )
            await _plan_snapshot(conn, user_id, workflow, origin, run_id)
            await _receipt(conn, user_id, source_key, workflow, "start", origin, run_id)
            return await _detail(conn, user_id, project_id)

    async def workflow_get(self, user_id: int, project_id, *, limit=20, offset=0):
        if (
            not isinstance(limit, int)
            or isinstance(limit, bool)
            or not 1 <= limit <= 100
            or not isinstance(offset, int)
            or isinstance(offset, bool)
            or offset < 0
        ):
            raise ValueError("Workflow history requires limit 1..100 and a nonnegative offset")
        async with self.connection(user_id) as conn:
            return await _detail(conn, user_id, _uid(project_id), limit, offset)

    @serialize_project_delivery
    async def workflow_observe(
        self, user_id: int, project_id, args: dict, source_key: str, run=None
    ):
        _args(args, {"revision", "observation", "observed_at"}, {"revision", "observation"})
        revision = _revision(args["revision"])
        source_key, project_id = _source_key(source_key), _uid(project_id)
        observed_at = args.get("observed_at")
        if isinstance(observed_at, str):
            try:
                observed_at = datetime.fromisoformat(observed_at)
            except ValueError:
                raise ValueError(
                    "Observation time requires an ISO timestamp with timezone"
                ) from None
        if observed_at is not None:
            if not isinstance(observed_at, datetime) or observed_at.utcoffset() is None:
                raise ValueError("Observation time requires a timezone")
            observed_at = observed_at.astimezone(UTC)
        async with self.connection(user_id) as conn:
            await _owner_lock(conn, user_id)
            origin, run_id = await _source(conn, user_id, run)
            project = await _project_row(conn, user_id, project_id)
            current = await _current(conn, user_id, project_id)
            if current is None or _needs_context(project, current):
                raise ValueError("Start a current workflow before recording observations")
            if await _existing_operation(conn, user_id, source_key, project_id, "observe", current):
                return await _detail(conn, user_id, project_id)
            if current["revision"] != revision:
                raise WorkflowRevisionConflict(
                    "Workflow changed; reload before recording this observation"
                )
            observation = _validate(current["kind"], 2, args["observation"])
            _validate_observation(
                current["kind"], current["parameters"], current["plan"], observation
            )
            previous = await _observations(conn, user_id, current["id"])
            _progress(
                current["kind"],
                current["parameters"],
                current["plan"],
                [*previous, observation],
                strict_last=True,
            )
            updated = await conn.fetchrow(
                """UPDATE workflows SET revision=revision+1,updated_at=clock_timestamp()
                WHERE user_id=$1 AND id=$2 AND revision=$3 RETURNING *""",
                user_id,
                current["id"],
                revision,
            )
            if updated is None:
                raise WorkflowRevisionConflict("Workflow changed; reload its current revision")
            observation_id = uuid4()
            await conn.execute(
                """INSERT INTO workflow_observations
                (id,user_id,project_id,workflow_id,revision,observation,observed_at,origin_conversation_id,run_id)
                VALUES($1,$2,$3,$4,$5,$6,COALESCE($7,clock_timestamp()),$8,$9)""",
                observation_id,
                user_id,
                project_id,
                current["id"],
                updated["revision"],
                observation,
                observed_at,
                origin,
                run_id,
            )
            project_revision = await conn.fetchval(
                "UPDATE projects SET revision=revision+1,updated_at=clock_timestamp() WHERE user_id=$1 AND id=$2 RETURNING revision",
                user_id,
                project_id,
            )
            await _change(
                conn,
                user_id,
                project_id,
                project_revision,
                "update",
                "workflow-observe:" + source_key,
                origin,
                run_id,
                {"workflow_observation_id": str(observation_id)},
            )
            await _receipt(conn, user_id, source_key, updated, "observe", origin, run_id)
            return await _detail(conn, user_id, project_id)

    @serialize_project_delivery
    async def workflow_replan(
        self, user_id: int, project_id, args: dict, source_key: str, run=None
    ):
        _args(
            args,
            {
                "revision",
                "project_revision",
                "plan",
                "parameters",
                "project_summary",
                "project_next_step",
            },
            {"revision", "project_revision", "plan", "project_summary", "project_next_step"},
        )
        revision = _revision(args["revision"])
        project_revision = _revision(args["project_revision"], "project_revision")
        if not isinstance(args["project_summary"], str) or not isinstance(
            args["project_next_step"], str
        ):
            raise ValueError("Project summary and next step must be text")
        source_key, project_id = _source_key(source_key), _uid(project_id)
        async with self.connection(user_id) as conn:
            await _owner_lock(conn, user_id)
            origin, run_id = await _source(conn, user_id, run)
            project = await _project_row(conn, user_id, project_id)
            current = await _current(conn, user_id, project_id)
            if current is None or _needs_context(project, current):
                raise ValueError("Start a current workflow before changing its plan")
            if await _existing_operation(conn, user_id, source_key, project_id, "replan", current):
                return await _detail(conn, user_id, project_id)
            if current["revision"] != revision:
                raise WorkflowRevisionConflict("Workflow changed; reload its current revision")
            patch = args.get("parameters", {})
            if not isinstance(patch, dict):
                raise ValueError("Workflow parameters must be an object")
            if current["kind"] == "nutrition" and "pantry" in patch:
                raise ValueError("Change inventory using a set/restock/consume observation")
            parameters = _validate(current["kind"], 0, {**current["parameters"], **patch})
            plan = _validate(current["kind"], 1, args["plan"])
            _validate_plan(current["kind"], parameters, plan)
            await _project_plan(
                conn,
                user_id,
                project,
                project_revision,
                args["project_summary"],
                args["project_next_step"],
                source_key,
                origin,
                run_id,
            )
            updated = await conn.fetchrow(
                """UPDATE workflows SET parameters=$3,plan=$4,revision=revision+1,
                updated_at=clock_timestamp() WHERE user_id=$1 AND id=$2 AND revision=$5 RETURNING *""",
                user_id,
                current["id"],
                parameters,
                plan,
                revision,
            )
            if updated is None:
                raise WorkflowRevisionConflict("Workflow changed; reload its current revision")
            await _plan_snapshot(conn, user_id, updated, origin, run_id)
            await _receipt(conn, user_id, source_key, updated, "replan", origin, run_id)
            return await _detail(conn, user_id, project_id)
