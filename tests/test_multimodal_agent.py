import asyncio
import base64
import copy
import io
import json
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from langgraph.checkpoint.memory import InMemorySaver
from PIL import Image

from cronos.agent import Agent
from cronos.multimodal import generated_image_ids
from cronos.providers import ProviderError
from cronos.settings import Settings


def png(color):
    output = io.BytesIO()
    with Image.new("RGB", (9, 6), color) as image:
        image.save(output, format="PNG")
    return output.getvalue()


def image_colors(messages):
    colors = []
    for message in messages:
        content = message.get("content")
        if not isinstance(content, list):
            continue
        for part in content:
            if part.get("type") == "image_url":
                data_url = part["image_url"]["url"]
                header, encoded = data_url.split(",", 1)
                assert header in {
                    "data:image/png;base64",
                    "data:image/jpeg;base64",
                    "data:image/webp;base64",
                }
                with Image.open(io.BytesIO(base64.b64decode(encoded, validate=True))) as image:
                    colors.append(image.convert("RGB").getpixel((0, 0)))
    return colors


def image_call(content=None, *, args=None, call_id="create-image"):
    return {
        "role": "assistant",
        "content": content,
        "tool_calls": [
            {
                "id": call_id,
                "type": "function",
                "function": {
                    "name": "image_generate",
                    "arguments": json.dumps(args or {"prompt": "Объедини изображения"}),
                },
            }
        ],
    }


@pytest.fixture
def case(tmp_path):
    owner = 41001
    artifacts = {}

    async def lookup(user_id, artifact_id):
        artifact = artifacts.get((user_id, artifact_id))
        if artifact is None:
            raise ValueError("Файл не найден")
        return artifact

    async def save(user_id, artifact):
        assert user_id == owner
        artifacts[(user_id, artifact["id"])] = artifact

    store = SimpleNamespace(
        get_artifact=AsyncMock(side_effect=lookup),
        save_artifact=AsyncMock(side_effect=save),
        version_operation=AsyncMock(return_value=None),
        register_artifact_version=AsyncMock(return_value={}),
        enqueue=AsyncMock(),
        enqueue_for_run=AsyncMock(),
        preferences=AsyncMock(return_value={}),
        ensure_user=AsyncMock(return_value={"plan": "FREE"}),
        memories=AsyncMock(return_value=[]),
        query_memories=AsyncMock(return_value=[]),
        history=AsyncMock(return_value=[]),
        conversation_recall=AsyncMock(return_value=[]),
        list_artifacts=AsyncMock(return_value=[]),
        list_schedules=AsyncMock(return_value=[]),
        recipe_list=AsyncMock(return_value=[]),
        list_pending_recipe=AsyncMock(return_value=[]),
        get_project=AsyncMock(return_value=None),
        list_projects=AsyncMock(return_value=[]),
        operation=AsyncMock(return_value=None),
        save_operation=AsyncMock(),
        run_active=AsyncMock(return_value=True),
    )
    provider = SimpleNamespace(
        generate_image=AsyncMock(
            return_value={
                "data": png("yellow"),
                "mime": "image/png",
                "usage": {"cost_rub": "0.5"},
            }
        ),
        complete=AsyncMock(),
    )
    transport = SimpleNamespace(draft=AsyncMock(), send=AsyncMock())
    agent = Agent(
        Settings(database_url="postgresql://unused", artifacts_dir=str(tmp_path)),
        store,
        provider,
        transport,
    )

    async def paid(run, op, function, **kwargs):
        return await function()

    agent.paid = AsyncMock(side_effect=paid)
    agent.active = AsyncMock()
    refs = []
    for color in ("red", "blue"):
        artifact = agent.artifacts.ingest(owner, color + ".png", png(color))
        artifacts[(owner, artifact["id"])] = artifact
        refs.append(artifact["id"])
    foreign = agent.artifacts.ingest(owner + 1, "foreign.png", png("green"))
    artifacts[(owner + 1, foreign["id"])] = foreign
    prompt = [{"type": "text", "text": "Объедини эти изображения"}] + [
        {"type": "image_ref", "artifact_id": ref} for ref in refs
    ]
    return SimpleNamespace(
        agent=agent,
        store=store,
        provider=provider,
        transport=transport,
        owner=owner,
        refs=refs,
        foreign=foreign,
        artifacts=artifacts,
        prompt=prompt,
        run={"id": uuid4(), "user_id": owner, "fence": 1},
        conversation={"id": uuid4(), "user_id": owner, "chat_id": owner, "thread_id": 77},
    )


async def test_image_replay_recovers_project_link_without_paid_generation(case):
    artifact_id = case.refs[0]
    version = {"artifact_id": artifact_id, "project_id": str(uuid4()), "version": 2}
    case.store.version_operation.return_value = version
    case.store.attach_project_artifact = AsyncMock()
    result = await case.agent.execute(
        "image_generate",
        {"prompt": "Повтор после сбоя"},
        "same-operation",
        case.run,
        case.conversation,
    )
    assert result["artifact_id"] == artifact_id and result["delivery"] == "prepared"
    case.provider.generate_image.assert_not_awaited()
    case.store.save_artifact.assert_not_awaited()
    case.store.attach_project_artifact.assert_awaited_once_with(
        case.owner,
        version["project_id"],
        artifact_id,
        source_key="same-operation:project-file",
        run_id=case.run["id"],
        run_fence=case.run["fence"],
        conversation_id=case.conversation["id"],
    )


async def test_file_replay_sends_original_without_regenerating(case):
    artifact_id = case.refs[0]
    case.store.version_operation.return_value = {"artifact_id": artifact_id, "version": 1}
    original = case.artifacts[(case.owner, artifact_id)]
    before = await asyncio.to_thread(Path(original["path"]).read_bytes)
    result = await case.agent.execute(
        "file_create",
        {"format": "invalid-on-purpose"},
        "same-file",
        case.run,
        case.conversation,
    )
    assert result["artifact_id"] == artifact_id and result["delivery"] == "queued"
    assert await asyncio.to_thread(Path(original["path"]).read_bytes) == before
    case.store.save_artifact.assert_not_awaited()
    assert case.store.enqueue_for_run.call_args.args[2]["document_path"] == original["path"]


@pytest.mark.parametrize("explicit", [False, True])
async def test_image_generate_uses_all_owned_references_and_prepares_without_delivery(
    case, explicit
):
    args = {"prompt": "Объедини изображения"}
    expected_refs = case.refs
    if explicit:
        args["artifact_ids"] = [case.refs[1], case.refs[0], case.refs[1]]
        expected_refs = list(reversed(case.refs))
    result = await case.agent.execute(
        "image_generate",
        args,
        "image-operation",
        case.run,
        case.conversation,
        image_context=case.refs,
    )
    call = case.provider.generate_image.call_args
    assert call.args == (args["prompt"],)
    urls = call.kwargs["image_urls"]
    expected_colors = [(0, 0, 255), (255, 0, 0)] if explicit else [(255, 0, 0), (0, 0, 255)]
    assert (
        image_colors(
            [{"content": [{"type": "image_url", "image_url": {"url": url}} for url in urls]}]
        )
        == expected_colors
    )
    assert [call.args for call in case.store.get_artifact.call_args_list] == [
        (case.owner, ref) for ref in expected_refs
    ]
    assert result["delivery"] == "prepared"
    assert result["reference_artifact_ids"] == expected_refs
    created = case.artifacts[(case.owner, result["artifact_id"])]
    assert await asyncio.to_thread(Path(created["path"]).read_bytes) == png("yellow")
    case.store.save_artifact.assert_awaited_once_with(case.owner, created)
    case.agent.paid.assert_awaited_once()
    assert case.agent.paid.call_args.args[:2] == (case.run, "image-operation:usage")
    case.store.enqueue.assert_not_awaited()
    case.store.enqueue_for_run.assert_not_awaited()
    case.transport.send.assert_not_awaited()


async def test_foreign_reference_stops_before_any_provider_or_file_creation(case):
    with pytest.raises(ValueError, match="Файл не найден"):
        await case.agent.execute(
            "image_generate",
            {"prompt": "Edit", "artifact_ids": [case.refs[0], case.foreign["id"]]},
            "image-operation",
            case.run,
            case.conversation,
        )
    case.provider.generate_image.assert_not_awaited()
    case.agent.paid.assert_not_awaited()
    case.store.save_artifact.assert_not_awaited()
    case.store.enqueue_for_run.assert_not_awaited()
    assert all(call.args[0] == case.owner for call in case.store.get_artifact.call_args_list)


@pytest.mark.parametrize("legacy", [False, True])
async def test_hydration_restores_every_latest_image_without_mutating_durable_messages(
    case, legacy
):
    content = case.prompt
    if legacy:
        content = "Сравни обе картинки\n" + "\n".join(
            f"[Пользователь приложил файл: image.png, artifact_id={ref}, mime=image/png]"
            for ref in case.refs
        )
    older = {
        "role": "user",
        "content": [
            {"type": "text", "text": "Старое вложение больше не нужно загружать"},
            {"type": "image_ref", "artifact_id": str(uuid4())},
        ],
    }
    messages = [
        older,
        {"role": "assistant", "content": "Покажи новые"},
        {"role": "user", "content": content},
    ]
    original = copy.deepcopy(messages)
    cache = {}
    first = await case.agent.visual_messages(case.owner, messages, cache)
    second = await case.agent.visual_messages(case.owner, messages, cache)
    assert first == second
    assert messages == original
    assert "base64" not in json.dumps(messages)
    assert image_colors(first) == [(255, 0, 0), (0, 0, 255)]
    assert isinstance(first[-1]["content"], list)
    assert all(part.get("type") != "image_ref" for part in first[-1]["content"])
    assert [call.args for call in case.store.get_artifact.call_args_list] == [
        (case.owner, ref) for ref in case.refs
    ]


async def test_previous_generated_image_is_real_visual_context_for_next_edit(case):
    messages = [
        {
            "role": "assistant",
            "content": [
                {"type": "text", "text": "Готово"},
                {"type": "image_ref", "artifact_id": case.refs[0]},
            ],
        },
        {"role": "user", "content": "Теперь поменяй фон"},
    ]
    hydrated = await case.agent.visual_messages(case.owner, messages, {})
    assert [message["role"] for message in hydrated] == ["assistant", "user", "user"]
    assert image_colors(hydrated[:1]) == []
    assert image_colors(hydrated[1:2]) == [(255, 0, 0)]
    assert hydrated[-1] == messages[-1]


def test_final_answer_attaches_only_prepared_image_tool_receipts():
    ids = [str(uuid4()), str(uuid4())]
    messages = []
    for index, artifact_id in enumerate(ids):
        call_id = f"image-{index}"
        messages.extend(
            [
                image_call(call_id=call_id),
                {
                    "role": "tool",
                    "tool_call_id": call_id,
                    "content": json.dumps({"artifact_id": artifact_id, "delivery": "prepared"}),
                },
            ]
        )
    messages.extend(
        [
            image_call(call_id="failed"),
            {
                "role": "tool",
                "tool_call_id": "failed",
                "content": json.dumps({"error": "failed", "completed": False}),
            },
            {
                "role": "tool",
                "tool_call_id": "unmatched",
                "content": json.dumps({"artifact_id": str(uuid4()), "delivery": "prepared"}),
            },
        ]
    )
    assert generated_image_ids(messages) == ids
    assert Agent.final_answer({"answer": "Вот результат", "messages": messages}) == {
        "text": "Вот результат",
        "image_artifact_ids": ids,
    }
    assert Agent.final_answer({"answer": "Обычный ответ", "messages": []}) == "Обычный ответ"


def install_graph(case, monkeypatch, responses):
    saver = InMemorySaver()

    @asynccontextmanager
    async def connection():
        yield SimpleNamespace(execute=AsyncMock())

    monkeypatch.setattr(
        "cronos.agent.psycopg.AsyncConnection.connect", AsyncMock(return_value=connection())
    )
    monkeypatch.setattr("cronos.agent.FencedSaver", lambda conn, run: saver)
    case.provider.complete.side_effect = responses
    return saver


@pytest.mark.parametrize(
    "question", ["Какой цвет выбрать?", "Какой цвет выбрать? После ответа продолжу."]
)
async def test_clarification_ends_graph_before_image_action(case, monkeypatch, question):
    install_graph(case, monkeypatch, [{"message": image_call(question)}])
    case.agent.execute = AsyncMock(wraps=case.agent.execute)
    result = await case.agent.run(case.run, case.conversation, case.prompt)
    assert result == question
    case.agent.execute.assert_not_awaited()
    case.provider.generate_image.assert_not_awaited()
    case.provider.complete.assert_awaited_once()
    case.transport.draft.assert_not_awaited()
    case.store.enqueue_for_run.assert_not_awaited()


@pytest.mark.parametrize("caption_failure", [False, True])
async def test_graph_returns_prepared_image_with_final_caption_and_lightweight_checkpoint(
    case, monkeypatch, caption_failure
):
    last = (
        ProviderError("Caption unavailable")
        if caption_failure
        else {"message": {"role": "assistant", "content": "Объединил обе картинки"}}
    )
    saver = install_graph(case, monkeypatch, [{"message": image_call()}, last])
    answer = await case.agent.run(case.run, case.conversation, case.prompt)
    assert len(answer["image_artifact_ids"]) == 1
    assert answer["image_artifact_ids"][0] in [
        key[1] for key in case.artifacts if key[0] == case.owner
    ]
    assert answer["text"] == (
        "Готово — изображение подготовлено." if caption_failure else "Объединил обе картинки"
    )
    case.provider.generate_image.assert_awaited_once()
    assert len(case.provider.generate_image.call_args.kwargs["image_urls"]) == 2
    assert case.provider.complete.await_count == 2
    for call in case.provider.complete.call_args_list:
        assert image_colors(call.args[0]) == [(255, 0, 0), (0, 0, 255)]
        assert "on_delta" not in call.kwargs
    checkpoint = await saver.aget_tuple(
        {"configurable": {"thread_id": str(case.run["id"]), "checkpoint_ns": ""}}
    )
    durable = checkpoint.checkpoint["channel_values"]["messages"]
    assert "base64," not in json.dumps(durable)
    assert generated_image_ids(durable) == answer["image_artifact_ids"]
    case.store.enqueue.assert_not_awaited()
    case.store.enqueue_for_run.assert_not_awaited()
    case.transport.draft.assert_not_awaited()
    case.transport.send.assert_not_awaited()
