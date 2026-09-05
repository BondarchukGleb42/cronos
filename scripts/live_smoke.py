"""Exercise real models and LangGraph on an isolated synthetic account; send nothing."""

import asyncio
import json
from decimal import Decimal
from tempfile import TemporaryDirectory
from uuid import uuid4

from cronos.agent import Agent, setup_checkpoints
from cronos.artifacts import ArtifactManager
from cronos.providers import Provider
from cronos.settings import get_settings
from cronos.storage import Store

USER = -930001


class SmokeStore(Store):
    async def enqueue_for_run(self, run, conversation, payload, dedupe):
        # Exercise real tools without exposing a synthetic delivery to the sender.
        assert run["user_id"] == USER
        return -1


class SmokeArtifacts(ArtifactManager):
    def _user_dir(self, user_id):
        # Production accepts positive Telegram IDs; isolate this negative DB owner
        # inside a unique temporary root while exercising the real file implementation.
        assert user_id == USER
        return super()._user_dir(-USER)


async def verify_scenario(scenario, tools, store, artifacts):
    if scenario == "memory":
        assert any(tool["kind"] == "memory_write" for tool in tools), "memory tool not used"
        memories = await store.memories(USER)
        persisted = " ".join(row["content"] for row in memories).casefold()
        assert "проверка" in persisted, "requested name was not persisted"
        preferences = await store.preferences(USER)
        answer_style = persisted + " " + str(preferences.get("tone", "")).casefold()
        assert any(stem in answer_style for stem in ("корот", "крат", "лаконич")), (
            "answer preference was not persisted"
        )
        return {"memory_persisted": True, "stored_facts": len(memories)}
    if scenario == "search":
        searches = [tool["result"] for tool in tools if tool["kind"] == "web_search"]
        assert any(result.get("sources") and not result.get("error") for result in searches), (
            "search returned no sources"
        )
        return {"search_sources": sum(len(result.get("sources", [])) for result in searches)}
    files = [
        tool["result"]
        for tool in tools
        if tool["kind"] == "file_create" and tool["result"].get("artifact_id")
    ]
    assert files, "file not created"
    artifact = await store.get_artifact(USER, files[-1]["artifact_id"])
    assert artifact["user_id"] == USER and artifact["mime"] == "text/csv", (
        "CSV metadata not persisted for the owner"
    )
    actual = await asyncio.to_thread(artifacts.read, USER, artifact["path"])
    tables = actual.get("tables", [])
    assert len(tables) == 1, "CSV does not contain one readable table"
    table = tables[0]
    assert table["columns"] == ["Продукт", "Цена"], "CSV columns do not match the request"
    assert len(table["rows"]) == 2 and all(len(row) == 2 for row in table["rows"]), (
        "CSV row shape does not match the request"
    )
    rows = [(str(row[0]).strip(), Decimal(str(row[1]).strip())) for row in table["rows"]]
    assert rows == [("Яблоко", Decimal(120)), ("Груша", Decimal(90))], (
        "CSV values do not match the request"
    )
    assert artifact["extracted"].get("tables") == tables, (
        "stored CSV extraction differs from file readback"
    )
    return {"csv_readback_rows": len(rows), "csv_metadata_matches": True}


async def main():
    settings = get_settings().model_copy(update={"max_output_tokens": 1024})
    store = SmokeStore(settings)
    provider = Provider(settings)
    await store.open()
    await setup_checkpoints(settings)
    conversation = await store.conversation(USER, USER, 0)

    class Preview:
        frames = 0

        async def draft(self, chat_id, thread_id, text, draft_id):
            assert chat_id == USER and text.strip(), "invalid streamed preview"
            self.frames += 1

    preview = Preview()
    async with store.connection(USER) as conn:
        await conn.execute("DELETE FROM memory WHERE user_id=$1", USER)
        await conn.execute("DELETE FROM artifacts WHERE user_id=$1", USER)
    agent = Agent(settings, store, provider, preview)
    scratch = TemporaryDirectory(prefix="synthetic-smoke-", dir=agent.artifacts.root)
    agent.artifacts = SmokeArtifacts(scratch.name)
    results = []
    try:
        for scenario, prompt in [
            (
                "memory",
                "Запомни: я предпочитаю короткие ответы, меня зовут Проверка. Мой часовой пояс Asia/Novosibirsk. Сохрани эти предпочтения.",
            ),
            (
                "search",
                "Найди официальный сайт библиотеки LangGraph через веб-поиск и дай ссылку. Не отвечай по памяти.",
            ),
            (
                "file",
                "Создай CSV с колонками Продукт, Цена и строками Яблоко 120, Груша 90. Нужен файл.",
            ),
        ]:
            event_id = uuid4()
            async with store.connection() as conn:
                await conn.execute(
                    "INSERT INTO events(id,kind,payload,state) VALUES($1,'smoke','{}','processing')",
                    event_id,
                )
            run = await store.start_run(event_id, USER, conversation["id"])
            before_frames = preview.frames
            answer = await agent.run(run, conversation, prompt)
            frames = preview.frames - before_frames
            assert frames > 0, f"{scenario}: no SSE preview frames received"
            await store.finish_run(run["id"], fence=run["fence"])
            async with store.connection(USER) as conn:
                usage = await conn.fetchval(
                    "SELECT COALESCE(sum(cost_micro),0) FROM usage WHERE run_id=$1", run["id"]
                )
                tools = await conn.fetch(
                    "SELECT kind,result FROM operations WHERE run_id=$1 AND kind<>'model'",
                    run["id"],
                )
            checks = await verify_scenario(scenario, tools, store, agent.artifacts)
            results.append(
                {
                    "scenario": scenario,
                    "preview_frames": frames,
                    "checks": checks,
                    "answer": answer,
                    "cost_rub": str(usage / 1_000_000),
                    "tools": [dict(x) for x in tools],
                }
            )
            print(json.dumps(results[-1], ensure_ascii=False, default=str), flush=True)
        print("LIVE_AGENT_SMOKE_PASS")
    finally:
        # Synthetic deliveries must never reach the real Telegram sender.
        async with store.connection() as conn:
            await conn.execute("DELETE FROM outbox WHERE user_id=$1", USER)
        await provider.close()
        await store.close()
        scratch.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
