"""Exercise real models and LangGraph on an isolated synthetic account; send nothing."""

import asyncio
import json
from uuid import uuid4

from cronos.agent import Agent, setup_checkpoints
from cronos.providers import Provider
from cronos.settings import get_settings
from cronos.storage import Store

USER = -930001


class SmokeStore(Store):
    async def enqueue_for_run(self, run, conversation, payload, dedupe_key):
        # Exercise real tools without exposing a synthetic delivery to the sender.
        assert run["user_id"] == USER
        return -1


async def main():
    settings = get_settings().model_copy(update={"max_output_tokens": 1024})
    store = SmokeStore(settings)
    provider = Provider(settings)
    await store.open()
    await setup_checkpoints(settings)
    conversation = await store.conversation(USER, USER, 0)

    class Preview:
        frames = 0

        async def draft(self, *args):
            self.frames += 1

    preview = Preview()
    async with store.connection(USER) as conn:
        await conn.execute("DELETE FROM memory WHERE user_id=$1", USER)
        await conn.execute("DELETE FROM artifacts WHERE user_id=$1", USER)
    agent = Agent(settings, store, provider, preview)
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
            answer = await agent.run(run, conversation, prompt)
            await store.finish_run(run["id"], fence=run["fence"])
            async with store.connection(USER) as conn:
                usage = await conn.fetchval(
                    "SELECT COALESCE(sum(cost_micro),0) FROM usage WHERE run_id=$1", run["id"]
                )
                tools = await conn.fetch(
                    "SELECT kind,result FROM operations WHERE run_id=$1 AND kind<>'model'",
                    run["id"],
                )
            results.append(
                {
                    "scenario": scenario,
                    "preview_frames": preview.frames,
                    "answer": answer,
                    "cost_rub": str(usage / 1_000_000),
                    "tools": [dict(x) for x in tools],
                }
            )
            print(json.dumps(results[-1], ensure_ascii=False, default=str), flush=True)
        assert any(x["kind"] == "memory_write" for x in results[0]["tools"]), "memory tool not used"
        assert any(
            x["kind"] == "web_search" and not x["result"].get("error") for x in results[1]["tools"]
        ), "search not completed"
        assert any(
            x["kind"] == "file_create" and x["result"].get("artifact_id")
            for x in results[2]["tools"]
        ), "file not created"
        print("LIVE_AGENT_SMOKE_PASS")
    finally:
        # Synthetic deliveries must never reach the real Telegram sender.
        async with store.connection() as conn:
            await conn.execute("DELETE FROM outbox WHERE user_id=$1", USER)
        await provider.close()
        await store.close()


if __name__ == "__main__":
    asyncio.run(main())
