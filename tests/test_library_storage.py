"""Library queries against real local PostgreSQL, using only dedicated synthetic owners."""

import os
from types import SimpleNamespace
from urllib.parse import urlsplit
from uuid import UUID, uuid4

import pytest

from cronos.settings import Settings
from cronos.storage import Store

A, B = -985101, -985102


@pytest.fixture
async def case():
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        pytest.skip("Real local PostgreSQL DATABASE_URL is required")
    parsed = urlsplit(database_url)
    assert parsed.hostname in {"localhost", "127.0.0.1"} and parsed.path == "/cronos_test"
    store = Store(Settings(database_url=database_url))
    await store.open()
    created = []
    try:
        for owner in (A, B):
            async with store.connection(owner) as conn:
                assert not await conn.fetchval("SELECT 1 FROM users WHERE user_id=$1", owner), (
                    "Synthetic library owner already exists; do not erase another test's data"
                )
            await store.ensure_user(owner)
            created.append(owner)
        conversation = await store.conversation(A, A, 710, "Справочник")
        other = await store.conversation(A, A, 711, "Другая тема")
        foreign = await store.conversation(B, B, 712, "Чужая тема")
        yield SimpleNamespace(store=store, conversation=conversation, other=other, foreign=foreign)
    finally:
        for owner in created:
            async with store.connection(owner) as conn:
                for table in (
                    "operations",
                    "messages",
                    "memory",
                    "projects",
                    "artifacts",
                    "ledger",
                    "conversations",
                    "users",
                ):
                    await conn.execute(f"DELETE FROM {table} WHERE user_id=$1", owner)
                await conn.execute("DELETE FROM events WHERE payload->>'user_id'=$1", str(owner))
        await store.close()


async def message(case, content, *, conversation=None, excluded=False):
    conversation = conversation or case.conversation
    async with case.store.connection(conversation["user_id"]) as conn:
        return str(
            await conn.fetchval(
                """INSERT INTO messages(user_id,conversation_id,role,content,excluded)
            VALUES($1,$2,'user',$3,$4) RETURNING id""",
                conversation["user_id"],
                conversation["id"],
                content,
                excluded,
            )
        )


async def artifact(case, text="", *, owner=A, filename="document.txt", extracted=None):
    artifact_id = uuid4()
    await case.store.save_artifact(
        owner,
        {
            "id": str(artifact_id),
            "filename": filename,
            "mime": "text/plain",
            "path": f"/synthetic/library/{artifact_id}",
            "extracted": {"text": text, **(extracted or {})},
        },
    )
    return str(artifact_id)


async def project(case, *, conversation=None, name="Цены овощей", goal="Собрать свежие цены"):
    conversation = conversation or case.conversation
    return await case.store.create_project(
        conversation["user_id"],
        conversation["id"],
        {"name": name, "goal": goal},
        f"library-project:{uuid4()}",
    )


@pytest.mark.parametrize("kind", ["message", "artifact", "project"])
async def test_owner_isolation_in_search_and_direct_reads(case, kind):
    if kind == "message":
        own = await message(case, "уникальный маркер")
        foreign = await message(case, "уникальный маркер", conversation=case.foreign)
    elif kind == "artifact":
        own = await artifact(case, "уникальный маркер")
        foreign = await artifact(case, "уникальный маркер", owner=B)
    else:
        own = (await project(case, name="уникальный маркер"))["id"]
        foreign = (await project(case, name="уникальный маркер", conversation=case.foreign))["id"]
    result = await case.store.library_search(A, "уникальный маркер", kinds=[kind])
    assert [hit["id"] for hit in result["hits"]] == [own]
    assert (await case.store.library_read(A, kind, own))["text"]
    with pytest.raises(ValueError, match="unavailable"):
        await case.store.library_read(A, kind, foreign)


async def test_russian_uppercase_inflection_and_relaxed_search_on_locale_c(case):
    target = await message(case, "Свежие цены огурцов: 130 рублей. ЦЕЛЬ достигнута.")
    for query in ("ОГУРЦЫ", "цель"):
        found = await case.store.library_search(A, query, kinds=["message"])
        assert found["match_mode"] == "full_text"
        assert found["hits"][0]["id"] == target
    relaxed = await case.store.library_search(A, "огурцы невероятнонесуществующиймаркер")
    assert relaxed["match_mode"] == "relaxed"
    assert [hit["id"] for hit in relaxed["hits"]] == [target]


async def test_literal_substring_fallback_is_casefolded_and_wildcards_are_escaped(case):
    target = await message(case, "ЦЕНООБРАЗОВАНИЕ; дословная строка 90%_готово.")
    result = await case.store.library_search(A, "ЦЕНООБРАЗОВ")
    assert result["match_mode"] == "substring"
    assert [hit["id"] for hit in result["hits"]] == [target]
    literal = await case.store.library_search(A, "%_")
    assert literal["match_mode"] == "substring" and literal["hits"][0]["id"] == target
    assert not (await case.store.library_search(A, "_%"))["hits"]
    assert not (await case.store.library_search(A, "'; DROP TABLE users; --"))["hits"]


async def test_message_multimodal_blocks_index_only_the_visible_text(case):
    target = await message(
        case,
        [
            {"type": "text", "text": "Договор «Овощи»"},
            {"type": "image_ref", "artifact_id": "secretinternalmarker"},
            {"type": "text", "text": "Сумма: 12 300 ₽."},
        ],
    )
    result = await case.store.library_search(A, "договор")
    assert result["hits"][0]["id"] == target
    text = (await case.store.library_read(A, "message", target))["text"]
    assert text == "Договор «Овощи»\nСумма: 12 300 ₽."
    assert not (await case.store.library_search(A, "secretinternalmarker"))["hits"]


async def test_project_scope_uses_only_bound_conversations_and_attached_files(case):
    current = await project(case)
    await project(case, conversation=case.other, name="Цены чужого проекта")
    own_message = await message(case, "Цены огурцов")
    await message(case, "Цены огурцов", conversation=case.other)
    attached = await artifact(case, "Цены огурцов")
    await artifact(case, "Цены огурцов")
    await case.store.attach_project_artifact(A, current["id"], attached)
    scoped = await case.store.library_search(A, "цены", project_id=current["id"])
    assert {(hit["kind"], hit["id"]) for hit in scoped["hits"]} == {
        ("message", own_message),
        ("artifact", attached),
        ("project", current["id"]),
    }
    foreign = await project(case, conversation=case.foreign)
    with pytest.raises(ValueError, match="unavailable"):
        await case.store.library_search(A, "цены", project_id=foreign["id"])


async def test_forget_invalidates_saved_message_and_project_refs_but_keeps_original_file(case):
    current = await project(case, name="Забываемый секрет")
    old_message = await message(case, "Забываемый секрет")
    original = await artifact(case, "Забываемый секрет")
    await case.store.remember(A, "Забываемый секрет")
    assert len((await case.store.library_search(A, "Забываемый секрет"))["hits"]) == 3
    await case.store.forget(A, "Забываемый секрет")
    remaining = await case.store.library_search(A, "Забываемый секрет")
    assert [(hit["kind"], hit["id"]) for hit in remaining["hits"]] == [("artifact", original)]
    for kind, source_id in (("message", old_message), ("project", current["id"])):
        with pytest.raises(ValueError, match="unavailable"):
            await case.store.library_read(A, kind, source_id)
    with pytest.raises(ValueError, match="unavailable"):
        await case.store.library_search(A, "секрет", project_id=current["id"])


async def test_quote_offsets_roundtrip_to_unchanged_original_source(case):
    text = ("Вступление без ключа. " * 40) + "Феникс: «текст», **формат**, 42% и emoji 🪐.\nКонец."
    source_id = await artifact(case, text)
    hit = (await case.store.library_search(A, "Феникс"))["hits"][0]
    assert hit["excerpt_offset"] > 0 and "«текст», **формат**" in hit["excerpt"]
    read = await case.store.library_read(
        A,
        "artifact",
        source_id,
        offset=hit["excerpt_offset"],
        length=len(hit["excerpt"]),
    )
    assert read["text"] == hit["excerpt"]
    assert hit["ref"] == f"artifact:{source_id}"
    assert hit["created_at"] and hit["date_kind"] == "uploaded_at"
    assert hit["page"] is None and hit["paragraph"] is None


async def test_pdf_page_provenance_and_truncation_use_only_stored_metadata(case):
    first, second = "Первая страница.", "Огурцы на второй странице."
    text = first + "\n\n" + second
    source_id = await artifact(
        case,
        text,
        filename="report.pdf",
        extracted={
            "text_truncated": True,
            "text_total_chars": len(text) + 500,
            "pages": [
                {"page": 1, "text": first, "text_total_chars": len(first)},
                {"page": 2, "text": second, "text_total_chars": len(second)},
            ],
        },
    )
    hit = (await case.store.library_search(A, "огурцы"))["hits"][0]
    assert hit["page"] == 1 and hit["page_end"] == 2 and hit["match_page"] == 2
    assert hit["paragraph"] is None
    read = await case.store.library_read(A, "artifact", source_id, offset=len(first) + 2, length=5)
    assert read["text"] == "Огурц"
    assert read["source"]["page"] == read["source"]["page_end"] == 2
    assert read["has_more"] and read["next_offset"] == len(first) + 7
    tail = await case.store.library_read(A, "artifact", source_id, offset=read["next_offset"])
    assert tail["has_more"] is False and tail["next_offset"] is None
    assert tail["source"]["source_truncated"] is True
    assert tail["source"]["available_chars"] == len(text)
    assert tail["source"]["total_source_chars"] == len(text) + 500


async def test_metadata_only_file_is_searchable_without_inventing_ocr_text_or_pages(case):
    source_id = await artifact(case, filename="ОГУРЦЫ.png", extracted={"needs_ocr": True})
    hit = (await case.store.library_search(A, "огурцы"))["hits"][0]
    assert hit["id"] == source_id and hit["excerpt"] == "" and hit["needs_ocr"] is True
    read = await case.store.library_read(A, "artifact", source_id)
    assert read["text"] == "" and read["source"]["page"] is None


async def test_pagination_is_stable_and_exhaustion_does_not_switch_to_relaxed_matches(case):
    first = await message(case, "Огурцы Москва")
    second = await message(case, "Огурцы Москва")
    await message(case, "Огурцы Казань")
    result = await case.store.library_search(A, "огурцы Москва", limit=1)
    assert result["has_more"] is True and result["next_offset"] == 1
    next_page = await case.store.library_search(A, "огурцы Москва", limit=1, offset=1)
    assert {result["hits"][0]["id"], next_page["hits"][0]["id"]} == {first, second}
    assert next_page["has_more"] is False
    exhausted = await case.store.library_search(A, "огурцы Москва", limit=1, offset=2)
    assert exhausted["hits"] == [] and exhausted["match_mode"] == "full_text"


async def test_implicit_recall_reads_only_older_current_conversation_visible_messages(case):
    old = await message(case, "Огурцы: старое решение")
    for number in range(30):
        await message(case, f"Огурцы: недавнее сообщение {number}")
    await message(case, "Огурцы: другая тема", conversation=case.other)
    await message(case, "Огурцы: чужой аккаунт", conversation=case.foreign)
    found = await case.store.conversation_recall(A, case.conversation["id"], "огурцы")
    assert [hit["id"] for hit in found] == [old]
    assert found[0]["conversation_id"] == str(case.conversation["id"])
    async with case.store.connection(A) as conn:
        await conn.execute("UPDATE messages SET excluded=true WHERE id=$1", int(old))
    assert await case.store.conversation_recall(A, case.conversation["id"], "огурцы") == []
    assert await case.store.conversation_recall(A, case.foreign["id"], "огурцы") == []


async def test_deleted_original_rows_disappear_without_secondary_content_cleanup(case):
    message_id = await message(case, "Библиотечный маркер")
    artifact_id = await artifact(case, "Библиотечный маркер")
    current = await project(case, name="Библиотечный маркер")
    async with case.store.connection(A) as conn:
        await conn.execute("DELETE FROM messages WHERE id=$1", int(message_id))
        await conn.execute("DELETE FROM artifacts WHERE id=$1", UUID(artifact_id))
        await conn.execute("DELETE FROM projects WHERE id=$1", UUID(current["id"]))
    assert not (await case.store.library_search(A, "Библиотечный маркер"))["hits"]


async def test_all_source_gin_indexes_exist(case):
    async with case.store.connection(A) as conn:
        indexes = await conn.fetch(
            "SELECT indexname,indexdef FROM pg_indexes WHERE indexname LIKE '%_library_%'"
        )
        owner_index = await conn.fetchval(
            "SELECT indexdef FROM pg_indexes WHERE indexname='messages_visible_owner'"
        )
    assert {row["indexname"] for row in indexes} == {
        f"{table}_library_{config}"
        for table in ("messages", "artifacts", "projects")
        for config in ("simple", "russian")
    }
    assert all("USING gin" in row["indexdef"] for row in indexes)
    assert "(user_id, conversation_id, id DESC)" in owner_index
    assert "WHERE (NOT excluded)" in owner_index


@pytest.mark.parametrize(
    "kwargs",
    [
        {"query": " "},
        {"query": "x" * 1001},
        {"query": "x", "limit": True},
        {"query": "x", "limit": 31},
        {"query": "x", "offset": -1},
        {"query": "x", "kinds": []},
        {"query": "x", "kinds": ["unknown"]},
    ],
)
async def test_query_bounds_are_rejected(case, kwargs):
    with pytest.raises(ValueError):
        await case.store.library_search(A, **kwargs)
