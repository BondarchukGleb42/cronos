import pytest
from aiogram.types import MessageEntity

from cronos.captions import caption_chunks, render_caption


def entity_text(text, entity):
    data = text.encode("utf-16-le")
    return data[entity["offset"] * 2 : (entity["offset"] + entity["length"]) * 2].decode(
        "utf-16-le"
    )


def assert_valid(text, entities):
    size = len(text.encode("utf-16-le")) // 2
    for entity in entities:
        MessageEntity.model_validate(entity)
        assert 0 <= entity["offset"] < size
        assert 0 < entity["length"] <= size - entity["offset"]
        assert entity_text(text, entity)


def test_phonix_caption_has_no_literal_markers_and_uses_utf16_after_emoji():
    text, entities = render_caption("🚀 **Phonix** — готово!")
    assert text == "🚀 Phonix — готово!"
    assert entities == [{"type": "bold", "offset": 3, "length": 6}]


@pytest.mark.parametrize(
    ("source", "plain", "kind"),
    [
        ("**жирный**", "жирный", "bold"),
        ("__жирный__", "жирный", "bold"),
        ("*курсив*", "курсив", "italic"),
        ("_курсив_", "курсив", "italic"),
        ("~~удалено~~", "удалено", "strikethrough"),
        ("||спойлер||", "спойлер", "spoiler"),
        ("`**literal** <b>x</b>`", "**literal** <b>x</b>", "code"),
        ("`` `x` ``", "`x`", "code"),
        ("```python\nprint('😀')\n```", "print('😀')", "pre"),
    ],
)
def test_supported_markdown_uses_entities(source, plain, kind):
    text, entities = render_caption(source)
    assert text == plain
    assert [entity["type"] for entity in entities] == [kind]
    assert entity_text(text, entities[0]) == plain
    if kind == "pre":
        assert entities[0]["language"] == "python"
    assert_valid(text, entities)


def test_nested_emphasis_and_code_have_legal_nonoverlapping_code_ranges():
    text, entities = render_caption("**Сначала *курсив* и `код` потом** — ***оба***")
    assert text == "Сначала курсив и код потом — оба"
    assert {entity["type"] for entity in entities} == {"bold", "italic", "code"}
    assert [entity_text(text, entity) for entity in entities if entity["type"] == "bold"] == [
        "Сначала курсив и ",
        " потом",
        "оба",
    ]
    code = next(entity for entity in entities if entity["type"] == "code")
    for entity in entities:
        if entity is not code:
            assert (
                entity["offset"] + entity["length"] <= code["offset"]
                or entity["offset"] >= code["offset"] + code["length"]
            )
    assert_valid(text, entities)


def test_heading_quote_and_nested_bold_keep_line_breaks():
    text, entities = render_caption("## Итог\n\n> Цитата **важно**\n> Вторая строка\n\nКонец")
    assert text == "Итог\n\nЦитата важно\nВторая строка\n\nКонец"
    assert [(entity["type"], entity_text(text, entity)) for entity in entities] == [
        ("bold", "Итог"),
        ("blockquote", "Цитата важно\nВторая строка"),
        ("bold", "важно"),
    ]
    assert_valid(text, entities)


def test_link_preserves_balanced_url_and_nested_bold_label():
    text, entities = render_caption("😀 [**Документ**](https://example.com/wiki/A_(B))")
    assert text == "😀 Документ"
    link = next(entity for entity in entities if entity["type"] == "text_link")
    assert link == {
        "type": "text_link",
        "offset": 3,
        "length": 8,
        "url": "https://example.com/wiki/A_(B)",
    }
    assert any(entity["type"] == "bold" and entity["offset"] == 3 for entity in entities)
    assert_valid(text, entities)


def test_quoted_link_remains_clickable_when_quote_entity_cannot_contain_it():
    text, entities = render_caption("> Источник [документ](https://example.com)")
    assert text == "Источник документ"
    assert entities == [
        {
            "type": "text_link",
            "offset": 9,
            "length": 8,
            "url": "https://example.com",
        }
    ]


@pytest.mark.parametrize(
    "source",
    [
        "<b>literal HTML</b> &amp; <script>alert(1)</script>",
        "[bad](javascript:alert(1))",
        "[bad](https://user:password@example.com/private)",
        "[bad](https:///missing-host)",
        "Незакрытый **Phonix",
        "Незакрытый `code",
        "```python\nunfinished **bold**",
        "[Незакрытая](https://example.com",
        "snake_case_name and @user_name",
    ],
)
def test_html_unsafe_links_and_incomplete_syntax_remain_literal(source):
    assert render_caption(source) == (source, [])


def test_markdown_escapes_do_not_reactivate_delimiters():
    assert render_caption(r"\*\*буквально\*\* и \_имя\_") == ("**буквально** и _имя_", [])


def coverage(text, entities, offset=0):
    return {
        (entity["type"], entity.get("url"), entity.get("language"), offset + index)
        for entity in entities
        for index in range(entity["offset"], entity["offset"] + entity["length"])
    }


@pytest.mark.parametrize("limit", [2, 7, 31, 1024])
def test_chunks_are_lossless_and_preserve_cross_boundary_entities(limit):
    source = (
        "**"
        + "Я😀 " * 400
        + "**\n```python\n"
        + "print('😀')\n" * 10
        + "```\n[длинная ссылка](https://example.com)"
    )
    text, entities = render_caption(source)
    chunks = caption_chunks(source, limit=limit)
    assert "".join(chunk["text"] for chunk in chunks) == text
    offset, actual = 0, set()
    for chunk in chunks:
        width = len(chunk["text"].encode("utf-16-le")) // 2
        assert 0 < width <= limit
        assert_valid(chunk["text"], chunk["entities"])
        actual |= coverage(chunk["text"], chunk["entities"], offset)
        offset += width
    assert actual == coverage(text, entities)


def test_caption_budget_applies_after_render_not_to_raw_markdown():
    chunks = caption_chunks("**" + "😀" * 512 + "**")
    assert len(chunks) == 1
    assert chunks[0] == {
        "text": "😀" * 512,
        "entities": [{"type": "bold", "offset": 0, "length": 1024}],
    }


def test_empty_and_invalid_chunk_limits():
    assert render_caption("") == ("", [])
    assert caption_chunks("") == []
    for limit in (0, 1, -1, 1.5):
        with pytest.raises(ValueError):
            caption_chunks("text", limit)
