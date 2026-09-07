"""A small Markdown subset rendered to literal text and Telegram UTF-16 entities.

No HTML or Telegram parse mode is used. Unsupported or incomplete syntax stays
readable as text; this is intentionally not a complete CommonMark parser.
"""

import re
import string
from collections.abc import Sequence
from typing import Any
from urllib.parse import urlsplit

_STYLES = {"bold", "italic", "underline", "strikethrough", "spoiler"}
_MARKERS = (
    ("***", ("bold", "italic")),
    ("___", ("bold", "italic")),
    ("**", ("bold",)),
    ("__", ("bold",)),
    ("~~", ("strikethrough",)),
    ("||", ("spoiler",)),
    ("*", ("italic",)),
    ("_", ("italic",)),
)
_FENCE = re.compile(r"^ {0,3}(`{3,}|~{3,})([^\r\n]*)$")
_HEADING = re.compile(r"^ {0,3}#{1,6}[ \t]+(.+?)\s*$")
_QUOTE = re.compile(r"^ {0,3}>[ \t]?")


def _units(text: str) -> int:
    return sum(2 if ord(char) > 0xFFFF else 1 for char in text)


def _escaped(text: str, position: int) -> bool:
    count = 0
    while position > 0 and text[position - 1] == "\\":
        count += 1
        position -= 1
    return bool(count % 2)


def _code_end(text: str, start: int, marker: str) -> int:
    position = start
    while (position := text.find(marker, position)) >= 0:
        end = position + len(marker)
        if (position == 0 or text[position - 1] != "`") and (end == len(text) or text[end] != "`"):
            return position
        position = end
    return -1


def _closing_marker(text: str, start: int, marker: str) -> int:
    position = start
    while position < len(text):
        if text[position] == "`" and not _escaped(text, position):
            run = len(text[position:]) - len(text[position:].lstrip("`"))
            end = _code_end(text, position + run, "`" * run)
            if end >= 0:
                position = end + run
                continue
        if text.startswith(marker, position) and not _escaped(text, position):
            end = position + len(marker)
            if marker[0] in "*_":
                while end < len(text) and text[end] == marker[0]:
                    end += 1
                if len(marker) == 1 and end - position != 1:
                    position = end
                    continue
                position = end - len(marker)
            if (
                position > start
                and not text[position - 1].isspace()
                and not (marker[0] == "_" and end < len(text) and text[end].isalnum())
            ):
                return position
        position += 1
    return -1


def _link(text: str, start: int) -> tuple[str, str, int] | None:
    label_end = start + 1
    while label_end < len(text):
        if text[label_end] == "]" and not _escaped(text, label_end):
            break
        label_end += 1
    if not text.startswith("](", label_end):
        return None
    position, depth = label_end + 2, 1
    while position < len(text) and depth:
        if not _escaped(text, position):
            depth += (text[position] == "(") - (text[position] == ")")
        position += 1
    if depth:
        return None
    url = re.sub(r"\\([\\()])", r"\1", text[label_end + 2 : position - 1])
    try:
        parsed = urlsplit(url)
        safe = (
            parsed.scheme in {"http", "https"}
            and bool(parsed.hostname)
            and parsed.username is None
            and parsed.password is None
            and not any(char.isspace() or ord(char) < 32 for char in url)
        )
    except ValueError:
        safe = False
    if not safe:
        return None
    return text[start + 1 : label_end], url, position


class _Text:
    def __init__(self):
        self.parts: list[str] = []
        self.entities: list[dict[str, Any]] = []
        self.length = 0

    def add(self, text: str, entities: Sequence[dict] = (), kinds: tuple[str, ...] = (), **extra):
        length = _units(text)
        self.parts.append(text)
        self.entities.extend(
            {**entity, "offset": entity["offset"] + self.length} for entity in entities
        )
        if length:
            self.entities.extend(
                {"type": kind, "offset": self.length, "length": length, **extra} for kind in kinds
            )
        self.length += length

    def result(self) -> tuple[str, list[dict]]:
        return "".join(self.parts), self.entities


def _inline(text: str, depth: int = 0, *, links: bool = True) -> tuple[str, list[dict]]:
    if depth > 12:
        return text, []
    output, position = _Text(), 0
    while position < len(text):
        char = text[position]
        if char == "\\" and position + 1 < len(text) and text[position + 1] in string.punctuation:
            output.add(text[position + 1])
            position += 2
            continue
        if char == "`":
            run = len(text[position:]) - len(text[position:].lstrip("`"))
            end = _code_end(text, position + run, "`" * run)
            if end >= 0:
                code = text[position + run : end].replace("\r\n", " ").replace("\n", " ")
                if code.startswith(" ") and code.endswith(" ") and code.strip():
                    code = code[1:-1]
                output.add(code, kinds=("code",))
                position = end + run
                continue
        if links and char == "[" and (position == 0 or text[position - 1] != "!"):
            link = _link(text, position)
            if link:
                label, url, end = link
                plain, entities = _inline(label, depth + 1, links=False)
                output.add(plain, entities, ("text_link",), url=url)
                position = end
                continue
        matched = False
        for marker, kinds in _MARKERS:
            if not text.startswith(marker, position):
                continue
            begin = position + len(marker)
            if (
                begin == len(text)
                or text[begin].isspace()
                or (marker[0] == "_" and position > 0 and text[position - 1].isalnum())
            ):
                continue
            end = _closing_marker(text, begin, marker)
            if end < 0:
                # Preserve unmatched delimiter runs instead of interpreting a
                # suffix of ** or *** as an unrelated italic opener.
                output.add(marker)
                position = begin
            else:
                plain, entities = _inline(text[begin:end], depth + 1, links=links)
                output.add(plain, entities, kinds)
                position = end + len(marker)
            matched = True
            break
        if not matched:
            output.add(char)
            position += 1
    return output.result()


def _ending(line: str) -> str:
    return "\r\n" if line.endswith("\r\n") else line[-1:] if line.endswith(("\r", "\n")) else ""


def _valid_entities(entities: list[dict]) -> list[dict]:
    """Code is exclusive; other nested entities follow Telegram's restrictions."""
    code = [
        (e["offset"], e["offset"] + e["length"]) for e in entities if e["type"] in {"code", "pre"}
    ]
    result = []
    for entity in entities:
        start, end = entity["offset"], entity["offset"] + entity["length"]
        if entity["type"] in _STYLES:
            segments = [(start, end)]
            for left, right in code:
                segments = [
                    part
                    for a, b in segments
                    for part in ((a, min(b, left)), (max(a, right), b))
                    if part[0] < part[1]
                ]
            result.extend({**entity, "offset": a, "length": b - a} for a, b in segments)
        elif entity["type"] in {"text_link", "blockquote"}:
            forbidden = [
                e
                for e in entities
                if e is not entity
                and e["type"] not in _STYLES
                and not (entity["type"] == "text_link" and e["type"] == "blockquote")
            ]
            if not any(
                start < other["offset"] + other["length"] and other["offset"] < end
                for other in forbidden
            ):
                result.append(entity)
        else:
            result.append(entity)
    return sorted(
        {tuple(sorted(entity.items())): entity for entity in result}.values(),
        key=lambda e: (e["offset"], -e["length"], e["type"]),
    )


def render_caption(text: str) -> tuple[str, list[dict]]:
    """Render supported Markdown; offsets and lengths are UTF-16 code units."""
    if not isinstance(text, str):
        raise TypeError("Caption must be a string")
    # Avoid quadratic work on pathological formatting while preserving its text.
    if len(text) > 64_000:
        return text, []
    lines, position, output = text.splitlines(keepends=True), 0, _Text()
    while position < len(lines):
        line = lines[position]
        ending = _ending(line)
        body = line[: -len(ending)] if ending else line
        fence = _FENCE.fullmatch(body)
        if fence:
            marker, language = fence.groups()
            closing = re.compile(
                r"^ {0,3}" + re.escape(marker[0]) + "{" + str(len(marker)) + r",}[ \t]*$"
            )
            end = position + 1
            while end < len(lines) and not closing.fullmatch(lines[end].rstrip("\r\n")):
                end += 1
            if end == len(lines):
                output.add("".join(lines[position:]))
                break
            code = "".join(lines[position + 1 : end])
            code_ending = _ending(code)
            code = code[: -len(code_ending)] if code_ending else code
            extra = (
                {"language": language.strip()}
                if re.fullmatch(r"[\w.+-]{1,40}", language.strip())
                else {}
            )
            output.add(code, kinds=("pre",), **extra)
            output.add(_ending(lines[end]))
            position = end + 1
            continue
        if _QUOTE.match(body):
            quote = []
            while position < len(lines) and _QUOTE.match(lines[position]):
                quote.append(_QUOTE.sub("", lines[position], count=1))
                position += 1
            quoted = "".join(quote)
            ending = _ending(quoted)
            quoted = quoted[: -len(ending)] if ending else quoted
            output.add(*_inline(quoted), kinds=("blockquote",))
            output.add(ending)
            continue
        heading = _HEADING.fullmatch(body)
        if heading:
            heading_text = re.sub(r"[ \t]+#+[ \t]*$", "", heading[1])
            output.add(*_inline(heading_text), kinds=("bold",))
            output.add(ending)
        else:
            output.add(*_inline(line))
        position += 1
    plain, entities = output.result()
    return plain, _valid_entities(entities)


def caption_chunks(text: str, limit: int = 1024) -> list[dict]:
    """Render once, then split losslessly and clip/rebase entities per chunk."""
    if not isinstance(limit, int) or limit < 2:
        raise ValueError("Caption chunk limit must be at least 2")
    plain, entities = render_caption(text)
    chunks, start, offset = [], 0, 0
    while start < len(plain):
        end, width, boundary = start, 0, start
        while end < len(plain):
            size = 2 if ord(plain[end]) > 0xFFFF else 1
            if width + size > limit:
                break
            width += size
            end += 1
            if plain[end - 1] in " \n\t":
                boundary = end
        if end < len(plain) and boundary > start + (end - start) // 2:
            end = boundary
        chunk = plain[start:end]
        width = _units(chunk)
        clipped = []
        for entity in entities:
            left = max(offset, entity["offset"])
            right = min(offset + width, entity["offset"] + entity["length"])
            if left < right:
                clipped.append({**entity, "offset": left - offset, "length": right - left})
        chunks.append({"text": chunk, "entities": clipped})
        start, offset = end, offset + width
    return chunks
