"""Owner-scoped source search and exact excerpts; original rows remain authoritative."""

import re
from contextlib import AbstractAsyncContextManager
from uuid import UUID

import asyncpg

LIBRARY_KINDS = frozenset({"message", "artifact", "project"})
_WORDS = re.compile(r"[^\W_]+", re.UNICODE)
_SOURCES = {
    "message": """
      SELECT 'message'::text AS kind,m.id::text AS id,c.title,
        cronos_library_message_body(m.content) AS body,m.created_at,m.conversation_id,
        jsonb_build_object('role',m.role) AS metadata,
        cronos_library_vector('simple'::regconfig,'',cronos_library_message_body(m.content)) AS vector_simple,
        cronos_library_vector('russian'::regconfig,'',cronos_library_message_body(m.content)) AS vector_russian
      FROM messages m JOIN conversations c ON c.id=m.conversation_id AND c.user_id=m.user_id
      WHERE m.user_id=$1 AND NOT m.excluded
        AND ($3::uuid IS NULL OR EXISTS(SELECT 1 FROM project_conversations pc
             WHERE pc.user_id=$1 AND pc.project_id=$3 AND pc.conversation_id=m.conversation_id))
    """,
    "artifact": """
      SELECT 'artifact'::text AS kind,a.id::text AS id,a.filename AS title,
        coalesce(a.extracted->>'text','') AS body,a.created_at,NULL::uuid AS conversation_id,
        (a.extracted-'text'-'tables'-'formulas') || jsonb_build_object('mime',a.mime) AS metadata,
        cronos_library_vector('simple'::regconfig,a.filename,coalesce(a.extracted->>'text','')) AS vector_simple,
        cronos_library_vector('russian'::regconfig,a.filename,coalesce(a.extracted->>'text','')) AS vector_russian
      FROM artifacts a WHERE a.user_id=$1
        AND ($3::uuid IS NULL OR EXISTS(SELECT 1 FROM project_artifacts pa
             WHERE pa.user_id=$1 AND pa.project_id=$3 AND pa.artifact_id=a.id))
    """,
    "project": """
      SELECT 'project'::text AS kind,p.id::text AS id,p.name AS title,
        cronos_library_project_body(p.name,p.goal,p.state) AS body,p.created_at,
        NULL::uuid AS conversation_id,jsonb_build_object('revision',p.revision,'status',p.status) AS metadata,
        cronos_library_vector('simple'::regconfig,p.name,cronos_library_project_body(p.name,p.goal,p.state)) AS vector_simple,
        cronos_library_vector('russian'::regconfig,p.name,cronos_library_project_body(p.name,p.goal,p.state)) AS vector_russian
      FROM projects p WHERE p.user_id=$1 AND NOT p.context_excluded
        AND ($3::uuid IS NULL OR p.id=$3)
    """,
}


def _bounded(value, name, minimum, maximum):
    if not isinstance(value, int) or isinstance(value, bool) or not minimum <= value <= maximum:
        raise ValueError(f"{name} must be between {minimum} and {maximum}")
    return value


def _query(value: str) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > 1000:
        raise ValueError("Search query must contain 1-1000 characters")
    return value.strip()


def _identifier(kind, value):
    if kind not in LIBRARY_KINDS:
        raise ValueError("Unknown library source kind")
    if kind == "message":
        if isinstance(value, bool) or not str(value).isdigit() or int(value) <= 0:
            raise ValueError("A positive message id is required")
        return str(int(value))
    return str(UUID(str(value)))


def _snippet(body: str, query: str, length=420) -> tuple[str, int, int]:
    words = sorted(set(_WORDS.findall(query)), key=len, reverse=True)
    match = (
        re.search("|".join(re.escape(word) for word in words), body, re.IGNORECASE)
        if words
        else None
    )
    anchor = match.start() if match else 0
    start = max(0, anchor - 100)
    return body[start : start + length], start, anchor


def _page(metadata, offset):
    """Only stored PDF page extraction establishes a page; never infer paragraphs."""
    position = 0
    for page in metadata.get("pages") or []:
        if not isinstance(page, dict):
            return None
        text = page.get("text")
        total = page.get("text_total_chars")
        if not isinstance(text, str) or not isinstance(total, int) or total < len(text):
            return None
        if position <= offset < position + len(text):
            number = page.get("page")
            return (
                number
                if isinstance(number, int) and not isinstance(number, bool) and number > 0
                else None
            )
        position += total + 2  # ArtifactManager joins PDF page bodies with exactly two newlines.
    return None


def _source(row, offset=0, end=None) -> dict:
    body, metadata = row["body"], row["metadata"] or {}
    created_at = row["created_at"]
    result = {
        "kind": row["kind"],
        "id": row["id"],
        "ref": f"{row['kind']}:{row['id']}",
        "title": row["title"],
        "created_at": created_at.isoformat() if created_at is not None else None,
        "date_kind": "uploaded_at" if row["kind"] == "artifact" else "recorded_at",
        "conversation_id": str(row["conversation_id"])
        if row["conversation_id"] is not None
        else None,
        "available_chars": len(body),
        "source_truncated": metadata.get("text_truncated") is True,
        "total_source_chars": metadata.get("text_total_chars", len(body)),
        "page": _page(metadata, offset),
        "paragraph": None,
    }
    if end is not None and end > offset:
        result["page_end"] = _page(metadata, end - 1)
    if row["kind"] == "artifact":
        result.update(mime=metadata.get("mime"), needs_ocr=metadata.get("needs_ocr") is True)
    return result


def _sql(kinds, mode, *, recall=False, exists=False):
    sources = " UNION ALL ".join(_SOURCES[kind] for kind in sorted(kinds))
    if mode == "substring":
        query_cte = ""
        condition = "cronos_library_fold(s.body) LIKE $2 ESCAPE '\\' OR cronos_library_fold(s.title) LIKE $2 ESCAPE '\\'"
        rank, query_join = "0::real", ""
    else:
        query_cte = "q AS (SELECT websearch_to_tsquery('simple',cronos_library_fold($2)) qs, websearch_to_tsquery('russian',cronos_library_fold($2)) qr),"
        condition = "s.vector_simple @@ q.qs OR s.vector_russian @@ q.qr"
        rank = "greatest(ts_rank_cd(s.vector_simple,q.qs),ts_rank_cd(s.vector_russian,q.qr))"
        query_join = "CROSS JOIN q"
    recall_filter = ""
    if recall:
        recall_filter = """AND s.conversation_id=$6::uuid AND s.id::bigint < (
          SELECT min(recent.id) FROM (SELECT id FROM messages WHERE user_id=$1
            AND conversation_id=$6::uuid AND NOT excluded ORDER BY id DESC LIMIT 30) recent)"""
    source_cte = f"WITH {query_cte} sources AS NOT MATERIALIZED ({sources})"
    base = f"FROM sources s {query_join} WHERE ({condition}) {recall_filter}"
    if exists:
        # Keep parameter types/arity identical to the paginated query.
        return f"{source_cte} SELECT EXISTS(SELECT 1 {base} AND $4::int>0 AND $5::int>=0)"
    return f"""{source_cte} SELECT s.kind,s.id,s.title,s.body,s.created_at,s.conversation_id,
      s.metadata,{rank} AS rank {base}
      ORDER BY rank DESC,created_at DESC NULLS LAST,s.kind,s.id LIMIT $4 OFFSET $5"""


class LibraryStoreMixin:
    def connection(
        self, user_id: int | None = None
    ) -> AbstractAsyncContextManager[asyncpg.Connection]:
        """Implemented by Store; all source access remains under the owner's RLS."""
        raise NotImplementedError

    async def library_search(
        self, user_id: int, query: str, *, kinds=None, project_id=None, limit=10, offset=0
    ) -> dict:
        query = _query(query)
        limit, offset = _bounded(limit, "limit", 1, 30), _bounded(offset, "offset", 0, 10000)
        if kinds is None:
            kinds = LIBRARY_KINDS
        elif (
            not isinstance(kinds, (list, tuple, set, frozenset))
            or not kinds
            or any(kind not in LIBRARY_KINDS for kind in kinds)
        ):
            raise ValueError("kinds must contain message, artifact or project")
        project = UUID(str(project_id)) if project_id is not None else None
        async with self.connection(user_id) as conn:
            if project is not None:
                allowed = await conn.fetchval(
                    "SELECT id FROM projects WHERE user_id=$1 AND id=$2 AND NOT context_excluded",
                    user_id,
                    project,
                )
                if allowed is None:
                    raise ValueError("Project source is unavailable")
            rows, mode = await self._library_rows(
                conn, user_id, query, kinds, project, limit, offset
            )
        hits = []
        for row in rows[:limit]:
            excerpt, start, anchor = _snippet(row["body"], query)
            hits.append(
                {
                    **_source(row, start, start + len(excerpt)),
                    "match_page": _page(row["metadata"] or {}, anchor),
                    "excerpt": excerpt,
                    "excerpt_offset": start,
                    "rank": float(row["rank"]),
                }
            )
        has_more = len(rows) > limit
        return {
            "hits": hits,
            "has_more": has_more,
            "next_offset": offset + len(hits) if has_more else None,
            "match_mode": mode,
        }

    async def _library_rows(
        self, conn, user_id, query, kinds, project, limit, offset, conversation_id=None
    ):
        recall = conversation_id is not None
        extra = (UUID(str(conversation_id)),) if recall else ()
        words = list(dict.fromkeys(word.casefold() for word in _WORDS.findall(query)))[:20]
        candidates = [("full_text", query.casefold())]
        if words:
            candidates.append(("relaxed", " OR ".join('"' + word + '"' for word in words)))
        literal = query.casefold().replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        candidates.append(("substring", "%" + literal + "%"))
        for mode, search_query in candidates:
            parameters = (user_id, search_query, project, limit + 1, offset, *extra)
            rows = await conn.fetch(_sql(kinds, mode, recall=recall), *parameters)
            if rows:
                return rows, mode
            if offset and await conn.fetchval(
                _sql(kinds, mode, recall=recall, exists=True), *parameters
            ):
                return [], mode
        return [], candidates[-1][0]

    async def library_read(self, user_id: int, kind: str, id, offset=0, length=4000) -> dict:
        source_id = _identifier(kind, id)
        offset, length = (
            _bounded(offset, "offset", 0, 10_000_000),
            _bounded(length, "length", 1, 8000),
        )
        # Recheck ownership and exclusion at read time, including after a search hit was saved.
        sql = f"""WITH source AS ({_SOURCES[kind]}) SELECT * FROM source WHERE id=$2::text"""
        async with self.connection(user_id) as conn:
            row = await conn.fetchrow(sql, user_id, source_id, None)
        if row is None:
            raise ValueError("Library source is unavailable")
        body = row["body"]
        end = min(offset + length, len(body))
        return {
            "text": body[offset:end],
            "offset": offset,
            "next_offset": end if end < len(body) else None,
            "has_more": end < len(body),
            "source": _source(row, offset, end),
        }

    async def conversation_recall(
        self, user_id: int, conversation_id, query: str, limit=3
    ) -> list[dict]:
        query = _query(query)
        limit = _bounded(limit, "limit", 1, 5)
        async with self.connection(user_id) as conn:
            rows, _ = await self._library_rows(
                conn, user_id, query, {"message"}, None, limit, 0, conversation_id
            )
        result = []
        for row in rows[:limit]:
            excerpt, start, anchor = _snippet(row["body"], query)
            result.append(
                {
                    **_source(row, start, start + len(excerpt)),
                    "match_page": _page(row["metadata"] or {}, anchor),
                    "excerpt": excerpt,
                    "excerpt_offset": start,
                }
            )
        return result
