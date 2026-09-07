"""Invalidate derived context without deleting original files or financial records."""


async def invalidate_memory_context(conn, user_id: int) -> dict[str, int]:
    """Participate in Store.forget's transaction under the user's delivery lock.

    The caller acquires the delivery advisory lock before opening this transaction.
    A sender which claimed an old row re-reads it under that same lock and therefore
    cannot send its stale in-memory payload after this transaction commits. Messages
    already being sent finish before the forget operation obtains the lock.

    Project readers must honor context_excluded and context_reset_revision. The first
    explicit edit afterwards starts fresh state; it must not merge the retained old
    state or expose change-history records at or before the reset revision.
    """
    if not conn.is_in_transaction():
        raise RuntimeError("Memory invalidation requires the forget transaction")
    owner = await conn.fetchval("SELECT user_id FROM users WHERE user_id=$1 FOR UPDATE", user_id)
    if owner is None:
        raise ValueError("Memory owner does not exist")
    deliveries = await conn.fetch(
        """UPDATE outbox SET state='cancelled',payload='{}',owner=NULL,
        lease_until=NULL,error=NULL
        WHERE user_id=$1 AND state IN ('pending','sending','cancelled')
        RETURNING id""",
        user_id,
    )
    projects = await conn.fetch(
        """UPDATE projects SET context_excluded=true,context_reset_revision=revision+1,
        revision=revision+1,updated_at=clock_timestamp()
        WHERE user_id=$1 RETURNING id""",
        user_id,
    )
    return {"cleared_deliveries": len(deliveries), "excluded_projects": len(projects)}
