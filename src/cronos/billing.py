"""Reconcile unknown provider charges without holding database locks over HTTP."""

import asyncio
import logging
from datetime import UTC, datetime, timedelta

from cronos.providers import Provider, ProviderError

log = logging.getLogger(__name__)


class Reconciler:
    def __init__(self, settings, store, *, provider=None):
        self.store = store
        self.provider = provider if provider is not None else Provider(settings)

    async def close(self):
        await self.provider.close()

    async def tick(self):
        result = {"processed": 0, "settled": 0, "released": 0, "pending": 0, "errors": 0}
        for reservation in await self.store.pending_reservations():
            result["processed"] += 1
            try:
                user_id, operation_id = reservation["user_id"], reservation["id"]
                stored = await self.store.reconciliation_usage(user_id, operation_id)
                original = stored["raw"] if stored else {}
                request_id = original.get("request_id")
                receipt = None
                if isinstance(request_id, str) and request_id:
                    try:
                        async with asyncio.timeout(20):
                            receipt = await self.provider.receipt(request_id)
                    except ProviderError, TimeoutError:
                        pass
                    except Exception as error:
                        # A malformed receipt must not extend the user's 24-hour hold.
                        result["errors"] += 1
                        log.warning("Provider receipt unavailable: %s", type(error).__name__)
                if receipt is not None and receipt.get("cost_rub") is not None:
                    receipt = {
                        **original,
                        **receipt,
                        "request_id": request_id,
                        "model": receipt.get("model") or (stored or {}).get("model") or "unknown",
                        "reconciliation": "reconciled",
                    }
                    await self.store.record_usage(user_id, stored["run_id"], operation_id, receipt)
                    result["settled"] += 1
                elif datetime.now(UTC) - reservation["created_at"] >= timedelta(hours=24):
                    released = await self.store.release_unresolved_reservation(
                        user_id, operation_id
                    )
                    result["released" if released else "pending"] += 1
                else:
                    result["pending"] += 1
            except Exception as error:
                result["errors"] += 1
                log.warning("Billing reconciliation delayed: %s", type(error).__name__)
        return result
