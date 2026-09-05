import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

from cronos.gateway import create_app


async def test_readiness_requires_successful_telegram_poll(monkeypatch):
    monkeypatch.setattr(time, "monotonic", lambda: 42.0)
    app = create_app()
    ready = next(
        route.endpoint for route in app.routes if getattr(route, "path", None) == "/readyz"
    )
    app.state.poll_task = SimpleNamespace(done=lambda: False)
    app.state.store = SimpleNamespace(ready=AsyncMock(return_value=True))
    app.state.last_poll_success = 0
    assert (await ready()).status_code == 503
    app.state.last_poll_success = time.monotonic()
    assert (await ready()).status_code == 200
    app.state.last_poll_success = time.monotonic() - 91
    assert (await ready()).status_code == 503
