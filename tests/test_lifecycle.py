import asyncio
import signal
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from cronos.coordinator import Coordinator
from cronos.lifecycle import cancel_tasks, close_all, run_until_stopped
from cronos.worker import Worker


async def test_sigterm_cancels_service_once_and_waits_for_async_cleanup(monkeypatch):
    loop = asyncio.get_running_loop()
    add, remove = Mock(), Mock()
    monkeypatch.setattr(loop, "add_signal_handler", add)
    monkeypatch.setattr(loop, "remove_signal_handler", remove)
    started, cleanup_started, finish_cleanup = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def service():
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cleanup_started.set()
            await finish_cleanup.wait()

    task = asyncio.create_task(run_until_stopped(service()))
    await started.wait()
    assert add.call_args.args[0] == signal.SIGTERM
    stop = add.call_args.args[1]
    stop()
    await cleanup_started.wait()
    stop()  # A repeated signal must not interrupt cleanup.
    await asyncio.sleep(0)
    assert not task.done()
    finish_cleanup.set()
    await task
    assert task.cancelling() == 1
    remove.assert_called_once_with(signal.SIGTERM)


async def test_external_cancellation_is_not_swallowed(monkeypatch):
    loop = asyncio.get_running_loop()
    monkeypatch.setattr(loop, "add_signal_handler", Mock())
    monkeypatch.setattr(loop, "remove_signal_handler", Mock())
    started = asyncio.Event()

    async def service():
        started.set()
        await asyncio.Event().wait()

    task = asyncio.create_task(run_until_stopped(service()))
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_actual_sigterm_exits_process_after_cleanup():
    script = """
import asyncio
from cronos.lifecycle import run_until_stopped
async def service():
    print('ready', flush=True)
    try:
        await asyncio.Event().wait()
    finally:
        await asyncio.sleep(0)
        print('cleaned', flush=True)
asyncio.run(run_until_stopped(service()))
"""
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-u",
        "-c",
        script,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        assert process.stdout is not None
        async with asyncio.timeout(5):
            assert await process.stdout.readline() == b"ready\n"
            process.send_signal(signal.SIGTERM)
            stdout, stderr = await process.communicate()
        assert process.returncode == 0
        assert stdout == b"cleaned\n"
        assert stderr == b""
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()


async def test_cleanup_attempts_remaining_resources_after_first_failure():
    order = []

    async def broken():
        order.append("provider")
        raise RuntimeError("close failed")

    async def telegram():
        order.append("telegram")

    async def database():
        order.append("database")

    with pytest.raises(ExceptionGroup) as caught:
        await close_all(broken, telegram, database)
    assert order == ["provider", "telegram", "database"]
    assert len(caught.value.exceptions) == 1


async def test_already_cancelled_child_can_finish_its_cleanup():
    started, cleanup_started, finish_cleanup = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def child():
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cleanup_started.set()
            await finish_cleanup.wait()

    task = asyncio.create_task(child())
    await started.wait()
    task.cancel()
    await cleanup_started.wait()
    cleanup = asyncio.create_task(cancel_tasks([task]))
    await asyncio.sleep(0)
    assert not task.done()
    assert not cleanup.done()
    finish_cleanup.set()
    await cleanup
    assert task.cancelling() == 1


@pytest.mark.parametrize("service_type", [Worker, Coordinator])
async def test_cancel_during_database_startup_still_closes_clients(service_type):
    started = asyncio.Event()

    async def open_database():
        started.set()
        await asyncio.Event().wait()

    service = service_type.__new__(service_type)
    service.store = SimpleNamespace(open=AsyncMock(side_effect=open_database), close=AsyncMock())
    service.transport = SimpleNamespace(close=AsyncMock())
    service.provider = SimpleNamespace(close=AsyncMock())
    service.reconciler = SimpleNamespace(close=AsyncMock())
    service.redis = SimpleNamespace(aclose=AsyncMock())
    service.connection = None
    task = asyncio.create_task(service.run())
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    service.transport.close.assert_awaited_once()
    service.store.close.assert_awaited_once()
    if service_type is Worker:
        service.provider.close.assert_awaited_once()
    else:
        service.reconciler.close.assert_awaited_once()
        service.redis.aclose.assert_awaited_once()


async def test_worker_waits_for_background_tasks_before_closing_clients(monkeypatch):
    monkeypatch.setattr("cronos.worker.setup_checkpoints", AsyncMock())
    started, cleanup = asyncio.Event(), []
    calls = 0

    async def background():
        nonlocal calls
        calls += 1
        if calls == 3:
            started.set()
        try:
            await asyncio.Event().wait()
        finally:
            await asyncio.sleep(0)
            cleanup.append("background")

    async def close_database():
        assert cleanup == ["background"] * 3

    worker = Worker.__new__(Worker)
    worker.settings = SimpleNamespace()
    worker.store = SimpleNamespace(open=AsyncMock(), close=AsyncMock(side_effect=close_database))
    worker.transport = SimpleNamespace(close=AsyncMock())
    worker.provider = SimpleNamespace(close=AsyncMock())
    worker.recovery = worker.consume = worker.health = background
    task = asyncio.create_task(worker.run())
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    worker.store.close.assert_awaited_once()
