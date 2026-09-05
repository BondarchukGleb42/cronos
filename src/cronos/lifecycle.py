"""Explicit SIGTERM handling for Python services running as container PID 1."""

import asyncio
import signal
from collections.abc import Awaitable, Callable, Iterable


async def run_until_stopped(service: Awaitable[None]) -> None:
    loop = asyncio.get_running_loop()
    task = asyncio.current_task()
    if task is None:
        raise RuntimeError("A service must run in an asyncio task")
    stopping = False

    def request_stop():
        nonlocal stopping
        if not stopping:
            stopping = True
            task.cancel()

    loop.add_signal_handler(signal.SIGTERM, request_stop)
    try:
        await service
    except asyncio.CancelledError:
        if not stopping:
            raise
    finally:
        loop.remove_signal_handler(signal.SIGTERM)


async def cancel_tasks(tasks: Iterable[asyncio.Task]) -> None:
    tasks = list(tasks)
    for task in tasks:
        # gather may already have cancelled these children. A second cancellation
        # would interrupt a child's asynchronous finally block.
        if not task.done() and not task.cancelling():
            task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


async def close_all(*closers: Callable[[], Awaitable[None]]) -> None:
    """Attempt every cleanup even when an earlier client fails to close."""
    errors = []
    for close in closers:
        try:
            await close()
        except Exception as error:
            errors.append(error)
    if errors:
        raise ExceptionGroup("Service cleanup failed", errors)
