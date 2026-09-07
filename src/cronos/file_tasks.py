"""Keep synchronous file writes inside their caller's lock, even on cancellation."""

import asyncio
from collections.abc import Callable


async def file_task[**P, R](function: Callable[P, R], *args: P.args, **kwargs: P.kwargs) -> R:
    """Run a file operation in a thread and defer cancellation until it finishes."""
    task = asyncio.create_task(asyncio.to_thread(function, *args, **kwargs))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        # Cancelling to_thread cannot stop a running thread. Shield every wait,
        # including after repeated cancellation, so the caller keeps its lock.
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                continue
            except BaseException:
                break
        try:
            task.result()
        except BaseException:
            # Retrieve any thread failure; the caller's cancellation takes priority.
            pass
        raise
