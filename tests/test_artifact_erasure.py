import asyncio
import threading
from pathlib import Path

import pytest

from cronos.artifacts import ArtifactManager
from cronos.file_tasks import file_task


def test_erase_removes_all_owner_files_and_orphans_but_preserves_foreign_user(tmp_path):
    manager = ArtifactManager(str(tmp_path / "artifacts"))
    artifact = manager.ingest(42, "saved.txt", b"saved")
    foreign = manager.ingest(43, "private.txt", b"foreign")
    owner = manager.root / "42"
    (owner / ".upload-interrupted").write_bytes(b"partial upload")
    (owner / "orphan.bin").write_bytes(b"unregistered")
    (owner / "nested").mkdir()
    (owner / "nested" / "orphan.bin").write_bytes(b"nested orphan")

    assert manager.erase_user(42) == {"removed": True}
    assert not owner.exists()
    assert not Path(artifact["path"]).exists()
    assert Path(foreign["path"]).read_bytes() == b"foreign"
    assert manager.erase_user(42) == {"removed": False}
    assert sorted(path.name for path in manager.root.iterdir()) == ["43"]


@pytest.mark.parametrize("user_id", [True, False, 0, -1, "42", "../43", 42.0, None])
def test_erase_rejects_non_positive_integer_without_side_effects(tmp_path, user_id):
    manager = ArtifactManager(str(tmp_path / "artifacts"))
    foreign = manager.ingest(43, "private.txt", b"foreign")
    with pytest.raises(ValueError):
        manager.erase_user(user_id)
    assert Path(foreign["path"]).read_bytes() == b"foreign"
    assert sorted(path.name for path in manager.root.iterdir()) == ["43"]


def test_erasing_absent_owner_does_not_create_directories(tmp_path):
    manager = ArtifactManager(str(tmp_path / "artifacts"))
    assert manager.erase_user(42) == {"removed": False}
    assert list(manager.root.iterdir()) == []
    manager.root.rmdir()
    assert manager.erase_user(42) == {"removed": False}
    assert not manager.root.exists()


def test_erase_unlinks_nested_symlinks_without_touching_targets(tmp_path):
    manager = ArtifactManager(str(tmp_path / "artifacts"))
    foreign = manager.ingest(43, "private.txt", b"foreign")
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "secret").write_bytes(b"outside")
    owner = manager._user_dir(42)
    (owner / "other-user").symlink_to(manager.root / "43", target_is_directory=True)
    (owner / "outside").symlink_to(outside, target_is_directory=True)
    (owner / "foreign-file").symlink_to(foreign["path"])
    (owner / "dangling").symlink_to(tmp_path / "missing")

    assert manager.erase_user(42) == {"removed": True}
    assert Path(foreign["path"]).read_bytes() == b"foreign"
    assert (outside / "secret").read_bytes() == b"outside"
    assert not owner.exists()


@pytest.mark.parametrize("dangling", [False, True])
def test_erase_unlinks_owner_symlink_itself_without_following_it(tmp_path, dangling):
    manager = ArtifactManager(str(tmp_path / "artifacts"))
    foreign = manager.ingest(43, "private.txt", b"foreign")
    owner = manager.root / "42"
    owner.symlink_to(manager.root / ("missing" if dangling else "43"), target_is_directory=True)

    assert manager.erase_user(42) == {"removed": True}
    assert not owner.is_symlink()
    assert Path(foreign["path"]).read_bytes() == b"foreign"
    assert manager.erase_user(42) == {"removed": False}


def test_erase_rejects_replaced_root_symlink(tmp_path):
    manager = ArtifactManager(str(tmp_path / "artifacts"))
    outside = tmp_path / "outside"
    (outside / "42").mkdir(parents=True)
    secret = outside / "42" / "secret"
    secret.write_bytes(b"outside")
    manager.root.rmdir()
    manager.root.symlink_to(outside, target_is_directory=True)

    with pytest.raises(OSError):
        manager.erase_user(42)
    assert secret.read_bytes() == b"outside"


async def loop_checkpoint():
    """Let already queued cancellation callbacks run without timing-based sleeps."""
    event = asyncio.Event()
    asyncio.get_running_loop().call_soon(event.set)
    await event.wait()


async def test_cancelled_writer_finishes_before_user_lock_allows_erasure(tmp_path):
    manager = ArtifactManager(str(tmp_path / "artifacts"))
    lock = asyncio.Lock()
    loop = asyncio.get_running_loop()
    started, released_lock, eraser_waiting = asyncio.Event(), asyncio.Event(), asyncio.Event()
    finish_write = threading.Event()
    writer_done = threading.Event()

    def write():
        loop.call_soon_threadsafe(started.set)
        assert finish_write.wait(timeout=5), "test did not release writer"
        manager.ingest(42, "late.txt", b"late write")
        writer_done.set()

    async def owner():
        async with lock:
            try:
                await file_task(write)
            finally:
                released_lock.set()

    async def erase():
        eraser_waiting.set()
        async with lock:
            assert writer_done.is_set()
            return await file_task(manager.erase_user, 42)

    writer = asyncio.create_task(owner())
    eraser = None
    try:
        async with asyncio.timeout(5):
            await started.wait()
            writer.cancel()
            await loop_checkpoint()
            eraser = asyncio.create_task(erase())
            await eraser_waiting.wait()
            assert not writer.done() and not released_lock.is_set()
            assert not eraser.done()
            writer.cancel()
            await loop_checkpoint()
            assert not writer.done() and not released_lock.is_set()
            finish_write.set()
            with pytest.raises(asyncio.CancelledError):
                await writer
            assert writer_done.is_set()
            assert await eraser == {"removed": True}
            assert not (manager.root / "42").exists()
    finally:
        finish_write.set()
        await asyncio.gather(writer, *([eraser] if eraser else []), return_exceptions=True)


async def test_file_task_returns_value_and_propagates_uncancelled_failure():
    assert await file_task(lambda left, *, right: left + right, 3, right=4) == 7

    def fail():
        raise OSError("write failed")

    with pytest.raises(OSError, match="write failed"):
        await file_task(fail)


async def test_cancelled_file_task_retrieves_failure_and_preserves_cancellation():
    loop = asyncio.get_running_loop()
    started = asyncio.Event()
    finish_write = threading.Event()
    finished = threading.Event()

    def fail():
        loop.call_soon_threadsafe(started.set)
        assert finish_write.wait(timeout=5), "test did not release writer"
        finished.set()
        raise OSError("late failure")

    task = asyncio.create_task(file_task(fail))
    try:
        async with asyncio.timeout(5):
            await started.wait()
            task.cancel()
            await loop_checkpoint()
            assert not task.done()
            finish_write.set()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert finished.is_set()
    finally:
        finish_write.set()
        await asyncio.gather(task, return_exceptions=True)
