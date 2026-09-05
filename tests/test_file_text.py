from cronos.artifacts import ArtifactManager
from cronos.file_text import text_window


def test_reads_beyond_preview_without_losing_original(tmp_path):
    manager = ArtifactManager(str(tmp_path))
    text = "А" * 260_000 + "Важный итог в конце документа"
    artifact = manager.ingest(101, "large.txt", text.encode())
    assert artifact["extracted"]["text_truncated"]
    result = text_window(artifact, 260_000, 1000)
    assert result["text"] == "Важный итог в конце документа"
    assert result["next_offset"] is None
    assert result["total_chars"] == len(text)
