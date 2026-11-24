import pytest

from src.common.download_paths import output_child, write_download


@pytest.mark.parametrize("name", ["", ".", "..", "nested/file", r"nested\file", "bad\x00name"])
def test_output_child_rejects_invalid_components(tmp_path, name):
    with pytest.raises(ValueError, match="unsafe output path component"):
        output_child(tmp_path, name)


def test_output_child_rejects_symlinks(tmp_path):
    target = tmp_path / "target.txt"
    target.write_text("original")
    (tmp_path / "link.txt").symlink_to(target)

    with pytest.raises(ValueError, match="unsafe output path component"):
        output_child(tmp_path, "link.txt")


def test_write_download_preserves_existing_file_if_stream_fails(tmp_path):
    destination = tmp_path / "result.txt"
    destination.write_bytes(b"original")

    def interrupted_chunks():
        yield b"partial"
        raise OSError("download interrupted")

    with pytest.raises(OSError, match="download interrupted"):
        write_download(destination, interrupted_chunks())

    assert destination.read_bytes() == b"original"
    assert list(tmp_path.iterdir()) == [destination]


def test_write_download_replaces_file_after_complete_stream(tmp_path):
    destination = output_child(tmp_path, "result.txt")
    destination.write_bytes(b"original")

    write_download(destination, [b"first", b"second"])

    assert destination.read_bytes() == b"firstsecond"
    assert list(tmp_path.iterdir()) == [destination]
