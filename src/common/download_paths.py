from collections.abc import Iterable
from pathlib import Path, PurePosixPath, PureWindowsPath
from tempfile import NamedTemporaryFile


def output_child(parent: Path, name: str) -> Path:
    """Resolve a single server-provided component without following a child symlink."""
    if (
        not name
        or name in {".", ".."}
        or "\0" in name
        or PurePosixPath(name).name != name
        or PureWindowsPath(name).name != name
    ):
        raise ValueError(f"Server returned an unsafe output path component: {name!r}")

    destination = parent.resolve() / name
    if destination.is_symlink():
        raise ValueError(f"Server returned an unsafe output path component: {name!r}")
    return destination


def write_download(destination: Path, chunks: Iterable[bytes]) -> None:
    """Replace the destination only after the complete download has been written."""
    with NamedTemporaryFile(
        dir=destination.parent, prefix=".download-", suffix=".part", delete=False
    ) as temporary_file:
        temporary_path = Path(temporary_file.name)
        try:
            for chunk in chunks:
                temporary_file.write(chunk)
            temporary_file.close()
            temporary_path.replace(destination)
        finally:
            temporary_path.unlink(missing_ok=True)
