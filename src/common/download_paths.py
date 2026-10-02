from pathlib import Path, PurePosixPath, PureWindowsPath


def output_child(parent: Path, name: str) -> Path:
    """Keep server-provided job IDs and filenames directly inside their parent."""
    if (
        not name
        or name in {".", ".."}
        or "\0" in name
        or PurePosixPath(name).name != name
        or PureWindowsPath(name).name != name
    ):
        raise ValueError(f"Server returned an invalid output path component: {name!r}")

    root = parent.resolve()
    destination = (root / name).resolve()
    if destination.parent != root:
        raise ValueError(f"Server returned an output path outside {root}: {name!r}")
    return destination
