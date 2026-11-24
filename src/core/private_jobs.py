"""Private job input storage and per-job access policy."""

import hashlib
import hmac
import re
import shutil
from pathlib import Path

PRIVATE_INPUT_DIRECTORY = ".private-inputs"
JOB_TOKEN_HEADER = "X-Job-Token"
# The old Tistory client uploaded plaintext account credentials as ordinary files.
LEGACY_TISTORY_JOB_NAME = "tistory_post"
_TOKEN_PATTERN = re.compile(r"[A-Za-z0-9_-]{43}")


def token_digest(token: str | None) -> str | None:
    if token is None or not _TOKEN_PATTERN.fullmatch(token):
        return None
    return hashlib.sha256(token.encode("ascii")).hexdigest()


def can_access_job(job_name: str, expected_digest: str | None, token: str | None) -> bool:
    if expected_digest is None:
        return job_name != LEGACY_TISTORY_JOB_NAME
    actual_digest = token_digest(token)
    return actual_digest is not None and hmac.compare_digest(expected_digest, actual_digest)


def remove_private_inputs(job_path: str | Path) -> None:
    private_directory = Path(job_path) / PRIVATE_INPUT_DIRECTORY
    if private_directory.is_symlink():
        private_directory.unlink()
    elif private_directory.exists():
        shutil.rmtree(private_directory)


def remove_legacy_tistory_credentials(job_path: str | Path) -> None:
    for filename in ("config.json", "cookies.json"):
        (Path(job_path) / filename).unlink(missing_ok=True)
