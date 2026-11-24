import asyncio
import logging
import shutil
from collections.abc import Set
from datetime import datetime, timedelta
from pathlib import Path

from pyvirtualdisplay import Display

from src.config import (
    CLEANUP_INTERVAL_HOURS,
    JOB_FOLDER,
    JOB_RETENTION_DAYS,
    VIRTUAL_DISPLAY_BACKEND,
)
from src.core import state_manager as state

logger = logging.getLogger(__name__)

_display: Display | None = None


def start_display() -> bool:
    """Start an independent X display for the shared headful browser."""
    global _display

    if _display is not None:
        logger.warning("Virtual display is already running.")
        return True

    if VIRTUAL_DISPLAY_BACKEND not in {"xvfb", "xephyr"}:
        logger.error("Unsupported virtual display backend: %s", VIRTUAL_DISPLAY_BACKEND)
        return False

    logger.info("Starting virtual display (%s).", VIRTUAL_DISPLAY_BACKEND)
    try:
        display = Display(
            visible=int(VIRTUAL_DISPLAY_BACKEND == "xephyr"),
            backend=VIRTUAL_DISPLAY_BACKEND,
            size=(1920, 1080),
        )
        display.start()
    except Exception:
        logger.exception("Could not start the virtual display.")
        return False

    _display = display
    logger.info("Virtual display started.")
    return True


def stop_display() -> None:
    """Stop the current virtual display, if one was started."""
    global _display

    display, _display = _display, None
    if display is None:
        return

    logger.info("Stopping virtual display.")
    try:
        display.stop()
    except Exception:
        logger.exception("Failed to stop the virtual display.")
    else:
        logger.info("Virtual display stopped.")


def clean_old_jobs(excluded_job_ids: Set[str] = frozenset()) -> list[str]:
    """Delete expired inactive job directories and return their job IDs."""
    cutoff = datetime.now() - timedelta(days=JOB_RETENTION_DAYS)
    job_root = Path(JOB_FOLDER)
    logger.info(
        "Deleting inactive job directories older than %s from %s", cutoff.isoformat(), job_root
    )

    try:
        job_directories = list(job_root.iterdir())
    except FileNotFoundError:
        logger.debug("Job directory %s does not exist; cleanup is not needed.", job_root)
        return []
    except OSError:
        logger.exception("Could not list job directory %s during cleanup.", job_root)
        return []

    deleted_job_ids: list[str] = []
    for job_directory in job_directories:
        job_id = job_directory.name
        if job_id in excluded_job_ids:
            continue

        try:
            if not job_directory.is_dir():
                continue
            modified_at = datetime.fromtimestamp(job_directory.stat().st_mtime)
            if modified_at >= cutoff:
                continue
            logger.info(
                "Deleting expired job directory %s (modified %s)", job_directory, modified_at
            )
            shutil.rmtree(job_directory)
        except FileNotFoundError:
            logger.debug("Job directory disappeared during cleanup: %s", job_directory)
            continue
        except OSError:
            logger.exception("Could not delete job directory %s.", job_directory)
            continue

        deleted_job_ids.append(job_id)

    return deleted_job_ids


async def periodic_cleanup() -> None:
    """Periodically delete expired inactive job data and matching runtime state."""
    logger.info("Periodic job cleanup started.")
    sleep_seconds = CLEANUP_INTERVAL_HOURS * 3600

    while True:
        try:
            active_job_ids = await state.get_active_job_ids()
            deleted_job_ids = await asyncio.to_thread(clean_old_jobs, active_job_ids)
            for job_id in deleted_job_ids:
                await state.remove_job_state(job_id)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Periodic job cleanup failed.")

        logger.debug("Periodic job cleanup finished; sleeping for %s seconds.", sleep_seconds)
        await asyncio.sleep(sleep_seconds)


def ensure_job_folder() -> None:
    """Create the configured job root when it does not exist."""
    job_root = Path(JOB_FOLDER)
    try:
        job_root.mkdir(parents=True, exist_ok=True)
    except OSError:
        logger.exception("Could not create job directory %s.", job_root)
        raise
