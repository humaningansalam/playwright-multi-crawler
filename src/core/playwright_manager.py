import asyncio
import logging
import os
import signal
import socket
from typing import Any
from urllib.parse import urlparse

import httpx
from playwright.async_api import Browser, Playwright, async_playwright

from src.config import BROWSER_EXECUTABLE_PATH, CDP_PORT

logger = logging.getLogger(__name__)

CDP_HOST = "127.0.0.1"
CDP_READY_TIMEOUT_SECONDS = 10.0
CDP_READY_POLL_INTERVAL_SECONDS = 0.1
CDP_REQUEST_TIMEOUT_SECONDS = 1.0

_playwright: Playwright | None = None
_browser: Browser | None = None
_shutting_down = False
_requested_exit_code: int | None = None
_exit_process = os._exit
_signal_process = os.kill


def _on_browser_disconnected(_disconnected_browser: Browser) -> None:
    global _requested_exit_code

    if _shutting_down:
        return

    _requested_exit_code = 1
    logger.critical("Shared Chromium disconnected unexpectedly; requesting service recovery.")
    try:
        _signal_process(os.getpid(), signal.SIGTERM)
    except OSError:
        logger.exception("Could not request graceful shutdown; exiting immediately.")
        _exit_process(_requested_exit_code)


def requested_exit_code() -> int | None:
    """Return the process exit code requested by a browser lifecycle failure."""
    return _requested_exit_code


def _browser_launch_options() -> dict[str, Any]:
    launch_options: dict[str, Any] = {
        "headless": False,
        "handle_sigint": False,
        "handle_sigterm": False,
        "handle_sighup": False,
        "args": [
            f"--remote-debugging-port={CDP_PORT}",
            f"--remote-debugging-address={CDP_HOST}",
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--disable-dev-shm-usage",
        ],
    }
    if BROWSER_EXECUTABLE_PATH:
        launch_options["executable_path"] = BROWSER_EXECUTABLE_PATH
    return launch_options


def _cdp_base_url() -> str:
    return f"http://{CDP_HOST}:{CDP_PORT}"


def _assert_cdp_port_available() -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as candidate_socket:
        try:
            candidate_socket.bind((CDP_HOST, CDP_PORT))
        except OSError as exception:
            raise RuntimeError(
                f"CDP endpoint {CDP_HOST}:{CDP_PORT} is already in use"
            ) from exception


async def _fetch_cdp_version() -> dict[str, Any]:
    async with httpx.AsyncClient(
        timeout=CDP_REQUEST_TIMEOUT_SECONDS,
        trust_env=False,
    ) as client:
        response = await client.get(f"{_cdp_base_url()}/json/version")
        response.raise_for_status()
        version_payload = response.json()

    if not isinstance(version_payload, dict):
        raise ValueError("CDP /json/version response must be a JSON object")
    return version_payload


async def _wait_for_cdp_ready() -> None:
    event_loop = asyncio.get_running_loop()
    deadline = event_loop.time() + CDP_READY_TIMEOUT_SECONDS
    last_error: Exception | None = None

    while True:
        try:
            version_payload = await _fetch_cdp_version()
            websocket_url = version_payload.get("webSocketDebuggerUrl")
            if not isinstance(websocket_url, str) or not websocket_url:
                raise ValueError("CDP /json/version is missing webSocketDebuggerUrl")

            parsed_url = urlparse(websocket_url)
            if (
                parsed_url.scheme not in {"ws", "wss"}
                or parsed_url.hostname != CDP_HOST
                or parsed_url.port != CDP_PORT
            ):
                raise ValueError(f"CDP webSocketDebuggerUrl does not target {CDP_HOST}:{CDP_PORT}")
            return
        except (httpx.HTTPError, TypeError, ValueError) as exception:
            last_error = exception

        remaining_seconds = deadline - event_loop.time()
        if remaining_seconds <= 0:
            break
        await asyncio.sleep(min(CDP_READY_POLL_INTERVAL_SECONDS, remaining_seconds))

    raise RuntimeError(
        f"CDP endpoint {_cdp_base_url()} did not become ready: {last_error}"
    ) from last_error


async def _close_resources() -> None:
    global _browser, _playwright, _shutting_down

    _shutting_down = True

    browser, _browser = _browser, None
    if browser is not None:
        try:
            await browser.close()
        except Exception:
            logger.exception("Failed to close Chromium.")
        else:
            logger.info("Chromium closed.")

    playwright, _playwright = _playwright, None
    if playwright is not None:
        try:
            await playwright.stop()
        except Exception:
            logger.exception("Failed to stop Playwright.")
        else:
            logger.info("Playwright stopped.")


async def start() -> None:
    """Launch the shared headful Chromium instance and verify its CDP endpoint."""
    global _browser, _playwright, _requested_exit_code, _shutting_down

    if _playwright is not None:
        logger.warning("Playwright is already running.")
        return

    try:
        _shutting_down = False
        _requested_exit_code = None
        _assert_cdp_port_available()
        _playwright = await async_playwright().start()
        logger.info("Launching shared Chromium on CDP port %s.", CDP_PORT)
        _browser = await _playwright.chromium.launch(**_browser_launch_options())
        await _wait_for_cdp_ready()
        _browser.on("disconnected", _on_browser_disconnected)
        logger.info("Shared Chromium is ready at %s.", _cdp_base_url())
    except Exception as exception:
        logger.critical("Failed to launch shared Chromium.", exc_info=True)
        await _close_resources()
        raise RuntimeError(f"Browser launch failed: {exception}") from exception


async def shutdown() -> None:
    """Close the shared browser and Playwright runtime."""
    await _close_resources()


def is_browser_connected() -> bool:
    """Return whether the shared Chromium connection is alive."""
    return _browser is not None and _browser.is_connected()
