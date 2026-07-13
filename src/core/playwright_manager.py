import asyncio
import logging
import os
import socket
from typing import Any, Optional
from urllib.parse import urlparse

import httpx
from playwright.async_api import Browser, Playwright, async_playwright
from src.config import BROWSER_EXECUTABLE_PATH, CDP_PORT

CDP_HOST = "127.0.0.1"
CDP_READY_TIMEOUT_SECONDS = 10.0
CDP_READY_POLL_INTERVAL_SECONDS = 0.1
CDP_REQUEST_TIMEOUT_SECONDS = 1.0

_playwright: Optional[Playwright] = None
_browser: Optional[Browser] = None
_shutting_down = False
_exit_process = os._exit


def _on_browser_disconnected() -> None:
    if _shutting_down:
        return
    logging.critical("Shared Chromium disconnected unexpectedly; exiting for service recovery.")
    _exit_process(1)


def _browser_launch_options() -> dict[str, Any]:
    options: dict[str, Any] = {
        "headless": False,
        "args": [
            f"--remote-debugging-port={CDP_PORT}",
            f"--remote-debugging-address={CDP_HOST}",
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--disable-dev-shm-usage",
        ],
    }
    if BROWSER_EXECUTABLE_PATH:
        options["executable_path"] = BROWSER_EXECUTABLE_PATH
    return options


def _cdp_base_url() -> str:
    return f"http://{CDP_HOST}:{CDP_PORT}"


def _assert_cdp_port_available() -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as candidate:
        try:
            candidate.bind((CDP_HOST, CDP_PORT))
        except OSError as exc:
            raise RuntimeError(
                f"CDP endpoint {CDP_HOST}:{CDP_PORT} is already in use"
            ) from exc


async def _fetch_cdp_version() -> dict[str, Any]:
    async with httpx.AsyncClient(
        timeout=CDP_REQUEST_TIMEOUT_SECONDS,
        trust_env=False,
    ) as client:
        response = await client.get(f"{_cdp_base_url()}/json/version")
        response.raise_for_status()
        payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError("CDP /json/version response must be a JSON object")
    return payload


async def _wait_for_cdp_ready() -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + CDP_READY_TIMEOUT_SECONDS
    last_error: Optional[Exception] = None

    while True:
        try:
            payload = await _fetch_cdp_version()
            websocket_url = payload.get("webSocketDebuggerUrl")
            if not isinstance(websocket_url, str) or not websocket_url:
                raise ValueError("CDP /json/version is missing webSocketDebuggerUrl")

            parsed_url = urlparse(websocket_url)
            if (
                parsed_url.scheme not in {"ws", "wss"}
                or parsed_url.hostname != CDP_HOST
                or parsed_url.port != CDP_PORT
            ):
                raise ValueError(
                    "CDP webSocketDebuggerUrl does not target "
                    f"{CDP_HOST}:{CDP_PORT}"
                )
            return
        except (httpx.HTTPError, TypeError, ValueError) as exc:
            last_error = exc

        remaining = deadline - loop.time()
        if remaining <= 0:
            break
        await asyncio.sleep(min(CDP_READY_POLL_INTERVAL_SECONDS, remaining))

    raise RuntimeError(
        f"CDP endpoint {_cdp_base_url()} did not become ready: {last_error}"
    ) from last_error


async def _close_resources() -> None:
    global _playwright, _browser, _shutting_down
    _shutting_down = True

    browser = _browser
    _browser = None
    if browser:
        try:
            await browser.close()
            logging.info("Browser closed.")
        except Exception:
            logging.exception("Failed to close browser.")

    playwright = _playwright
    _playwright = None
    if playwright:
        try:
            await playwright.stop()
            logging.info("Playwright stopped.")
        except Exception:
            logging.exception("Failed to stop Playwright.")


async def start() -> None:
    """
    Playwright 브라우저를 '서버 모드'로 시작합니다.
    headless=False로 설정하여 화면을 띄우고,
    --remote-debugging-port 옵션으로 외부 프로세스 접속을 허용합니다.
    """
    global _playwright, _browser, _shutting_down
    if _playwright:
        logging.warning("Playwright already started.")
        return

    try:
        _shutting_down = False
        _assert_cdp_port_available()
        _playwright = await async_playwright().start()
        logging.info(f"Launching Playwright Browser Server on port {CDP_PORT}...")
        _browser = await _playwright.chromium.launch(**_browser_launch_options())
        await _wait_for_cdp_ready()
        _browser.on("disconnected", _on_browser_disconnected)
        logging.info(f"Browser Server launched. Listening at {_cdp_base_url()}")
    except Exception as e:
        logging.critical(f"Failed to launch browser server: {e}")
        await _close_resources()
        raise RuntimeError(f"Browser launch failed: {e}") from e


async def shutdown() -> None:
    """Playwright 관련 자원 정리"""
    await _close_resources()


def is_browser_connected() -> bool:
    """브라우저 프로세스 연결 상태 확인"""
    return _browser is not None and _browser.is_connected()
