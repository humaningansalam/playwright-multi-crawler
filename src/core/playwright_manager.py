

# Playwright 관련 객체 (모듈 레벨에서 관리)
import json
import logging
import os
from typing import Optional
from playwright.async_api import (
    async_playwright, Playwright, Browser, BrowserContext
)
from src.config import CONTEXT_PATH

_playwright: Optional[Playwright] = None
_browser: Optional[Browser] = None
_context: Optional[BrowserContext] = None

async def start() -> None:
    """Playwright 시작, 브라우저 실행, 컨텍스트 로드/생성"""
    global _playwright, _browser, _context
    if _playwright:
        logging.warning("Playwright already started.")
        return

    _playwright = await async_playwright().start()
    logging.info("Launching Playwright browser...")
    try:
        # 브라우저 실행 옵션 (필요시 config에서 가져오도록 수정)
        _browser = await _playwright.chromium.launch(
            headless=False, 
            args=['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage']
        )
        logging.info(f"Browser launched: {_browser.version}")
    except Exception as e:
        logging.critical(f"Failed to launch browser: {e}")
        # 애플리케이션 시작 실패 처리 필요
        raise RuntimeError("Browser launch failed") from e

    _context = await load_context(_browser, CONTEXT_PATH)

async def shutdown() -> None:
    """Playwright 관련 자원 정리"""
    global _playwright, _browser, _context
    if _context:
        await save_context(_context, CONTEXT_PATH)
        try:
            await _context.close()
            logging.info("Browser context closed.")
        except Exception as e:
            logging.error(f"Error closing browser context: {e}")
        _context = None
    if _browser:
        try:
            await _browser.close()
            logging.info("Browser closed.")
        except Exception as e:
            logging.error(f"Error closing browser: {e}")
        _browser = None
    if _playwright:
        try:
            await _playwright.stop()
            logging.info("Playwright stopped.")
        except Exception as e:
            logging.error(f"Error stopping Playwright: {e}")
        _playwright = None

async def get_context() -> Optional[BrowserContext]:
    """현재 활성화된 브라우저 컨텍스트 반환"""
    if not _context:
        logging.error("Browser context requested but not available.")
    return _context

async def get_browser() -> Optional[Browser]:
    """현재 활성화된 브라우저 반환"""
    if not _browser:
        logging.error("Browser requested but not available.")
    return _browser

async def save_context(context_to_save: BrowserContext, path: str):
    """브라우저 컨텍스트 저장"""
    if not context_to_save:
        logging.warning("Attempted to save a null context.")
        return
    try:
        state = await context_to_save.storage_state()
        # 저장 경로의 디렉토리 존재 확인 및 생성
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'w') as f:
            json.dump(state, f, indent=2) 
        logging.info(f"Browser context saved to {path}")
    except Exception as e:
        logging.error(f"Failed to save browser context to {path}: {e}")

async def load_context(browser_instance: Browser, path: str) -> BrowserContext:
    """브라우저 컨텍스트 로드 또는 새로 생성"""
    if not browser_instance or not browser_instance.is_connected():
         logging.error("Cannot load context, browser is not available or connected.")
         raise RuntimeError("Browser not available for context loading")

    try:
        if os.path.exists(path):
            logging.info(f"Attempting to load browser context from {path}")
            with open(path, 'r') as f:
                state = json.load(f)
            # 저장된 상태로 새 컨텍스트 생성
            loaded_context = await browser_instance.new_context(storage_state=state)
            logging.info(f"Browser context loaded successfully from {path}")
            return loaded_context
        else:
            logging.info(f"No saved context found at {path}. Creating new context.")
            # 새 컨텍스트 생성
            return await browser_instance.new_context()
    except Exception as e:
        logging.error(f"Failed to load context from {path}: {e}. Creating new context instead.")
        # 실패 시에도 새 컨텍스트 생성하여 반환
        return await browser_instance.new_context()

def is_browser_connected() -> bool:
    """브라우저 연결 상태 확인"""
    return _browser is not None and _browser.is_connected()