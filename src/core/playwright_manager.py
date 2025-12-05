

# Playwright 관련 객체 (모듈 레벨에서 관리)
import logging
import os
from typing import Optional
from playwright.async_api import async_playwright, Playwright, Browser
from src.config import CDP_PORT

_playwright: Optional[Playwright] = None
_browser: Optional[Browser] = None

async def start() -> None:
    """
    Playwright 브라우저를 '서버 모드'로 시작합니다.
    headless=False로 설정하여 화면을 띄우고,
    --remote-debugging-port 옵션으로 외부 프로세스 접속을 허용합니다.
    """
    global _playwright, _browser
    if _playwright:
        logging.warning("Playwright already started.")
        return

    _playwright = await async_playwright().start()
    logging.info(f"Launching Playwright Browser Server on port {CDP_PORT}...")
    
    try:
        # 핵심: 원격 디버깅 포트 활성화
        _browser = await _playwright.chromium.launch(
            headless=False,  # 화면에 보임
            args=[
                f'--remote-debugging-port={CDP_PORT}',
                '--no-sandbox', 
                '--disable-setuid-sandbox', 
                '--disable-dev-shm-usage'
            ]
        )
        logging.info(f"Browser Server launched. Listening at http://127.0.0.1:{CDP_PORT}")
    except Exception as e:
        logging.critical(f"Failed to launch browser server: {e}")
        raise RuntimeError("Browser launch failed") from e

async def shutdown() -> None:
    """Playwright 관련 자원 정리"""
    global _playwright, _browser
    
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

def is_browser_connected() -> bool:
    """브라우저 프로세스 연결 상태 확인"""
    return _browser is not None and _browser.is_connected()
