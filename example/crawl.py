import logging
from pathlib import Path

from playwright.async_api import BrowserContext, Page

logger = logging.getLogger(__name__)


async def crawl(page: Page, context: BrowserContext, job_path: str) -> dict[str, str]:
    """Visit a stable public page and return a downloadable screenshot."""
    logger.info("Crawl started in %s", job_path)

    target_url = "https://example.com/"
    logger.info("Navigating to %s", target_url)
    await page.goto(target_url, wait_until="domcontentloaded", timeout=30_000)

    page_title = await page.title()
    logger.info("Page title: %s", page_title)

    screenshot_filename = "screenshot.png"
    screenshot_path = Path(job_path) / screenshot_filename
    await page.screenshot(path=str(screenshot_path), full_page=True)
    logger.info("Screenshot saved to %s", screenshot_path)

    return {
        "target_url": target_url,
        "title": page_title,
        "screenshot_file": screenshot_filename,
    }
