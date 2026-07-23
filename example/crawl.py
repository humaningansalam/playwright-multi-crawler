
import logging
from pathlib import Path

from playwright.async_api import BrowserContext, Page


async def crawl(page: Page, context: BrowserContext, job_path: str):
    """Visit a stable public page and return a downloadable screenshot."""
    logging.info(f"Crawling started. Job path: {job_path}")

    target_url = "https://example.com/"
    logging.info(f"Navigating to {target_url}")
    await page.goto(target_url, wait_until="domcontentloaded", timeout=30_000)

    title = await page.title()
    logging.info(f"Page title: {title}")

    screenshot_filename = "screenshot.png"
    screenshot_path = Path(job_path) / screenshot_filename
    await page.screenshot(path=str(screenshot_path), full_page=True)
    logging.info(f"Screenshot saved to {screenshot_path}")

    return {
        "target_url": target_url,
        "title": title,
        "screenshot_file": screenshot_filename,
    }
