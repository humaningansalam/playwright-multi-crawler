
import logging
import os

from playwright.async_api import BrowserContext, Page


async def crawl(page: Page, context: BrowserContext, job_path: str):
    """
    지정된 페이지에서 데이터를 크롤링하고 결과를 반환하는 예제 함수.

    Args:
        page: Playwright의 Page 객체. 이 페이지를 사용하여 탐색 및 상호작용.
        context: Page가 속한 BrowserContext 객체. 쿠키 관리 등에 사용 가능.
        job_path: 이 작업의 파일들이 저장된 폴더 경로. 결과 파일 저장 시 사용.

    Returns:
        dict: 완료 결과 데이터. 예: {'title': '...', 'main_text_snippet': '...', 'screenshot': 'screenshot.png'}

    Raises:
        Exception: 탐색이나 데이터 수집이 실패하면 runner가 작업을 FAILED로 기록하도록 예외를 전달합니다.
    """
    logging.info(f"Crawling started. Job path: {job_path}")
    # context 사용 예시 (필요 없다면 사용 안 함)
    # cookies = await context.cookies()
    # logging.info(f"Current context has {len(cookies)} cookies.")

    # 페이지로 이동 (예: 네이버 뉴스)
    target_url = 'https://news.naver.com/'
    logging.info(f"Navigating to {target_url}")
    await page.goto(target_url, wait_until='networkidle', timeout=60000) # 타임아웃 증가

    # 페이지의 제목 가져오기
    title = await page.title()
    logging.info(f"Page title: {title}")

    # 특정 요소의 텍스트 가져오기 (예: 첫 번째 헤드라인 뉴스 제목)
    # 선택자는 실제 페이지 구조에 맞게 변경 필요
    headline_selector = 'div.main_component_area ul.hdline_article_list li.hdline_article_item a.hdline_article_lnk'
    main_text = await page.locator(headline_selector).first.inner_text()
    logging.info(f"Headline text found: {main_text[:50]}...") # 일부만 로깅

    # 스크린샷 저장 (job_path 사용)
    screenshot_filename = "screenshot.png"
    screenshot_path = os.path.join(job_path, screenshot_filename)
    await page.screenshot(path=screenshot_path, full_page=True) # 전체 페이지 스크린샷
    logging.info(f"Screenshot saved to {screenshot_path}")

    result = {
        'target_url': target_url,
        'title': title,
        'headline_text_snippet': main_text[:100],
        'screenshot_file': screenshot_filename,
    }
    logging.info("Crawling finished successfully.")
    return result
