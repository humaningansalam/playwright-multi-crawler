async def crawl(page, job_path):
    try:
        # 페이지로 이동
        await page.goto('https://example.com', wait_until='networkidle')

        # 페이지의 제목 가져오기
        title = await page.title()

        # 특정 요소의 텍스트 가져오기 
        main_text = await page.inner_text('div.main_news')  

        # 스크린샷 저장
        screenshot_path = os.path.join(job_path, "screenshot.png")
        await page.screenshot(path=screenshot_path)

        # 결과 데이터 구성
        result = {
            'title': title,
            'main_text_snippet': main_text[:100],  # 텍스트의 일부만 반환
            'screenshot': "screenshot.png"  # 스크린샷 파일명
        }

        return result  # 결과 반환

    except Exception as e:
        # 에러 발생 시 에러 메시지를 반환
        return {'error': str(e)}