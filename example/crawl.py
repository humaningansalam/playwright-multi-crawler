# crawl_type_a.py

async def crawl(page):
    try:
        # 페이지로 이동
        await page.goto('https://example.com', wait_until='networkidle')

        # 페이지의 제목 가져오기
        title = await page.title()

        # 특정 요소의 텍스트 가져오기 
        main_text = await page.inner_text('div.main_news') 

        # 결과 데이터 구성
        result = {
            'title': title,
            'main_text_snippet': main_text[:100]  # 텍스트의 일부만 반환
        }

        return result  # 결과 반환

    except Exception as e:
        # 에러 발생 시 에러 메시지를 반환
        return {'error': str(e)}
