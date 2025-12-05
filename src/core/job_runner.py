import asyncio
import sys
import json
import traceback
import importlib.util
import os
from playwright.async_api import async_playwright

# 프로젝트 루트 경로를 sys.path에 추가하여 config import 가능하게 함
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(project_root)

from src.config import CDP_URL

async def run_user_script(job_id, script_path, job_path):
    result_data = None
    error_info = None
    
    async with async_playwright() as p:
        browser = None
        context = None
        page = None
        try:
            # 1. 메인 서버가 띄워둔 브라우저에 접속
            browser = await p.chromium.connect_over_cdp(CDP_URL)
            
            # 2. 작업 격리를 위해 독립적인 Context 생성
            # 필요하다면 여기서 user_data_dir을 지정하거나 쿠키를 로드할 수 있음
            context = await browser.new_context()
            page = await context.new_page()

            # 3. 사용자 스크립트 동적 로드
            spec = importlib.util.spec_from_file_location("user_module", script_path)
            if spec is None or spec.loader is None:
                 raise ImportError(f"Could not load script from {script_path}")
            
            user_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(user_module)

            # 4. crawl 함수 실행
            if hasattr(user_module, 'crawl') and asyncio.iscoroutinefunction(user_module.crawl):
                result_data = await user_module.crawl(page, context, job_path)
            else:
                raise AttributeError("The script must contain an async function named 'crawl'.")

        except Exception as e:
            error_info = {
                "error": str(e),
                "traceback": traceback.format_exc()
            }
        finally:
            # 5. 리소스 정리 (브라우저는 끄지 않음)
            if page: await page.close()
            if context: await context.close()
            if browser: await browser.disconnect()

    # 결과 JSON 출력 (부모 프로세스가 읽음)
    output = {
        "status": "FAILED" if error_info else "COMPLETED",
        "result": result_data,
        "error": error_info
    }
    print(json.dumps(output))

if __name__ == "__main__":
    # 인자: [1]=job_id, [2]=script_path, [3]=job_path
    if len(sys.argv) < 4:
        print(json.dumps({"status": "FAILED", "error": {"error": "Invalid arguments provided to worker"}}))
        sys.exit(1)
        
    try:
        asyncio.run(run_user_script(sys.argv[1], sys.argv[2], sys.argv[3]))
    except Exception as e:
        # 런타임 자체 에러 캡처
        print(json.dumps({"status": "FAILED", "error": {"error": f"Worker runtime error: {str(e)}"}}))