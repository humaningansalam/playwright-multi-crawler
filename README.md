# playwright-multi-crawler


## 주요 기능
- **작업 제출**: 사용자는 스크립트 파일과 작업 이름을 제출하여 크롤링 작업을 요청할 수 있습니다.
- **비동기 작업 처리**: FastAPI와 Playwright를 통해 비동기적으로 작업을 처리하며, 동시에 여러 작업을 처리할 수 있습니다.
- **결과 반환**: 작업이 완료되면 작업 결과를 즉시 반환합니다.

## 요구 사항
- Python 3.11 이상
- uv
- FastAPI
- Uvicorn
- Playwright
- xserver-xephyr

## 설치 및 실행 방법

```bash
# 패키지 설치
uv sync
uv run playwright install-deps
uv run playwright install chromium

# 서버 실행
uv run python -m src.main
```

## 테스트

```bash
uv run pytest -q
```

## 사용법

작업 제출

```bash
curl -X POST http://localhost:5000/api/jobs/submit \
     -F "jobname=crawl_job" \
     -F "script_file=@example/crawl.py" \
     -F "additional_files=@textfile.txt"
```

작업 상태 확인

```bash
curl http://localhost:5000/api/jobs/status/<job_id>
```

작업 결과 확인

```bash
curl http://localhost:5000/api/jobs/results/<job_id>
```

결과 파일 다운로드

```bash
curl -OJ http://localhost:5000/api/jobs/download/<job_id>/<filename>
```

예시 스크립트는 `example/crawl.py`를 참고해주세요.

## 라이선스
이 프로젝트는 MIT 라이선스를 따릅니다. 자세한 내용은 LICENSE 파일을 참고하세요.
