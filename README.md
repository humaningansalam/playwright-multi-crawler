# playwright-multi-crawler

Playwright 기반 작업을 HTTP API로 제출하고, 공유 Chromium 브라우저에서 비동기로 실행하는 서비스입니다.

## 주요 기능

- **작업 제출**: Python 크롤링 스크립트, 작업 이름, 선택 입력 파일을 업로드합니다.
- **비동기 작업 처리**: 작업 ID를 즉시 받고 상태, 결과, 로그를 별도 API로 조회합니다.
- **결과와 산출물**: 반환값, stdout/stderr 로그, 스크립트가 만든 파일을 작업 폴더에서 제공합니다.
- **운영 readiness**: `/health`는 브라우저와 워커가 모두 준비된 경우에만 성공합니다.

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
uv run python -m playwright install-deps
uv run python -m playwright install chromium

# 서버 실행
uv run python -m src.main
```

브라우저와 가상 디스플레이가 시작된 뒤에만 작업을 제출할 수 있습니다. 가벼운 import 또는 ASGI 테스트에는 다음처럼 heavy startup을 끌 수 있지만, 이 모드에서는 작업 제출이 의도적으로 `503 Service Unavailable`을 반환합니다.

```bash
RUN_HEAVY_STARTUP=false uv run python -c "import src.main; print(src.main.app.title)"
```

## 테스트

```bash
uv run pytest -q
```

## 작업 API 사용법

| Method | Route | 설명 |
| --- | --- | --- |
| `POST` | `/api/jobs/submit` | 작업 스크립트와 선택 입력 파일을 제출합니다. |
| `GET` | `/api/jobs/status/{job_id}` | 현재 작업 상태를 조회합니다. |
| `GET` | `/api/jobs/results/{job_id}` | 처리 중 또는 종료된 작업 결과를 조회합니다. |
| `POST` | `/api/jobs/{job_id}/cancel` | 대기 또는 실행 중인 작업을 취소합니다. |
| `GET` | `/api/jobs/logs/{job_id}` | stdout/stderr를 SSE로 스트리밍합니다. |
| `GET` | `/api/jobs/download/{job_id}/{filename}` | 작업 폴더의 파일을 다운로드합니다. |
| `GET` | `/health` | browser/worker readiness를 조회합니다. |
| `GET` | `/metrics` | Prometheus 형식 런타임 지표를 제공합니다. |

### 작업 제출

```bash
curl -X POST http://localhost:5000/api/jobs/submit \
     -F "jobname=crawl_job" \
     -F "script_file=@example/crawl.py"
```

추가 입력 파일이 필요한 스크립트는 `-F "additional_files=@<file-path>"` 옵션을 반복해서 함께 전달할 수 있습니다.

성공하면 `202 Accepted`와 `job_id`, `PENDING` 상태를 받습니다. 동일한 `jobname`이 대기 또는 실행 중이면 `409 Conflict`를 받습니다. 워커가 준비되지 않았으면 작업 폴더나 상태를 만들지 않고 `503`을 반환합니다.

`additional_files`는 작업 폴더의 최상위 파일명만 사용할 수 있습니다. 경로, `..`, 중복 파일명, 그리고 서비스가 사용하는 `script.py`, `result.json`, `result.json.tmp`, `stdout.log`, `stderr.log`, `state.json`은 거부됩니다.

### 작업 상태와 취소

```bash
curl http://localhost:5000/api/jobs/status/<job_id>
```

대기 또는 실행 중인 작업은 취소할 수 있습니다.

```bash
curl -X POST http://localhost:5000/api/jobs/<job_id>/cancel
```

취소 성공 시 상태는 `CANCELLED`입니다. 이미 종료한 작업을 취소하면 `409`, 없는 작업 ID는 `404`입니다.

### 결과, 파일, 로그

```bash
curl http://localhost:5000/api/jobs/results/<job_id>
```

`PENDING` 또는 `RUNNING` 결과 조회는 아직 처리 중이라는 응답을 반환합니다. 종료 상태인 `COMPLETED`, `FAILED`, `CANCELLED`, `INTERRUPTED`에서는 결과 데이터, 로그의 마지막 64 KiB, 그리고 작업 폴더 파일의 다운로드 URL을 받습니다.

상태와 결과 응답에는 가능한 범위에서 `submitted_at`, `started_at`, `completed_at`, `queue_wait_seconds`, `run_duration_seconds`가 포함됩니다. 기존 `duration_seconds`는 호환성을 위해 실제 실행 시간과 같은 값으로 유지됩니다.

결과 파일 다운로드

```bash
curl -OJ http://localhost:5000/api/jobs/download/<job_id>/<filename>
```

실행 중에도 stdout/stderr를 Server-Sent Events로 따라갈 수 있습니다. `curl -N`은 연결을 버퍼링하지 않습니다.

```bash
curl -N http://localhost:5000/api/jobs/logs/<job_id>
```

이 스트림은 `event: stdout` 및 `event: stderr`를 전송하고 작업이 종료하면 끝납니다. 다운로드 API는 작업 폴더 밖으로 나가는 경로를 허용하지 않습니다.

## Crawler CLI

설치된 `crawler` 명령은 스크립트를 제출하고, stdout/stderr 로그를 실시간으로 따라간 뒤, 종료 결과와 산출물을 내려받습니다.

```bash
uv run crawler example/crawl.py \
  --job-name naver-news \
  --file example/input.json \
  --server http://localhost:5000 \
  --output downloads
```

`--file`은 필요한 만큼 반복할 수 있습니다. 완료된 파일은 `<output>/<job_id>/`에 저장됩니다. 작업이 `FAILED`, `CANCELLED`, 또는 `INTERRUPTED`로 끝나면 CLI는 결과 JSON을 출력하고 종료 코드 `1`을 반환합니다.

로그를 따라가는 동안 `Ctrl+C`를 누르면 CLI는 현재 원격 작업의 cancel endpoint를 호출하고 종료 코드 `130`으로 끝납니다.

### 작업 스크립트 계약

업로드 스크립트에는 아래 시그니처의 async `crawl` 함수가 있어야 합니다. runner는 작업 폴더를 현재 작업 디렉터리와 import 경로에 추가하므로, 함께 업로드한 `helper.py` 같은 모듈을 일반 Python import로 사용할 수 있습니다.

```python
async def crawl(page, context, job_path):
    await page.goto("https://example.com")
    return {"title": await page.title()}
```

- `page`와 `context`는 이 작업 전용 Playwright 객체입니다. 함수가 끝나면 runner가 닫습니다.
- `job_path`는 이 작업의 전용 폴더입니다. 스크린샷이나 다운로드 파일은 이 경로에 저장하세요.
- `crawl`이 정상 반환한 값은 API JSON 응답으로 직렬화할 수 있어야 하며, 직렬화된 값이 `COMPLETED` 결과로 보존됩니다. Tuple은 JSON array로 저장되고 `{"error": "..."}`도 일반 사용자 데이터입니다.
- JSON object key는 string이어야 하고 float는 finite 값이어야 합니다. 값 손실이 필요한 변환이나 JSON으로 직렬화할 수 없는 값을 반환하면 작업은 `WORKER_RESULT_INVALID` 오류와 함께 `FAILED`가 됩니다.
- 작업을 `FAILED`로 기록하려면 `crawl`에서 예외를 발생시키세요. runner가 예외와 traceback을 structured error로 기록합니다.
- runner가 `result.json`을 atomic write로 관리하므로 스크립트와 추가 파일은 이 이름 및 `result.json.tmp`를 사용하면 안 됩니다.
- 작업 프로세스의 stdout/stderr는 각각 `stdout.log`, `stderr.log`에 기록됩니다.

예시 스크립트는 `example/crawl.py`를 참고해주세요.

## 상태, 브라우저, 재시작 정책

서비스는 하나의 headful Chromium 인스턴스를 시작하고, 각 작업은 CDP를 통해 그 브라우저에 연결한 뒤 독립적인 context와 page를 사용합니다. 가상 디스플레이 또는 Chromium 시작에 실패하면 서비스는 준비 상태가 되지 않습니다.

`GET /health`는 다음과 같은 정보를 반환합니다.

```json
{
  "status": "ok",
  "browser_connected": true,
  "workers_ready": true,
  "queued_tasks": 0
}
```

브라우저 연결 또는 워커 풀이 준비되지 않으면 응답은 `503`이고 `status`는 `unavailable`입니다. 공유 Chromium이 정상 shutdown 밖에서 끊기면 서비스 프로세스는 exit code 1로 끝나므로 systemd 같은 process manager가 서비스를 재시작해야 합니다.

Prometheus-compatible runtime metrics는 `GET /metrics`에서 조회할 수 있습니다.

작업 상태는 현재 서비스 프로세스의 메모리에 보관됩니다. 작업 폴더와 파일은 retention 기간 동안 남을 수 있지만, 서버 재시작 뒤에는 이전 작업 ID의 status/results API 조회나 대기 작업의 자동 재개를 보장하지 않습니다. 클라이언트는 중요한 결과 파일을 완료 직후 내려받아 보관해야 합니다.

## 환경 변수

| 변수 | 기본값 | 설명 |
| --- | --- | --- |
| `PORT` | `5000` | HTTP 수신 포트 |
| `HOST` | `0.0.0.0` | HTTP 바인드 주소 |
| `RUN_HEAVY_STARTUP` | `true` | `true`일 때 가상 디스플레이, Chromium, 워커를 시작합니다. `false`는 테스트/import 전용입니다. |
| `CDP_PORT` | `9222` | 공유 Chromium 원격 디버깅 포트 |
| `BROWSER_EXECUTABLE_PATH` | unset | Playwright 관리형 Chromium 대신 실행할 Chromium/Chrome 바이너리 절대 경로 |
| `MAX_CONCURRENT_TASKS` | `3` | 동시에 실행할 워커 수 |
| `JOB_TIMEOUT_SECONDS` | `3600` | 작업 subprocess timeout(초) |
| `JOB_FOLDER` | `submitted_jobs` | 프로젝트 루트 기준 작업 폴더 |
| `JOB_RETENTION_DAYS` | `3` | 오래된 작업 폴더 삭제 기준(일) |
| `CLEANUP_INTERVAL_HOURS` | `24` | retention cleanup 실행 간격(시간) |
| `UVICORN_RELOAD` | `false` | 개발용 Uvicorn reload 활성화 |
| `TZ` | `Asia/Seoul` | 서비스 시간대 |
| `LOG_LEVEL` | `INFO` | 애플리케이션 로그 레벨 |

systemd 배포 workflow는 서비스 restart 뒤 최대 30초 동안 `/health`를 확인합니다. 이 검사에 실패하면 배포 job도 실패합니다.

## 라이선스
이 프로젝트는 MIT 라이선스를 따릅니다. 자세한 내용은 LICENSE 파일을 참고하세요.
