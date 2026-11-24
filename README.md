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
- Playwright Chromium 실행에 필요한 OS 패키지
- headful Chromium을 위한 Xephyr (`xserver-xephyr`)

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

## 개발 검증

```bash
uv sync --locked
uv run ruff check src tests example
uv run ruff format --check src tests example
uv run pytest -q
uv build
```

변경 원칙과 리뷰 기준은 [CONTRIBUTING.md](CONTRIBUTING.md), 저장소 구조와 핵심 불변식은 [AGENTS.md](AGENTS.md)를 참고하세요.

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

`additional_files`는 작업 폴더의 최상위 파일명만 사용할 수 있습니다. 경로, `..`, 중복 파일명, 그리고 서비스가 사용하는 `script.py`, `result.json`, `result.json.tmp`, `stdout.log`, `stderr.log`, `state.json`, `.private-inputs`은 거부됩니다.

### 비공개 작업

비밀번호·세션 같은 인증 입력이 있는 작업은 `GET /api/jobs/capabilities`의 `private_jobs: 1` 지원을 확인한 뒤 제출합니다. 클라이언트는 작업마다 독립적인 32바이트 난수의 base64url 토큰(패딩 제외 43자)을 생성하고 `X-Job-Token` 헤더로 전송합니다. 동일 헤더가 상태, 취소, 결과, 로그, 다운로드 요청에 필요합니다. 토큰 원문은 서버 상태에 저장하지 않으며, 인증되지 않은 요청에는 작업을 찾을 수 없다는 응답을 반환합니다.

민감한 첨부파일은 `private_files` multipart 필드에 넣습니다. 스크립트는 `job_path/.private-inputs/`에서 읽으며, 이 디렉터리는 결과 목록·다운로드에서 제외됩니다. 서버는 작업이 성공·실패·취소되거나 실행 중 재시작으로 중단될 때 비공개 입력을 삭제합니다. 재시작으로 복원되는 정상 대기 작업의 입력은 실행을 위해 유지됩니다. 새 세션 같은 출력은 작업 루트에 작성할 수 있으며, 비공개 작업의 인증된 결과 요청에서만 접근 가능합니다. 보존 기간 이후 정기 정리됩니다.

기존 Tistory의 공개 방식 `tistory_post` 작업은 새 제출과 조회가 차단됩니다. 복구 가능한 과거 작업에 남은 루트의 `config.json`과 `cookies.json`은 시작 시 삭제하고, 대기 중인 이전 작업은 중단 상태로 전환합니다. 다른 공개 작업의 API는 그대로 유지됩니다. 이 기능은 신뢰하는 스크립트 실행 서비스의 작업 단위 접근 제한이며, 서비스 전체 인증 또는 악성 스크립트 격리 기능은 아닙니다. 원격 연결에는 TLS 또는 신뢰된 사설 전송 경계를 사용하고 토큰을 로그·URL에 남기지 마십시오.

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
  --job-name example-domain \
  --server http://localhost:5000 \
  --output downloads
```

`--file`은 필요한 만큼 반복할 수 있습니다. 완료된 파일은 `<output>/<job_id>/`에 저장됩니다. 작업이 `FAILED`, `CANCELLED`, 또는 `INTERRUPTED`로 끝나면 CLI는 결과 JSON을 출력하고 종료 코드 `1`을 반환합니다.

서버가 structured API error를 반환하면 CLI는 해당 error `code`와 `message`를 stderr에 표시하고 종료 코드 `1`을 반환합니다.

로그를 따라가는 동안 `Ctrl+C`를 누르면 CLI는 현재 원격 작업의 cancel endpoint를 호출하고 종료 코드 `130`으로 끝납니다.

### Python API client example

`example/job.py`는 애플리케이션 코드에서 HTTP API를 직접 사용하는 예제입니다. bundled `example/crawl.py`를 업로드하고, status endpoint를 polling한 뒤 결과와 생성 파일을 다운로드합니다. 실행 중 `Ctrl+C`를 누르면 원격 작업도 취소합니다.

```bash
uv run python example/job.py
```

터미널에서 한 번 실행할 때는 `crawler` 명령을 사용하고, 제출·상태 조회·취소·결과 처리를 Python 코드에 통합할 때는 `example/job.py`의 함수 구성을 참고하세요.

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

작업 상태는 각 작업 폴더의 `state.json`에 저장됩니다. 서버가 재시작되면 종료된 작업은 기존 상태로 복구되고, `PENDING` 작업은 다시 queue에 등록되며, 재시작 당시 `RUNNING`이던 작업은 `INTERRUPTED`와 `SERVICE_SHUTDOWN` 오류로 종료됩니다. 작업 폴더는 retention 정책에 따라 삭제될 수 있으므로 클라이언트는 중요한 결과 파일을 완료 직후 내려받아 보관해야 합니다.

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
| `DISPLAY` | unset | Xephyr가 연결할 상위 X display. 배포 workflow 기본값은 `:0`입니다. |
| `PLAYWRIGHT_BROWSERS_PATH` | Playwright 기본값 | Chromium 설치 및 조회 경로 |
| `APP_NAME` | `playwright-runner` | 로그/모니터링 애플리케이션 이름 |
| `APP_ENV` | `dev` | 로그/모니터링 환경 이름 |
| `LOG_LEVEL` | `INFO` | 애플리케이션 로그 레벨 |
| `LOKI_URL` | unset | 선택적 Loki push endpoint |

## systemd 배포

`.github/workflows/systemd-deploy.yaml`은 `main`에 반영된 커밋만 직렬로 배포합니다. 배포 전에 lint, format check, 전체 테스트, package build를 다시 통과해야 하며, 원격 서버에서는 `uv.lock`에 고정된 runtime dependency만 설치합니다.

GitHub Actions에는 다음 repository secret이 필요합니다.

- `SERVER_HOST`: SSH 대상 호스트
- `SERVER_USER`: SSH 사용자이자 systemd 서비스 사용자
- `SERVER_SSH_KEY`: SSH 개인 키
- `DEPLOY_PATH`: 공백 없는 절대 배포 경로

`SERVER_SSH_FINGERPRINT` secret을 설정하면 SSH 호스트 키를 검증합니다. 설정을 권장합니다.

다음 repository variable은 선택 사항입니다. 비어 있으면 괄호 안의 기본값을 사용합니다.

- `SERVER_PORT` (`5000`)
- `DISPLAY` (`:0`)
- `APP_NAME` (`playwright-runner`)
- `APP_ENV` (`production`)
- `LOG_LEVEL` (`INFO`)

원격 서버에는 `git`, `curl`, `uv`, 비대화형 `sudo` 권한이 필요합니다. workflow는 `origin/main`에 포함된 검증 대상 커밋을 배포하고, 권한 `0600`의 `.env`를 작성한 뒤 `example/playwright-multi-crawler.service`와 같은 실행 계약으로 서비스를 설치합니다. 서비스 재시작 뒤 최대 30회(1초 간격)로 `/health`를 확인하며, 실패하면 systemd 상태와 최근 journal을 출력하고 배포 job을 실패시킵니다.
