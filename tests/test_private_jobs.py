import json
from pathlib import Path

import pytest

from src.config import JOB_FOLDER
from src.core import state_manager as state
from src.core.private_jobs import JOB_TOKEN_HEADER, PRIVATE_INPUT_DIRECTORY, token_digest
from src.models.job import JobError, JobErrorCode, JobStatus

JOB_TOKEN = "A" * 43
OTHER_TOKEN = "B" * 43
HEADERS = {JOB_TOKEN_HEADER: JOB_TOKEN}


async def submit_private_job(client, jobname="private-test"):
    response = await client.post(
        "/api/jobs/submit",
        data={"jobname": jobname},
        headers=HEADERS,
        files=[
            ("script_file", ("crawl.py", b"pass\n", "text/x-python")),
            ("additional_files", ("public.txt", b"public-input", "text/plain")),
            ("private_files", ("config.json", b"synthetic-private-input", "application/json")),
            ("private_files", ("cookies.json", b"[]", "application/json")),
        ],
    )
    assert response.status_code == 202
    job_id = response.json()["job_id"]
    return job_id, Path(JOB_FOLDER) / job_id


@pytest.mark.asyncio
async def test_private_job_requires_token_for_every_job_route(client):
    capabilities = await client.get("/api/jobs/capabilities")
    assert capabilities.json() == {"private_jobs": 1}
    job_id, job_dir = await submit_private_job(client)
    private_dir = job_dir / PRIVATE_INPUT_DIRECTORY
    assert private_dir.stat().st_mode & 0o777 == 0o700
    assert (private_dir / "config.json").stat().st_mode & 0o777 == 0o600
    assert not (job_dir / "config.json").exists()
    persisted = json.loads((job_dir / "state.json").read_text())
    assert persisted["access_token_sha256"] == token_digest(JOB_TOKEN)
    assert JOB_TOKEN not in json.dumps(persisted)
    for headers in ({}, {JOB_TOKEN_HEADER: OTHER_TOKEN}):
        for route in ("status", "results", "logs"):
            response = await client.get(f"/api/jobs/{route}/{job_id}", headers=headers)
            assert response.status_code == 404
        response = await client.get(f"/api/jobs/download/{job_id}/public.txt", headers=headers)
        assert response.status_code == 404
        response = await client.post(f"/api/jobs/{job_id}/cancel", headers=headers)
        assert response.status_code == 404
    assert (await client.get(f"/api/jobs/status/{job_id}", headers=HEADERS)).status_code == 200
    assert (
        await client.get(f"/api/jobs/download/{job_id}/config.json", headers=HEADERS)
    ).status_code == 404
    assert (
        await client.get(f"/api/jobs/download/{job_id}/state.json", headers=HEADERS)
    ).status_code == 404
    response = await client.post(f"/api/jobs/{job_id}/cancel", headers=HEADERS)
    assert response.json()["status"] == "CANCELLED"
    assert not private_dir.exists()
    terminal = await client.get(f"/api/jobs/results/{job_id}", headers=HEADERS)
    assert "config.json" not in terminal.json()["files"]
    assert "cookies.json" not in terminal.json()["files"]


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.INTERRUPTED])
async def test_terminal_transition_removes_private_inputs(client, status):
    job_id, job_dir = await submit_private_job(client)
    await state.update_job_status(job_id, JobStatus.RUNNING)
    result = (
        {"done": True}
        if status == JobStatus.COMPLETED
        else JobError(code=JobErrorCode.PROCESSING_FAILED, message="synthetic terminal result")
    )
    await state.update_job_status(job_id, status, result)
    assert not (job_dir / PRIVATE_INPUT_DIRECTORY).exists()
    terminal = await client.get(f"/api/jobs/results/{job_id}", headers=HEADERS)
    assert terminal.status_code == 200
    assert terminal.headers["Cache-Control"] == "no-store"
    assert JOB_TOKEN_HEADER in terminal.headers["Vary"]
    assert "config.json" not in terminal.json()["files"]
    assert "cookies.json" not in terminal.json()["files"]


@pytest.mark.asyncio
async def test_private_session_output_is_not_the_uploaded_session(client):
    job_id, job_dir = await submit_private_job(client, "tistory_post")
    await state.update_job_status(job_id, JobStatus.RUNNING)
    (job_dir / "cookies.json").write_text('[{"name":"synthetic-new-session"}]')
    await state.update_job_status(job_id, JobStatus.COMPLETED, {"done": True})
    assert not (job_dir / PRIVATE_INPUT_DIRECTORY).exists()
    terminal = await client.get(f"/api/jobs/results/{job_id}", headers=HEADERS)
    assert "cookies.json" in terminal.json()["files"]
    assert "config.json" not in terminal.json()["files"]
    route = terminal.json()["files"]["cookies.json"]
    assert (await client.get(route)).status_code == 404
    response = await client.get(route, headers=HEADERS)
    assert response.json() == [{"name": "synthetic-new-session"}]
    assert response.headers["Cache-Control"] == "no-store"


@pytest.mark.asyncio
async def test_private_uploads_and_legacy_tistory_require_private_contract(client):
    files = [
        ("script_file", ("crawl.py", b"pass\n", "text/x-python")),
        ("private_files", ("config.json", b"{}", "application/json")),
    ]
    for headers in ({}, {JOB_TOKEN_HEADER: "too-short"}):
        response = await client.post(
            "/api/jobs/submit", data={"jobname": "generic"}, headers=headers, files=files
        )
        assert response.status_code == 400
    legacy_files = [
        ("script_file", ("crawl.py", b"pass\n", "text/x-python")),
        ("additional_files", ("config.json", b"{}", "application/json")),
    ]
    for headers in ({}, HEADERS):
        response = await client.post(
            "/api/jobs/submit",
            data={"jobname": "tistory_post"},
            headers=headers,
            files=legacy_files,
        )
        assert response.status_code == 400
    assert not list(Path(JOB_FOLDER).iterdir())


@pytest.mark.asyncio
async def test_restart_cleans_terminal_and_abandoned_inputs_and_keeps_pending(client):
    pending_id, pending_dir = await submit_private_job(client, "pending-private")
    running_id, running_dir = await submit_private_job(client, "running-private")
    await state.update_job_status(running_id, JobStatus.RUNNING)
    done_id, done_dir = await submit_private_job(client, "done-private")
    await state.update_job_status(done_id, JobStatus.COMPLETED, {"done": True})
    (done_dir / PRIVATE_INPUT_DIRECTORY).mkdir()
    (done_dir / PRIVATE_INPUT_DIRECTORY / "stale").write_text("synthetic")
    orphan_dir = Path(JOB_FOLDER) / "abandoned-submission" / PRIVATE_INPUT_DIRECTORY
    orphan_dir.mkdir(parents=True)
    (orphan_dir / "orphan").write_text("synthetic")
    state._job_status_and_results.clear()
    state._submitted_jobs.clear()
    recovered = await state.recover_persisted_jobs(JOB_FOLDER)
    assert [job.job_id for job in recovered] == [pending_id]
    assert (pending_dir / PRIVATE_INPUT_DIRECTORY / "config.json").exists()
    assert not (running_dir / PRIVATE_INPUT_DIRECTORY).exists()
    assert not (done_dir / PRIVATE_INPUT_DIRECTORY).exists()
    assert not orphan_dir.exists()
    assert await state.get_job_status(running_id) == JobStatus.INTERRUPTED
    assert (await client.get(f"/api/jobs/status/{pending_id}")).status_code == 404
    assert (await client.get(f"/api/jobs/status/{pending_id}", headers=HEADERS)).status_code == 200


@pytest.mark.asyncio
async def test_legacy_tistory_recovery_discards_old_account_inputs(client):
    job_id = "legacy-tistory"
    job_dir = Path(JOB_FOLDER) / job_id
    job_dir.mkdir()
    (job_dir / "script.py").write_text("pass\n")
    (job_dir / "config.json").write_text("synthetic")
    (job_dir / "cookies.json").write_text("[]")
    await state.set_initial_status(job_id, "tistory_post", str(job_dir))
    assert (await client.get(f"/api/jobs/status/{job_id}")).status_code == 404
    state._job_status_and_results.clear()
    state._submitted_jobs.clear()
    assert await state.recover_persisted_jobs(JOB_FOLDER) == []
    assert await state.get_job_status(job_id) == JobStatus.INTERRUPTED
    assert not (job_dir / "config.json").exists()
    assert not (job_dir / "cookies.json").exists()
