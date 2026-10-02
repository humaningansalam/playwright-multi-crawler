import httpx
import pytest

from example import job
from src import cli


@pytest.fixture(params=["cli", "example"])
def download_files(request):
    def download(client, job_id, files, output_dir):
        if request.param == "cli":
            return cli.fetch_result_and_download(
                client, "http://service.local", job_id, output_dir
            )
        return job.download_files(
            client, job_id, files, server="http://service.local", output_dir=output_dir
        )

    return download


def _client(job_id, files, downloads):
    def handler(request):
        if "/results/" in request.url.path:
            return httpx.Response(
                200,
                json={"job_id": job_id, "status": "COMPLETED", "files": files},
            )
        downloads.append(request.url.path)
        return httpx.Response(200, content=b"artifact")

    return httpx.Client(transport=httpx.MockTransport(handler))


def test_download_rejects_filename_traversal(download_files, tmp_path):
    outside = tmp_path / "outside.txt"
    outside.write_bytes(b"original")
    files = {"../../outside.txt": "/api/jobs/download/job-1/output.txt"}
    downloads = []

    with _client("job-1", files, downloads) as client:
        with pytest.raises(ValueError, match="output path"):
            download_files(client, "job-1", files, tmp_path / "downloads")

    assert outside.read_bytes() == b"original"
    assert downloads == []


@pytest.mark.parametrize(
    "filename",
    [
        "",
        ".",
        "..",
        "../outside.txt",
        "subdir/output.txt",
        "./output.txt",
        "..\\outside.txt",
        "subdir\\output.txt",
        "C:\\outside.txt",
        "C:outside.txt",
        "\\\\server\\share\\outside.txt",
        "invalid\0.txt",
    ],
)
def test_download_rejects_non_filename_components(download_files, tmp_path, filename):
    files = {filename: "/api/jobs/download/job-1/output.txt"}
    downloads = []

    with _client("job-1", files, downloads) as client:
        with pytest.raises(ValueError, match="output path"):
            download_files(client, "job-1", files, tmp_path / "downloads")

    assert downloads == []


def test_download_rejects_absolute_filename(download_files, tmp_path):
    outside = tmp_path / "outside.txt"
    outside.write_bytes(b"original")
    files = {str(outside): "/api/jobs/download/job-1/output.txt"}
    downloads = []

    with _client("job-1", files, downloads) as client:
        with pytest.raises(ValueError, match="output path"):
            download_files(client, "job-1", files, tmp_path / "downloads")

    assert outside.read_bytes() == b"original"
    assert downloads == []


@pytest.mark.parametrize("job_id", ["../outside", "..\\outside", ".", ".."])
def test_download_rejects_job_directory_traversal(download_files, tmp_path, job_id):
    files = {"output.txt": "/api/jobs/download/job-1/output.txt"}
    downloads = []

    with _client(job_id, files, downloads) as client:
        with pytest.raises(ValueError, match="output path"):
            download_files(client, job_id, files, tmp_path / "downloads")

    assert not (tmp_path / "outside").exists()
    assert not (tmp_path / "downloads").exists()
    assert downloads == []


@pytest.mark.parametrize("symlink_kind", ["job_directory", "file"])
def test_download_rejects_existing_symlink_escape(download_files, tmp_path, symlink_kind):
    outside = tmp_path / "outside"
    outside.mkdir()
    original = outside / "output.txt"
    original.write_bytes(b"original")
    output_dir = tmp_path / "downloads"
    output_dir.mkdir()
    job_output = output_dir / "job-1"
    if symlink_kind == "job_directory":
        job_output.symlink_to(outside, target_is_directory=True)
    else:
        job_output.mkdir()
        (job_output / "output.txt").symlink_to(original)
    files = {"output.txt": "/api/jobs/download/job-1/output.txt"}
    downloads = []

    with _client("job-1", files, downloads) as client:
        with pytest.raises(ValueError, match="output path"):
            download_files(client, "job-1", files, output_dir)

    assert original.read_bytes() == b"original"
    assert downloads == []


@pytest.mark.parametrize("filename", ["output.txt", "보고서 2026.csv", "report?.txt", ".result"])
def test_download_preserves_safe_filenames(download_files, tmp_path, filename):
    files = {filename: "/api/jobs/download/job-1/output.txt"}
    downloads = []

    with _client("job-1", files, downloads) as client:
        download_files(client, "job-1", files, tmp_path / "downloads")

    assert (tmp_path / "downloads" / "job-1" / filename).read_bytes() == b"artifact"
    assert downloads == ["/api/jobs/download/job-1/output.txt"]
