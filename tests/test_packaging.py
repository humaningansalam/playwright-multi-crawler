import os
import subprocess
import sys
import zipfile
from email.parser import Parser


def test_wheel_preserves_src_application_package(tmp_path):
    subprocess.run(
        ["uv", "build", "--no-sources", "--out-dir", str(tmp_path)],
        check=True,
        capture_output=True,
        text=True,
    )

    wheel_path = next(tmp_path.glob("*.whl"))
    with zipfile.ZipFile(wheel_path) as wheel:
        names = set(wheel.namelist())
        metadata_name = next(name for name in names if name.endswith(".dist-info/METADATA"))
        metadata = Parser().parsestr(wheel.read(metadata_name).decode("utf-8"))

    assert {
        "src/__init__.py",
        "src/main.py",
        "src/api/jobs.py",
        "src/worker/job_runner.py",
    } <= names
    assert "main.py" not in names
    requirements = metadata.get_all("Requires-Dist", [])
    assert "pydantic<3,>=2" in requirements
    assert (
        "his-monitoring @ "
        "git+https://github.com/humaningansalam/his-monitoring.git@"
        "1ca4734971cf03862de43276a6d54d45d14932ea"
    ) in requirements


def test_application_import_is_quiet_and_runtime_free(tmp_path):
    environment = os.environ.copy()
    environment["RUN_HEAVY_STARTUP"] = "false"

    result = subprocess.run(
        [sys.executable, "-c", "import src.main"],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout == ""
    assert result.stderr == ""
    assert list(tmp_path.iterdir()) == []
