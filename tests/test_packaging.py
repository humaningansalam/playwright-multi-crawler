import subprocess
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
    assert "pydantic<3,>=2" in metadata.get_all("Requires-Dist", [])
