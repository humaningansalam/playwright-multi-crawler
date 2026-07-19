import importlib.util
import inspect
from pathlib import Path

import pytest

from example import crawl as example_crawl
from example.job import default_crawl_script_path
from src.api.jobs import RESERVED_JOB_FILENAMES


def test_example_client_default_crawl_script_exists():
    script_path = Path(default_crawl_script_path())

    assert script_path.name == "crawl.py"
    assert script_path.exists()
    assert script_path.parent.name == "example"


def test_readme_submit_example_uses_bundled_crawl_script():
    readme = Path(__file__).resolve().parents[1] / "README.md"

    assert '-F "script_file=@example/crawl.py"' in readme.read_text(encoding="utf-8")


def test_readme_submit_example_does_not_require_missing_additional_file():
    readme = Path(__file__).resolve().parents[1] / "README.md"

    assert '@textfile.txt' not in readme.read_text(encoding="utf-8")


def test_readme_documents_all_reserved_additional_filenames():
    readme = Path(__file__).resolve().parents[1] / "README.md"
    submission_section = readme.read_text(encoding="utf-8").split(
        "### 작업 상태와 취소",
        maxsplit=1,
    )[0]

    for filename in RESERVED_JOB_FILENAMES:
        assert f"`{filename}`" in submission_section


def test_bundled_example_is_importable_with_worker_loader():
    script_path = Path(__file__).resolve().parents[1] / "example" / "crawl.py"
    spec = importlib.util.spec_from_file_location("example_crawl", str(script_path))
    assert spec is not None
    assert spec.loader is not None

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    assert module.crawl.__name__ == "crawl"
    assert inspect.iscoroutinefunction(module.crawl)


@pytest.mark.asyncio
async def test_bundled_example_propagates_crawl_failures(tmp_path):
    class FailingPage:
        async def goto(self, *_args, **_kwargs):
            raise RuntimeError("navigation failed")

    with pytest.raises(RuntimeError, match="navigation failed"):
        await example_crawl.crawl(FailingPage(), object(), str(tmp_path))
