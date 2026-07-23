import importlib.util
import inspect
from pathlib import Path

import pytest

from example import crawl as example_crawl
from src.api.jobs import RESERVED_JOB_FILENAMES


def test_readme_submit_example_uses_bundled_crawl_script():
    readme = Path(__file__).resolve().parents[1] / "README.md"
    content = readme.read_text(encoding="utf-8")

    assert '-F "script_file=@example/crawl.py"' in content
    assert "uv run crawler example/crawl.py" in content


def test_readme_submit_example_does_not_require_missing_additional_file():
    readme = Path(__file__).resolve().parents[1] / "README.md"
    content = readme.read_text(encoding="utf-8")

    assert '@textfile.txt' not in content
    assert "example/input.json" not in content


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


@pytest.mark.asyncio
async def test_bundled_example_returns_downloadable_screenshot(tmp_path):
    class Page:
        async def goto(self, url, **options):
            assert url == "https://example.com/"
            assert options == {"wait_until": "domcontentloaded", "timeout": 30_000}

        async def title(self):
            return "Example Domain"

        async def screenshot(self, *, path, full_page):
            assert full_page is True
            Path(path).write_bytes(b"screenshot")

    result = await example_crawl.crawl(Page(), object(), str(tmp_path))

    assert result == {
        "target_url": "https://example.com/",
        "title": "Example Domain",
        "screenshot_file": "screenshot.png",
    }
    assert (tmp_path / "screenshot.png").read_bytes() == b"screenshot"
