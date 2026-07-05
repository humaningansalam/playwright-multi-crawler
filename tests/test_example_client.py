from pathlib import Path

from example.job import default_crawl_script_path


def test_example_client_default_crawl_script_exists():
    script_path = Path(default_crawl_script_path())

    assert script_path.name == "crawl.py"
    assert script_path.exists()
    assert script_path.parent.name == "example"
