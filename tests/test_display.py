from unittest.mock import Mock

import pytest

from src.common import tool_utils


@pytest.mark.parametrize(
    ("backend", "visible"),
    [("xvfb", 0), ("xephyr", 1)],
)
def test_display_backend_selection(monkeypatch, backend, visible):
    monkeypatch.setattr(tool_utils, "_display", None)
    monkeypatch.setattr(tool_utils, "VIRTUAL_DISPLAY_BACKEND", backend)
    monkeypatch.delenv("DISPLAY", raising=False)
    display = Mock()
    display_factory = Mock(return_value=display)
    monkeypatch.setattr(tool_utils, "Display", display_factory)

    assert tool_utils.start_display()
    display_factory.assert_called_once_with(visible=visible, backend=backend, size=(1920, 1080))
    display.start.assert_called_once()
    assert tool_utils.start_display()
    display_factory.assert_called_once()

    tool_utils.stop_display()
    display.stop.assert_called_once()
    assert tool_utils._display is None


def test_display_start_failure_leaves_no_active_display(monkeypatch):
    monkeypatch.setattr(tool_utils, "_display", None)
    monkeypatch.setattr(tool_utils, "VIRTUAL_DISPLAY_BACKEND", "xvfb")
    display = Mock()
    display.start.side_effect = RuntimeError("display failed")
    monkeypatch.setattr(tool_utils, "Display", Mock(return_value=display))

    assert not tool_utils.start_display()
    assert tool_utils._display is None


def test_unknown_display_backend_is_rejected(monkeypatch):
    monkeypatch.setattr(tool_utils, "_display", None)
    monkeypatch.setattr(tool_utils, "VIRTUAL_DISPLAY_BACKEND", "invalid")
    display_factory = Mock()
    monkeypatch.setattr(tool_utils, "Display", display_factory)

    assert not tool_utils.start_display()
    display_factory.assert_not_called()
