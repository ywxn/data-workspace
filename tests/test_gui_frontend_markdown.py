import importlib
import os
import sys
import types
from unittest.mock import MagicMock, patch

import pytest


os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

pytest.importorskip("PySide6")

from PySide6.QtWidgets import QApplication


@pytest.fixture(scope="module")
def qt_app():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


@pytest.fixture
def gui_module(monkeypatch):
    stubbed_modules = {
        "gui_backend_markdown": types.SimpleNamespace(
            DataWorkspaceBackend=type("DataWorkspaceBackend", (), {})
        ),
        "agents": types.SimpleNamespace(AIAgent=type("AIAgent", (), {})),
        "processing": types.SimpleNamespace(
            load_data=lambda *args, **kwargs: None,
            add_files_to_sqlite=lambda *args, **kwargs: None,
        ),
        "connector": types.SimpleNamespace(
            DatabaseConnector=type("DatabaseConnector", (), {})
        ),
        "markdown_converter": types.SimpleNamespace(
            markdown_to_html=lambda value: value
        ),
        "nlp_table_selector": types.SimpleNamespace(
            NLPTableSelector=type("NLPTableSelector", (), {})
        ),
        "logger": types.SimpleNamespace(get_logger=lambda name: MagicMock()),
    }

    original_modules = {}
    for name, stub in stubbed_modules.items():
        original_modules[name] = sys.modules.get(name)
        monkeypatch.setitem(sys.modules, name, stub)

    sys.modules.pop("gui_frontend_markdown", None)
    module = importlib.import_module("gui_frontend_markdown")
    yield module
    sys.modules.pop("gui_frontend_markdown", None)

    for name, original in original_modules.items():
        if original is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = original


@pytest.mark.regression
def test_api_key_dialog_defaults_to_saved_provider(gui_module, qt_app):
    with (
        patch.object(gui_module.ConfigManager, "get_default_api", return_value="claude"),
        patch.object(
            gui_module.ConfigManager,
            "get_api_key",
            side_effect=lambda provider: {
                "openai": "openai-test-key",
                "claude": "claude-test-key",
            }.get(provider),
        ),
    ):
        dialog = gui_module.APIKeyDialog()

    assert dialog.provider_combo.currentText() == "Claude"
    assert dialog.api_key_input.text() == "claude-test-key"

    dialog.close()