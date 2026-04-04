"""LLM host configuration dialogs (local, self-hosted, cloud)."""

import os
from typing import Optional
from PySide6.QtCore import Qt, Signal, QThread
from PySide6.QtGui import QFont, QIcon
from PySide6.QtWidgets import (
    QDialog,
    QVBoxLayout,
    QFormLayout,
    QDialogButtonBox,
    QLabel,
    QLineEdit,
    QComboBox,
    QPushButton,
    QCheckBox,
    QMessageBox,
    QTabWidget,
    QGroupBox,
    QWidget,
)
from core.config import ConfigManager
from core.logger import get_logger
from ui.components.llm_server_panel import LLMServerPanel

logger = get_logger(__name__)


class AIHostConfigDialog(QDialog):
    """Dialog to configure AI host settings.

    When *include_cloud* is True the combo also offers OpenAI / Claude so the
    user can switch the active host to a cloud provider.  This is the only
    place where the active AI host can be changed.
    """

    def __init__(self, parent=None, include_cloud: bool = False):
        super().__init__(parent)
        self.setWindowTitle("AI Host Configuration")
        self.setModal(True)
        self.setMinimumWidth(560)
        self.windowIcon = QIcon("icon.ico")
        self.setWindowIcon(self.windowIcon)

        self._include_cloud = include_cloud

        layout = QVBoxLayout(self)

        # Title
        title = QLabel("Configure AI Host")
        title.setFont(QFont("Roboto", 14, QFont.Weight.Bold))
        title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(title)

        # Instructions
        cloud_lines = (
            (
                "• OpenAI: Use your OpenAI API key (cloud)\n"
                "• Claude: Use your Anthropic API key (cloud)\n"
            )
            if include_cloud
            else ""
        )
        self.instructions = QLabel(
            "Configure your AI host.\n\n"
            + cloud_lines
            + "• Local LLM: Connect to an existing server (Ollama, llama-cpp-python, etc.)\n"
            "• Self-Host Model: Download & run a model locally with built-in server management"
        )
        self.instructions.setWordWrap(True)
        layout.addWidget(self.instructions)

        layout.addSpacing(15)

        # Form layout
        form_layout = QFormLayout()

        # Host type selection
        self.provider_combo = QComboBox()
        items = []
        if include_cloud:
            items += ["OpenAI", "Claude"]
        items += ["Local LLM", "Self-Host Model"]
        self.provider_combo.addItems(items)
        self.provider_combo.setToolTip(
            "Choose how the AI processes your queries.\n"
            "• Cloud providers (OpenAI, Claude) require an API key and internet.\n"
            "• Local LLM connects to a server you already have running.\n"
            "• Self-Host Model downloads and runs a model for you."
        )
        # Signal connected later, after all widgets are created
        form_layout.addRow("Host Type:", self.provider_combo)

        # Local LLM fields
        local_llm_config = ConfigManager.get_local_llm_config()

        self.local_url_input = QLineEdit()
        self.local_url_input.setPlaceholderText("http://localhost:11434/v1")
        self.local_url_input.setText(local_llm_config["local_llm_url"])
        self.local_url_input.setToolTip(
            "The base URL of your local LLM server's OpenAI-compatible API.\n"
            "Examples:\n"
            "  Ollama: http://localhost:11434/v1\n"
            "  LM Studio: http://localhost:1234/v1"
        )
        self.local_url_label = QLabel("Server URL:")
        form_layout.addRow(self.local_url_label, self.local_url_input)

        self.local_model_input = QLineEdit()
        self.local_model_input.setPlaceholderText("mistral")
        self.local_model_input.setText(local_llm_config["local_llm_model"])
        self.local_model_input.setToolTip(
            "The model identifier your local server uses.\n"
            "This is sent in API requests so the server knows which model to run.\n"
            "Check your server's model list for available names."
        )
        self.local_model_label = QLabel("Model Name:")
        form_layout.addRow(self.local_model_label, self.local_model_input)

        layout.addLayout(form_layout)

        # ---- Self-Host Model section (hidden by default) ----
        self.self_host_group = QGroupBox("Self-Host Model")
        sh_layout = QVBoxLayout(self.self_host_group)

        self.server_panel = LLMServerPanel(show_auto_start=False, parent=self)
        sh_layout.addWidget(self.server_panel)

        self.self_host_group.hide()
        layout.addWidget(self.self_host_group)

        layout.addSpacing(10)

        # Set as default checkbox
        self.set_default_checkbox = QCheckBox("Set as default provider")
        self.set_default_checkbox.setChecked(True)
        layout.addWidget(self.set_default_checkbox)

        layout.addSpacing(5)

        # Buttons
        button_box = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel
        )
        button_box.accepted.connect(self.validate_and_save)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)

        # All widgets are now created — pre-select the current host and
        # connect the signal so on_provider_changed can safely reference
        # every widget.
        current_default = ConfigManager.get_default_api()
        default_map = {"openai": "OpenAI", "claude": "Claude", "local": "Local LLM"}
        preselect = default_map.get(current_default, items[0])
        if preselect:
            idx = self.provider_combo.findText(preselect)
            if idx >= 0:
                self.provider_combo.setCurrentIndex(idx)

        # Apply initial visibility for the pre-selected host
        self.on_provider_changed(self.provider_combo.currentText())

        # Now connect the signal for future changes
        self.provider_combo.currentTextChanged.connect(self.on_provider_changed)

    def _is_local_mode(self) -> bool:
        """Check if the current provider selection is Local LLM."""
        return self.provider_combo.currentText() == "Local LLM"

    def _is_self_host_mode(self) -> bool:
        """Check if the current provider selection is Self-Host Model."""
        return self.provider_combo.currentText() == "Self-Host Model"

    def _is_cloud_mode(self) -> bool:
        """Check if the current selection is a cloud provider."""
        return self.provider_combo.currentText() in ("OpenAI", "Claude")

    def on_provider_changed(self, provider):
        """Toggle between cloud, local LLM fields and self-host panel."""
        logger.debug(f"AI host changed to: {provider}")
        is_cloud = provider in ("OpenAI", "Claude")
        is_local = provider == "Local LLM"
        is_self_host = provider == "Self-Host Model"

        # Local fields
        self.local_url_label.setVisible(is_local)
        self.local_url_input.setVisible(is_local)
        self.local_model_label.setVisible(is_local)
        self.local_model_input.setVisible(is_local)

        # Self-host panel
        self.self_host_group.setVisible(is_self_host)
        if is_self_host:
            self.server_panel.refresh_server_status()

        # Resize dialog to fit current content
        self.adjustSize()

    def validate_and_save(self):
        """Validate and save the host settings."""
        provider = self.provider_combo.currentText()
        logger.debug(f"Validating settings for host type: {provider}")

        if self._is_cloud_mode():
            # --- Cloud provider path (just switch the default host) ---
            provider_key = provider.lower()
            # Check that the user actually has an API key stored for this provider
            existing_key = ConfigManager.get_api_key(provider_key)
            if not existing_key:
                QMessageBox.warning(
                    self,
                    "No API Key",
                    f"No API key is configured for {provider}.\n\n"
                    "Please set your API key first via\n"
                    "Settings \u2192 API Key Settings.",
                )
                return

            # Verify key validity with the provider so stale/invalid keys are caught.
            is_valid, validation_msg = ConfigManager.verify_api_key(
                provider_key, existing_key
            )
            if not is_valid:
                QMessageBox.warning(
                    self,
                    "Invalid API Key",
                    f"The stored API key for {provider} could not be verified.\n\n"
                    f"Reason: {validation_msg}\n\n"
                    "Please update your API key in Settings -> API Key Settings.",
                )
                return

            if self.set_default_checkbox.isChecked():
                ConfigManager.set_default_api(provider_key)
            logger.info(f"AI host switched to cloud provider: {provider}")
            QMessageBox.information(
                self,
                "Success",
                f"AI host set to {provider}.\n\n" "The existing API key will be used.",
            )
            self.accept()
            return

        if self._is_self_host_mode():
            # --- Self-Host path ---
            from model_manager import get_hosted_url, is_server_running

            info = self.server_panel.get_selected_model_info()
            if not info:
                QMessageBox.warning(self, "No Model", "Please select a model.")
                return

            model_path = info["path"]
            port = self.server_panel.port_spin.value()
            context_size = self.server_panel.context_spin.value()
            gpu_layers = self.server_panel.gpu_spin.value()

            # Save hosted config
            ok, msg = ConfigManager.set_hosted_llm_config(
                model_path=model_path,
                port=port,
                context_size=context_size,
                gpu_layers=gpu_layers,
                auto_start=True,
            )
            if not ok:
                QMessageBox.critical(self, "Error", f"Failed to save config: {msg}")
                return

            # Point local LLM settings at the hosted server URL
            hosted_url = get_hosted_url(port=port)
            # Derive a model name from the GGUF filename so the value
            # sent in /chat/completions requests matches what the hosted
            # server actually exposes (llama-cpp-python uses the stem).
            _model_id = os.path.splitext(os.path.basename(model_path))[0]
            ok2, msg2 = ConfigManager.set_local_llm_config(hosted_url, _model_id)
            if not ok2:
                QMessageBox.critical(
                    self, "Error", f"Failed to save local config: {msg2}"
                )
                return

            if self.set_default_checkbox.isChecked():
                ConfigManager.set_default_api("local")

            running = is_server_running()
            status_note = (
                "Server is running."
                if running
                else "Server is not running yet — start it above or it will auto-start next launch."
            )
            logger.info("Self-host model settings saved successfully")
            QMessageBox.information(
                self,
                "Success",
                f"Self-Host Model configured!\n\n"
                f"Model: {os.path.basename(model_path)}\n"
                f"URL: {hosted_url}\n\n"
                f"{status_note}",
            )
            self.accept()
            return

        if self._is_local_mode():
            # --- Local LLM path ---
            url = self.local_url_input.text().strip()
            model = self.local_model_input.text().strip()

            if not url:
                QMessageBox.warning(
                    self, "Missing URL", "Please enter the local LLM server URL."
                )
                return
            if not model:
                QMessageBox.warning(
                    self,
                    "Missing Model",
                    "Please enter the local model name (e.g. 'mistral').",
                )
                return

            success, message = ConfigManager.set_local_llm_config(url, model)
            if success:
                if self.set_default_checkbox.isChecked():
                    ConfigManager.set_default_api("local")
                logger.info("Local LLM settings saved successfully")
                QMessageBox.information(
                    self,
                    "Success",
                    f"Local LLM configured!\n\nURL: {url}\nModel: {model}",
                )
                self.accept()
            else:
                logger.error(f"Failed to save local LLM config: {message}")
                QMessageBox.critical(
                    self, "Error", f"Failed to save settings: {message}"
                )
            return


class ModelDownloadThread(QThread):
    """Background thread for downloading a GGUF model."""

    progress = Signal(
        float, float, float
    )  # pct, downloaded, total (float to avoid int32 overflow on large files)
    finished = Signal(bool, str)  # success, message

    def __init__(self, url: str, filename: str, parent=None):
        super().__init__(parent)
        self.url = url
        self.filename = filename

    def run(self):
        from model_manager import download_model

        def _cb(pct, downloaded, total):
            self.progress.emit(pct, downloaded, total)

        ok, msg = download_model(self.url, self.filename, callback=_cb)
        self.finished.emit(ok, msg)


class ServerStartThread(QThread):
    """Background thread for starting the hosted model server."""

    finished = Signal(bool, str)  # success, message

    def __init__(
        self,
        model_path: str,
        port: int,
        gpu_layers: int,
        context_size: int = 4096,
        parent=None,
    ):
        super().__init__(parent)
        self.model_path = model_path
        self.port = port
        self.gpu_layers = gpu_layers
        self.context_size = context_size

    def run(self):
        from model_manager import start_model_server

        ok, msg = start_model_server(
            self.model_path,
            port=self.port,
            n_ctx=self.context_size,
            n_gpu_layers=self.gpu_layers,
        )
        self.finished.emit(ok, msg)


class LocalLLMSettingsDialog(QDialog):
    """
    Dialog for configuring local LLM access.

    Tab 1 — "Connect to Server": point at an existing local server (Ollama, etc.)
    Tab 2 — "Host a Model":     download a model and run a built-in server
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Local LLM Settings")
        self.setModal(True)
        self.setMinimumWidth(560)

        root_layout = QVBoxLayout(self)

        # Title
        title = QLabel("Local LLM Configuration")
        title.setFont(QFont("Roboto", 14, QFont.Weight.Bold))
        title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        root_layout.addWidget(title)

        root_layout.addSpacing(5)

        # Tabs
        self.tabs = QTabWidget()
        self.tabs.addTab(self._build_connect_tab(), "Connect to Server")
        self.tabs.addTab(self._build_host_tab(), "Host a Model")
        self.tabs.currentChanged.connect(self._on_tab_changed)
        root_layout.addWidget(self.tabs)

        root_layout.addSpacing(5)

        # Dialog buttons
        button_box = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel
        )
        button_box.accepted.connect(self._save)
        button_box.rejected.connect(self.reject)
        root_layout.addWidget(button_box)

        self._resize_to_tab()

    # ------------------------------------------------------------------
    #  Resize dialog to fit current tab
    # ------------------------------------------------------------------
    def _on_tab_changed(self, _index: int):
        self._resize_to_tab()

    def _resize_to_tab(self):
        current = self.tabs.currentWidget()
        if current is None:
            return
        tab_hint = current.sizeHint()
        # tab bar height + frame around the tab content
        tab_bar_h = self.tabs.tabBar().sizeHint().height()
        tab_margins = self.tabs.contentsMargins()
        frame_h = tab_margins.top() + tab_margins.bottom()
        # non-tab layout overhead: title, spacing, button box, margins
        margins = self.layout().contentsMargins()
        spacing = self.layout().spacing()
        overhead = margins.top() + margins.bottom()
        for i in range(self.layout().count()):
            item = self.layout().itemAt(i)
            w = item.widget()
            if w is not None and w is not self.tabs:
                overhead += w.sizeHint().height() + spacing
            elif item.spacerItem():
                overhead += item.spacerItem().sizeHint().height() + spacing
        new_h = tab_hint.height() + tab_bar_h + frame_h + overhead
        new_w = max(self.minimumWidth(), tab_hint.width() + 40)
        self.resize(new_w, max(new_h, 200))

    # ------------------------------------------------------------------
    #  Tab 1 — Connect to an existing server
    # ------------------------------------------------------------------
    def _build_connect_tab(self) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)

        instructions = QLabel(
            "Point at an existing local LLM server.\n"
            "Supported: Ollama, llama-cpp-python, LM Studio, vLLM, etc."
        )
        instructions.setWordWrap(True)
        layout.addWidget(instructions)

        layout.addSpacing(8)

        local_cfg = ConfigManager.get_local_llm_config()
        form = QFormLayout()

        self.url_input = QLineEdit()
        self.url_input.setText(local_cfg["local_llm_url"])
        self.url_input.setPlaceholderText("http://localhost:11434/v1")
        self.url_input.setToolTip(
            "The base URL of your local LLM server's OpenAI-compatible API.\n"
            "Examples:\n"
            "  Ollama: http://localhost:11434/v1\n"
            "  LM Studio: http://localhost:1234/v1"
        )
        form.addRow("Server URL:", self.url_input)

        self.model_input = QLineEdit()
        self.model_input.setText(local_cfg["local_llm_model"])
        self.model_input.setPlaceholderText("mistral")
        self.model_input.setToolTip(
            "The model identifier your local server uses.\n"
            "This is sent in API requests so the server knows which model to run.\n"
            "Check your server's model list for available names."
        )
        form.addRow("Model Name:", self.model_input)

        layout.addLayout(form)
        layout.addSpacing(8)

        test_btn = QPushButton("Test Connection")
        test_btn.clicked.connect(self._test_connection)
        layout.addWidget(test_btn)

        layout.addStretch()
        return tab

    # ------------------------------------------------------------------
    #  Tab 2 — Download & host a model (delegated to LLMServerPanel)
    # ------------------------------------------------------------------
    def _build_host_tab(self) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)

        self.server_panel = LLMServerPanel(show_auto_start=True, parent=tab)
        self.server_panel.server_started.connect(self._on_panel_server_started)
        layout.addWidget(self.server_panel)

        return tab

    def _on_panel_server_started(self, success: bool, message: str):
        """When the panel's server starts, auto-update the connect-tab URL."""
        if success:
            from model_manager import get_hosted_url

            hosted_url = get_hosted_url(port=self.server_panel.port_spin.value())
            self.url_input.setText(hosted_url)

    # ------------------------------------------------------------------
    #  Test connection (existing server tab)
    # ------------------------------------------------------------------
    def _test_connection(self):
        """Try to reach the local LLM server and report status."""
        url = self.url_input.text().strip().rstrip("/")
        if not url:
            QMessageBox.warning(self, "Missing URL", "Enter a server URL first.")
            return
        try:
            import httpx

            resp = httpx.get(f"{url}/models", timeout=10.0)
            resp.raise_for_status()
            models = resp.json().get("data", [])
            model_names = [m.get("id", "?") for m in models]
            QMessageBox.information(
                self,
                "Connection Successful",
                f"Connected to {url}\n\nAvailable models:\n"
                + "\n".join(f"  \u2022 {n}" for n in model_names[:15]),
            )
        except Exception as e:
            QMessageBox.warning(
                self,
                "Connection Failed",
                f"Could not reach {url}:\n\n{e}\n\n" "Make sure the server is running.",
            )

    # ------------------------------------------------------------------
    #  Save
    # ------------------------------------------------------------------
    def _save(self):
        """Persist all local LLM settings (connect tab + host tab)."""
        url = self.url_input.text().strip()
        model = self.model_input.text().strip()

        if not url:
            QMessageBox.warning(self, "Missing URL", "Server URL is required.")
            return
        if not model:
            QMessageBox.warning(self, "Missing Model", "Model name is required.")
            return

        ok1, msg1 = ConfigManager.set_local_llm_config(url, model)
        if not ok1:
            QMessageBox.critical(self, "Error", f"Failed to save: {msg1}")
            return

        # Save hosted server settings
        info = self.server_panel.get_selected_model_info()
        model_path = info["path"] if info else ""
        auto_start = (
            self.server_panel.auto_start_cb.isChecked()
            if self.server_panel.auto_start_cb is not None
            else False
        )
        ok2, msg2 = ConfigManager.set_hosted_llm_config(
            model_path=model_path,
            port=self.server_panel.port_spin.value(),
            context_size=self.server_panel.context_spin.value(),
            gpu_layers=self.server_panel.gpu_spin.value(),
            auto_start=auto_start,
        )
        if not ok2:
            QMessageBox.critical(self, "Error", f"Failed to save hosted config: {msg2}")
            return

        self.accept()
