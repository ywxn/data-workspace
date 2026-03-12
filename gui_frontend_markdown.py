import sys
import asyncio
import webbrowser
import json
from datetime import datetime
from PySide6.QtCore import Qt, Signal, QThread, QTimer, QMimeData
from PySide6.QtGui import QFont, QKeyEvent, QAction, QActionGroup, QIcon
from PySide6.QtWidgets import (
    QApplication,
    QMainWindow,
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QTextEdit,
    QPushButton,
    QLabel,
    QListWidget,
    QListWidgetItem,
    QDialog,
    QFileDialog,
    QLineEdit,
    QComboBox,
    QFormLayout,
    QDialogButtonBox,
    QMessageBox,
    QMenu,
    QProgressBar,
    QCheckBox,
    QSpinBox,
    QTabWidget,
    QGroupBox,
)
from gui_backend_markdown import DataWorkspaceBackend
from agents import AIAgent
from processing import load_data, add_files_to_sqlite
from connector import DatabaseConnector
from config import ConfigManager
from markdown_converter import markdown_to_html
from PySide6.QtGui import QPalette
from typing import Optional, Dict, Any, List
import random
import os
from nlp_table_selector import NLPTableSelector
from constants import (
    NLP_PLACEHOLDER_TEXT,
    PLACEHOLDER_PROJECT_NAMES,
    PLACEHOLDER_PROJECT_DESCRIPTIONS,
    DARK_THEME_STYLESHEET,
    LIGHT_THEME_STYLESHEET,
)
from logger import get_logger

# Initialize logger for this module
logger = get_logger(__name__)


class MessageTextEdit(QTextEdit):
    """Custom QTextEdit that submits on Enter and adds newline on Shift+Enter"""

    submit_signal = Signal()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def keyPressEvent(self, e: QKeyEvent):
        """Handle key press events for Enter vs Shift+Enter"""
        if e.key() in (Qt.Key.Key_Return, Qt.Key.Key_Enter):
            if e.modifiers() & Qt.KeyboardModifier.ShiftModifier:
                # Shift+Enter: insert newline
                super().keyPressEvent(e)
            else:
                # Enter alone: submit message
                self.submit_signal.emit()
        else:
            # All other keys: default behavior
            super().keyPressEvent(e)

    def insertFromMimeData(self, source: QMimeData) -> None:
        """Paste only plain text to prevent external rich-text formatting."""
        if source and source.hasText():
            self.insertPlainText(source.text())
            return
        super().insertFromMimeData(source)


class InteractionModeDialog(QDialog):
    """Dialog to select interaction mode at startup (CxO or Analyst)."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Select Interaction Mode")
        self.setModal(True)
        self.setMinimumWidth(480)
        self.windowIcon = QIcon("icon.ico")
        self.setWindowIcon(self.windowIcon)

        layout = QVBoxLayout(self)

        # Title
        title = QLabel("Choose Your Interaction Mode")
        title.setFont(QFont("Roboto", 14, QFont.Weight.Bold))
        title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(title)

        layout.addSpacing(10)

        # Description
        description = QLabel("Select how you'd like to interact with your data:\n")
        description.setWordWrap(True)
        layout.addWidget(description)

        layout.addSpacing(5)

        # CxO mode button
        self.cxo_btn = QPushButton("CxO Mode")
        self.cxo_btn.setFont(QFont("Roboto", 11, QFont.Weight.Bold))
        self.cxo_btn.setMinimumHeight(60)
        self.cxo_btn.setToolTip(
            "Executive-friendly: concise insights and charts.\n"
            "No SQL or technical details shown."
        )
        self.cxo_btn.clicked.connect(self._select_cxo)
        layout.addWidget(self.cxo_btn)

        cxo_desc = QLabel(
            "  \u2022 One-step insights written for executives\n"
            "  \u2022 Charts and visualizations without technical detail\n"
            "  \u2022 No SQL or intermediate steps shown"
        )
        cxo_desc.setStyleSheet("color: gray;")
        cxo_desc.setWordWrap(True)
        layout.addWidget(cxo_desc)

        layout.addSpacing(10)

        # Analyst mode button
        self.analyst_btn = QPushButton("Analyst Mode")
        self.analyst_btn.setFont(QFont("Roboto", 11, QFont.Weight.Bold))
        self.analyst_btn.setMinimumHeight(60)
        self.analyst_btn.setToolTip(
            "Full detail: SQL queries, intermediate results,\n" "and detailed analysis."
        )
        self.analyst_btn.clicked.connect(self._select_analyst)
        layout.addWidget(self.analyst_btn)

        analyst_desc = QLabel(
            "  \u2022 Full data analysis workflow with SQL visibility\n"
            "  \u2022 Intermediate results and detailed breakdowns\n"
            "  \u2022 Technical details available for deeper exploration"
        )
        analyst_desc.setStyleSheet("color: gray;")
        analyst_desc.setWordWrap(True)
        layout.addWidget(analyst_desc)

        layout.addSpacing(15)

        self.selected_mode: Optional[str] = None

    def _select_cxo(self):
        self.selected_mode = "cxo"
        self.accept()

    def _select_analyst(self):
        self.selected_mode = "analyst"
        self.accept()

    def get_selected_mode(self) -> str:
        return self.selected_mode or "analyst"


class APIKeyDialog(QDialog):
    """Simple dialog to configure cloud API keys (OpenAI / Claude)."""

    def __init__(self, parent=None, first_time_setup: bool = False):
        super().__init__(parent)
        self.setWindowTitle("API Key Configuration")
        self.setModal(True)
        self.setMinimumWidth(480)
        self.windowIcon = QIcon("icon.ico")
        self.setWindowIcon(self.windowIcon)
        self._first_time_setup = first_time_setup

        layout = QVBoxLayout(self)

        # Title
        title = QLabel("Configure API Key")
        title.setFont(QFont("Roboto", 14, QFont.Weight.Bold))
        title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(title)

        # Instructions
        instructions = QLabel(
            "Enter your API key for the selected cloud provider.\n\n"
            "• OpenAI: https://platform.openai.com/api-keys\n"
            "• Claude (Anthropic): https://console.anthropic.com/account/keys"
        )
        instructions.setWordWrap(True)
        layout.addWidget(instructions)

        layout.addSpacing(15)

        # Form layout
        form_layout = QFormLayout()

        self.provider_combo = QComboBox()
        self.provider_combo.addItems(["OpenAI", "Claude"])
        self.provider_combo.currentTextChanged.connect(self._load_existing_key)
        form_layout.addRow("Provider:", self.provider_combo)

        self.api_key_input = QLineEdit()
        self.api_key_input.setEchoMode(QLineEdit.EchoMode.Password)
        self.api_key_input.setPlaceholderText("Paste your API key here")
        form_layout.addRow("API Key:", self.api_key_input)

        self.toggle_visibility_btn = QPushButton("Show Key")
        self.toggle_visibility_btn.setMaximumWidth(100)
        self.toggle_visibility_btn.clicked.connect(self._toggle_key_visibility)
        form_layout.addRow("", self.toggle_visibility_btn)

        layout.addLayout(form_layout)

        layout.addSpacing(10)

        # Set as default checkbox — only visible on first-time setup
        self.set_default_checkbox = QCheckBox("Set as default provider")
        self.set_default_checkbox.setChecked(True)
        self.set_default_checkbox.setVisible(first_time_setup)
        layout.addWidget(self.set_default_checkbox)

        layout.addSpacing(5)

        # Buttons
        button_box = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel
        )
        button_box.accepted.connect(self._validate_and_save)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)

        self._key_visible = False
        self._load_existing_key(self.provider_combo.currentText())

    def _load_existing_key(self, provider: str):
        """Load the existing API key for a cloud provider into the input field."""
        provider_map = {"OpenAI": "openai", "Claude": "claude"}
        key_name = provider_map.get(provider)
        if key_name:
            existing = ConfigManager.get_api_key(key_name)
            self.api_key_input.setText(existing or "")
        else:
            self.api_key_input.clear()
        # Reset visibility on provider change
        self._key_visible = False
        self.toggle_visibility_btn.setText("Show Key")
        self.api_key_input.setEchoMode(QLineEdit.EchoMode.Password)

    def _toggle_key_visibility(self):
        """Toggle visibility of API key."""
        self._key_visible = not self._key_visible
        if self._key_visible:
            self.api_key_input.setEchoMode(QLineEdit.EchoMode.Normal)
            self.toggle_visibility_btn.setText("Hide Key")
        else:
            self.api_key_input.setEchoMode(QLineEdit.EchoMode.Password)
            self.toggle_visibility_btn.setText("Show Key")

    def _validate_and_save(self):
        """Validate and save the API key."""
        provider = self.provider_combo.currentText()
        api_key = self.api_key_input.text().strip()

        if not api_key:
            logger.warning(f"Empty API key provided for provider: {provider}")
            QMessageBox.warning(
                self, "Empty API Key", f"Please enter a valid {provider} API key."
            )
            return

        success, message = ConfigManager.set_api_key(provider.lower(), api_key)
        logger.info(f"API key save attempt for {provider}: {success}")

        if success:
            if self._first_time_setup and self.set_default_checkbox.isChecked():
                ConfigManager.set_default_api(provider.lower())
            logger.info(f"{provider} API key configured successfully")
            QMessageBox.information(
                self, "Success", f"{provider} API key configured successfully!"
            )
            self.accept()
        else:
            logger.error(f"Failed to save {provider} API key: {message}")
            QMessageBox.critical(self, "Error", f"Failed to save API key: {message}")


class ModelSettingsDialog(QDialog):
    """Dialog to configure the default model for the currently active LLM provider."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Model Settings")
        self.setModal(True)
        self.setMinimumWidth(480)
        self.windowIcon = QIcon("icon.ico")
        self.setWindowIcon(self.windowIcon)

        layout = QVBoxLayout(self)

        # Get current default provider and model defaults
        self.current_provider = ConfigManager.get_default_api()
        self.model_defaults = ConfigManager.get_model_defaults()

        # Provider display (read-only)
        provider_display = QLabel(
            f"Current Provider: <b>{self.current_provider.upper()}</b>"
        )
        provider_display.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(provider_display)

        layout.addSpacing(15)

        # Title
        title = QLabel(f"Configure {self._get_provider_display_name()} Model")
        title.setFont(QFont("Roboto", 14, QFont.Weight.Bold))
        title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(title)

        # Instructions
        instructions = QLabel(
            f"Enter the model ID for {self._get_provider_display_name()}.\n"
            "This model will be used for all queries unless overridden at the session level."
        )
        instructions.setWordWrap(True)
        layout.addWidget(instructions)

        layout.addSpacing(15)

        # Form layout - only show field for current provider
        form_layout = QFormLayout()

        self.model_input = QLineEdit()
        self.model_input.setText(self.model_defaults.get(self.current_provider, ""))
        self.model_input.setPlaceholderText(
            self._get_placeholder_for_provider(self.current_provider)
        )
        form_layout.addRow(
            f"{self._get_provider_display_name()} Model ID:", self.model_input
        )

        layout.addLayout(form_layout)

        layout.addSpacing(15)

        # Help text with provider-specific links
        help_text = self._get_help_text_for_provider(self.current_provider)
        help_label = QLabel(help_text)
        help_label.setStyleSheet("color: gray; font-size: 9pt;")
        help_label.setWordWrap(True)
        help_label.setOpenExternalLinks(True)
        layout.addWidget(help_label)

        layout.addSpacing(10)

        # Buttons
        button_box = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel
        )
        button_box.accepted.connect(self._save_settings)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)

    def _get_provider_display_name(self) -> str:
        """Get human-readable provider name."""
        mapping = {
            "openai": "OpenAI",
            "claude": "Claude",
        }
        return mapping.get(self.current_provider, self.current_provider.title())

    def _get_placeholder_for_provider(self, provider: str) -> str:
        """Get placeholder text for the provider."""
        placeholders = {
            "openai": "gpt-4o-2024-08-06",
            "claude": "claude-3-5-sonnet-20241022",
        }
        return placeholders.get(provider, "Enter model ID")

    def _get_help_text_for_provider(self, provider: str) -> str:
        """Get help text and link for the provider."""
        help_texts = {
            "openai": (
                "Available models: "
                '<a href="https://platform.openai.com/docs/models">'
                "https://platform.openai.com/docs/models</a>"
            ),
            "claude": (
                "Available models: "
                '<a href="https://docs.anthropic.com/en/docs/about-claude/models">'
                "https://docs.anthropic.com/en/docs/about-claude/models</a>"
            ),
        }
        return help_texts.get(
            provider, "Check your provider's documentation for available models."
        )

    def _save_settings(self):
        """Save the model setting for the current provider."""
        model_id = self.model_input.text().strip()

        if not model_id:
            QMessageBox.warning(self, "Empty Model ID", "Please enter a model ID.")
            return

        success, msg = ConfigManager.set_model_default(self.current_provider, model_id)
        if not success:
            QMessageBox.warning(self, "Error", f"Failed to save model: {msg}")
            return

        logger.info(f"Model settings saved for {self.current_provider}: {model_id}")
        QMessageBox.information(
            self,
            "Success",
            f"{self._get_provider_display_name()} model updated successfully!",
        )
        self.accept()


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
        self._download_thread: Optional[ModelDownloadThread] = None
        self._server_thread: Optional[ServerStartThread] = None

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

        self._build_self_host_ui(sh_layout)

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

    # ------------------------------------------------------------------
    #  Self-Host UI builder
    # ------------------------------------------------------------------
    def _build_self_host_ui(self, layout: QVBoxLayout):
        """Build the self-host model widgets inside the given layout."""
        from model_manager import (
            get_recommended_models,
            list_available_models,
            is_llama_cpp_available,
        )

        # Dependency check banner
        if not is_llama_cpp_available():
            warn = QLabel(
                "\u26a0 llama-cpp-python is not installed.\n"
                "Run:  pip install llama-cpp-python\n"
                "Then restart the application."
            )
            warn.setWordWrap(True)
            warn.setStyleSheet("color: #e8a838; font-weight: bold;")
            layout.addWidget(warn)
            layout.addSpacing(3)

        desc = QLabel(
            "Download a model and let the application host it for you.\n"
            "No separate server setup required."
        )
        desc.setWordWrap(True)
        layout.addWidget(desc)
        layout.addSpacing(5)

        # -- Model selection --
        self.sh_model_combo = QComboBox()
        catalog = get_recommended_models()
        self._sh_catalog_keys: List[str] = []
        for key, info in catalog.items():
            label = f"{info['name']}  ({info['size_gb']} GB)"
            if info.get("recommended"):
                label += "  \u2605"
            self.sh_model_combo.addItem(label)
            self._sh_catalog_keys.append(key)

        existing = list_available_models()
        catalog_filenames = {v["filename"] for v in catalog.values()}
        for fname in existing:
            if fname not in catalog_filenames:
                self.sh_model_combo.addItem(f"[downloaded] {fname}")
                self._sh_catalog_keys.append(f"__local__{fname}")

        layout.addWidget(QLabel("Model:"))
        layout.addWidget(self.sh_model_combo)

        # Browse for custom GGUF
        browse_row = QHBoxLayout()
        self.sh_custom_path = QLineEdit()
        self.sh_custom_path.setPlaceholderText("Or browse for a .gguf file\u2026")
        sh_browse_btn = QPushButton("Browse\u2026")
        sh_browse_btn.setMaximumWidth(90)
        sh_browse_btn.clicked.connect(self._sh_browse_model)
        browse_row.addWidget(self.sh_custom_path)
        browse_row.addWidget(sh_browse_btn)
        layout.addLayout(browse_row)

        layout.addSpacing(5)

        # -- Download --
        self.sh_download_btn = QPushButton("Download Selected Model")
        self.sh_download_btn.clicked.connect(self._sh_start_download)
        layout.addWidget(self.sh_download_btn)

        self.sh_progress_bar = QProgressBar()
        self.sh_progress_bar.setRange(0, 100)
        self.sh_progress_bar.setValue(0)
        self.sh_progress_bar.setVisible(False)
        layout.addWidget(self.sh_progress_bar)

        self.sh_progress_label = QLabel("")
        self.sh_progress_label.setVisible(False)
        layout.addWidget(self.sh_progress_label)

        layout.addSpacing(5)

        # -- Server controls --
        srv_form = QFormLayout()
        hosted_cfg = ConfigManager.get_hosted_llm_config()

        self.sh_port_spin = QSpinBox()
        self.sh_port_spin.setRange(1024, 65535)
        self.sh_port_spin.setValue(hosted_cfg.get("hosted_port", 8911))
        self.sh_port_spin.setToolTip(
            "The network port the local LLM server listens on.\n"
            "Change this if another service is already using the default port."
        )
        srv_form.addRow("Port:", self.sh_port_spin)

        self.sh_context_spin = QSpinBox()
        self.sh_context_spin.setRange(512, 131072)
        self.sh_context_spin.setSingleStep(512)
        self.sh_context_spin.setValue(hosted_cfg.get("hosted_context_size", 4096))
        self.sh_context_spin.setToolTip(
            "Maximum number of tokens the model can process in a single prompt/response.\n"
            "Higher values allow longer conversations but use more RAM/VRAM.\n"
            "Common values: 2048, 4096, 8192, 16384, 32768."
        )
        srv_form.addRow("Context Size:", self.sh_context_spin)

        self.sh_gpu_spin = QSpinBox()
        self.sh_gpu_spin.setRange(0, 999)
        self.sh_gpu_spin.setValue(hosted_cfg.get("hosted_gpu_layers", 0))
        self.sh_gpu_spin.setToolTip(
            "Number of model layers to offload to the GPU for faster inference.\n"
            "Set to 0 for CPU-only mode. Higher values use more VRAM but run faster.\n"
            "Set to 999 to offload all layers (requires sufficient VRAM)."
        )
        srv_form.addRow("GPU Layers:", self.sh_gpu_spin)
        layout.addLayout(srv_form)

        btn_row = QHBoxLayout()
        self.sh_start_btn = QPushButton("Start Server")
        self.sh_start_btn.clicked.connect(self._sh_start_server)
        btn_row.addWidget(self.sh_start_btn)

        self.sh_stop_btn = QPushButton("Stop Server")
        self.sh_stop_btn.clicked.connect(self._sh_stop_server)
        btn_row.addWidget(self.sh_stop_btn)
        layout.addLayout(btn_row)

        self.sh_status_label = QLabel("")
        layout.addWidget(self.sh_status_label)

        self._sh_refresh_server_status()

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
            self._sh_refresh_server_status()

        # Resize dialog to fit current content
        self.adjustSize()

    # ------------------------------------------------------------------
    #  Self-Host: model helpers
    # ------------------------------------------------------------------
    def _sh_browse_model(self):
        path, _ = QFileDialog.getOpenFileName(
            self, "Select GGUF Model", "", "GGUF Models (*.gguf);;All Files (*)"
        )
        if path:
            self.sh_custom_path.setText(path)

    def _sh_get_selected_model_info(self) -> Optional[dict]:
        """Return catalog info dict or pseudo-dict for a local file."""
        from model_manager import get_models_directory, get_recommended_models

        custom = self.sh_custom_path.text().strip()
        if custom:
            return {
                "filename": os.path.basename(custom),
                "url": None,
                "path": custom,
                "size_gb": 0,
            }

        idx = self.sh_model_combo.currentIndex()
        if idx < 0:
            return None

        key = self._sh_catalog_keys[idx]
        catalog = get_recommended_models()

        if key.startswith("__local__"):
            fname = key.replace("__local__", "")
            return {
                "filename": fname,
                "url": None,
                "path": os.path.join(get_models_directory(), fname),
                "size_gb": 0,
            }
        else:
            info = catalog[key]
            return {
                "filename": info["filename"],
                "url": info["url"],
                "path": os.path.join(get_models_directory(), info["filename"]),
                "size_gb": info["size_gb"],
            }

    # ------------------------------------------------------------------
    #  Self-Host: download
    # ------------------------------------------------------------------
    def _sh_start_download(self):
        info = self._sh_get_selected_model_info()
        if not info:
            QMessageBox.warning(self, "No Model", "Select a model first.")
            return

        if not info.get("url"):
            if os.path.isfile(info.get("path", "")):
                QMessageBox.information(
                    self,
                    "Already Available",
                    f"Model file already exists:\n{info['path']}",
                )
            else:
                QMessageBox.warning(
                    self,
                    "No URL",
                    "This model has no download URL. Browse for a local .gguf file.",
                )
            return

        from model_manager import check_disk_space

        if not check_disk_space(info["size_gb"] * 1.1):
            QMessageBox.warning(
                self,
                "Disk Space",
                f"Not enough disk space. ~{info['size_gb']:.1f} GB required.",
            )
            return

        self.sh_download_btn.setEnabled(False)
        self.sh_download_btn.setText("Downloading\u2026")
        self.sh_progress_bar.setVisible(True)
        self.sh_progress_bar.setValue(0)
        self.sh_progress_label.setVisible(True)
        self.sh_progress_label.setText("Starting download\u2026")

        self._download_thread = ModelDownloadThread(info["url"], info["filename"], self)
        self._download_thread.progress.connect(self._sh_on_download_progress)
        self._download_thread.finished.connect(self._sh_on_download_finished)
        self._download_thread.start()

    def _sh_on_download_progress(self, pct: float, downloaded: float, total: float):
        self.sh_progress_bar.setValue(int(pct))
        dl_mb = downloaded / (1024 * 1024)
        tot_mb = total / (1024 * 1024)
        self.sh_progress_label.setText(
            f"{dl_mb:.0f} MB / {tot_mb:.0f} MB  ({pct:.1f}%)"
        )

    def _sh_on_download_finished(self, success: bool, message: str):
        self.sh_download_btn.setEnabled(True)
        self.sh_download_btn.setText("Download Selected Model")
        self.sh_progress_bar.setVisible(False)
        self.sh_progress_label.setVisible(False)

        if success:
            QMessageBox.information(self, "Download Complete", message)
        else:
            QMessageBox.critical(self, "Download Failed", message)

    # ------------------------------------------------------------------
    #  Self-Host: server start / stop
    # ------------------------------------------------------------------
    def _sh_start_server(self):
        from model_manager import is_llama_cpp_available

        if not is_llama_cpp_available():
            QMessageBox.warning(
                self,
                "Missing Dependency",
                "llama-cpp-python is not installed.\n\n"
                "Install it with:\n  pip install llama-cpp-python\n\n"
                "Then restart the application.",
            )
            return

        info = self._sh_get_selected_model_info()
        if not info:
            QMessageBox.warning(self, "No Model", "Select a model first.")
            return

        model_path = info["path"]
        if not os.path.isfile(model_path):
            QMessageBox.warning(
                self,
                "Model Not Found",
                f"Model file not found:\n{model_path}\n\n" "Download it first.",
            )
            return

        port = self.sh_port_spin.value()
        context_size = self.sh_context_spin.value()
        gpu_layers = self.sh_gpu_spin.value()

        self.sh_start_btn.setEnabled(False)
        self.sh_start_btn.setText("Starting\u2026")
        self.sh_status_label.setText(
            "Starting server \u2014 this may take a moment\u2026"
        )

        self._server_thread = ServerStartThread(
            model_path, port, gpu_layers, context_size, self
        )
        self._server_thread.finished.connect(self._sh_on_server_started)
        self._server_thread.start()

    def _sh_on_server_started(self, success: bool, message: str):
        self.sh_start_btn.setEnabled(True)
        self.sh_start_btn.setText("Start Server")

        if success:
            QMessageBox.information(self, "Server Started", message)
        else:
            QMessageBox.critical(self, "Server Failed", message)

        self._sh_refresh_server_status()

    def _sh_stop_server(self):
        from model_manager import stop_model_server

        ok, msg = stop_model_server()
        if ok:
            QMessageBox.information(self, "Server Stopped", msg)
        else:
            QMessageBox.warning(self, "Error", msg)
        self._sh_refresh_server_status()

    def _sh_refresh_server_status(self):
        from model_manager import get_server_status

        status = get_server_status()
        if status["running"]:
            self.sh_status_label.setText(
                f"\u25cf Server running  (PID {status['pid']},  {status['url']})"
            )
            self.sh_status_label.setStyleSheet("color: #4caf50; font-weight: bold;")
            self.sh_start_btn.setEnabled(False)
            self.sh_stop_btn.setEnabled(True)
        else:
            self.sh_status_label.setText("\u25cb Server not running")
            self.sh_status_label.setStyleSheet("color: gray;")
            self.sh_start_btn.setEnabled(True)
            self.sh_stop_btn.setEnabled(False)

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

            info = self._sh_get_selected_model_info()
            if not info:
                QMessageBox.warning(self, "No Model", "Please select a model.")
                return

            model_path = info["path"]
            port = self.sh_port_spin.value()
            context_size = self.sh_context_spin.value()
            gpu_layers = self.sh_gpu_spin.value()

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
        self.setMinimumSize(560, 520)

        self._download_thread: Optional[ModelDownloadThread] = None
        self._server_thread: Optional[ServerStartThread] = None

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
        root_layout.addWidget(self.tabs)

        root_layout.addSpacing(5)

        # Dialog buttons
        button_box = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel
        )
        button_box.accepted.connect(self._save)
        button_box.rejected.connect(self.reject)
        root_layout.addWidget(button_box)

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
    #  Tab 2 — Download & host a model
    # ------------------------------------------------------------------
    def _build_host_tab(self) -> QWidget:
        from model_manager import (
            get_recommended_models,
            list_available_models,
            get_models_directory,
            is_llama_cpp_available,
            is_server_running,
        )

        tab = QWidget()
        layout = QVBoxLayout(tab)

        # Dependency check banner
        if not is_llama_cpp_available():
            warn = QLabel(
                "⚠ llama-cpp-python is not installed.\n"
                "Run:  pip install llama-cpp-python\n"
                "Then restart the application."
            )
            warn.setWordWrap(True)
            warn.setStyleSheet("color: #e8a838; font-weight: bold;")
            layout.addWidget(warn)
            layout.addSpacing(5)

        instructions = QLabel(
            "Don't have a local LLM server? Download a model and let\n"
            "the application host it for you automatically."
        )
        instructions.setWordWrap(True)
        layout.addWidget(instructions)

        layout.addSpacing(5)

        # ---- Model selection ----
        model_group = QGroupBox("1. Select a Model")
        mg_layout = QVBoxLayout(model_group)

        self.model_combo = QComboBox()
        catalog = get_recommended_models()

        # Populate combo: catalog models first, then already-downloaded models
        self._catalog_keys: List[str] = []
        for key, info in catalog.items():
            label = f"{info['name']}  ({info['size_gb']} GB)"
            if info.get("recommended"):
                label += "  ★"
            self.model_combo.addItem(label)
            self._catalog_keys.append(key)

        # Add already-downloaded models not in catalog
        existing = list_available_models()
        catalog_filenames = {v["filename"] for v in catalog.values()}
        for fname in existing:
            if fname not in catalog_filenames:
                self.model_combo.addItem(f"[downloaded] {fname}")
                self._catalog_keys.append(f"__local__{fname}")

        mg_layout.addWidget(self.model_combo)

        # Browse for a custom GGUF file
        browse_row = QHBoxLayout()
        self.custom_path_input = QLineEdit()
        self.custom_path_input.setPlaceholderText("Or browse for a .gguf file…")
        browse_btn = QPushButton("Browse…")
        browse_btn.setMaximumWidth(90)
        browse_btn.clicked.connect(self._browse_model)
        browse_row.addWidget(self.custom_path_input)
        browse_row.addWidget(browse_btn)
        mg_layout.addLayout(browse_row)

        layout.addWidget(model_group)

        # ---- Download ----
        dl_group = QGroupBox("2. Download Model")
        dl_layout = QVBoxLayout(dl_group)

        self.download_btn = QPushButton("Download Selected Model")
        self.download_btn.clicked.connect(self._start_download)
        dl_layout.addWidget(self.download_btn)

        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        self.progress_bar.setVisible(False)
        dl_layout.addWidget(self.progress_bar)

        self.progress_label = QLabel("")
        self.progress_label.setVisible(False)
        dl_layout.addWidget(self.progress_label)

        layout.addWidget(dl_group)

        # ---- Server controls ----
        srv_group = QGroupBox("3. Start / Stop Server")
        sg_layout = QVBoxLayout(srv_group)

        srv_form = QFormLayout()
        self.port_spin = QSpinBox()
        self.port_spin.setRange(1024, 65535)
        hosted_cfg = ConfigManager.get_hosted_llm_config()
        self.port_spin.setValue(hosted_cfg.get("hosted_port", 8911))
        self.port_spin.setToolTip(
            "The network port the local LLM server listens on.\n"
            "Change this if another service is already using the default port."
        )
        srv_form.addRow("Port:", self.port_spin)

        self.context_spin = QSpinBox()
        self.context_spin.setRange(512, 131072)
        self.context_spin.setSingleStep(512)
        self.context_spin.setValue(hosted_cfg.get("hosted_context_size", 4096))
        self.context_spin.setToolTip(
            "Maximum number of tokens the model can process in a single prompt/response.\n"
            "Higher values allow longer conversations but use more RAM/VRAM.\n"
            "Common values: 2048, 4096, 8192, 16384, 32768."
        )
        srv_form.addRow("Context Size:", self.context_spin)

        self.gpu_spin = QSpinBox()
        self.gpu_spin.setRange(0, 999)
        self.gpu_spin.setValue(hosted_cfg.get("hosted_gpu_layers", 0))
        self.gpu_spin.setToolTip(
            "Number of model layers to offload to the GPU for faster inference.\n"
            "Set to 0 for CPU-only mode. Higher values use more VRAM but run faster.\n"
            "Set to 999 to offload all layers (requires sufficient VRAM)."
        )
        srv_form.addRow("GPU Layers:", self.gpu_spin)

        sg_layout.addLayout(srv_form)

        btn_row = QHBoxLayout()
        self.start_btn = QPushButton("Start Server")
        self.start_btn.clicked.connect(self._start_server)
        btn_row.addWidget(self.start_btn)

        self.stop_btn = QPushButton("Stop Server")
        self.stop_btn.clicked.connect(self._stop_server)
        btn_row.addWidget(self.stop_btn)
        sg_layout.addLayout(btn_row)

        self.server_status_label = QLabel("")
        sg_layout.addWidget(self.server_status_label)

        self.auto_start_cb = QCheckBox("Auto-start server when app launches")
        self.auto_start_cb.setChecked(hosted_cfg.get("hosted_auto_start", False))
        sg_layout.addWidget(self.auto_start_cb)

        layout.addWidget(srv_group)

        # Refresh status
        self._refresh_server_status()

        return tab

    # ------------------------------------------------------------------
    #  Actions
    # ------------------------------------------------------------------
    def _browse_model(self):
        path, _ = QFileDialog.getOpenFileName(
            self, "Select GGUF Model", "", "GGUF Models (*.gguf);;All Files (*)"
        )
        if path:
            self.custom_path_input.setText(path)

    def _get_selected_model_info(self) -> Optional[dict]:
        """Return catalog info dict or a pseudo-dict for an already-local file."""
        from model_manager import get_models_directory, get_recommended_models

        # Custom path takes priority
        custom = self.custom_path_input.text().strip()
        if custom:
            return {
                "filename": os.path.basename(custom),
                "url": None,
                "path": custom,
                "size_gb": 0,
            }

        idx = self.model_combo.currentIndex()
        if idx < 0:
            return None

        key = self._catalog_keys[idx]
        catalog = get_recommended_models()

        if key.startswith("__local__"):
            fname = key.replace("__local__", "")
            return {
                "filename": fname,
                "url": None,
                "path": os.path.join(get_models_directory(), fname),
                "size_gb": 0,
            }
        else:
            info = catalog[key]
            return {
                "filename": info["filename"],
                "url": info["url"],
                "path": os.path.join(get_models_directory(), info["filename"]),
                "size_gb": info["size_gb"],
            }

    def _start_download(self):
        info = self._get_selected_model_info()
        if not info:
            QMessageBox.warning(self, "No Model", "Select a model first.")
            return

        if not info.get("url"):
            if os.path.isfile(info.get("path", "")):
                QMessageBox.information(
                    self,
                    "Already Available",
                    f"Model file already exists:\n{info['path']}",
                )
            else:
                QMessageBox.warning(
                    self,
                    "No URL",
                    "This model has no download URL. Browse for a local .gguf file.",
                )
            return

        from model_manager import check_disk_space

        if not check_disk_space(info["size_gb"] * 1.1):
            QMessageBox.warning(
                self,
                "Disk Space",
                f"Not enough disk space. ~{info['size_gb']:.1f} GB required.",
            )
            return

        # Disable button and show progress
        self.download_btn.setEnabled(False)
        self.download_btn.setText("Downloading…")
        self.progress_bar.setVisible(True)
        self.progress_bar.setValue(0)
        self.progress_label.setVisible(True)
        self.progress_label.setText("Starting download…")

        self._download_thread = ModelDownloadThread(info["url"], info["filename"], self)
        self._download_thread.progress.connect(self._on_download_progress)
        self._download_thread.finished.connect(self._on_download_finished)
        self._download_thread.start()

    def _on_download_progress(self, pct: float, downloaded: float, total: float):
        self.progress_bar.setValue(int(pct))
        dl_mb = downloaded / (1024 * 1024)
        tot_mb = total / (1024 * 1024)
        self.progress_label.setText(f"{dl_mb:.0f} MB / {tot_mb:.0f} MB  ({pct:.1f}%)")

    def _on_download_finished(self, success: bool, message: str):
        self.download_btn.setEnabled(True)
        self.download_btn.setText("Download Selected Model")
        self.progress_bar.setVisible(False)
        self.progress_label.setVisible(False)

        if success:
            QMessageBox.information(self, "Download Complete", message)
        else:
            QMessageBox.critical(self, "Download Failed", message)

    def _start_server(self):
        from model_manager import is_llama_cpp_available

        if not is_llama_cpp_available():
            QMessageBox.warning(
                self,
                "Missing Dependency",
                "llama-cpp-python is not installed.\n\n"
                "Install it with:\n  pip install llama-cpp-python\n\n"
                "Then restart the application.",
            )
            return

        info = self._get_selected_model_info()
        if not info:
            QMessageBox.warning(self, "No Model", "Select a model first.")
            return

        model_path = info["path"]
        if not os.path.isfile(model_path):
            QMessageBox.warning(
                self,
                "Model Not Found",
                f"Model file not found:\n{model_path}\n\n" "Download it first.",
            )
            return

        port = self.port_spin.value()
        context_size = self.context_spin.value()
        gpu_layers = self.gpu_spin.value()

        self.start_btn.setEnabled(False)
        self.start_btn.setText("Starting…")
        self.server_status_label.setText("Starting server — this may take a moment…")

        self._server_thread = ServerStartThread(
            model_path, port, gpu_layers, context_size, self
        )
        self._server_thread.finished.connect(self._on_server_started)
        self._server_thread.start()

    def _on_server_started(self, success: bool, message: str):
        self.start_btn.setEnabled(True)
        self.start_btn.setText("Start Server")

        if success:
            QMessageBox.information(self, "Server Started", message)
            # Auto-update the connect tab URL to point at the hosted server
            from model_manager import get_hosted_url

            hosted_url = get_hosted_url(port=self.port_spin.value())
            self.url_input.setText(hosted_url)
        else:
            QMessageBox.critical(self, "Server Failed", message)

        self._refresh_server_status()

    def _stop_server(self):
        from model_manager import stop_model_server

        ok, msg = stop_model_server()
        if ok:
            QMessageBox.information(self, "Server Stopped", msg)
        else:
            QMessageBox.warning(self, "Error", msg)
        self._refresh_server_status()

    def _refresh_server_status(self):
        from model_manager import get_server_status

        status = get_server_status()
        if status["running"]:
            self.server_status_label.setText(
                f"● Server running  (PID {status['pid']},  {status['url']})"
            )
            self.server_status_label.setStyleSheet("color: #4caf50; font-weight: bold;")
            self.start_btn.setEnabled(False)
            self.stop_btn.setEnabled(True)
        else:
            self.server_status_label.setText("○ Server not running")
            self.server_status_label.setStyleSheet("color: gray;")
            self.start_btn.setEnabled(True)
            self.stop_btn.setEnabled(False)

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
                + "\n".join(f"  • {n}" for n in model_names[:15]),
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
        info = self._get_selected_model_info()
        model_path = info["path"] if info else ""
        ok2, msg2 = ConfigManager.set_hosted_llm_config(
            model_path=model_path,
            port=self.port_spin.value(),
            context_size=self.context_spin.value(),
            gpu_layers=self.gpu_spin.value(),
            auto_start=self.auto_start_cb.isChecked(),
        )
        if not ok2:
            QMessageBox.critical(self, "Error", f"Failed to save hosted config: {msg2}")
            return

        self.accept()


class CreateProjectDialog(QDialog):
    """Dialog to create a new project or load an existing one"""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Create or Load Project")
        self.setModal(True)
        self.setMinimumWidth(480)
        self.windowIcon = QIcon("icon.ico")
        self.setWindowIcon(self.windowIcon)

        self.project_id: Optional[str] = None
        self.backend = DataWorkspaceBackend()

        layout = QVBoxLayout(self)

        # Title and top options
        title = QLabel("Project Options")
        title.setFont(QFont("Roboto", 14, QFont.Weight.Bold))
        title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(title)

        subtitle = QLabel("Create a new project or load an existing one from disk:")
        subtitle.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(subtitle)

        layout.addSpacing(8)

        top_button_layout = QHBoxLayout()
        self.create_btn = QPushButton("Create New Project")
        self.create_btn.setMinimumHeight(36)
        self.create_btn.clicked.connect(self.show_create_form)
        top_button_layout.addWidget(self.create_btn)

        self.load_btn = QPushButton("Load Existing Project")
        self.load_btn.setMinimumHeight(36)
        self.load_btn.clicked.connect(self.open_load_dialog)
        top_button_layout.addWidget(self.load_btn)

        layout.addLayout(top_button_layout)
        layout.addSpacing(10)

        # Form layout for creating a new project (hidden until Create New Project clicked)
        form_layout = QFormLayout()

        random_placeholder_index = random.randint(0, len(PLACEHOLDER_PROJECT_NAMES) - 1)

        self.project_name_input = QLineEdit()
        self.project_name_input.setPlaceholderText(
            PLACEHOLDER_PROJECT_NAMES[random_placeholder_index]
        )
        form_layout.addRow("Project Name:", self.project_name_input)

        self.project_desc_input = QLineEdit()
        self.project_desc_input.setPlaceholderText(
            PLACEHOLDER_PROJECT_DESCRIPTIONS[random_placeholder_index]
        )
        self.project_desc_input.returnPressed.connect(self.create_project)
        form_layout.addRow("Description:", self.project_desc_input)

        # Put form in a container so we can show/hide easily
        self.form_container = QWidget()
        container_layout = QVBoxLayout(self.form_container)
        container_layout.setContentsMargins(0, 0, 0, 0)
        container_layout.addLayout(form_layout)
        self.form_container.setVisible(False)

        layout.addWidget(self.form_container)
        layout.addSpacing(10)

    def show_create_form(self):
        """Show the create project form when the user clicks the Create button."""
        logger.debug("Showing project creation form")
        self.form_container.setVisible(True)
        self.project_name_input.setFocus()

        # Add buttons to the form container (only add once)
        if not hasattr(self, "create_button_box"):
            self.create_button_box = QDialogButtonBox(
                QDialogButtonBox.StandardButton.Ok
                | QDialogButtonBox.StandardButton.Cancel
            )
            self.create_button_box.accepted.connect(self.create_project)
            self.create_button_box.rejected.connect(self.reject)
            # Use the container's layout rather than the local 'layout' variable
            cont_layout = self.form_container.layout()
            if cont_layout:
                cont_layout.addWidget(self.create_button_box)

    def open_load_dialog(self):
        """Open a dialog that lists local saved projects and attempt to load the selected one."""
        logger.debug("Opening project load dialog")
        files = self.backend.list_saved_projects()
        projects_dir = os.path.abspath("projects")
        if not files:
            logger.info(f"No saved projects found in {projects_dir}")
            QMessageBox.information(
                self, "No Projects", f"No saved projects found in {projects_dir}"
            )
            return

        logger.debug(f"Found {len(files)} saved project(s)")
        proj_dialog = ProjectLoadDialog(files, self)
        if proj_dialog.exec() != QDialog.DialogCode.Accepted:
            logger.debug("User cancelled project load dialog")
            return

        file_name = proj_dialog.get_selected_file()
        if not file_name:
            logger.warning("Project file selected but name is empty")
            QMessageBox.warning(self, "Error", "No project file selected.")
            return

        success, msg, project = self.backend.load_project_from_disk(file_name)
        if not success or project is None:
            QMessageBox.critical(self, "Load Failed", msg)
            return

        # If the loaded project references a data source, prompt for credentials (if database)
        ds = project.data_source
        if ds and isinstance(ds, dict) and ds.get("db_type"):
            db_type = ds.get("db_type")
            credentials = ds.get("credentials", {})

            db_dialog = DatabaseConnectionDialog(
                self,
                force_nlp=ConfigManager.get_interaction_mode() == "cxo",
                semantic_layer=project.semantic_layer,
            )
            # Pre-fill known fields; avoid pre-filling password for security
            db_dialog.db_type_combo.setCurrentText(db_type)
            if "host" in credentials:
                db_dialog.host_input.setText(credentials.get("host", ""))
            if "port" in credentials:
                db_dialog.port_input.setText(str(credentials.get("port", "")))
            if "database" in credentials:
                db_dialog.database_input.setText(credentials.get("database", ""))
            if "user" in credentials:
                db_dialog.user_input.setText(credentials.get("user", ""))

            if db_dialog.exec() != QDialog.DialogCode.Accepted:
                QMessageBox.information(
                    self, "Cancelled", "Project load cancelled by user."
                )
                return

            new_config = db_dialog.get_config()
            connector = DatabaseConnector()
            while True:
                success_conn, message = connector.connect(
                    new_config["db_type"], new_config["credentials"]
                )
                if success_conn:
                    break

                retry = QMessageBox.question(
                    self,
                    "Database Connection Failed",
                    f"{message}\n\nWould you like to try again?",
                    QMessageBox.StandardButton.Retry
                    | QMessageBox.StandardButton.Cancel,
                )
                if retry == QMessageBox.StandardButton.Retry:
                    if db_dialog.exec() != QDialog.DialogCode.Accepted:
                        connector.close()
                        return
                    new_config = db_dialog.get_config()
                else:
                    connector.close()
                    return

            try:
                tables = connector.get_tables()
            except Exception as e:
                QMessageBox.critical(self, "Table Discovery Failed", str(e))
                connector.close()
                return

            if not tables:
                connector.close()
                QMessageBox.critical(
                    self, "No Tables Found", "The database does not contain any tables."
                )
                return

            selection_method = ds.get(
                "table_selection_method",
                ConfigManager.get_table_selection_method(),
            )
            # Get semantic layer from project, not from data_source
            semantic_layer = project.semantic_layer
            is_cxo = ConfigManager.get_interaction_mode() == "cxo"

            # CxO mode: skip table selection, store lightweight context for NLP at query time
            if is_cxo:
                connector.close()
                logger.info(
                    f"CxO mode: skipping table selection on project load. {len(tables)} tables available."
                )
                cxo_context = {
                    "source_type": "database",
                    "cxo_mode": True,
                    "db_type": new_config["db_type"],
                    "credentials": new_config["credentials"],
                    "all_tables": tables,
                    "tables": [],
                    "table_info": {},
                    "semantic_layer": semantic_layer,
                }
                self.backend.data_context = cxo_context
                credentials_to_store = new_config["credentials"].copy()
                if "password" in credentials_to_store:
                    credentials_to_store["password"] = ""
                project.data_source = {
                    "db_type": new_config["db_type"],
                    "credentials": credentials_to_store,
                    "table_selection_method": "nlp",
                    "cxo_mode": True,
                }
                # Update project semantic layer from dialog if changed
                if db_dialog.semantic_layer:
                    project.semantic_layer = db_dialog.semantic_layer
                QMessageBox.information(
                    self,
                    "Project Loaded",
                    f"Project '{project.title}' loaded and database connected in CxO mode.",
                )

            else:
                # Analyst mode: normal table selection flow
                # If the project previously stored a table selection, try to reuse it
                saved_table = ds.get("table")
                if saved_table:
                    selected_tables = (
                        saved_table if isinstance(saved_table, list) else [saved_table]
                    )
                    connector.close()
                else:
                    selected_tables = select_tables_with_method(
                        self,
                        connector,
                        tables,
                        selection_method,
                        semantic_layer,
                    )
                    connector.close()
                    if not selected_tables:
                        return

                source_config = {
                    "db_type": new_config["db_type"],
                    "credentials": new_config["credentials"],
                    "table": selected_tables,
                }
                data_context, status = load_data("database", source_config)

                if data_context is not None:
                    self.backend.data_context = data_context
                    # Update project data_source to reflect the used config (save host/port but don't store password)
                    credentials_to_store = new_config["credentials"].copy()
                    if "password" in credentials_to_store:
                        credentials_to_store["password"] = ""
                    project.data_source = {
                        "db_type": new_config["db_type"],
                        "credentials": credentials_to_store,
                        "table": selected_tables,
                        "table_selection_method": selection_method,
                    }
                    # Update project semantic layer from dialog if changed
                    if db_dialog.semantic_layer:
                        project.semantic_layer = db_dialog.semantic_layer
                    QMessageBox.information(
                        self,
                        "Project Loaded",
                        f"Project '{project.title}' loaded and data connected.",
                    )
                else:
                    QMessageBox.warning(
                        self,
                        "Data Load",
                        f"Project loaded but failed to load data: {status}",
                    )

        elif ds and isinstance(ds, dict) and ds.get("file_paths"):
            # Try to load files recorded in the saved project
            file_paths = ds.get("file_paths", [])
            data_context, welcome_msg = self.backend.load_file_data_with_ui(file_paths)
            if data_context is not None:
                self.backend.data_context = data_context
                QMessageBox.information(
                    self,
                    "Project Loaded",
                    f"Project '{project.title}' loaded and files restored.",
                )
            else:
                QMessageBox.warning(
                    self,
                    "Data Load",
                    f"Project loaded but failed to load files: {welcome_msg}",
                )

        else:
            QMessageBox.information(
                self, "Project Loaded", f"Project '{project.title}' loaded."
            )

        # Keep the loaded project as the new active project and set ID so main picks it up
        self.project_id = project.project_id
        # Do not close dialog immediately — accept to return to main flow
        self.accept()

    def create_project(self):
        """Create project, save it to disk, and close dialog"""
        logger.debug("Creating new project from dialog")
        project_name = self.project_name_input.text().strip()
        description = self.project_desc_input.text().strip()

        if not project_name:
            logger.warning("Project creation attempted without project name")
            QMessageBox.warning(self, "Validation Error", "Project name is required!")
            return

        logger.info(f"Creating project: '{project_name}'")
        success, message, project_id = self.backend.create_project(
            project_name, description
        )

        if not success:
            logger.error(f"Project creation failed: {message}")
            QMessageBox.critical(self, "Project Creation Failed", message)
            return

        logger.info(f"Project created successfully: {project_id}")

        # Create an initial chat for the new project
        logger.debug(f"Creating initial chat for project {project_id}")
        success, msg, chat_id = self.backend.create_chat_session("Chat 1")
        if not success:
            logger.warning(f"Failed to create initial chat: {msg}")
            QMessageBox.warning(
                self,
                "Warning",
                f"Project created but failed to create initial chat: {msg}",
            )

        # Attempt to persist project to ./projects
        logger.debug(f"Saving project {project_id} to disk")
        saved, save_msg = self.backend.save_project_to_disk(project_id)
        if not saved:
            logger.warning(f"Failed to save project to disk: {save_msg}")
            QMessageBox.warning(
                self,
                "Project Saved (Memory Only)",
                f"Project created but failed to save to disk: {save_msg}",
            )
        else:
            logger.info(f"Project saved to disk successfully: {save_msg}")
            QMessageBox.information(self, "Project Saved", save_msg)

        self.project_id = project_id
        # Make sure the saved project shows up in the sidebar immediately
        try:
            # If parent has a refresh method (main window), call it; otherwise rely on main to refresh
            parent = self.parent()
            if (
                parent
                and isinstance(parent, DataWorkspaceGUI)
                and hasattr(parent, "refresh_project_list")
            ):
                parent.refresh_project_list()
        except Exception:
            pass

        self.accept()


class DataSourceDialog(QDialog):
    """Startup dialog to select data source type"""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Select Data Source")
        self.setModal(True)
        self.setMinimumWidth(400)
        self.windowIcon = QIcon("icon.ico")
        self.setWindowIcon(self.windowIcon)

        self.data_source_type = None  # 'database', 'file', or None (cancelled)
        self.data_source_config = {}

        layout = QVBoxLayout(self)

        # Title
        title = QLabel("Select Data Source")
        title.setFont(QFont("Roboto", 14, QFont.Weight.Bold))
        title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(title)

        subtitle = QLabel("Please select a data source to analyze:")
        subtitle.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(subtitle)

        layout.addSpacing(20)

        # Buttons
        self.database_btn = QPushButton("Connect to Database")
        self.database_btn.setMinimumHeight(50)
        self.database_btn.clicked.connect(self.select_database)
        layout.addWidget(self.database_btn)

        self.file_btn = QPushButton("Select Local File(s)")
        self.file_btn.setMinimumHeight(50)
        self.file_btn.clicked.connect(self.select_files)
        layout.addWidget(self.file_btn)

        self.multi_db_btn = QPushButton("Connect Multiple Databases")
        self.multi_db_btn.setMinimumHeight(50)
        self.multi_db_btn.setToolTip(
            "Connect to two or more databases and unify them\n"
            "into a single queryable context."
        )
        self.multi_db_btn.clicked.connect(self.select_multi_database)
        layout.addWidget(self.multi_db_btn)

        layout.addSpacing(20)

        # Cancel button
        cancel_btn = QPushButton("Cancel")
        cancel_btn.clicked.connect(self.reject)
        layout.addWidget(cancel_btn)

    def select_database(self):
        """Show database connection dialog"""
        logger.debug("Opening database connection dialog")
        is_cxo = ConfigManager.get_interaction_mode() == "cxo"
        # Get semantic layer from active project if available
        semantic_layer = None
        if hasattr(self, "backend") and self.backend.active_project:
            semantic_layer = self.backend.active_project.semantic_layer
        db_dialog = DatabaseConnectionDialog(
            self, force_nlp=is_cxo, semantic_layer=semantic_layer
        )
        if db_dialog.exec() == QDialog.DialogCode.Accepted:
            logger.info("Database connection configuration accepted")
            self.data_source_type = "database"
            self.data_source_config = db_dialog.get_config()
            self.accept()

    def select_multi_database(self):
        """Show multi-database connection dialog"""
        logger.debug("Opening multi-database connection dialog")
        multi_dialog = MultiDatabaseConnectionDialog(self)
        if multi_dialog.exec() == QDialog.DialogCode.Accepted:
            configs = multi_dialog.get_configs()
            if configs:
                logger.info(
                    f"Multi-database configuration accepted: {len(configs)} connections"
                )
                self.data_source_type = "multi_database"
                self.data_source_config = {"connections": configs}
                self.accept()

    def select_files(self):
        """Show file selection dialog"""
        logger.debug("Opening file selection dialog")
        files, _ = QFileDialog.getOpenFileNames(
            self,
            "Select Data File(s)",
            "",
            "Data Files (*.csv *.xlsx *.xls);;CSV Files (*.csv);;Excel Files (*.xlsx *.xls);;All Files (*.*)",
        )

        if files:
            logger.info(f"User selected {len(files)} data file(s)")
            self.data_source_type = "file"
            self.data_source_config = {"file_paths": files}
            self.accept()


class MultiDatabaseConnectionDialog(QDialog):
    """Dialog for configuring one or more database connections."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Database Connections")
        self.setModal(True)
        self.setMinimumWidth(600)
        self.windowIcon = QIcon("icon.ico")
        self.setWindowIcon(self.windowIcon)

        self.connections: List[Dict[str, Any]] = []

        layout = QVBoxLayout(self)

        # Title
        title = QLabel("Configure Multiple Database Connections")
        title.setFont(QFont("Roboto", 14, QFont.Weight.Bold))
        title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(title)

        subtitle = QLabel(
            "Add two or more database connections. Tables will be prefixed\n"
            "with an alias to avoid name collisions."
        )
        subtitle.setWordWrap(True)
        subtitle.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(subtitle)

        layout.addSpacing(10)

        # Connection list
        self.connection_list = QListWidget()
        self.connection_list.setMinimumHeight(120)
        layout.addWidget(self.connection_list)

        # Buttons row
        btn_row = QHBoxLayout()

        self.add_btn = QPushButton("Add Connection")
        self.add_btn.clicked.connect(self._add_connection)
        btn_row.addWidget(self.add_btn)

        self.remove_btn = QPushButton("Remove Selected")
        self.remove_btn.clicked.connect(self._remove_selected)
        btn_row.addWidget(self.remove_btn)

        layout.addLayout(btn_row)

        layout.addSpacing(15)

        # OK / Cancel
        button_box = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel
        )
        button_box.accepted.connect(self._validate_and_accept)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)

    def _add_connection(self):
        """Open a DatabaseConnectionDialog to add a new connection."""
        is_cxo = ConfigManager.get_interaction_mode() == "cxo"
        # Try to get semantic layer from parent's backend if available
        semantic_layer = None
        if hasattr(self.parent(), "backend") and self.parent().backend.active_project:
            semantic_layer = self.parent().backend.active_project.semantic_layer
        db_dialog = DatabaseConnectionDialog(
            self, force_nlp=is_cxo, semantic_layer=semantic_layer
        )
        if db_dialog.exec() != QDialog.DialogCode.Accepted:
            return

        config = db_dialog.get_config()
        # Generate default alias
        idx = len(self.connections) + 1
        db_name = config.get("credentials", {}).get("database", f"db{idx}")
        alias = f"db{idx}"
        if db_name:
            # Use the database name (sanitised) as alias
            safe_name = "".join(
                c if c.isalnum() or c == "_" else "_" for c in os.path.basename(db_name)
            )
            if safe_name:
                alias = safe_name

        config["alias"] = alias

        self.connections.append(config)
        display = f"[{alias}]  {config['db_type']} — {config['credentials'].get('database', '?')}"
        item = QListWidgetItem(display)
        self.connection_list.addItem(item)
        logger.info(f"Added multi-db connection: {alias} ({config['db_type']})")

    def _remove_selected(self):
        """Remove the currently selected connection from the list."""
        row = self.connection_list.currentRow()
        if row < 0:
            return
        self.connection_list.takeItem(row)
        self.connections.pop(row)
        logger.info(f"Removed multi-db connection at index {row}")

    def _validate_and_accept(self):
        """Ensure at least two connections before accepting."""
        if len(self.connections) < 2:
            QMessageBox.warning(
                self,
                "Not Enough Connections",
                "Please add at least two database connections for multi-database mode.\n"
                "For a single database, use the regular 'Connect to Database' option.",
            )
            return
        # Ensure aliases are unique
        aliases = [c["alias"] for c in self.connections]
        if len(set(aliases)) != len(aliases):
            QMessageBox.warning(
                self,
                "Duplicate Aliases",
                "Each connection must have a unique alias. Please remove duplicates.",
            )
            return
        self.accept()

    def get_configs(self) -> List[Dict[str, Any]]:
        """Return the list of connection configurations."""
        return self.connections


class DatabaseConnectionDialog(QDialog):
    """Dialog for database connection details"""

    def __init__(
        self,
        parent=None,
        force_nlp: bool = False,
        semantic_layer: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(parent)
        self.setWindowTitle("Database Connection")
        self.setModal(True)
        self.setWindowModality(Qt.WindowModality.ApplicationModal)
        self.setMinimumWidth(500)
        self.windowIcon = QIcon("icon.ico")
        self.setWindowIcon(self.windowIcon)

        self.force_nlp = force_nlp

        self.semantic_layer: Optional[Dict[str, Any]] = semantic_layer

        layout = QVBoxLayout(self)
        form_layout = QFormLayout()

        # Database type selection
        self.db_type_combo = QComboBox()
        self.db_type_combo.addItems(
            ["sqlite", "mysql", "mariadb", "postgresql", "sqlserver", "oracle", "odbc"]
        )
        self.db_type_combo.currentTextChanged.connect(self.on_db_type_changed)
        form_layout.addRow("Database Type:", self.db_type_combo)

        # Table selection method
        self.selection_method_row_label: Optional[QLabel] = None
        self.selection_method_combo = QComboBox()
        self.selection_method_combo.addItems(["Manual", "Semantic Filter (slow)"])
        # Should default to manual if in analyst mode
        # TODO: Add global variable to deal with this while testing
        self.selection_method_combo.setCurrentIndex(0)

        data_already_loaded = False
        if hasattr(self.parent(), "backend") and self.parent().backend:
            if self.parent().backend.data_context is not None or (
                self.parent().backend.active_project
                and self.parent().backend.active_project.data_source
            ):
                data_already_loaded = True

        if self.force_nlp:
            self.selection_method_combo.setCurrentIndex(1)
            self.selection_method_combo.setEnabled(False)
        self.selection_method_combo.currentTextChanged.connect(
            self.on_selection_method_changed
        )
        if not data_already_loaded:
            self.selection_method_row_label = QLabel("Table Selection:")
            form_layout.addRow(
                self.selection_method_row_label, self.selection_method_combo
            )

        # In CxO / force_nlp mode, hide the table selection row entirely
        if self.force_nlp:
            self.selection_method_combo.setVisible(False)
            if self.selection_method_row_label is not None:
                self.selection_method_row_label.setVisible(False)

        # Common fields
        self.host_input = QLineEdit()
        self.host_input.setPlaceholderText("localhost")
        self.host_label = form_layout.addRow("Host:", self.host_input)

        self.port_input = QLineEdit()
        self.port_input.setPlaceholderText("Default port")
        self.port_label = form_layout.addRow("Port:", self.port_input)

        self.database_input = QLineEdit()
        self.database_input.setPlaceholderText("Database name or file path")
        form_layout.addRow("Database:", self.database_input)

        self.user_input = QLineEdit()
        self.user_input.setPlaceholderText("Username")
        self.user_label = form_layout.addRow("User:", self.user_input)

        self.password_input = QLineEdit()
        self.password_input.setEchoMode(QLineEdit.EchoMode.Password)
        self.password_input.setPlaceholderText("Password")
        self.password_label = form_layout.addRow("Password:", self.password_input)

        # Semantic layer import
        self.semantic_layer_label = QLabel("No semantic layer loaded")
        self.semantic_layer_button = QPushButton("Import Semantic Layer (JSON)")
        self.semantic_layer_button.clicked.connect(self.import_semantic_layer)
        self.semantic_layer_container = QWidget()
        semantic_layout = QVBoxLayout(self.semantic_layer_container)
        semantic_layout.setContentsMargins(0, 0, 0, 0)
        semantic_layout.addWidget(self.semantic_layer_button)
        semantic_layout.addWidget(self.semantic_layer_label)
        form_layout.addRow("", self.semantic_layer_container)

        layout.addLayout(form_layout)

        # Buttons
        button_box = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel
        )
        button_box.accepted.connect(self.validate_and_accept)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)

        # Initialize field visibility
        self.on_db_type_changed(self.db_type_combo.currentText())
        self.on_selection_method_changed(self.selection_method_combo.currentText())

        # If semantic layer was provided, show it as loaded
        if self.semantic_layer:
            self.semantic_layer_label.setText("Semantic layer loaded from project")
            logger.info("Semantic layer loaded from project")
        else:
            # Auto-load semantic layer from config if available and no project layer
            self._try_autoload_semantic_layer()

    def on_db_type_changed(self, db_type):
        """Show/hide fields based on database type"""
        logger.debug(f"Database type changed to: {db_type}")
        is_sqlite = db_type == "sqlite"

        # Hide host/port/user/password for SQLite
        self.host_input.setVisible(not is_sqlite)
        self.port_input.setVisible(not is_sqlite)
        self.user_input.setVisible(not is_sqlite)
        self.password_input.setVisible(not is_sqlite)

        # Update placeholder for database field
        if is_sqlite:
            self.database_input.setPlaceholderText("Path to .db or .sqlite file")
        else:
            self.database_input.setPlaceholderText("Database name")

    def on_selection_method_changed(self, method_text):
        """Show/hide semantic layer controls based on selection method."""
        logger.debug(f"Table selection method changed to: {method_text}")
        method = self._normalize_selection_method(method_text)
        is_nlp = method == "nlp" or self.force_nlp
        self.semantic_layer_container.setVisible(is_nlp)

    @staticmethod
    def _normalize_selection_method(method_text: Optional[str]) -> str:
        normalized = (method_text or "").lower()
        return (
            "nlp"
            if ("nlp" in normalized or "semantic filter" in normalized)
            else "manual"
        )

    def validate_and_accept(self):
        """Validate inputs before accepting"""
        logger.debug(
            f"Validating database connection for type: {self.db_type_combo.currentText()}"
        )
        db_type = self.db_type_combo.currentText()
        database = self.database_input.text().strip()

        if not database:
            logger.warning("Database validation failed: missing database name/path")
            QMessageBox.warning(self, "Validation Error", "Database field is required!")
            return

        if db_type != "sqlite":
            host = self.host_input.text().strip()
            if not host:
                logger.warning("Database validation failed: missing host")
                QMessageBox.warning(self, "Validation Error", "Host is required!")
                return

        method_text = self.selection_method_combo.currentText()
        method = self._normalize_selection_method(method_text)

        # Keep analyst default behavior stable: force_nlp is a CxO-only runtime
        # constraint and should not overwrite the user's global preference.
        if not self.force_nlp:
            ConfigManager.set_table_selection_method(method)
            logger.info(f"Table selection method set to: {method}")

        self.accept()

    def get_config(self) -> Dict[str, Any]:
        """Get database configuration"""
        db_type = self.db_type_combo.currentText()

        if db_type == "sqlite":
            credentials: Dict[str, Any] = {
                "database": self.database_input.text().strip()
            }
        else:
            credentials: Dict[str, Any] = {
                "host": self.host_input.text().strip(),
                "database": self.database_input.text().strip(),
                "user": self.user_input.text().strip(),
                "password": self.password_input.text().strip(),
            }

            port = self.port_input.text().strip()
            if port:
                credentials["port"] = int(port)

        method_text = self.selection_method_combo.currentText()
        method = self._normalize_selection_method(method_text)

        return {
            "db_type": db_type,
            "credentials": credentials,
            "table_selection_method": method,
            "semantic_layer": self.semantic_layer,
        }

    def populate_from_config(self, config: dict) -> None:
        """Pre-populate all dialog fields from a previous connection config."""
        db_type = (config.get("db_type") or "sqlite").lower()
        logger.debug(f"Restoring db config: {config}")
        logger.debug(
            f"DB type combo items: {[self.db_type_combo.itemText(i) for i in range(self.db_type_combo.count())]}"
        )

        idx = self.db_type_combo.findText(db_type, Qt.MatchFlag.MatchFixedString)
        if idx >= 0:
            self.db_type_combo.setCurrentIndex(idx)
        else:
            logger.warning(f"Unknown db_type '{db_type}', defaulting to sqlite")

        # Force UI refresh regardless of whether the index changed
        self.on_db_type_changed(db_type)

        creds = config.get("credentials", {})
        self.database_input.setText(creds.get("database", ""))
        if db_type != "sqlite":
            self.host_input.setText(creds.get("host", ""))
            self.user_input.setText(creds.get("user", ""))
            self.password_input.setText(creds.get("password", ""))
            port = creds.get("port")
            if port:
                self.port_input.setText(str(port))

        method = config.get("table_selection_method", "manual")
        method_text = "Semantic Filter (slow)" if method == "nlp" else "Manual"
        m_idx = self.selection_method_combo.findText(method_text)
        if m_idx >= 0:
            self.selection_method_combo.setCurrentIndex(m_idx)

    def import_semantic_layer(self) -> None:
        """Load semantic layer mapping from JSON file."""
        logger.debug("Attempting to import semantic layer")
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "Import Semantic Layer JSON",
            "",
            "JSON Files (*.json);;All Files (*.*)",
        )
        if not file_path:
            logger.debug("Semantic layer import cancelled by user")
            return

        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(
                    f
                )  # if this fails it will raise an exception so we don't need to check
            self.semantic_layer = data
            self.semantic_layer_label.setText(f"Loaded: {os.path.basename(file_path)}")
            # Note: Semantic layer will be saved with the project, not in global config
            logger.info(f"Semantic layer loaded: {file_path}")
        except Exception as e:
            QMessageBox.warning(
                self,
                "Semantic Layer Error",
                f"Failed to load semantic layer: {str(e)}",
            )

    def _try_autoload_semantic_layer(self) -> None:
        """Auto-load semantic layer from the saved config path if available."""
        saved_path = ConfigManager.get_semantic_layer_path()
        if not saved_path or not os.path.isfile(saved_path):
            return

        try:
            with open(saved_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            self.semantic_layer = data
            self.semantic_layer_label.setText(
                f"Auto-loaded: {os.path.basename(saved_path)}"
            )
            logger.info(f"Semantic layer auto-loaded from config: {saved_path}")
        except Exception as e:
            logger.warning(f"Failed to auto-load semantic layer from {saved_path}: {e}")


class NLPPromptDialog(QDialog):
    """Dialog to capture an NLP prompt for table selection."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Describe the Data You Need")
        self.setModal(True)
        self.setMinimumWidth(500)
        self.windowIcon = QIcon("icon.ico")
        self.setWindowIcon(self.windowIcon)

        layout = QVBoxLayout(self)

        title = QLabel("Describe the tables you want to analyze")
        title.setFont(QFont("Roboto", 12, QFont.Weight.Bold))
        layout.addWidget(title)

        self.prompt_input = QLineEdit()
        self.prompt_input.setPlaceholderText(
            NLP_PLACEHOLDER_TEXT[random.randint(0, len(NLP_PLACEHOLDER_TEXT) - 1)]
        )
        self.prompt_input.returnPressed.connect(self.accept)
        layout.addWidget(self.prompt_input)

        button_box = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel
        )
        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)

    def get_prompt(self) -> str:
        return self.prompt_input.text().strip()


class TableSelectionDialog(QDialog):
    """Dialog to select one or more database tables."""

    def __init__(
        self,
        table_names: List[str],
        parent=None,
        preselected: Optional[List[str]] = None,
    ):
        super().__init__(parent)
        self.setWindowTitle("Select Tables")
        self.setModal(True)
        self.setMinimumWidth(400)
        self.windowIcon = QIcon("icon.ico")
        self.setWindowIcon(self.windowIcon)

        self._table_names = table_names

        layout = QVBoxLayout(self)

        title = QLabel("Select Table(s) to Load")
        title.setFont(QFont("Roboto", 14, QFont.Weight.Bold))
        title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(title)

        subtitle = QLabel("Select one or multiple tables from the database:")
        subtitle.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(subtitle)

        layout.addSpacing(10)

        self.table_list = QListWidget()
        self.table_list.setSelectionMode(QListWidget.SelectionMode.MultiSelection)
        preselected_set = set(preselected or [])
        for table_name in table_names:
            item = QListWidgetItem(table_name)
            self.table_list.addItem(item)
            if table_name in preselected_set:
                item.setSelected(True)
        layout.addWidget(self.table_list)

        layout.addSpacing(10)

        button_box = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel
        )
        button_box.accepted.connect(self.validate_and_accept)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)

    def validate_and_accept(self):
        if not self.get_selected_tables():
            QMessageBox.warning(
                self, "Validation Error", "Please select at least one table."
            )
            return
        self.accept()

    def get_selected_tables(self) -> List[str]:
        return [item.text() for item in self.table_list.selectedItems()]


class ProjectLoadDialog(QDialog):
    """Dialog to show locally saved project files."""

    def __init__(self, files: List[str], parent=None):
        super().__init__(parent)
        self.setWindowTitle("Load Local Project")
        self.setModal(True)
        self.setMinimumWidth(600)
        self.windowIcon = QIcon("icon.ico")
        self.setWindowIcon(self.windowIcon)

        layout = QVBoxLayout(self)

        title = QLabel("Local Projects")
        title.setFont(QFont("Roboto", 14, QFont.Weight.Bold))
        title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(title)

        subtitle = QLabel("Select a previously saved project to load:")
        subtitle.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(subtitle)

        layout.addSpacing(8)

        self.file_list = QListWidget()
        self.file_list.setSpacing(4)

        def _get_created_at(file_name: str) -> str:
            data = self._load_project_data(file_name)
            return (data or {}).get("created_at") or ""

        files = sorted(files, key=_get_created_at, reverse=True)

        for f in files:
            project_data = self._load_project_data(f)
            if project_data:
                # Create list item with project title and date
                project_title = project_data.get("title", f)
                project_desc = project_data.get("description", "")
                created_at = project_data.get("created_at")

                # Format the creation date
                date_str = self._format_date(created_at)

                # Create custom widget for this list item
                item_widget = QWidget()
                item_layout = QHBoxLayout(item_widget)
                item_layout.setContentsMargins(8, 4, 8, 4)

                # Left side: title and description
                left_widget = QWidget()
                left_layout = QVBoxLayout(left_widget)
                left_layout.setContentsMargins(0, 0, 0, 0)
                left_layout.setSpacing(2)

                title_label = QLabel(project_title)
                title_label.setFont(QFont("Roboto", 10, QFont.Weight.Bold))
                left_layout.addWidget(title_label)

                if project_desc:
                    desc_label = QLabel(project_desc)
                    desc_label.setFont(QFont("Roboto", 9))
                    desc_label.setStyleSheet("color: gray;")
                    left_layout.addWidget(desc_label)

                item_layout.addWidget(left_widget, 1)

                # Right side: creation date
                date_label = QLabel(date_str)
                date_label.setFont(QFont("Roboto", 9))
                date_label.setStyleSheet("color: gray;")
                date_label.setAlignment(
                    Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter
                )
                item_layout.addWidget(date_label)

                # Create list item and set custom widget
                item = QListWidgetItem(self.file_list)
                item.setSizeHint(item_widget.sizeHint())
                item.setData(Qt.ItemDataRole.UserRole, f)
                self.file_list.addItem(item)
                self.file_list.setItemWidget(item, item_widget)
            else:
                # Fallback to filename if we can't load project data
                item = QListWidgetItem(f)
                item.setData(Qt.ItemDataRole.UserRole, f)
                self.file_list.addItem(item)

        layout.addWidget(self.file_list)

        layout.addSpacing(10)

        button_box = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel
        )
        button_box.accepted.connect(self.load_selected)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)

    def load_selected(self):
        if not self.file_list.selectedItems():
            QMessageBox.warning(
                self, "Validation Error", "Please select a project file."
            )
            return
        self.accept()

    def get_selected_file(self) -> Optional[str]:
        items = self.file_list.selectedItems()
        if not items:
            return None
        # Retrieve the actual filename from the item's data
        return items[0].data(Qt.ItemDataRole.UserRole)

    def _load_project_data(self, file_name: str) -> Optional[Dict[str, Any]]:
        """Load project data from JSON file."""
        path = os.path.join("projects", file_name)
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.debug(f"Failed to load project data for '{file_name}': {e}")
            return None

    def _format_date(self, date_str: Optional[str]) -> str:
        """Format ISO date string to human-readable format."""
        if not date_str:
            return "Unknown date"
        try:
            dt = datetime.fromisoformat(date_str)
            return dt.strftime("%b %d, %Y")  # e.g. "Sep 15, 2023"
        except Exception:
            return "Unknown date"


def select_tables_with_method(
    parent: QWidget,
    connector: DatabaseConnector,
    tables: List[str],
    selection_method: str,
    semantic_layer: Optional[Dict[str, Any]] = None,
) -> Optional[List[str]]:
    """Select tables using manual list or NLP-based selection."""
    method_text = (selection_method or "manual").lower()
    method = "nlp" if ("nlp" in method_text or "filter" in method_text) else "manual"

    if method == "nlp":
        prompt_dialog = NLPPromptDialog(parent)
        if prompt_dialog.exec() != QDialog.DialogCode.Accepted:
            return None

        prompt = prompt_dialog.get_prompt()
        if not prompt:
            QMessageBox.warning(
                parent,
                "Missing Prompt",
                "Please provide a description to select tables.",
            )
            return None

        # --- Expand prompt via LLM middleman if enabled ---
        if ConfigManager.get_prompt_expansion_enabled():
            logger.info("Prompt expansion enabled — attempting LLM expansion")
            try:
                import asyncio

                agent = AIAgent()
                schema_meta = {"tables": tables}
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    expanded = loop.run_until_complete(
                        agent.prompt_expansion_agent(
                            prompt, schema_meta, semantic_layer
                        )
                    )
                finally:
                    loop.run_until_complete(loop.shutdown_asyncgens())
                    loop.close()
                if expanded and expanded.strip():
                    logger.info(f"Expanded prompt: {expanded[:200]}")
                    prompt = expanded
                else:
                    logger.warning(
                        "Prompt expansion returned empty result, using original"
                    )
            except Exception as e:
                logger.warning(f"Prompt expansion failed, using original: {e}")

        try:
            selector = NLPTableSelector(
                connector,
                semantic_layer=semantic_layer or {},
            )
            result = selector.select_tables(prompt, top_k=3)
        except Exception as e:
            QMessageBox.warning(
                parent,
                "NLP Table Selection Failed",
                f"{str(e)}\n\nFalling back to manual selection.",
            )
            method = "manual"
        else:
            if result.status == "no_match" or not result.tables:
                QMessageBox.information(
                    parent,
                    "No Matches",
                    "No relevant tables found. Please select manually.",
                )
                method = "manual"
            else:
                candidate_tables = result.tables[:]
                if result.top_candidates:
                    candidate_tables = list(
                        dict.fromkeys(result.tables + result.top_candidates)
                    )

                table_dialog = TableSelectionDialog(
                    candidate_tables, parent, preselected=result.tables
                )
                if table_dialog.exec() != QDialog.DialogCode.Accepted:
                    return None
                return table_dialog.get_selected_tables()

    if method == "manual":
        table_dialog = TableSelectionDialog(tables, parent)
        if table_dialog.exec() != QDialog.DialogCode.Accepted:
            return None
        return table_dialog.get_selected_tables()

    QMessageBox.warning(parent, "Selection Error", "Invalid table selection method.")
    return None


class QueryWorker(QThread):
    """Worker thread to handle long-running queries without blocking UI"""

    result_signal = Signal(str)
    error_signal = Signal(str)
    clarification_signal = Signal(str)  # New signal for clarification requests
    progress_signal = Signal(str)
    stream_signal = Signal(str)

    def __init__(
        self,
        query: str,
        data_context: Dict[str, Any],
        clarification_context: Optional[str] = None,
        project_id: Optional[str] = None,
    ):
        super().__init__()
        self.query = query
        self.data_context = data_context
        self.clarification_context = (
            clarification_context  # Previous clarification answer if any
        )
        self.agent = AIAgent()
        # Initialize memory service with project context
        if project_id:
            self.agent.set_project_context(project_id)

    def run(self):
        try:
            logger.debug(
                f"QueryWorker starting execution for query: {self.query[:100]}..."
            )

            def _emit_status(message: str) -> None:
                if message:
                    self.progress_signal.emit(message)

            def _emit_stream(chunk: str) -> None:
                if chunk:
                    self.stream_signal.emit(chunk)

            # If we have clarification context, enrich the query
            effective_query = self.query
            if self.clarification_context:
                effective_query = (
                    f"{self.query}\n\nAdditional context: {self.clarification_context}"
                )
                logger.info(f"Query enriched with clarification context")

            # CxO mode: run NLP table selection first, then build context
            if self.data_context.get("cxo_mode"):
                cached_cxo_context = self.data_context.get("_cxo_selected_context")
                base_query = (self.query or "").strip().lower()

                if self._has_usable_cxo_context(cached_cxo_context):
                    logger.info(
                        "CxO mode: reusing existing chat context without NLP table reselection"
                    )
                    effective_context = cached_cxo_context
                else:
                    if cached_cxo_context:
                        logger.info(
                            "CxO mode: cached chat context has no selected tables, rerunning NLP selection"
                        )
                    # In CxO mode, table selection should always be based on the
                    # initial user prompt, not clarification follow-up text.
                    effective_context = self._build_cxo_context(self.query)
                    if effective_context is not None:
                        # Persist selected context for clarification round-trips.
                        self.data_context["_cxo_selected_context"] = effective_context
                        self.data_context["_cxo_selected_prompt"] = base_query
                if effective_context is None:
                    self.error_signal.emit(
                        "Could not identify relevant tables for your question. "
                        "Please try rephrasing with more specific terms."
                    )
                    return
            else:
                effective_context = self.data_context

            # Clarification follow-up should continue execution, not ask for
            # another ambiguity question for the same intent.
            effective_context = dict(effective_context)
            effective_context["_skip_clarification"] = bool(self.clarification_context)

            # Run async agent methods in a new event loop
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            # Use the new orchestrated execute_query method
            tables = effective_context.get("tables", [])
            logger.debug(f"Executing query asynchronously (tables: {tables})")
            result = loop.run_until_complete(
                self.agent.execute_query(
                    effective_query,
                    effective_context,
                    status_callback=_emit_status,
                    stream_callback=_emit_stream,
                )
            )

            # Check if result is a clarification request
            if result.startswith("[[CLARIFICATION_NEEDED]]"):
                clarification_text = result.replace(
                    "[[CLARIFICATION_NEEDED]]", ""
                ).strip()
                logger.debug("Clarification requested; emitting clarification signal")
                self.clarification_signal.emit(clarification_text)
            else:
                logger.info(
                    f"Query execution completed successfully (result length: {len(result)} chars)"
                )
                self.result_signal.emit(result)

            # Prevent "Task was destroyed but it is pending" warnings from
            # async generators used by streaming clients.
            loop.run_until_complete(loop.shutdown_asyncgens())
            loop.close()
        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}", exc_info=True)
            self.error_signal.emit(f"Error: {str(e)}")

    def _has_high_confidence_cache_hit(self, query: str) -> bool:
        """Check whether query memory already has a reusable high-confidence SQL hit."""
        if not self.agent.memory_service:
            return False

        try:
            similar_queries = self.agent.memory_service.search_similar_queries(
                prompt=query,
                limit=3,
                project_scoped=True,
                similarity_threshold=0.75,
            )
            for result in similar_queries:
                if (
                    result.similarity_score >= 0.85
                    and result.record.execution_success
                    and result.record.generated_sql
                ):
                    return True
        except Exception as cache_err:
            logger.warning(f"CxO mode: cache pre-check failed: {cache_err}")

        return False

    @staticmethod
    def _has_usable_cxo_context(context: Optional[Dict[str, Any]]) -> bool:
        """Return True when CxO context has selected tables ready for SQL planning."""
        if not isinstance(context, dict):
            return False
        tables = context.get("tables")
        return isinstance(tables, list) and len(tables) > 0

    def _build_cxo_context(self, query: str) -> Optional[Dict[str, Any]]:
        """
        In CxO mode, connect to the database, run NLP table selection on the
        user's prompt, collect table info for the selected tables, and return
        a complete data context ready for execute_query.

        Args:
            query: The user query (potentially enriched with clarification)
        """
        from processing import _collect_table_info

        db_type = self.data_context["db_type"]
        credentials = self.data_context["credentials"]
        semantic_layer = self.data_context.get("semantic_layer")

        # If query memory has a high-confidence SQL hit, first try to reuse an
        # existing selected-table context; otherwise run NLP once to seed context
        # for stable follow-up questions in the same chat.
        has_cache_hit = self._has_high_confidence_cache_hit(query)
        if has_cache_hit:
            logger.info("CxO mode: cache hit detected, attempting context reuse")
            cached_context = self.data_context.get("_cxo_selected_context")
            if self._has_usable_cxo_context(cached_context):
                logger.info("CxO mode: cache hit using existing selected-table context")
                return cached_context
            logger.info(
                "CxO mode: cache hit has no selected-table context; running NLP selection to seed follow-up context"
            )

        logger.info(f"CxO mode: connecting to {db_type} for NLP table selection...")
        connector = DatabaseConnector()
        success, message = connector.connect(db_type, credentials)
        if not success:
            logger.error(f"CxO mode: DB connection failed: {message}")
            connector.close()
            return None

        try:
            # --- Expand prompt via LLM middleman if enabled ---
            effective_prompt = query
            if ConfigManager.get_prompt_expansion_enabled():
                logger.info(
                    "CxO mode: prompt expansion enabled — attempting LLM expansion"
                )
                try:
                    all_tables = self.data_context.get("all_tables", [])
                    schema_meta = {"tables": all_tables}
                    exp_agent = AIAgent()
                    exp_loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(exp_loop)
                    try:
                        expanded = exp_loop.run_until_complete(
                            exp_agent.prompt_expansion_agent(
                                query, schema_meta, semantic_layer
                            )
                        )
                    finally:
                        exp_loop.run_until_complete(exp_loop.shutdown_asyncgens())
                        exp_loop.close()
                    if expanded and expanded.strip():
                        logger.info(f"CxO prompt expanded: {expanded[:200]}")
                        effective_prompt = expanded
                    else:
                        logger.warning(
                            "CxO prompt expansion returned empty result, using original"
                        )
                except Exception as e:
                    logger.warning(f"CxO prompt expansion failed, using original: {e}")

            # Run NLP table selector
            selector = NLPTableSelector(
                connector,
                semantic_layer=semantic_layer or {},
            )
            result = selector.select_tables(effective_prompt, top_k=5)

            if result.status == "no_match" or not result.tables:
                logger.warning("CxO mode: NLP found no matching tables")
                connector.close()
                return None

            # Use all selected + top candidates
            selected_tables = result.tables[:]
            if result.top_candidates:
                selected_tables = list(
                    dict.fromkeys(result.tables + result.top_candidates)
                )

            logger.info(f"CxO mode: NLP selected tables: {selected_tables}")

            # Collect table info for the selected tables
            table_info: Dict[str, Any] = {}
            for table_name in selected_tables:
                info, _skipped = _collect_table_info(connector, table_name)
                table_info[table_name] = info

            context = {
                "source_type": "database",
                "db_type": db_type,
                "credentials": credentials,
                "tables": selected_tables,
                "table_info": table_info,
                "semantic_layer": semantic_layer,
            }

            logger.info(f"CxO mode: built context with {len(selected_tables)} tables")
            return context

        except Exception as e:
            logger.error(f"CxO mode: NLP table selection failed: {e}", exc_info=True)
            return None
        finally:
            connector.close()


class DataWorkspaceGUI(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("AI Data Workspace")
        self.setGeometry(100, 100, 1400, 800)
        self.windowIcon = QIcon("icon.ico")
        self.setWindowIcon(self.windowIcon)

        # Create menu bar
        menu_bar = self.menuBar()

        # ===== File Menu =====
        file_menu = menu_bar.addMenu("File")

        new_project_action = QAction("New Project", self)
        new_project_action.setShortcut("Ctrl+N")
        new_project_action.triggered.connect(self.new_project)
        file_menu.addAction(new_project_action)

        load_project_action = QAction("Load Project", self)
        load_project_action.setShortcut("Ctrl+O")
        load_project_action.triggered.connect(self.load_project_menu)
        file_menu.addAction(load_project_action)

        save_project_action = QAction("Save Project", self)
        save_project_action.setShortcut("Ctrl+S")
        save_project_action.triggered.connect(self.save_project)
        file_menu.addAction(save_project_action)

        file_menu.addSeparator()

        connect_additional_action = QAction("Connect Data Source", self)
        connect_additional_action.triggered.connect(self.connect_additional_data_source)
        file_menu.addAction(connect_additional_action)

        change_tables_action = QAction("Change Selected Tables", self)
        change_tables_action.setToolTip(
            "Pick different tables from the connected database and start a new chat."
        )
        change_tables_action.triggered.connect(self.change_selected_tables)
        file_menu.addAction(change_tables_action)

        file_menu.addSeparator()

        export_results_action = QAction("Export Results", self)
        export_results_action.setShortcut("Ctrl+E")
        export_results_action.triggered.connect(self.export_results_dialog)
        file_menu.addAction(export_results_action)

        export_chat_action = QAction("Export Chat", self)
        export_chat_action.triggered.connect(self.export_chat_dialog)
        file_menu.addAction(export_chat_action)

        clear_query_cache_action = QAction("Clear Query Cache", self)
        clear_query_cache_action.triggered.connect(self.clear_query_cache)
        file_menu.addAction(clear_query_cache_action)

        file_menu.addSeparator()

        exit_action = QAction("Exit", self)
        exit_action.setShortcut("Ctrl+Q")
        exit_action.triggered.connect(self.close)
        file_menu.addAction(exit_action)

        # ===== Edit Menu =====
        edit_menu = menu_bar.addMenu("Edit")

        clear_conv_action = QAction("Clear Conversation", self)
        clear_conv_action.triggered.connect(self.clear_conversation)
        edit_menu.addAction(clear_conv_action)

        reset_ws_action = QAction("Reset Workspace", self)
        reset_ws_action.triggered.connect(self.reset_workspace)
        edit_menu.addAction(reset_ws_action)

        # ===== View Menu =====
        view_menu = menu_bar.addMenu("View")

        # Theme selection with exclusive group
        self.theme_group = QActionGroup(self)
        self.theme_group.setExclusive(True)

        self.dark_theme_action = QAction("Dark Theme", self, checkable=True)
        self.dark_theme_action.triggered.connect(lambda: self.set_theme("dark"))
        self.theme_group.addAction(self.dark_theme_action)
        view_menu.addAction(self.dark_theme_action)

        self.light_theme_action = QAction("Light Theme", self, checkable=True)
        self.light_theme_action.triggered.connect(lambda: self.set_theme("light"))
        self.theme_group.addAction(self.light_theme_action)
        view_menu.addAction(self.light_theme_action)

        self.system_theme_action = QAction("System Theme", self, checkable=True)
        self.system_theme_action.setChecked(True)
        self.system_theme_action.triggered.connect(lambda: self.set_theme("system"))
        self.theme_group.addAction(self.system_theme_action)
        view_menu.addAction(self.system_theme_action)

        view_menu.addSeparator()

        inc_font_action = QAction("Increase Font Size", self)
        inc_font_action.setShortcut("Ctrl++")
        inc_font_action.triggered.connect(lambda: self.adjust_font(1))
        view_menu.addAction(inc_font_action)

        dec_font_action = QAction("Decrease Font Size", self)
        dec_font_action.setShortcut("Ctrl+-")
        dec_font_action.triggered.connect(lambda: self.adjust_font(-1))
        view_menu.addAction(dec_font_action)

        view_menu.addSeparator()

        self.show_sql_response_action = QAction("Show SQL In Responses", self)
        self.show_sql_response_action.setCheckable(True)
        self.show_sql_response_action.setChecked(
            ConfigManager.get_show_sql_in_responses()
        )
        self.show_sql_response_action.triggered.connect(
            self.toggle_show_sql_in_responses
        )
        view_menu.addAction(self.show_sql_response_action)

        # ===== Settings Menu =====
        settings_menu = menu_bar.addMenu("Settings")

        api_settings_action = QAction("API Key Settings", self)
        api_settings_action.triggered.connect(self.change_api_settings)
        settings_menu.addAction(api_settings_action)

        ai_host_settings_action = QAction("AI Host Settings", self)
        ai_host_settings_action.triggered.connect(self.change_ai_host_settings)
        settings_menu.addAction(ai_host_settings_action)

        model_settings_action = QAction("Model Settings", self)
        model_settings_action.triggered.connect(self.change_model_settings)
        settings_menu.addAction(model_settings_action)

        settings_menu.addSeparator()

        # Interaction Mode submenu
        mode_menu = settings_menu.addMenu("Interaction Mode")
        self.mode_action_group = QActionGroup(self)
        self.mode_action_group.setExclusive(True)

        current_mode = ConfigManager.get_interaction_mode()

        self.cxo_mode_action = QAction("CxO Mode", self)
        self.cxo_mode_action.setCheckable(True)
        self.cxo_mode_action.setChecked(current_mode == "cxo")
        self.cxo_mode_action.triggered.connect(lambda: self.set_interaction_mode("cxo"))
        self.mode_action_group.addAction(self.cxo_mode_action)
        mode_menu.addAction(self.cxo_mode_action)

        self.analyst_mode_action = QAction("Analyst Mode", self)
        self.analyst_mode_action.setCheckable(True)
        self.analyst_mode_action.setChecked(current_mode == "analyst")
        self.analyst_mode_action.triggered.connect(
            lambda: self.set_interaction_mode("analyst")
        )
        self.mode_action_group.addAction(self.analyst_mode_action)
        mode_menu.addAction(self.analyst_mode_action)

        # Prompt Expansion toggle
        settings_menu.addSeparator()
        self.prompt_expansion_action = QAction("Prompt Expansion (NLP)", self)
        self.prompt_expansion_action.setCheckable(True)
        self.prompt_expansion_action.setChecked(
            ConfigManager.get_prompt_expansion_enabled()
        )
        self.prompt_expansion_action.setToolTip(
            "When enabled, user prompts are expanded via the LLM\n"
            "into precise business terms before NLP table selection."
        )
        self.prompt_expansion_action.triggered.connect(self.toggle_prompt_expansion)
        settings_menu.addAction(self.prompt_expansion_action)

        # Local LLM settings
        settings_menu.addSeparator()
        local_llm_action = QAction("Local LLM Settings", self)
        local_llm_action.setToolTip(
            "Configure a local LLM server (e.g. Ollama) for fully offline execution."
        )
        local_llm_action.triggered.connect(self.open_local_llm_settings)
        settings_menu.addAction(local_llm_action)

        # ===== Help Menu =====
        help_menu = menu_bar.addMenu("Help")

        docs_action = QAction("Documentation", self)
        docs_action.triggered.connect(self.open_docs)
        help_menu.addAction(docs_action)

        about_action = QAction("About", self)
        about_action.triggered.connect(self.show_about)
        help_menu.addAction(about_action)

        # Create central widget and main layout
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QHBoxLayout(central_widget)

        # Left sidebar: Project info and chat list
        sidebar = QWidget()
        sidebar_layout = QVBoxLayout(sidebar)

        # Project info display
        self.project_name_label = QLabel("No Project Loaded")
        self.project_name_label.setFont(QFont("Roboto", 12, QFont.Weight.Bold))
        self.project_name_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.project_name_label.setWordWrap(True)
        sidebar_layout.addWidget(self.project_name_label)

        # Separator
        sidebar_layout.addSpacing(8)

        # Chats label
        chat_title = QLabel("Chats")
        chat_title.setFont(QFont("Roboto", 14, QFont.Weight.Bold))
        chat_title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        sidebar_layout.addWidget(chat_title)

        # Chat list
        self.chat_list = QListWidget()
        self.chat_list.itemClicked.connect(self.on_chat_selected)
        self.chat_list.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.chat_list.customContextMenuRequested.connect(self.show_chat_context_menu)
        sidebar_layout.addWidget(self.chat_list)

        # New chat button
        self.new_chat_button = QPushButton("+ New Chat")
        self.new_chat_button.clicked.connect(self.create_new_chat)
        sidebar_layout.addWidget(self.new_chat_button)

        # Save button
        self.save_button = QPushButton("Save Project")
        self.save_button.clicked.connect(self.save_project)
        sidebar_layout.addWidget(self.save_button)

        sidebar.setMaximumWidth(250)
        # Main content area
        content_widget = QWidget()
        content_layout = QVBoxLayout(content_widget)
        content_layout.setContentsMargins(0, 0, 0, 0)

        # Conversation area (scrollable), supports Markdown
        conversation_title = QLabel("Conversation")
        conversation_title.setFont(QFont("Roboto", 14, QFont.Weight.Bold))
        content_layout.addWidget(conversation_title)

        self.conversation_display = QTextEdit()
        self.conversation_display.setReadOnly(True)
        self.conversation_display.setPlaceholderText(
            "Select a chat or start a new conversation..."
        )
        content_layout.addWidget(self.conversation_display, 1)

        # Input area at bottom
        input_section = QWidget()
        input_title = QLabel("Chat")
        input_title.setFont(QFont("Roboto", 14, QFont.Weight.Bold))
        input_layout = QVBoxLayout(input_section)
        input_layout.addWidget(input_title, 0)

        self.query_input = MessageTextEdit()
        self.query_input.setPlaceholderText(
            "Type your message here... (Press Enter to send, Shift+Enter for new line)"
        )
        self.query_input.setMaximumHeight(100)
        self.query_input.submit_signal.connect(self.submit_query)

        button_layout = QHBoxLayout()
        self.submit_button = QPushButton("Send")
        self.submit_button.clicked.connect(self.submit_query)
        self.clear_button = QPushButton("Clear Chat")
        self.clear_button.clicked.connect(self.clear_fields)

        button_layout.addStretch()
        button_layout.addWidget(self.submit_button)
        button_layout.addWidget(self.clear_button)

        input_layout.addWidget(self.query_input)
        input_layout.addLayout(button_layout)
        input_layout.addStretch()

        content_layout.addWidget(input_section)

        # Add sidebar and content to main layout
        main_layout.addWidget(sidebar, 1)
        main_layout.addWidget(content_widget, 3)

        self.worker = None
        self.backend = DataWorkspaceBackend()
        self.project_id: Optional[str] = None
        self.chat_id: Optional[str] = None
        self.data_context: Optional[Dict[str, Any]] = None
        self.is_running = False
        self.processing_marker = "**Assistant:** _Processing..._"
        self.processing_token_start = "<!--PROCESSING_BLOCK_START-->"
        self.processing_token_end = "<!--PROCESSING_BLOCK_END-->"
        self.current_processing_block: Optional[str] = None
        self.current_processing_status = ""
        self.current_partial_response = ""
        self.current_markdown: Optional[str] = None
        self.current_theme = "system"
        self.font_point_size = QApplication.instance().font().pointSize()
        # Clarification flow state
        self.pending_clarification_query: Optional[str] = None
        self.clarification_question: Optional[str] = None
        self.last_submitted_query: Optional[str] = None

        # Processing animation state
        self.animation_timer = QTimer(self)
        self.animation_timer.timeout.connect(self._update_processing_animation)
        self.animation_frame = 0
        self.animation_frames = [".", "..", "...", ".."]  # Pulsing dots
        self.processing_refresh_timer = QTimer(self)
        self.processing_refresh_timer.setSingleShot(True)
        self.processing_refresh_timer.setInterval(120)
        self.processing_refresh_timer.timeout.connect(self._flush_processing_refresh)

        # Load saved theme preference or use system theme
        config = ConfigManager.load_config()
        saved_theme = config.get("theme", "system")
        if saved_theme in ["dark", "light", "system"]:
            self.current_theme = saved_theme

        # Apply theme on startup
        self._apply_theme(self.current_theme)

    @staticmethod
    def _build_cxo_context_signature(context: Dict[str, Any]) -> str:
        """Build a stable signature for the currently loaded CxO base context."""
        safe_credentials = dict(context.get("credentials", {}) or {})
        safe_credentials.pop("password", None)
        signature_payload = {
            "source_type": context.get("source_type"),
            "db_type": context.get("db_type"),
            "credentials": safe_credentials,
            "all_tables": context.get("all_tables", []),
        }
        return json.dumps(signature_payload, sort_keys=True, default=str)

    def _get_active_chat_query_context(self) -> Optional[Dict[str, Any]]:
        """Resolve query context, using chat-scoped runtime state in CxO mode."""
        base_context = self.backend.data_context or self.data_context
        if not isinstance(base_context, dict):
            return None

        if not base_context.get("cxo_mode"):
            return base_context

        active_chat = self.backend.active_chat
        if active_chat is None:
            return base_context

        signature = self._build_cxo_context_signature(base_context)
        runtime_context = active_chat.runtime_context
        if (
            not isinstance(runtime_context, dict)
            or runtime_context.get("_cxo_base_signature") != signature
        ):
            runtime_context = dict(base_context)
            runtime_context.pop("_cxo_selected_context", None)
            runtime_context.pop("_cxo_selected_prompt", None)
            runtime_context["_cxo_base_signature"] = signature
            active_chat.runtime_context = runtime_context

        return runtime_context

    def _sync_data_context_for_active_chat(self) -> None:
        """Update GUI-level context pointer to match active chat state."""
        resolved = self._get_active_chat_query_context()
        if resolved is not None:
            self.data_context = resolved

    def show_chat_context_menu(self, position):
        """Show context menu for chat item on right-click"""
        logger.debug("Chat context menu requested")
        item = self.chat_list.itemAt(position)
        if not item:
            logger.debug("Chat context menu requested but no item at position")
            return

        menu = QMenu(self)

        clear_action = menu.addAction("Clear Chat")
        delete_action = menu.addAction("Delete Chat")
        export_action = menu.addAction("Export Chat")

        action = menu.exec(self.chat_list.mapToGlobal(position))

        if action == clear_action:
            self.clear_chat_action(item)
        elif action == delete_action:
            self.delete_chat_action(item)
        elif action == export_action:
            chat_id = item.data(Qt.ItemDataRole.UserRole)
            self._export_single_chat(chat_id)

    def clear_chat_action(self, item: QListWidgetItem):
        """Clear messages from a chat"""
        chat_id = item.data(Qt.ItemDataRole.UserRole)
        logger.info(f"User requested to clear chat (chat_id: {chat_id})")

        reply = QMessageBox.question(
            self,
            "Clear Chat",
            "Clear all messages in this chat?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
        )
        if reply == QMessageBox.StandardButton.Yes:
            try:
                success, msg = self.backend.delete_chat_session(chat_id)
                if not success:
                    logger.error(f"Failed to clear chat: {msg}")
                    QMessageBox.warning(self, "Error", f"Failed to clear chat: {msg}")
                    return

                # Create a new chat with the same name to replace it
                chat_session = (
                    self.backend.active_project.get_chat(chat_id)
                    if self.backend.active_project
                    else None
                )
                if chat_session:
                    original_title = chat_session.title
                else:
                    original_title = item.text()

                success, msg, new_chat_id = self.backend.create_chat_session(
                    original_title
                )
                if success:
                    self.refresh_chat_list()
                    logger.info(
                        f"Chat cleared successfully. New chat_id: {new_chat_id}"
                    )

                    # If the cleared chat was the currently selected one, update the display
                    if self.chat_id == chat_id:
                        logger.debug(
                            f"Clearing display for current chat, updating to new chat_id: {new_chat_id}"
                        )
                        self.chat_id = new_chat_id
                        self.backend.load_chat_session(new_chat_id)
                        self._set_current_markdown(
                            "Chat cleared. Start typing to begin a new conversation."
                        )
                else:
                    logger.error(f"Failed to create replacement chat: {msg}")
                    QMessageBox.warning(
                        self, "Error", f"Failed to create replacement chat: {msg}"
                    )
            except Exception as e:
                logger.error(f"Error clearing chat: {str(e)}", exc_info=True)
                QMessageBox.critical(self, "Error", f"Failed to clear chat: {str(e)}")
        else:
            logger.info("User cancelled chat clear")

    def delete_chat_action(self, item: QListWidgetItem):
        """Delete a chat entirely"""
        chat_id = item.data(Qt.ItemDataRole.UserRole)
        chat_title = item.text()
        logger.info(f"User requested to delete chat (chat_id: {chat_id})")

        reply = QMessageBox.question(
            self,
            "Delete Chat",
            f"Are you sure you want to delete '{chat_title}'? This cannot be undone.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
        )
        if reply == QMessageBox.StandardButton.Yes:
            try:
                success, msg = self.backend.delete_chat_session(chat_id)
                if not success:
                    logger.error(f"Failed to delete chat: {msg}")
                    QMessageBox.warning(self, "Error", f"Failed to delete chat: {msg}")
                    return

                # Clear the conversation display if this was the active chat
                if self.chat_id == chat_id:
                    self.chat_id = None
                    self.conversation_display.clear()

                # Refresh the chat list
                self.refresh_chat_list()
                logger.info(f"Chat deleted successfully: {chat_id}")
            except Exception as e:
                logger.error(f"Error deleting chat: {str(e)}", exc_info=True)
                QMessageBox.critical(self, "Error", f"Failed to delete chat: {str(e)}")
        else:
            logger.info("User cancelled chat delete")

    def on_chat_selected(self, item: QListWidgetItem):
        """Handle chat selection"""
        self.chat_id = item.data(Qt.ItemDataRole.UserRole)
        logger.debug(f"Chat selected: {self.chat_id}")
        if self.chat_id:
            success, _ = self.backend.load_chat_session(self.chat_id)
            if success:
                logger.info(f"Chat session loaded: {self.chat_id}")
                self._sync_data_context_for_active_chat()
                history = self.backend.get_chat_history()
                if history:
                    chat_history = self._format_chat_history(history)
                    self._set_current_markdown(chat_history)
                else:
                    self._set_current_markdown(
                        "No chat history yet. Start typing to begin the conversation."
                    )
            else:
                logger.warning(f"Failed to load chat session: {self.chat_id}")
                self._set_current_markdown("Failed to load chat.")

        # Highlight the selected chat
        try:
            idx = self.chat_list.row(item)
            self.chat_list.setCurrentRow(idx)
        except Exception:
            pass

    def create_new_chat(self):
        """Create a new chat in the active project"""
        logger.info("User requested to create new chat")
        if self.backend.active_project is None:
            logger.warning("New chat creation attempted but no project loaded")
            QMessageBox.warning(self, "No Project", "Please load a project first.")
            return

        # Auto-generate chat name
        chat_num = len(self.backend.active_project.chats) + 1
        logger.debug(f"Creating new chat: Chat {chat_num}")
        success, msg, chat_id = self.backend.create_chat_session(f"Chat {chat_num}")

        if not success:
            logger.error(f"Failed to create chat: {msg}")
            QMessageBox.warning(self, "Error", f"Failed to create chat: {msg}")
            return

        logger.info(f"New chat created: {chat_id}")
        # Ensure the new chat is both selected in UI and active in backend.
        if chat_id:
            self._activate_chat(chat_id)

    def _activate_chat(self, chat_id: str) -> bool:
        """Activate a chat in backend + UI and render its current history."""
        if not chat_id:
            return False

        self.refresh_chat_list()

        self.chat_id = chat_id
        success, _ = self.backend.load_chat_session(chat_id)
        if not success:
            logger.warning(f"Failed to activate chat session: {chat_id}")
            return False

        self._sync_data_context_for_active_chat()
        history = self.backend.get_chat_history()
        if history:
            self._set_current_markdown(self._format_chat_history(history))
        else:
            self._set_current_markdown(
                "New chat created. Start typing to begin the conversation."
            )

        for i in range(self.chat_list.count()):
            item = self.chat_list.item(i)
            if item and item.data(Qt.ItemDataRole.UserRole) == chat_id:
                self.chat_list.setCurrentRow(i)
                break

        return True

    def _start_fresh_chat_for_active_project(self) -> None:
        """Start a fresh chat when opening a project, avoiding reuse of historical Chat 1."""
        if self.backend.active_project is None:
            return

        chats = self.backend.active_project.get_all_chats()

        # For a brand-new project, reuse its single empty chat.
        if len(chats) == 1 and not chats[0].messages:
            self._activate_chat(chats[0].session_id)
            return

        chat_num = len(chats) + 1
        success, msg, new_chat_id = self.backend.create_chat_session(f"Chat {chat_num}")
        if not success or not new_chat_id:
            logger.warning(f"Failed to create fresh chat on project open: {msg}")
            return

        self._activate_chat(new_chat_id)

    def _format_chat_history(self, messages: List[Dict[str, str]]) -> str:
        """Format chat messages as Markdown"""
        parts: List[str] = []
        for msg in messages:
            role = msg.get("role", "unknown").capitalize()
            content = msg.get("content", "")
            formatted_content = self.backend.markdown_to_qt(content)
            # Keep role and body on separate lines so markdown headings/lists
            # in the body render correctly when chat history is reloaded.
            if formatted_content:
                parts.append(f"**{role}:**\n\n{formatted_content}")
            else:
                parts.append(f"**{role}:**")
        return "\n\n".join(parts)

    def _get_current_markdown(self) -> str:
        if self.current_markdown is None:
            self.current_markdown = self.conversation_display.toMarkdown()
        return self.current_markdown

    def _set_current_markdown(self, md: str) -> None:
        scroll_bar = self.conversation_display.verticalScrollBar()
        was_near_bottom = True
        saved_ratio: Optional[float] = None
        if scroll_bar:
            old_max = scroll_bar.maximum()
            old_value = scroll_bar.value()
            was_near_bottom = (old_max - old_value) <= 24
            if not was_near_bottom and old_max > 0:
                saved_ratio = old_value / old_max

        self.current_markdown = md
        self.conversation_display.setHtml(markdown_to_html(md))

        if scroll_bar:
            if was_near_bottom:
                QTimer.singleShot(0, lambda: scroll_bar.setValue(scroll_bar.maximum()))
            elif saved_ratio is not None:
                QTimer.singleShot(
                    0,
                    lambda: scroll_bar.setValue(int(scroll_bar.maximum() * saved_ratio)),
                )

    def _build_processing_block(self) -> str:
        # Animated dots for processing indicator
        dots = self.animation_frames[self.animation_frame]
        lines = [self.processing_token_start, f"**Assistant:** _Processing{dots}_"]
        if self.current_processing_status:
            lines.append(f"**Status:** {self.current_processing_status}")
        if self.current_partial_response:
            lines.append("**Partial response:**")
            lines.append(self.current_partial_response)
        lines.append(self.processing_token_end)
        return "\n\n".join(lines)

    def _update_processing_animation(self) -> None:
        """Update the processing animation frame."""
        self.animation_frame = (self.animation_frame + 1) % len(self.animation_frames)
        # Only update if we have a processing block active
        if self.current_processing_block is not None:
            # While stream text is arriving, avoid extra animation repaints.
            if self.current_partial_response:
                return
            self._schedule_processing_refresh()

    def _schedule_processing_refresh(self) -> None:
        """Coalesce fast status/stream updates to reduce repaint flicker."""
        if self.current_processing_block is None:
            return
        if not self.processing_refresh_timer.isActive():
            self.processing_refresh_timer.start()

    def _flush_processing_refresh(self) -> None:
        """Apply the latest processing block state in a single repaint."""
        if self.current_processing_block is None:
            return
        new_block = self._build_processing_block()
        self._set_processing_block(new_block)

    def _set_processing_block(self, new_block: str) -> None:
        current_md = self._get_current_markdown()
        start_idx = current_md.find(self.processing_token_start)
        end_idx = current_md.find(self.processing_token_end, start_idx)
        if start_idx != -1 and end_idx != -1:
            end_idx += len(self.processing_token_end)
            current_md = current_md[:start_idx] + new_block + current_md[end_idx:]
        else:
            current_md = "\n\n".join(
                [segment for segment in [current_md.strip(), new_block] if segment]
            )
        self._set_current_markdown(current_md)
        self.current_processing_block = new_block

    def _reset_processing_state(self) -> None:
        self.current_processing_status = ""
        self.current_partial_response = ""
        self.current_processing_block = None
        # Stop animation timer
        if self.animation_timer.isActive():
            self.animation_timer.stop()
        if self.processing_refresh_timer.isActive():
            self.processing_refresh_timer.stop()
        self.animation_frame = 0

    def _replace_processing_block(self, replacement: str) -> None:
        current_md = self._get_current_markdown()
        start_idx = current_md.find(self.processing_token_start)
        end_idx = current_md.find(self.processing_token_end, start_idx)
        if start_idx != -1 and end_idx != -1:
            end_idx += len(self.processing_token_end)
            current_md = current_md[:start_idx] + replacement + current_md[end_idx:]
            self._set_current_markdown(current_md)

    def update_status(self, status: str) -> None:
        """Update progress status text while a query runs."""
        self.current_processing_status = status
        self._schedule_processing_refresh()

    def update_stream(self, chunk: str) -> None:
        """Render streaming analysis output as it arrives."""
        if not chunk:
            return
        self.current_partial_response += chunk
        self._schedule_processing_refresh()

    def submit_query(self):
        """Handle query submission or stop running query"""
        # If currently running, stop the query
        if self.is_running:
            logger.info("User requested to stop running query")
            self.stop_query()
            return

        query = self.query_input.toPlainText().strip()

        if not query:
            logger.debug("Empty query submitted, ignoring")
            return

        query_context = self._get_active_chat_query_context()
        if query_context is None:
            logger.warning("Query submitted but no data loaded")
            QMessageBox.warning(
                self, "No Data", "No data loaded. Please restart and load data first."
            )
            return
        self.data_context = query_context

        # Check if we're responding to a clarification
        is_clarification_response = self.pending_clarification_query is not None

        if is_clarification_response:
            logger.info(f"Submitting clarification response: {query[:100]}...")
            actual_query = self.pending_clarification_query
            clarification_context = query
        else:
            logger.info(f"Submitting query: {query[:100]}...")
            actual_query = query
            clarification_context = None

        self.last_submitted_query = actual_query

        # Change button to Stop
        self.submit_button.setText("Stop")
        self.is_running = True

        # Display user message
        current_md = self._get_current_markdown()
        user_message_md = f"**You:** {query}"
        self.current_processing_status = "Starting..."
        self.current_partial_response = ""

        # Start processing animation
        self.animation_frame = 0
        self.animation_timer.start(500)  # Update every 500ms for smooth pulsing

        processing_md = self._build_processing_block()
        self.current_processing_block = processing_md
        combined = "\n\n".join(
            [
                segment
                for segment in [current_md.strip(), user_message_md, processing_md]
                if segment
            ]
        )
        self._set_current_markdown(combined)

        # Scroll to bottom
        scroll_bar = self.conversation_display.verticalScrollBar()
        if scroll_bar:
            QTimer.singleShot(0, lambda: scroll_bar.setValue(scroll_bar.maximum()))

        self.query_input.clear()

        # Reset UI if we were in clarification mode
        if is_clarification_response:
            self.submit_button.setText("Send")
            self.query_input.setPlaceholderText(
                "Type your message here... (Press Enter to send, Shift+Enter for new line)"
            )
            self.pending_clarification_query = None
            self.clarification_question = None

        # Add user message to chat history
        self.add_message_to_chat("user", query)

        # Create and start worker thread
        logger.debug("Creating query worker for active SQL context")
        self.worker = QueryWorker(
            actual_query,
            query_context,
            clarification_context,
            project_id=self.project_id,  # Pass project_id for memory service
        )
        self.worker.result_signal.connect(self.display_result)
        self.worker.error_signal.connect(self.display_error)
        self.worker.clarification_signal.connect(self.handle_clarification)
        self.worker.progress_signal.connect(self.update_status)
        self.worker.stream_signal.connect(self.update_stream)
        self.worker.finished.connect(self.on_query_finished)
        logger.debug("Starting query worker thread")
        self.worker.start()

    def stop_query(self):
        """Stop the currently running query"""
        logger.info("User requested query cancellation")
        if self.worker and self.worker.isRunning():
            logger.debug("Terminating running query worker thread")
            self.worker.terminate()
            self.worker.wait()
            logger.info("Query worker thread successfully terminated")

            # Display cancellation message
            current_md = self._get_current_markdown()
            cancelled_md = "**Status:** _Query cancelled by user._"
            self._replace_processing_block(cancelled_md)
            self._reset_processing_state()

        self.is_running = False
        self.submit_button.setText("Send")

    def on_query_finished(self):
        """Handle query worker finishing"""
        self.is_running = False
        self.submit_button.setText("Send")

    def display_result(self, result: str):
        """Display query result with Markdown formatting"""
        logger.info(f"Displaying query result (length: {len(result)} chars)")
        formatted_result = self.backend.markdown_to_qt(result)

        # Replace the "Processing..." message with the actual result
        current_md = self._get_current_markdown()
        result_md = f"**Assistant:**\n{formatted_result}"

        if self.processing_token_start in current_md:
            self._replace_processing_block(result_md)
        else:
            combined = "\n\n".join(
                [segment for segment in [current_md.strip(), result_md] if segment]
            )
            self._set_current_markdown(combined)

        self._reset_processing_state()

        # Scroll to bottom
        scroll_bar = self.conversation_display.verticalScrollBar()
        if scroll_bar:
            QTimer.singleShot(0, lambda: scroll_bar.setValue(scroll_bar.maximum()))

        # Add to chat history
        self.add_message_to_chat("assistant", result)

    def display_error(self, error: str):
        """Display error message with actionable suggestions"""
        logger.error(f"Query error: {error}")

        suggestions = self.backend.get_error_suggestions(error)
        suggestion_md = ""
        if suggestions:
            items = "\n".join(f"- {s}" for s in suggestions)
            suggestion_md = f"\n\n**Suggestions:**\n{items}"

        error_md = f"**Error:**\n{error}{suggestion_md}"
        current_md = self._get_current_markdown()

        if self.processing_token_start in current_md:
            self._replace_processing_block(error_md)
        else:
            combined = "\n\n".join(
                [segment for segment in [current_md.strip(), error_md] if segment]
            )
            self._set_current_markdown(combined)

        self._reset_processing_state()

    def handle_clarification(self, clarification_question: str):
        """Handle clarification request from agent"""
        logger.info(f"Clarification requested: {clarification_question}")

        # Store the clarification state
        self.pending_clarification_query = (
            self.last_submitted_query
            or self.pending_clarification_query
            or self.query_input.toPlainText().strip()
        )
        self.clarification_question = clarification_question

        # Display clarification question
        current_md = self._get_current_markdown()
        clarification_md = f"**Clarification Needed:**\n{clarification_question}"
        if self.processing_token_start in current_md:
            self._replace_processing_block(clarification_md)
        else:
            current_md = "\n\n".join(
                [
                    segment
                    for segment in [current_md.strip(), clarification_md]
                    if segment
                ]
            )

        if self.processing_token_start not in current_md:
            self._set_current_markdown(current_md)

        self._reset_processing_state()

        # Update UI to show we're waiting for clarification
        self.query_input.clear()
        self.query_input.setPlaceholderText("Please provide the clarification...")
        self.submit_button.setText("Submit Clarification")
        self.is_running = False

        # Scroll to bottom
        scroll_bar = self.conversation_display.verticalScrollBar()
        if scroll_bar:
            QTimer.singleShot(0, lambda: scroll_bar.setValue(scroll_bar.maximum()))

        self.query_input.setFocus()

    def clear_fields(self):
        """Clear conversation"""
        logger.debug(f"Clearing chat fields for chat_id: {self.chat_id}")
        self.conversation_display.clear()
        self.current_markdown = None
        self.query_input.clear()
        self.backend.clear_session()
        logger.info("Chat fields cleared successfully")

    def _autosave_project(self):
        """Silently save the active project without showing dialogs."""
        if self.project_id is None:
            return
        if self.project_id in self.backend.projects:
            success, msg = self.backend.save_project_to_disk(self.project_id)
            if success:
                logger.info(f"Project auto-saved: {msg}")
            else:
                logger.warning(f"Project auto-save failed: {msg}")

    def save_project(self):
        """Save current project with all chats"""
        logger.info(f"Attempting to save project: {self.project_id}")
        if self.project_id is None:
            logger.warning("Save attempted but no project loaded")
            QMessageBox.warning(self, "No Project", "No project is currently loaded.")
            return

        if self.project_id in self.backend.projects:
            success, msg = self.backend.save_project_to_disk(self.project_id)
            if success:
                logger.info(f"Project saved successfully: {msg}")
                QMessageBox.information(self, "Project Saved", msg)
            else:
                logger.error(f"Failed to save project: {msg}")
                QMessageBox.warning(self, "Save Failed", msg)
        else:
            logger.error(f"Project ID {self.project_id} not found in backend storage")
            QMessageBox.warning(
                self, "Project Not Found", "Could not find project to save."
            )

    # ------------------------------------------------------------------
    #  Export helpers
    # ------------------------------------------------------------------

    def export_results_dialog(self):
        """Show a file dialog and export the current data preview to a file."""
        if self.backend.data_context is None:
            QMessageBox.information(
                self, "No Data", "Load a data source before exporting."
            )
            return

        file_path, selected_filter = QFileDialog.getSaveFileName(
            self,
            "Export Results",
            "",
            "CSV Files (*.csv);;Excel Files (*.xlsx);;JSON Files (*.json)",
        )
        if not file_path:
            return

        # Determine format from the chosen filter / extension
        ext = os.path.splitext(file_path)[1].lower()
        fmt_map = {".csv": "csv", ".xlsx": "excel", ".json": "json"}
        fmt = fmt_map.get(ext, "csv")

        # Gather data from all tables in the context
        table_info = self.backend.data_context.get("table_info", {})
        rows: list = []
        columns: list = []
        for _table, info in table_info.items():
            sample = info.get("sample_rows", [])
            cols = info.get("columns", [])
            if not columns:
                columns = cols
            rows.extend(sample)

        data = {"columns": columns, "rows": rows}
        success, msg = self.backend.export_results(data, fmt, file_path)
        if success:
            QMessageBox.information(self, "Export Complete", msg)
        else:
            QMessageBox.warning(self, "Export Failed", msg)

    def export_chat_dialog(self):
        """Export the currently active chat session."""
        if self.chat_id is None:
            QMessageBox.information(
                self, "No Chat", "No chat session is currently active."
            )
            return
        self._export_single_chat(self.chat_id)

    def _export_single_chat(self, chat_id: str):
        """Prompt for a file path and export a chat session."""
        file_path, selected_filter = QFileDialog.getSaveFileName(
            self,
            "Export Chat",
            "",
            "Markdown Files (*.md);;JSON Files (*.json);;Text Files (*.txt)",
        )
        if not file_path:
            return

        ext = os.path.splitext(file_path)[1].lower()
        fmt_map = {".md": "markdown", ".json": "json", ".txt": "txt"}
        fmt = fmt_map.get(ext, "markdown")

        success, msg, content = self.backend.export_chat_session(chat_id, fmt)
        if not success:
            QMessageBox.warning(self, "Export Failed", msg)
            return

        try:
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(content)
            QMessageBox.information(
                self, "Export Complete", f"Chat exported to {file_path}"
            )
        except Exception as e:
            QMessageBox.warning(self, "Export Failed", f"Could not write file: {e}")

    def refresh_chat_list(self):
        """Refresh the chat list in the sidebar for the active project."""
        logger.debug("Refreshing chat list for active project")
        self.chat_list.clear()

        if self.backend.active_project is None:
            logger.debug("No active project, chat list cleared")
            return

        # Populate chat list with all chats in the active project
        chats = self.backend.active_project.get_all_chats()
        logger.debug(f"Loading {len(chats)} chats into chat list")
        for chat in chats:
            item = QListWidgetItem(chat.title)
            item.setData(Qt.ItemDataRole.UserRole, chat.session_id)
            self.chat_list.addItem(item)
        logger.info(f"Chat list refreshed with {len(chats)} chats")

    def refresh_project_list(self):
        """Refresh the entire UI when a project is loaded."""
        logger.debug("Refreshing project UI")
        # Update project name label
        if self.backend.active_project:
            project_title = self.backend.active_project.title
            logger.info(f"Loading project into UI: {project_title}")
            self.project_name_label.setText(f"Project: {project_title}")
        else:
            logger.debug("No active project, clearing project name label")
            self.project_name_label.setText("No Project Loaded")

        # Refresh chat list
        self.refresh_chat_list()

    def add_message_to_chat(self, role: str, content: str):
        """Add a message to the active chat"""
        if self.chat_id and self.backend.active_chat:
            logger.debug(
                f"Adding {role} message to chat {self.chat_id} (length: {len(content)} chars)"
            )
            self.backend.add_message_to_session(role, content)

    # ========================
    # Menu Action Handlers
    # ========================

    def new_project(self):
        """Open dialog to create a new project"""
        logger.info("User initiated new project creation")
        reply = QMessageBox.question(
            self,
            "New Project",
            "Start a new project? Unsaved work in the current project will be lost.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
        )
        if reply == QMessageBox.StandardButton.Yes:
            logger.debug("User confirmed new project creation")
            # Open the create project dialog
            try:
                project_dialog = CreateProjectDialog(self)
                if project_dialog.exec() == QDialog.DialogCode.Accepted:
                    self.backend = project_dialog.backend
                    self.project_id = project_dialog.project_id
                    logger.info(f"New project created with ID: {self.project_id}")

                    if self.project_id is not None:
                        self.backend.load_project(self.project_id)
                        self.refresh_project_list()
                        self._start_fresh_chat_for_active_project()
                    logger.info("Project creation successful, UI refreshed")
                    QMessageBox.information(
                        self, "New Project", "Project created successfully."
                    )
                else:
                    logger.info("User cancelled new project creation")
            except Exception as e:
                logger.error(f"Error creating new project: {str(e)}", exc_info=True)
                QMessageBox.critical(
                    self, "Error", f"Failed to create new project: {str(e)}"
                )

    def load_project_menu(self):
        """Load an existing project from disk"""
        logger.info("User initiated project load from menu")
        try:
            project_dialog = CreateProjectDialog(self)
            # Simulate clicking the "Load Existing Project" button
            project_dialog.open_load_dialog()
            if project_dialog.result() == QDialog.DialogCode.Accepted:
                self.backend = project_dialog.backend
                self.project_id = project_dialog.project_id
                logger.info(f"Project loaded with ID: {self.project_id}")

                if self.project_id is not None:
                    self.backend.load_project(self.project_id)
                    self.refresh_project_list()
                    if self.backend.active_project:
                        logger.debug(
                            f"Loaded project: {self.backend.active_project.title}"
                        )
                    self._start_fresh_chat_for_active_project()
                logger.info("Project load successful, UI refreshed")
            else:
                logger.info("User cancelled project load")
        except Exception as e:
            logger.error(f"Error loading project: {str(e)}", exc_info=True)
            QMessageBox.critical(self, "Error", f"Failed to load project: {str(e)}")

    def change_selected_tables(self):
        """Re-pick tables from the currently connected database without re-entering credentials."""
        dc = self.backend.data_context or self.data_context
        if not dc or dc.get("source_type") != "database":
            QMessageBox.warning(
                self,
                "No Database Connected",
                "Change Selected Tables is only available when a database is connected.\n\n"
                "Please connect a database first via File > Connect Data Source.",
            )
            return

        db_type = dc.get("db_type")
        credentials = dc.get("credentials", {})
        if not db_type or not credentials:
            QMessageBox.warning(
                self,
                "No Connection Info",
                "Connection information is missing. Please reconnect via File > Connect Data Source.",
            )
            return

        connector = DatabaseConnector()
        success, message = connector.connect(db_type, credentials)
        if not success:
            QMessageBox.critical(
                self,
                "Connection Failed",
                f"Could not reconnect to the database: {message}\n\n"
                "Please reconnect via File > Connect Data Source.",
            )
            return

        try:
            all_tables = connector.get_tables()
            if not all_tables:
                QMessageBox.warning(
                    self, "No Tables", "The database contains no tables."
                )
                return

            current_tables = dc.get("tables") or []
            semantic_layer = None
            if self.backend.active_project:
                semantic_layer = self.backend.active_project.semantic_layer

            table_dialog = TableSelectionDialog(
                all_tables, self, preselected=current_tables
            )
            if table_dialog.exec() != QDialog.DialogCode.Accepted:
                return

            selected_tables = table_dialog.get_selected_tables()
            if not selected_tables:
                return

            source_config = {
                "db_type": db_type,
                "credentials": credentials,
                "table": selected_tables,
            }
            data_context, status = load_data("database", source_config)
            if data_context is None:
                QMessageBox.warning(
                    self, "Load Failed", f"Failed to load selected tables: {status}"
                )
                return

            self.backend.data_context = data_context
            self.data_context = data_context

            if self.backend.active_project:
                creds_to_store = credentials.copy()
                creds_to_store.pop("password", None)
                self.backend.active_project.data_source = {
                    "db_type": db_type,
                    "credentials": creds_to_store,
                    "table": selected_tables,
                }

            self._autosave_project()
            logger.info(f"Table selection changed to: {selected_tables}")
            self.create_new_chat()

        finally:
            connector.close()

    def connect_data_source(self):
        """Open dialog to connect to a data source"""
        logger.info("User initiated data source connection")

        retry_config = None  # Pre-filled config to restore on connection failure

        # Loop until data is successfully loaded or user cancels
        while True:
            if retry_config:
                # Retry after a failed connection: re-open the DB dialog pre-populated
                is_cxo = ConfigManager.get_interaction_mode() == "cxo"
                db_dialog = DatabaseConnectionDialog(
                    self,
                    force_nlp=is_cxo,
                    semantic_layer=retry_config.get("semantic_layer"),
                )
                logger.info("Applying retry DB config to dialog")
                db_dialog.populate_from_config(retry_config)
                retry_config = None
                if db_dialog.exec() != QDialog.DialogCode.Accepted:
                    logger.info("User cancelled connection retry")
                    return
                source_type = "database"
                source_config = db_dialog.get_config()
            else:
                source_dialog = DataSourceDialog(self)
                if source_dialog.exec() != QDialog.DialogCode.Accepted:
                    logger.info("User cancelled data source connection")
                    return
                source_type = source_dialog.data_source_type
                source_config = source_dialog.data_source_config

            if source_type and source_config:
                try:
                    if source_type == "database":
                        # Database connection flow
                        db_type = source_config.get("db_type")
                        credentials = source_config.get("credentials", {})
                        selection_method = source_config.get(
                            "table_selection_method",
                            ConfigManager.get_table_selection_method(),
                        )
                        semantic_layer = source_config.get("semantic_layer")
                        logger.debug(f"Attempting to connect to {db_type} database")
                        connector = DatabaseConnector()
                        success, message = connector.connect(db_type, credentials)

                        if success:
                            logger.info(f"Successfully connected to {db_type} database")
                            tables = connector.get_tables()

                            if not tables:
                                connector.close()
                                QMessageBox.warning(
                                    self,
                                    "No Tables Found",
                                    "The database does not contain any tables.",
                                )
                                continue

                            is_cxo = ConfigManager.get_interaction_mode() == "cxo"

                            if is_cxo and tables:
                                # CxO mode: skip table selection, defer to query time
                                connector.close()
                                logger.info(
                                    f"CxO mode: skipping table selection. {len(tables)} tables available."
                                )
                                cxo_context = {
                                    "source_type": "database",
                                    "cxo_mode": True,
                                    "db_type": db_type,
                                    "credentials": credentials,
                                    "all_tables": tables,
                                    "tables": [],
                                    "table_info": {},
                                    "semantic_layer": semantic_layer,
                                }
                                self.backend.data_context = cxo_context
                                self.data_context = cxo_context
                                if self.backend.active_project:
                                    creds_to_store = credentials.copy()
                                    if "password" in creds_to_store:
                                        creds_to_store["password"] = ""
                                    self.backend.active_project.data_source = {
                                        "db_type": db_type,
                                        "credentials": creds_to_store,
                                        "table_selection_method": "nlp",
                                        "cxo_mode": True,
                                    }
                                    # Store semantic layer on project, not in data_source
                                    if semantic_layer:
                                        self.backend.active_project.semantic_layer = (
                                            semantic_layer
                                        )
                                table_count = len(tables)
                                welcome_msg = (
                                    f"## Connected to {db_type} database\n\n"
                                    f"**{table_count}** tables available. "
                                    f"In CxO mode, relevant tables are automatically selected based on your questions.\n\n"
                                    f"Simply type your question below to get started."
                                )
                                self.conversation_display.setHtml(
                                    markdown_to_html(welcome_msg)
                                )
                                self._autosave_project()
                                QMessageBox.information(
                                    self,
                                    "Data Loaded",
                                    "Database connected in CxO mode.",
                                )
                                return

                            elif tables:
                                selected_tables = select_tables_with_method(
                                    self,
                                    connector,
                                    tables,
                                    selection_method,
                                    semantic_layer,
                                )
                                connector.close()

                                if selected_tables is None:
                                    logger.info(
                                        "User cancelled table selection, returning to data source selection"
                                    )
                                    continue

                                if selected_tables:
                                    # Use load_data from processing module
                                    data_source_config = {
                                        "db_type": db_type,
                                        "credentials": credentials,
                                        "table": (
                                            selected_tables[0]
                                            if len(selected_tables) == 1
                                            else selected_tables
                                        ),
                                    }
                                    data_context, status = load_data(
                                        "database", data_source_config
                                    )
                                    if data_context is not None:
                                        self.backend.data_context = data_context
                                        self.data_context = data_context
                                        logger.info(
                                            f"Successfully loaded data from tables: {selected_tables}"
                                        )
                                        welcome_msg = self.backend.format_database_welcome_message(
                                            db_type,
                                            selected_tables,
                                            data_context,
                                            status,
                                        )
                                        self.conversation_display.setHtml(
                                            markdown_to_html(welcome_msg)
                                        )
                                        # Store data source in project
                                        if self.backend.active_project:
                                            creds_to_store = credentials.copy()
                                            if "password" in creds_to_store:
                                                creds_to_store["password"] = ""
                                            self.backend.active_project.data_source = {
                                                "db_type": db_type,
                                                "credentials": creds_to_store,
                                                "table": selected_tables,
                                                "table_selection_method": selection_method,
                                            }
                                            # Store semantic layer on project, not in data_source
                                            if semantic_layer:
                                                self.backend.active_project.semantic_layer = (
                                                    semantic_layer
                                                )
                                        self._autosave_project()
                                        QMessageBox.information(
                                            self,
                                            "Data Loaded",
                                            "Database data loaded successfully.",
                                        )
                                        return
                                    else:
                                        logger.warning(
                                            f"Failed to load data from database: {status}"
                                        )
                                        QMessageBox.warning(self, "Load Failed", status)
                                        continue
                        else:
                            logger.warning(f"Database connection failed: {message}")
                            QMessageBox.warning(self, "Connection Failed", message)
                            retry_config = source_config
                            continue

                    elif source_type == "multi_database":
                        # Multi-database connection flow
                        configs = source_config.get("connections", [])
                        logger.info(
                            f"Loading multi-database with {len(configs)} connections"
                        )
                        from processing import load_multi_database

                        data_context, status = load_multi_database(configs)
                        if data_context is not None:
                            self.backend.data_context = data_context
                            self.data_context = data_context
                            logger.info(f"Multi-database loaded: {status}")
                            # Persist configs (passwords stripped)
                            ConfigManager.save_multi_db_config(configs)
                            if self.backend.active_project:
                                safe_configs = []
                                for cfg in configs:
                                    safe = dict(cfg)
                                    c = safe.get("credentials", {}).copy()
                                    c.pop("password", None)
                                    safe["credentials"] = c
                                    safe_configs.append(safe)
                                self.backend.active_project.data_source = {
                                    "source_type": "multi_database",
                                    "connections": safe_configs,
                                }
                            aliases = list(data_context.get("connections", {}).keys())
                            table_count = len(data_context.get("tables", []))
                            welcome_msg = (
                                f"## Multi-Database Connected\n\n"
                                f"**{table_count}** tables loaded across "
                                f"**{len(aliases)}** databases: {', '.join(aliases)}\n\n"
                                f"Tables are prefixed with their database alias "
                                f"(e.g. `alias__table`). Ask your question below."
                            )
                            self.conversation_display.setHtml(
                                markdown_to_html(welcome_msg)
                            )
                            self._autosave_project()
                            QMessageBox.information(
                                self,
                                "Data Loaded",
                                f"Multi-database loaded: {table_count} tables.",
                            )
                            return
                        else:
                            logger.warning(f"Multi-database load failed: {status}")
                            QMessageBox.warning(self, "Load Failed", status)
                            continue

                    elif source_type == "file":
                        # File load flow
                        file_paths = source_config.get("file_paths", [])
                        logger.debug(f"Loading {len(file_paths)} file(s): {file_paths}")
                        if file_paths:
                            data_context, welcome_msg = (
                                self.backend.load_file_data_with_ui(file_paths)
                            )
                            if data_context is not None:
                                self.data_context = data_context
                                logger.info(
                                    f"Successfully loaded {len(file_paths)} file(s)"
                                )
                                self.conversation_display.setHtml(
                                    markdown_to_html(welcome_msg)
                                )
                                QMessageBox.information(
                                    self,
                                    "Data Loaded",
                                    "Files loaded successfully.",
                                )
                                return
                            else:
                                logger.warning(f"Failed to load files: {welcome_msg}")
                                QMessageBox.warning(self, "Load Failed", welcome_msg)
                                continue
                except Exception as e:
                    logger.error(f"Error loading data source: {str(e)}", exc_info=True)
                    QMessageBox.critical(
                        self, "Error", f"Failed to load data: {str(e)}"
                    )
                    continue

    def connect_additional_data_source(self):
        """Connect a data source, prompting to overwrite or merge when data exists."""
        logger.info("User initiated additional data source connection")

        if self.data_context is None:
            logger.info(
                "No existing data context found; opening primary data source flow"
            )
            self.connect_data_source()
            return

        prompt = QMessageBox(self)
        prompt.setIcon(QMessageBox.Icon.Question)
        prompt.setWindowTitle("Data Source Already Loaded")
        prompt.setText("A data source is already loaded. What would you like to do?")
        prompt.setInformativeText(
            "Choose 'Overwrite' to replace current data, or 'Merge' to add new data to the current workspace."
        )
        overwrite_btn = prompt.addButton(
            "Overwrite Current Data Source", QMessageBox.ButtonRole.AcceptRole
        )
        merge_btn = prompt.addButton(
            "Merge New Data", QMessageBox.ButtonRole.ActionRole
        )
        cancel_btn = prompt.addButton(QMessageBox.StandardButton.Cancel)
        prompt.setDefaultButton(merge_btn)
        prompt.exec()

        clicked = prompt.clickedButton()
        if clicked == cancel_btn:
            logger.info("User cancelled data source merge/overwrite prompt")
            return

        if clicked == overwrite_btn:
            logger.info("User chose to overwrite current data source")
            self.connect_data_source()
            return

        logger.info("User chose to merge new data with current data source")

        source_dialog = DataSourceDialog(self)
        if source_dialog.exec() != QDialog.DialogCode.Accepted:
            logger.info("User cancelled additional data source connection")
            return

        source_type = source_dialog.data_source_type
        source_config = source_dialog.data_source_config
        logger.info(f"Additional data source type selected: {source_type}")

        try:
            if source_type == "database":
                db_type = source_config.get("db_type")
                credentials = source_config.get("credentials", {})
                selection_method = source_config.get(
                    "table_selection_method",
                    ConfigManager.get_table_selection_method(),
                )
                semantic_layer = source_config.get("semantic_layer")
                connector = DatabaseConnector()
                success, message = connector.connect(db_type, credentials)

                if not success:
                    QMessageBox.warning(self, "Connection Failed", message)
                    return

                tables = connector.get_tables()
                if not tables:
                    connector.close()
                    QMessageBox.warning(
                        self,
                        "No Tables Found",
                        "The database does not contain any tables.",
                    )
                    return

                selected_tables = select_tables_with_method(
                    self,
                    connector,
                    tables,
                    selection_method,
                    semantic_layer,
                )
                connector.close()

                if not selected_tables:
                    return

                data_source_config = {
                    "db_type": db_type,
                    "credentials": credentials,
                    "table": (
                        selected_tables[0]
                        if len(selected_tables) == 1
                        else selected_tables
                    ),
                }
                new_context, status = load_data("database", data_source_config)
                if new_context is None:
                    QMessageBox.warning(self, "Load Failed", status)
                    return

                current_source_type = self.data_context.get("source_type")

                if current_source_type == "multi_database":
                    alias = self._build_db_alias(db_type, credentials)
                    self._append_to_multi_database_context(alias, new_context)
                    self._persist_multi_db_project_source()

                    aliases = list(self.data_context.get("connections", {}).keys())
                    table_count = len(self.data_context.get("tables", []))
                    welcome_msg = (
                        f"## Multi-Database Connected\n\n"
                        f"**{table_count}** tables loaded across "
                        f"**{len(aliases)}** databases: {', '.join(aliases)}\n\n"
                        f"Tables are prefixed with their database alias "
                        f"(e.g. `alias__table`). Ask your question below."
                    )
                    self.conversation_display.setHtml(markdown_to_html(welcome_msg))
                    QMessageBox.information(
                        self,
                        "Data Loaded",
                        f"Database added as alias '{alias}'.",
                    )
                    return

                if current_source_type != "database":
                    QMessageBox.warning(
                        self,
                        "Not Supported",
                        "Additional database sources can only be added to a database workspace.",
                    )
                    return

                if (
                    self.data_context.get("db_type") != db_type
                    or self.data_context.get("credentials") != credentials
                ):
                    existing_db_type = self.data_context.get("db_type")
                    existing_credentials = self.data_context.get("credentials", {})
                    existing_alias = self._build_db_alias(
                        existing_db_type, existing_credentials
                    )
                    new_alias = self._build_db_alias(
                        db_type, credentials, reserved={existing_alias}
                    )

                    merged_context: Dict[str, Any] = {
                        "source_type": "multi_database",
                        "connections": {},
                        "tables": [],
                        "table_info": {},
                        "table_to_connection": {},
                    }

                    existing_sub_context = {
                        "source_type": "database",
                        "db_type": existing_db_type,
                        "credentials": existing_credentials,
                        "tables": self.data_context.get("tables", []),
                        "table_info": self.data_context.get("table_info", {}),
                        "skipped_columns": self.data_context.get("skipped_columns", {}),
                    }

                    for alias, sub_ctx in (
                        (existing_alias, existing_sub_context),
                        (new_alias, new_context),
                    ):
                        merged_context["connections"][alias] = sub_ctx
                        for table_name in sub_ctx.get("tables", []):
                            qualified = f"{alias}__{table_name}"
                            merged_context["tables"].append(qualified)
                            merged_context["table_info"][qualified] = sub_ctx.get(
                                "table_info", {}
                            ).get(table_name, {})
                            merged_context["table_to_connection"][qualified] = alias

                    self.data_context = merged_context
                    self.backend.data_context = merged_context
                    self._persist_multi_db_project_source()

                    aliases = [existing_alias, new_alias]
                    table_count = len(merged_context.get("tables", []))
                    welcome_msg = (
                        f"## Multi-Database Connected\n\n"
                        f"**{table_count}** tables loaded across "
                        f"**{len(aliases)}** databases: {', '.join(aliases)}\n\n"
                        f"Tables are prefixed with their database alias "
                        f"(e.g. `alias__table`). Ask your question below."
                    )
                    self.conversation_display.setHtml(markdown_to_html(welcome_msg))
                    QMessageBox.information(
                        self,
                        "Data Loaded",
                        "Different database connection added. Workspace switched to multi-database mode.",
                    )
                    return

                existing_tables = set(self.data_context.get("tables", []))
                table_info = self.data_context.get("table_info", {})
                skipped_cols = self.data_context.get("skipped_columns", {})

                for table_name in new_context.get("tables", []):
                    if table_name not in existing_tables:
                        existing_tables.add(table_name)
                        table_info[table_name] = new_context.get("table_info", {}).get(
                            table_name, {}
                        )
                        skipped = new_context.get("skipped_columns", {}).get(table_name)
                        if skipped:
                            skipped_cols[table_name] = skipped

                self.data_context["tables"] = list(existing_tables)
                self.data_context["table_info"] = table_info
                self.data_context["skipped_columns"] = skipped_cols
                self.backend.data_context = self.data_context

                if self.backend.active_project is not None:
                    creds_to_store = credentials.copy()
                    if "password" in creds_to_store:
                        creds_to_store["password"] = ""
                    self.backend.active_project.data_source = {
                        "db_type": db_type,
                        "credentials": creds_to_store,
                        "table": list(existing_tables),
                        "table_selection_method": selection_method,
                    }
                    # Store semantic layer on project, not in data_source
                    if semantic_layer:
                        self.backend.active_project.semantic_layer = semantic_layer

                welcome_msg = self.backend.format_database_welcome_message(
                    db_type, list(existing_tables), self.data_context, status
                )
                self.conversation_display.setHtml(markdown_to_html(welcome_msg))
                QMessageBox.information(
                    self,
                    "Data Loaded",
                    "Additional tables added to the database workspace.",
                )

            elif source_type == "file":
                file_paths = source_config.get("file_paths", [])
                if not file_paths:
                    QMessageBox.warning(self, "Load Failed", "No files selected.")
                    return

                if self.data_context.get("source_type") != "file":
                    QMessageBox.warning(
                        self,
                        "Not Supported",
                        "Additional files can only be added to an existing file workspace.",
                    )
                    return

                updated_context, status = add_files_to_sqlite(
                    self.data_context, file_paths
                )
                if updated_context is None:
                    QMessageBox.warning(self, "Load Failed", status)
                    return

                self.data_context = updated_context
                self.backend.data_context = updated_context
                welcome_msg = self.backend.format_file_welcome_message(
                    file_paths, updated_context, status
                )
                self.conversation_display.setHtml(markdown_to_html(welcome_msg))
                QMessageBox.information(
                    self,
                    "Data Loaded",
                    "Additional files added to the SQLite workspace.",
                )

            else:
                QMessageBox.warning(self, "Error", "Unknown source type.")
                return

        except Exception as e:
            logger.error(
                f"Error loading additional data source: {str(e)}", exc_info=True
            )
            QMessageBox.critical(
                self, "Error", f"Failed to load additional data: {str(e)}"
            )

    def _build_db_alias(
        self,
        db_type: Optional[str],
        credentials: Dict[str, Any],
        reserved: Optional[set] = None,
    ) -> str:
        """Build a stable, unique alias for a database connection."""
        import re

        reserved_aliases = set(reserved or set())
        if (
            self.data_context
            and self.data_context.get("source_type") == "multi_database"
        ):
            reserved_aliases.update(self.data_context.get("connections", {}).keys())

        base_candidate = (
            credentials.get("database") or credentials.get("host") or db_type or "db"
        )
        base_name = os.path.basename(str(base_candidate))
        safe_base = re.sub(r"[^A-Za-z0-9_]", "_", base_name).strip("_").lower() or "db"

        alias = safe_base
        suffix = 2
        while alias in reserved_aliases:
            alias = f"{safe_base}_{suffix}"
            suffix += 1
        return alias

    def _append_to_multi_database_context(
        self, alias: str, sub_context: Dict[str, Any]
    ) -> None:
        """Append a single database sub-context to the active multi-db context."""
        if (
            self.data_context is None
            or self.data_context.get("source_type") != "multi_database"
        ):
            return

        self.data_context.setdefault("connections", {})[alias] = sub_context
        self.data_context.setdefault("tables", [])
        self.data_context.setdefault("table_info", {})
        self.data_context.setdefault("table_to_connection", {})

        for table_name in sub_context.get("tables", []):
            qualified = f"{alias}__{table_name}"
            if qualified not in self.data_context["tables"]:
                self.data_context["tables"].append(qualified)
            self.data_context["table_info"][qualified] = sub_context.get(
                "table_info", {}
            ).get(table_name, {})
            self.data_context["table_to_connection"][qualified] = alias

        self.backend.data_context = self.data_context

    def _persist_multi_db_project_source(self) -> None:
        """Persist current multi-db source details to active project (passwords stripped)."""
        if self.backend.active_project is None:
            return
        if (
            self.data_context is None
            or self.data_context.get("source_type") != "multi_database"
        ):
            return

        safe_configs = []
        for alias, sub_ctx in self.data_context.get("connections", {}).items():
            creds = dict(sub_ctx.get("credentials", {}))
            creds.pop("password", None)
            safe_configs.append(
                {
                    "alias": alias,
                    "db_type": sub_ctx.get("db_type"),
                    "credentials": creds,
                }
            )

        self.backend.active_project.data_source = {
            "source_type": "multi_database",
            "connections": safe_configs,
        }

    def change_api_settings(self):
        """Open API key settings dialog"""
        logger.info("User opened API key settings dialog")
        try:
            api_dialog = APIKeyDialog(self)
            if api_dialog.exec() == QDialog.DialogCode.Accepted:
                logger.info("API key settings updated successfully")
                QMessageBox.information(
                    self, "API Settings", "API key settings updated successfully."
                )
            else:
                logger.info("User cancelled API key settings change")
        except Exception as e:
            logger.error(f"Error changing API key settings: {str(e)}", exc_info=True)
            QMessageBox.critical(
                self, "Error", f"Failed to update API key settings: {str(e)}"
            )

    def change_ai_host_settings(self):
        """Open AI host settings dialog"""
        logger.info("User opened AI host settings dialog")
        try:
            host_dialog = AIHostConfigDialog(self, include_cloud=True)
            if host_dialog.exec() == QDialog.DialogCode.Accepted:
                logger.info("AI host settings updated successfully")
                QMessageBox.information(
                    self, "AI Host Settings", "AI host settings updated successfully."
                )
            else:
                logger.info("User cancelled AI host settings change")
        except Exception as e:
            logger.error(f"Error changing AI host settings: {str(e)}", exc_info=True)
            QMessageBox.critical(
                self, "Error", f"Failed to update AI host settings: {str(e)}"
            )

    def change_model_settings(self):
        """Open model settings dialog for online providers only"""
        logger.info("User opened model settings dialog")

        current_provider = ConfigManager.get_default_api()

        # Local model switching is handled in Local LLM Settings
        if current_provider == "local":
            logger.info("User attempted to open model settings for local provider")
            QMessageBox.information(
                self,
                "Local Model Settings",
                "Local model configuration is handled in Settings → Local LLM Settings.\n\n"
                "To change your local model, go to that dialog instead.",
            )
            return

        try:
            model_dialog = ModelSettingsDialog(self)
            if model_dialog.exec() == QDialog.DialogCode.Accepted:
                logger.info("Model settings updated successfully")
            else:
                logger.info("User cancelled model settings change")
        except Exception as e:
            logger.error(f"Error changing model settings: {str(e)}", exc_info=True)
            QMessageBox.critical(
                self, "Error", f"Failed to update model settings: {str(e)}"
            )

    def clear_conversation(self):
        """Clear current conversation"""
        logger.info(f"User requested to clear conversation (chat_id: {self.chat_id})")
        reply = QMessageBox.question(
            self,
            "Clear Conversation",
            "Clear all messages in the current chat?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
        )
        if reply == QMessageBox.StandardButton.Yes:
            try:
                self.conversation_display.clear()
                self.current_markdown = None
                self.backend.clear_session()
                logger.info(
                    f"Conversation cleared successfully (chat_id: {self.chat_id})"
                )
            except Exception as e:
                logger.error(f"Error clearing conversation: {str(e)}", exc_info=True)
                QMessageBox.critical(
                    self, "Error", f"Failed to clear conversation: {str(e)}"
                )
        else:
            logger.info("User cancelled conversation clear")

    def reset_workspace(self):
        """Reset the entire workspace"""
        logger.warning("User requested workspace reset")
        reply = QMessageBox.question(
            self,
            "Reset Workspace",
            "Reset the entire workspace? This will clear all chats, projects, and data.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
        )
        if reply == QMessageBox.StandardButton.Yes:
            try:
                logger.info("Resetting workspace...")
                old_project_id = self.project_id
                self.backend = DataWorkspaceBackend()
                self.project_id = None
                self.chat_id = None
                self.data_context = None
                self.conversation_display.clear()
                self.query_input.clear()
                self.chat_list.clear()
                self.project_name_label.setText("No Project Loaded")
                logger.info(
                    f"Workspace reset complete. Previous project: {old_project_id}"
                )
                QMessageBox.information(
                    self, "Workspace Reset", "Workspace has been reset."
                )
            except Exception as e:
                logger.error(f"Error resetting workspace: {str(e)}", exc_info=True)
                QMessageBox.critical(
                    self, "Error", f"Failed to reset workspace: {str(e)}"
                )
        else:
            logger.info("User cancelled workspace reset")

    def clear_query_cache(self):
        """Delete the persisted query-memory index file."""
        cache_file = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "data",
            "query_memory_index.jsonl",
        )

        reply = QMessageBox.warning(
            self,
            "Clear Query Cache",
            "This will permanently delete the query cache index.\n\n"
            "This action cannot be undone and will likely slow down analysis "
            "until cache entries are rebuilt.\n\n"
            "Do you want to continue?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,
        )

        if reply != QMessageBox.StandardButton.Yes:
            logger.info("User cancelled query cache clear")
            return

        try:
            if os.path.isfile(cache_file):
                os.remove(cache_file)
                logger.info(f"Query cache index deleted: {cache_file}")
                QMessageBox.information(
                    self,
                    "Query Cache Cleared",
                    "Query cache index cleared successfully.",
                )
            else:
                logger.info(f"Query cache index not found: {cache_file}")
                QMessageBox.information(
                    self,
                    "Nothing To Clear",
                    "Query cache index file was not found.",
                )
        except Exception as e:
            logger.error(f"Failed to clear query cache index: {e}", exc_info=True)
            QMessageBox.critical(
                self, "Error", f"Failed to clear query cache index: {e}"
            )

    def toggle_prompt_expansion(self, checked: bool):
        """Toggle the prompt-expansion middleman agent on or off."""
        success, message = ConfigManager.set_prompt_expansion_enabled(checked)
        if success:
            state = "enabled" if checked else "disabled"
            logger.info(f"Prompt expansion {state}")
        else:
            logger.error(f"Failed to toggle prompt expansion: {message}")
            # Revert the checkbox
            self.prompt_expansion_action.setChecked(not checked)
            QMessageBox.warning(
                self, "Settings Error", f"Failed to save setting: {message}"
            )

    def toggle_show_sql_in_responses(self, checked: bool):
        """Toggle whether analyst and CxO responses include generated SQL blocks."""
        success, message = ConfigManager.set_show_sql_in_responses(checked)
        if success:
            state = "enabled" if checked else "disabled"
            logger.info(f"Show SQL in responses {state}")
        else:
            logger.error(f"Failed to toggle SQL visibility: {message}")
            self.show_sql_response_action.setChecked(not checked)
            QMessageBox.warning(
                self, "Settings Error", f"Failed to save setting: {message}"
            )

    def open_local_llm_settings(self):
        """Open a dialog to configure the local LLM server connection or host a model."""
        logger.info("User opened Local LLM settings dialog")
        try:
            dialog = LocalLLMSettingsDialog(self)
            if dialog.exec() == QDialog.DialogCode.Accepted:
                logger.info("Local LLM settings updated successfully")
        except Exception as e:
            logger.error(f"Error opening Local LLM settings: {e}", exc_info=True)
            QMessageBox.critical(
                self, "Error", f"Failed to open Local LLM settings: {e}"
            )

    def set_interaction_mode(self, mode: str):
        """Set the interaction mode (cxo or analyst) and persist it."""
        # If switching to analyst mode while in CxO mode, attempt to go straight
        # to table selection using the in-memory credentials (no re-login needed).
        if (
            mode == "analyst"
            and self.data_context
            and self.data_context.get("cxo_mode")
        ):
            dc = self.backend.data_context or self.data_context
            db_type = dc.get("db_type") if dc else None
            credentials = dc.get("credentials", {}) if dc else {}
            all_tables = dc.get("all_tables", []) if dc else []

            if db_type and credentials and all_tables:
                # Silently reconnect using in-memory credentials and show table picker
                connector = DatabaseConnector()
                success, message = connector.connect(db_type, credentials)
                if success:
                    try:
                        semantic_layer = None
                        if self.backend.active_project:
                            semantic_layer = self.backend.active_project.semantic_layer

                        table_dialog = TableSelectionDialog(all_tables, self)
                        if table_dialog.exec() != QDialog.DialogCode.Accepted:
                            self.cxo_mode_action.setChecked(True)
                            return

                        selected_tables = table_dialog.get_selected_tables()
                        if not selected_tables:
                            self.cxo_mode_action.setChecked(True)
                            return

                        source_config = {
                            "db_type": db_type,
                            "credentials": credentials,
                            "table": selected_tables,
                        }
                        data_context, status = load_data("database", source_config)
                        if data_context is None:
                            QMessageBox.warning(
                                self,
                                "Load Failed",
                                f"Failed to load selected tables: {status}",
                            )
                            self.cxo_mode_action.setChecked(True)
                            return

                        ConfigManager.set_interaction_mode("analyst")
                        self.analyst_mode_action.setChecked(True)
                        self.backend.data_context = data_context
                        self.data_context = data_context

                        if self.backend.active_project:
                            creds_to_store = credentials.copy()
                            creds_to_store.pop("password", None)
                            self.backend.active_project.data_source = {
                                "db_type": db_type,
                                "credentials": creds_to_store,
                                "table": selected_tables,
                            }

                        logger.info(
                            f"Switched to Analyst mode with tables: {selected_tables}"
                        )
                        self.create_new_chat()
                        return
                    finally:
                        connector.close()
                else:
                    connector.close()
                    # Credentials may have expired; fall back to full reconnect dialog
                    logger.warning(
                        f"Silent reconnect failed ({message}), prompting for reconnect"
                    )
                    reply = QMessageBox.question(
                        self,
                        "Reconnection Required",
                        f"Could not reconnect automatically: {message}\n\n"
                        "Would you like to reconnect and select tables manually?",
                        QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                    )
                    if reply != QMessageBox.StandardButton.Yes:
                        self.cxo_mode_action.setChecked(True)
                        return
                    ConfigManager.set_interaction_mode("analyst")
                    self.analyst_mode_action.setChecked(True)
                    self.connect_data_source()
                    return
            else:
                # No DB context available - fall back to full reconnect
                reply = QMessageBox.question(
                    self,
                    "Analyst Mode Requires Tables",
                    "Analyst mode requires loaded tables, but no tables are currently selected.\n\n"
                    "Would you like to switch to Analyst mode and reconnect to select tables?",
                    QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                )
                if reply != QMessageBox.StandardButton.Yes:
                    self.cxo_mode_action.setChecked(True)
                    return
                ConfigManager.set_interaction_mode("analyst")
                self.analyst_mode_action.setChecked(True)
                logger.info(
                    "Switched to Analyst mode, re-opening data source connection for table selection"
                )
                self.connect_data_source()
                return

        success, message = ConfigManager.set_interaction_mode(mode)
        if success:
            logger.info(f"Interaction mode changed to: {mode}")
            mode_label = "CxO" if mode == "cxo" else "Analyst"
            QMessageBox.information(
                self,
                "Mode Changed",
                f"Interaction mode set to {mode_label}.\n\n"
                + (
                    "Responses will now focus on concise executive insights."
                    if mode == "cxo"
                    else "Responses will include full technical detail and SQL."
                ),
            )
        else:
            logger.error(f"Failed to set interaction mode: {message}")
            QMessageBox.warning(self, "Error", f"Failed to change mode: {message}")

    def set_theme(self, theme: str):
        """Set application theme"""
        logger.info(f"User requested theme change to: {theme}")
        if theme in ["dark", "light", "system"]:
            try:
                old_theme = self.current_theme
                self.current_theme = theme
                self._apply_theme(theme)
                # Save theme preference
                config = ConfigManager.load_config()
                config["theme"] = theme
                ConfigManager.save_config(config)
                self._refresh_conversation_after_theme_change()
                logger.info(
                    f"Theme changed from '{old_theme}' to '{theme}' and saved to config"
                )

                theme_names = {"dark": "Dark", "light": "Light", "system": "System"}
                QMessageBox.information(
                    self,
                    "Theme Changed",
                    f"{theme_names.get(theme, theme)} theme applied.",
                )
            except Exception as e:
                logger.error(
                    f"Error setting theme to '{theme}': {str(e)}", exc_info=True
                )
                QMessageBox.critical(self, "Error", f"Failed to apply theme: {str(e)}")
        else:
            logger.warning(f"Invalid theme requested: {theme}")

    def _refresh_conversation_after_theme_change(self) -> None:
        """Re-render current conversation so markdown styling follows the active theme."""
        if not self.conversation_display:
            return

        current_md = self.current_markdown
        if current_md is None and self.chat_id:
            try:
                history = self.backend.get_chat_history(self.chat_id)
                if history:
                    current_md = self._format_chat_history(history)
            except Exception as e:
                logger.debug(
                    f"Theme refresh fallback to chat history failed: {e}",
                    exc_info=False,
                )

        if current_md is None:
            current_md = self.conversation_display.toMarkdown()

        self.current_markdown = current_md
        self.conversation_display.setHtml(markdown_to_html(current_md))

    def _apply_theme(self, theme: str) -> None:
        """Apply the selected theme to the application"""
        stylesheet = self._build_theme_stylesheet(theme)
        if theme == "dark":
            QApplication.instance().setStyleSheet(stylesheet)
            self.dark_theme_action.setChecked(True)
        elif theme == "light":
            QApplication.instance().setStyleSheet(stylesheet)
            self.light_theme_action.setChecked(True)
        elif theme == "system":
            # Reset to system theme (empty stylesheet)
            QApplication.instance().setStyleSheet(stylesheet)
            self.system_theme_action.setChecked(True)

    def _build_theme_stylesheet(self, theme: str) -> str:
        if theme == "dark":
            base = DARK_THEME_STYLESHEET
        elif theme == "light":
            base = LIGHT_THEME_STYLESHEET
        else:
            base = ""

        if not self.font_point_size:
            return base

        font_override = f"\nQWidget {{ font-size: {self.font_point_size}pt; }}\n"
        return f"{base}{font_override}"

    def adjust_font(self, delta: int):
        """Adjust font size"""
        try:
            font = self.font()
            current_size = font.pointSize()
            new_size = max(6, current_size + delta)
            logger.info(
                f"Adjusting font size from {current_size}pt to {new_size}pt (delta: {delta})"
            )
            font.setPointSize(new_size)
            self.setFont(font)
            QApplication.instance().setFont(font)
            self.font_point_size = new_size
            self._apply_theme(self.current_theme)
            logger.debug("Font size adjusted successfully")
        except Exception as e:
            logger.error(f"Failed to adjust font size: {str(e)}", exc_info=True)

    def open_docs(self):
        """Open documentation in default browser"""
        docs_url = "https://github.com/ywxn/data-workspace/blob/main/README.md"
        logger.info(f"User requested to open documentation: {docs_url}")
        try:
            webbrowser.open(docs_url)
            logger.info("Documentation opened in browser successfully")
        except Exception as e:
            logger.error(
                f"Failed to open documentation in browser: {str(e)}", exc_info=True
            )
            QMessageBox.warning(
                self,
                "Documentation",
                f"Could not open browser. Please visit: {docs_url}\n\nError: {str(e)}",
            )

    def show_about(self):
        """Show about dialog"""
        logger.info("User opened About dialog")
        about_text = (
            "<h2>Data Workspace</h2>"
            "<p>Version 0.3.146</p>"
            "<p>A powerful AI-driven workspace for analyzing data across multiple sources with natural language queries.</p>"
            "<p><b>Key Features:</b></p>"
            "<ul>"
            "<li>Connect to databases, files, or multiple data sources simultaneously</li>"
            "<li>Query data using natural language, no SQL expertise required</li>"
            "<li>AI-powered insights tailored for executives (CxO) or deep technical analysis (Analyst)</li>"
            "<li>Organize analysis into projects with persistent chat histories</li>"
            "<li>Import semantic layers for business-aligned table descriptions</li>"
            "<li>Support for local LLM servers for fully offline operation</li>"
            "</ul>"
            "<p><b>Supported Databases:</b> SQLite, MySQL, MariaDB, PostgreSQL, SQL Server, Oracle, ODBC</p>"
            "<p><b>Supported File Formats:</b> CSV, Excel (XLSX/XLS)</p>"
            "<p>© 2026 Write Frame Communications. All rights reserved.</p>"
        )
        QMessageBox.about(self, "About Data Workspace", about_text)


def _preload_theme(app: QApplication) -> None:
    """Preload the saved theme before any dialogs are shown."""
    try:
        config = ConfigManager.load_config()
        saved_theme = config.get("theme", "system")
        if saved_theme == "dark":
            app.setStyleSheet(DARK_THEME_STYLESHEET)
        elif saved_theme == "light":
            app.setStyleSheet(LIGHT_THEME_STYLESHEET)
        else:
            app.setStyleSheet("")
        logger.info(f"Preloaded theme: {saved_theme}")
    except Exception as e:
        logger.warning(f"Failed to preload theme: {str(e)}")


def _ensure_api_configured() -> bool:
    """Ensure API keys or local LLM are configured. Shows dialog if needed. Returns True if configured."""
    # Local LLM doesn't need an API key
    if ConfigManager.get_default_api() == "local":
        logger.info(
            "Local LLM is configured as default provider, skipping API key check"
        )
        return True

    if not ConfigManager.has_any_api_key():
        logger.warning("No API keys configured, prompting user for setup")

        # Ask user whether they want cloud API keys or a local AI host
        choice = QMessageBox.question(
            None,
            "AI Provider Setup",
            "No AI provider is configured yet.\n\n"
            "Would you like to set up a Cloud API Key (OpenAI / Claude)?\n\n"
            "Click 'Yes' for a cloud API key, or 'No' to configure a local AI host instead.",
            QMessageBox.StandardButton.Yes
            | QMessageBox.StandardButton.No
            | QMessageBox.StandardButton.Cancel,
            QMessageBox.StandardButton.Yes,
        )

        if choice == QMessageBox.StandardButton.Cancel:
            logger.error("AI provider setup cancelled by user")
            return False

        if choice == QMessageBox.StandardButton.No:
            # Show AI Host config (Local LLM / Self-Host)
            host_dialog = AIHostConfigDialog()
            if host_dialog.exec() != QDialog.DialogCode.Accepted:
                logger.error(
                    "AI host setup cancelled by user, application cannot start"
                )
                QMessageBox.warning(
                    None,
                    "AI Provider Required",
                    "An AI provider (cloud API key or local LLM) is required.\n"
                    "Please configure one and try again.",
                )
                return False
            logger.info("AI host configured successfully")
        else:
            # Show API Key dialog (OpenAI / Claude)
            api_config_dialog = APIKeyDialog(first_time_setup=True)
            if api_config_dialog.exec() != QDialog.DialogCode.Accepted:
                logger.error(
                    "API key setup cancelled by user, application cannot start"
                )
                QMessageBox.warning(
                    None,
                    "API Key Required",
                    "An API key (or local LLM) is required to use this application.\n"
                    "Please configure your API key and try again.",
                )
                return False
            logger.info("API key configured successfully")
    return True


def _ensure_interaction_mode_configured() -> bool:
    """
    Show mode selection dialog if no interaction mode has been configured yet.
    Returns True if a mode is set, False if the user cancelled.
    """
    config = ConfigManager.load_config()
    if "interaction_mode" not in config:
        logger.info("No interaction mode configured, prompting user")
        mode_dialog = InteractionModeDialog()
        if mode_dialog.exec() != QDialog.DialogCode.Accepted:
            logger.warning(
                "User cancelled interaction mode selection, defaulting to analyst"
            )
            ConfigManager.set_interaction_mode("analyst")
            return True
        selected = mode_dialog.get_selected_mode()
        ConfigManager.set_interaction_mode(selected)
        logger.info(f"Interaction mode set to: {selected}")
    return True


def _show_project_dialog() -> Optional[tuple[DataWorkspaceGUI, str]]:
    """Show project creation/load dialog. Returns (window, project_id) or None if cancelled."""
    project_dialog = CreateProjectDialog()
    if project_dialog.exec() != QDialog.DialogCode.Accepted:
        logger.info("Project dialog cancelled, exiting application")
        return None

    logger.debug("Creating main application window")
    window = DataWorkspaceGUI()
    window.backend = project_dialog.backend
    window.project_id = project_dialog.project_id
    logger.info(f"Main window created with project ID: {window.project_id}")
    return (window, window.project_id)


def _load_project_and_chats(window: DataWorkspaceGUI) -> None:
    """Load the project and display chat history."""
    if window.project_id is not None:
        window.backend.load_project(window.project_id)
        window.refresh_project_list()
        if window.backend.active_project:
            logger.debug(f"Active project: {window.backend.active_project.title}")
            chats = window.backend.active_project.get_all_chats()
            logger.info(f"Loaded {len(chats)} existing chat(s) from project")
            window._start_fresh_chat_for_active_project()
    else:
        window.refresh_project_list()


def _display_loaded_project_data(window: DataWorkspaceGUI) -> None:
    """Display welcome message for data that was already loaded from project."""
    if window.backend.data_context is None:
        return

    data_context = window.backend.data_context
    logger.debug("Loaded SQL context from project")

    if window.backend.active_project and window.backend.active_project.data_source:
        ds = window.backend.active_project.data_source
        if (
            ds.get("source_type") == "multi_database"
            or data_context.get("source_type") == "multi_database"
        ):
            aliases = list(data_context.get("connections", {}).keys())
            table_count = len(data_context.get("tables", []))
            welcome_msg = (
                f"## Multi-Database Connected\n\n"
                f"**{table_count}** tables loaded across "
                f"**{len(aliases)}** databases: {', '.join(aliases)}\n\n"
                f"Tables are prefixed with their database alias "
                f"(e.g. `alias__table`). Ask your question below."
            )
            window.conversation_display.setHtml(markdown_to_html(welcome_msg))
        elif ds.get("db_type"):
            # CxO mode: show the CxO-specific welcome message
            if data_context.get("cxo_mode"):
                all_tables = data_context.get("all_tables", [])
                table_count = len(all_tables)
                db_type = ds.get("db_type", "database")
                welcome_msg = (
                    f"## Connected to {db_type} database\n\n"
                    f"**{table_count}** tables available. "
                    f"In CxO mode, relevant tables are automatically selected based on your questions.\n\n"
                    f"Simply type your question below to get started."
                )
                window.conversation_display.setHtml(markdown_to_html(welcome_msg))
            else:
                selected_tables = ds.get("table", [])
                if not isinstance(selected_tables, list):
                    selected_tables = [selected_tables]
                welcome_msg = window.backend.format_database_welcome_message(
                    ds.get("db_type"),
                    selected_tables,
                    data_context,
                    "Data loaded from project",
                )
                window.conversation_display.setHtml(markdown_to_html(welcome_msg))
        elif ds.get("file_paths"):
            welcome_msg = window.backend.format_file_welcome_message(
                ds.get("file_paths", []),
                data_context,
                "Data loaded from project",
            )
            window.conversation_display.setHtml(markdown_to_html(welcome_msg))

    window.data_context = data_context


def _load_initial_data(window: DataWorkspaceGUI) -> bool:
    """
    Load data source on startup. Loops until data is loaded or user cancels.
    Returns True if data was successfully loaded, False if user cancelled.
    """
    logger.info("No data loaded, prompting for data source")

    while True:
        source_dialog = DataSourceDialog()
        if source_dialog.exec() != QDialog.DialogCode.Accepted:
            logger.info("User cancelled data source selection")
            return False

        source_type = source_dialog.data_source_type
        source_config = source_dialog.data_source_config
        logger.info(f"Data source selected: {source_type}")

        try:
            if source_type == "database":
                if _load_database_data(window, source_config):
                    return True
            elif source_type == "multi_database":
                if _load_multi_database_data(window, source_config):
                    return True
            elif source_type == "file":
                if _load_file_data(window, source_config):
                    return True
            else:
                logger.error(f"Unknown data source type: {source_type}")
                QMessageBox.critical(
                    window, "Error", f"Unknown source type: {source_type}"
                )
                continue
        except Exception as e:
            logger.error(f"Error loading data: {str(e)}", exc_info=True)
            error_msg = (
                f"### Error Loading Data\n{str(e)}\nPlease restart and try again."
            )
            window.conversation_display.setHtml(markdown_to_html(error_msg))
            continue


def _load_database_data(
    window: DataWorkspaceGUI, source_config: Dict[str, Any]
) -> bool:
    """
    Load data from a database. Returns True if successful, False if user wants to retry.

    In CxO mode, skips table selection entirely. Tables are selected at query time
    via NLP based on the user's prompt.
    """
    db_type: str = source_config["db_type"]
    credentials: Dict[str, Any] = source_config["credentials"]
    selection_method = source_config.get(
        "table_selection_method",
        ConfigManager.get_table_selection_method(),
    )
    semantic_layer = source_config.get("semantic_layer")
    is_cxo = ConfigManager.get_interaction_mode() == "cxo"
    logger.info(f"Connecting to {db_type} database... (CxO mode: {is_cxo})")

    connector = DatabaseConnector()
    while True:
        success, message = connector.connect(db_type, credentials)
        if success:
            logger.info(f"Successfully connected to {db_type} database")
            break

        logger.warning(f"Database connection failed: {message}")
        retry = QMessageBox.question(
            None,
            "Database Connection Failed",
            f"{message}\n\nWould you like to try again?",
            QMessageBox.StandardButton.Retry | QMessageBox.StandardButton.Cancel,
        )

        if retry == QMessageBox.StandardButton.Retry:
            # Try to get semantic layer from source_config if available
            retry_semantic_layer = source_config.get("semantic_layer")
            db_dialog = DatabaseConnectionDialog(
                force_nlp=is_cxo, semantic_layer=retry_semantic_layer
            )
            logger.info("Applying retry DB config to dialog")
            db_dialog.populate_from_config(source_config)
            if db_dialog.exec() != QDialog.DialogCode.Accepted:
                connector.close()
                return False
            source_config = db_dialog.get_config()
            db_type = source_config["db_type"]
            credentials = source_config["credentials"]
        else:
            logger.info("User cancelled database connection retry")
            connector.close()
            return False

    try:
        tables = connector.get_tables()
        logger.debug(f"Retrieved {len(tables) if tables else 0} tables from database")
    except Exception as e:
        logger.error(f"Table discovery failed: {str(e)}", exc_info=True)
        QMessageBox.critical(None, "Table Discovery Failed", str(e))
        connector.close()
        return False

    if not tables:
        QMessageBox.critical(
            None, "No Tables Found", "The database does not contain any tables."
        )
        connector.close()
        return False

    # ---- CxO mode: skip table selection, defer to query time via NLP ----
    if is_cxo:
        connector.close()
        logger.info(
            f"CxO mode: skipping table selection. {len(tables)} tables available for NLP selection at query time."
        )

        cxo_context = {
            "source_type": "database",
            "cxo_mode": True,
            "db_type": db_type,
            "credentials": credentials,
            "all_tables": tables,
            "tables": [],  # no tables loaded yet; filled at query time
            "table_info": {},
            "semantic_layer": semantic_layer,
        }

        window.data_context = cxo_context
        window.backend.data_context = cxo_context

        if window.backend.active_project:
            creds_to_store = credentials.copy()
            if "password" in creds_to_store:
                creds_to_store["password"] = ""
            window.backend.active_project.data_source = {
                "db_type": db_type,
                "credentials": creds_to_store,
                "table_selection_method": "nlp",
                "cxo_mode": True,
            }
            # Store semantic layer on project, not in data_source
            if semantic_layer:
                window.backend.active_project.semantic_layer = semantic_layer

        table_count = len(tables)
        welcome_msg = (
            f"## Connected to {db_type} database\n\n"
            f"**{table_count}** tables available. "
            f"In CxO mode, relevant tables are automatically selected based on your questions.\n\n"
            f"Simply type your question below to get started."
        )
        window.conversation_display.setHtml(markdown_to_html(welcome_msg))
        return True

    # ---- Analyst mode: normal table selection flow ----
    selected_tables = select_tables_with_method(
        window,
        connector,
        tables,
        selection_method,
        semantic_layer,
    )
    connector.close()

    if selected_tables is None:
        logger.info(
            "User cancelled table selection, returning to data source selection"
        )
        return False

    table_value: Any = (
        selected_tables if len(selected_tables) > 1 else selected_tables[0]
    )
    source_config["table"] = table_value

    data_context, status = load_data("database", source_config)

    if data_context is not None:
        logger.info("Successfully loaded database data")
        window.data_context = data_context
        window.backend.data_context = data_context

        if window.backend.active_project:
            creds_to_store = credentials.copy()
            if "password" in creds_to_store:
                creds_to_store["password"] = ""
            window.backend.active_project.data_source = {
                "db_type": db_type,
                "credentials": creds_to_store,
                "table": selected_tables,
                "table_selection_method": selection_method,
            }
            # Store semantic layer on project, not in data_source
            if semantic_layer:
                window.backend.active_project.semantic_layer = semantic_layer

        welcome_msg = window.backend.format_database_welcome_message(
            db_type, selected_tables, data_context, status
        )
        window.conversation_display.setHtml(markdown_to_html(welcome_msg))
        return True
    else:
        window.conversation_display.setHtml(
            markdown_to_html(
                f"Connected to {db_type} database, but no data loaded.\n"
                f"{status}\nYou can still ask questions!"
            )
        )
        return True


def _load_multi_database_data(
    window: DataWorkspaceGUI, source_config: Dict[str, Any]
) -> bool:
    """
    Load data from multiple databases. Returns True if successful, False to retry.
    """
    from processing import load_multi_database

    configs = source_config.get("connections", [])
    logger.info(f"Loading multi-database with {len(configs)} connections")

    data_context, status = load_multi_database(configs)
    if data_context is None:
        logger.warning(f"Multi-database load failed: {status}")
        QMessageBox.warning(window, "Load Failed", status)
        return False

    window.data_context = data_context
    window.backend.data_context = data_context
    # Persist configs (passwords stripped)
    ConfigManager.save_multi_db_config(configs)

    if window.backend.active_project:
        safe_configs = []
        for cfg in configs:
            safe = dict(cfg)
            c = safe.get("credentials", {}).copy()
            c.pop("password", None)
            safe["credentials"] = c
            safe_configs.append(safe)
        window.backend.active_project.data_source = {
            "source_type": "multi_database",
            "connections": safe_configs,
        }

    aliases = list(data_context.get("connections", {}).keys())
    table_count = len(data_context.get("tables", []))
    welcome_msg = (
        f"## Multi-Database Connected\n\n"
        f"**{table_count}** tables loaded across "
        f"**{len(aliases)}** databases: {', '.join(aliases)}\n\n"
        f"Tables are prefixed with their database alias "
        f"(e.g. `alias__table`). Ask your question below."
    )
    window.conversation_display.setHtml(markdown_to_html(welcome_msg))
    return True


def _load_file_data(window: DataWorkspaceGUI, source_config: Dict[str, Any]) -> bool:
    """
    Load data from files. Returns True if successful, False if user wants to retry.
    """
    file_paths = source_config["file_paths"]
    logger.info(f"Loading {len(file_paths)} file(s): {file_paths}")
    data_context, welcome_msg = window.backend.load_file_data_with_ui(file_paths)

    if data_context is not None:
        window.data_context = data_context
        if window.backend.active_project:
            window.backend.active_project.data_source = {"file_paths": file_paths}
        window.conversation_display.setHtml(markdown_to_html(welcome_msg))
        return True
    else:
        logger.error(f"Failed to load file data: {welcome_msg}")
        window.conversation_display.setHtml(markdown_to_html(welcome_msg))
        QMessageBox.critical(window, "Data Loading Error", "Failed to load any files.")
        return False


def start_application():
    """Start the AI Data Workspace application."""
    logger.info("=" * 60)
    logger.info("Starting AI Data Workspace application")
    logger.info("=" * 60)
    app = QApplication(sys.argv)

    _preload_theme(app)

    if not _ensure_api_configured():
        return

    _ensure_interaction_mode_configured()

    result = _show_project_dialog()
    if result is None:
        return
    window, _ = result

    _load_project_and_chats(window)

    data_already_loaded = window.backend.data_context is not None
    logger.info(f"Data already loaded from project: {data_already_loaded}")

    if data_already_loaded:
        logger.info("Using data already loaded from project")
        _display_loaded_project_data(window)
    else:
        if not _load_initial_data(window):
            return

    logger.info("Displaying main application window")
    window.show()
    logger.info("Application started successfully")
    sys.exit(app.exec())


if __name__ == "__main__":
    start_application()
