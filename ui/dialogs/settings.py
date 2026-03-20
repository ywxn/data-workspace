"""Settings dialogs for API keys, model configuration, and memory retention."""
from PySide6.QtCore import Qt
from PySide6.QtGui import QFont, QIcon
from PySide6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QFormLayout, QDialogButtonBox,
    QLabel, QLineEdit, QComboBox, QPushButton, QCheckBox, QSpinBox,
    QMessageBox,
)
from core.config import ConfigManager
from core.logger import get_logger
logger = get_logger(__name__)


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

        current_default = ConfigManager.get_default_api()
        default_map = {"openai": "OpenAI", "claude": "Claude"}
        preselect = default_map.get(current_default)
        if preselect:
            idx = self.provider_combo.findText(preselect)
            if idx >= 0:
                self.provider_combo.setCurrentIndex(idx)


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

        # Set as default checkbox
        self.set_default_checkbox = QCheckBox("Set as default provider")
        self.set_default_checkbox.setChecked(True)
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
            if self.set_default_checkbox.isChecked():
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


class MemoryRetentionDialog(QDialog):
    """Dialog to configure memory retention policy for query history."""

    _POLICY_LABEL_TO_VALUE = {
        "Keep all queries": "keep_all",
        "Keep latest N queries": "rolling_n",
        "Keep queries from last N days": "ttl_days",
    }

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Memory Retention Policy")
        self.setModal(True)
        self.setMinimumWidth(520)
        self.setWindowIcon(QIcon("icon.ico"))

        retention = ConfigManager.get_memory_retention_policy()

        layout = QVBoxLayout(self)

        title = QLabel("Configure Query Memory Retention")
        title.setFont(QFont("Roboto", 14, QFont.Weight.Bold))
        title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(title)

        instructions = QLabel(
            "Choose how long query memory is kept. This affects new retention checks "
            "for project memory entries."
        )
        instructions.setWordWrap(True)
        layout.addWidget(instructions)

        layout.addSpacing(12)

        form_layout = QFormLayout()

        self.policy_combo = QComboBox()
        self.policy_combo.addItems(list(self._POLICY_LABEL_TO_VALUE.keys()))
        policy_value = retention.get("policy", "keep_all")
        policy_label = next(
            (
                label
                for label, value in self._POLICY_LABEL_TO_VALUE.items()
                if value == policy_value
            ),
            "Keep all queries",
        )
        self.policy_combo.setCurrentText(policy_label)
        self.policy_combo.currentTextChanged.connect(self._update_input_state)
        form_layout.addRow("Policy:", self.policy_combo)

        self.rolling_n_spin = QSpinBox()
        self.rolling_n_spin.setRange(1, 1000000)
        self.rolling_n_spin.setValue(int(retention.get("rolling_n", 100)))
        self.rolling_n_spin.setToolTip("Maximum number of query records to keep.")
        form_layout.addRow("Rolling N:", self.rolling_n_spin)

        self.ttl_days_spin = QSpinBox()
        self.ttl_days_spin.setRange(1, 36500)
        self.ttl_days_spin.setValue(int(retention.get("ttl_days", 90)))
        self.ttl_days_spin.setToolTip(
            "Delete query records older than this many days."
        )
        form_layout.addRow("TTL Days:", self.ttl_days_spin)

        layout.addLayout(form_layout)
        self._update_input_state(self.policy_combo.currentText())

        layout.addSpacing(10)

        help_text = QLabel(
            "Tip: Use 'Keep latest N queries' for bounded memory size, or "
            "'Keep queries from last N days' for time-based cleanup."
        )
        help_text.setStyleSheet("color: gray; font-size: 9pt;")
        help_text.setWordWrap(True)
        layout.addWidget(help_text)

        button_box = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel
        )
        button_box.accepted.connect(self._save_settings)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)

    def _update_input_state(self, label: str) -> None:
        """Enable only the input relevant to the selected policy."""
        policy = self._POLICY_LABEL_TO_VALUE.get(label, "keep_all")
        self.rolling_n_spin.setEnabled(policy == "rolling_n")
        self.ttl_days_spin.setEnabled(policy == "ttl_days")

    def _save_settings(self) -> None:
        """Persist retention policy to config."""
        policy_label = self.policy_combo.currentText()
        policy = self._POLICY_LABEL_TO_VALUE.get(policy_label, "keep_all")

        success, message = ConfigManager.set_memory_retention_policy(
            policy=policy,
            rolling_n=self.rolling_n_spin.value(),
            ttl_days=self.ttl_days_spin.value(),
        )

        if not success:
            QMessageBox.warning(self, "Settings Error", f"Failed to save setting: {message}")
            return

        logger.info(
            "Memory retention policy updated: "
            f"policy={policy}, rolling_n={self.rolling_n_spin.value()}, ttl_days={self.ttl_days_spin.value()}"
        )
        QMessageBox.information(self, "Success", "Memory retention policy saved.")
        self.accept()
