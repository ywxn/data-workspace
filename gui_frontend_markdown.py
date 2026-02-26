import sys
import asyncio
import webbrowser
import json
from PySide6.QtCore import Qt, Signal, QThread
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
    QScrollArea,
    QDialog,
    QFileDialog,
    QLineEdit,
    QComboBox,
    QFormLayout,
    QDialogButtonBox,
    QMessageBox,
    QMenu,
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


class APIKeyConfigDialog(QDialog):
    """Dialog to set up API keys on first startup."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("API Key Configuration")
        self.setModal(True)
        self.setMinimumWidth(500)
        self.windowIcon = QIcon("icon.svg")
        self.setWindowIcon(self.windowIcon)

        layout = QVBoxLayout(self)

        # Title
        title = QLabel("Configure API Keys")
        title.setFont(QFont("Roboto", 14, QFont.Weight.Bold))
        title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(title)

        # Instructions
        instructions = QLabel(
            "Select an AI provider and enter your API key.\n"
            "You can add both OpenAI and Claude keys, or just one.\n\n"
            "Get your API key from:\n"
            "• OpenAI: https://platform.openai.com/api-keys\n"
            "• Claude (Anthropic): https://console.anthropic.com/account/keys"
        )
        instructions.setWordWrap(True)
        layout.addWidget(instructions)

        layout.addSpacing(15)

        # Form layout
        form_layout = QFormLayout()

        # Provider selection
        self.provider_combo = QComboBox()
        self.provider_combo.addItems(["OpenAI", "Claude"])
        self.provider_combo.currentTextChanged.connect(self.on_provider_changed)
        form_layout.addRow("AI Provider:", self.provider_combo)

        # API Key input
        self.api_key_input = QLineEdit()
        self.api_key_input.setEchoMode(QLineEdit.EchoMode.Password)
        self.api_key_input.setPlaceholderText("Paste your API key here")
        form_layout.addRow("API Key:", self.api_key_input)

        # Show/hide key toggle
        self.toggle_visibility_btn = QPushButton("Show Key")
        self.toggle_visibility_btn.setMaximumWidth(100)
        self.toggle_visibility_btn.clicked.connect(self.toggle_key_visibility)
        form_layout.addRow("", self.toggle_visibility_btn)

        layout.addLayout(form_layout)

        layout.addSpacing(10)

        # Buttons
        button_box = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel
        )
        button_box.accepted.connect(self.validate_and_save)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)

        self.key_is_visible = False

    def on_provider_changed(self, provider):
        """Clear input when provider changes."""
        logger.debug(f"API provider changed to: {provider}")
        self.api_key_input.clear()
        self.key_is_visible = False
        self.toggle_visibility_btn.setText("Show Key")
        if self.api_key_input.echoMode() == QLineEdit.EchoMode.Normal:
            self.api_key_input.setEchoMode(QLineEdit.EchoMode.Password)

    def toggle_key_visibility(self):
        """Toggle visibility of API key."""
        self.key_is_visible = not self.key_is_visible
        logger.debug(f"API key visibility toggled to: {self.key_is_visible}")
        if self.key_is_visible:
            self.api_key_input.setEchoMode(QLineEdit.EchoMode.Normal)
            self.toggle_visibility_btn.setText("Hide Key")
        else:
            self.api_key_input.setEchoMode(QLineEdit.EchoMode.Password)
            self.toggle_visibility_btn.setText("Show Key")

    def validate_and_save(self):
        """Validate and save the API key."""
        provider = self.provider_combo.currentText()
        api_key = self.api_key_input.text().strip()
        logger.debug(f"Validating API key for provider: {provider}")

        if not api_key:
            logger.warning(f"Empty API key provided for provider: {provider}")
            QMessageBox.warning(
                self, "Empty API Key", f"Please enter a valid {provider} API key."
            )
            return

        # Save to config
        success, message = ConfigManager.set_api_key(provider.lower(), api_key)
        logger.info(f"API key save attempt for {provider}: {success}")

        if success:
            # Set as default API if it's the first one
            if (
                not ConfigManager.has_any_api_key()
                or ConfigManager.get_default_api() == "openai"
            ):
                ConfigManager.set_default_api(provider.lower())

            logger.info(f"{provider} API key configured successfully")
            QMessageBox.information(
                self, "Success", f"{provider} API key configured successfully!"
            )
            self.accept()
        else:
            logger.error(f"Failed to save {provider} API key: {message}")
            QMessageBox.critical(self, "Error", f"Failed to save API key: {message}")


class CreateProjectDialog(QDialog):
    """Dialog to create a new project or load an existing one"""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Create or Load Project")
        self.setModal(True)
        self.setMinimumWidth(480)
        self.windowIcon = QIcon("icon.svg")
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

            db_dialog = DatabaseConnectionDialog(self)
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
            semantic_layer = ds.get("semantic_layer")

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
                    "semantic_layer": semantic_layer,
                }
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
            data_context, welcome_msg = self.backend.load_file_data_with_ui(
                file_paths
            )
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
        self.windowIcon = QIcon("icon.svg")
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

        layout.addSpacing(20)

        # Cancel button
        cancel_btn = QPushButton("Cancel")
        cancel_btn.clicked.connect(self.reject)
        layout.addWidget(cancel_btn)

    def select_database(self):
        """Show database connection dialog"""
        logger.debug("Opening database connection dialog")
        db_dialog = DatabaseConnectionDialog(self)
        if db_dialog.exec() == QDialog.DialogCode.Accepted:
            logger.info("Database connection configuration accepted")
            self.data_source_type = "database"
            self.data_source_config = db_dialog.get_config()
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


class DatabaseConnectionDialog(QDialog):
    """Dialog for database connection details"""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Database Connection")
        self.setModal(True)
        self.setMinimumWidth(500)
        self.windowIcon = QIcon("icon.svg")
        self.setWindowIcon(self.windowIcon)

        self.semantic_layer: Optional[Dict[str, Any]] = None

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
        self.selection_method_combo = QComboBox()
        self.selection_method_combo.addItems(["Manual", "NLP (semantic)"])
        method_pref = ConfigManager.get_table_selection_method()
        if method_pref == "nlp":
            self.selection_method_combo.setCurrentIndex(1)
        self.selection_method_combo.currentTextChanged.connect(
            self.on_selection_method_changed
        )
        form_layout.addRow("Table Selection:", self.selection_method_combo)

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
        is_nlp = "nlp" in method_text.lower()
        self.semantic_layer_container.setVisible(is_nlp)

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

        method_text = self.selection_method_combo.currentText().lower()
        method = "nlp" if "nlp" in method_text else "manual"
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

        method_text = self.selection_method_combo.currentText().lower()
        method = "nlp" if "nlp" in method_text else "manual"

        return {
            "db_type": db_type,
            "credentials": credentials,
            "table_selection_method": method,
            "semantic_layer": self.semantic_layer,
        }

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
                data = json.load(f) # if this fails it will raise an exception so we don't need to check
            self.semantic_layer = data
            self.semantic_layer_label.setText(f"Loaded: {os.path.basename(file_path)}")
        except Exception as e:
            QMessageBox.warning(
                self,
                "Semantic Layer Error",
                f"Failed to load semantic layer: {str(e)}",
            )


class NLPPromptDialog(QDialog):
    """Dialog to capture an NLP prompt for table selection."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Describe the Data You Need")
        self.setModal(True)
        self.setMinimumWidth(500)
        self.windowIcon = QIcon("icon.svg")
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
        self.windowIcon = QIcon("icon.svg")
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
            if table_name in preselected_set:
                item.setSelected(True)
            self.table_list.addItem(item)
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
        self.setMinimumWidth(450)
        self.windowIcon = QIcon("icon.svg")
        self.setWindowIcon(self.windowIcon)

        layout = QVBoxLayout(self)

        title = QLabel("Local Projects")
        title.setFont(QFont("Roboto", 14, QFont.Weight.Bold))
        title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(title)

        subtitle = QLabel("Select a previously saved project file to load:")
        subtitle.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(subtitle)

        layout.addSpacing(8)

        self.file_list = QListWidget()
        for f in files:
            item = QListWidgetItem(f)
            item.setToolTip(self._get_project_description(f))
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
        return items[0].text()

    def _get_project_description(self, file_name: str) -> str:
        """Return project description for tooltip display."""
        path = os.path.join("projects", file_name)
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            description = (data or {}).get("description")
            if isinstance(description, str) and description.strip():
                return description.strip()
        except Exception as e:
            logger.debug(
                f"Failed to read description for project '{file_name}': {e}"
            )
        return "No description available."


def select_tables_with_method(
    parent: QWidget,
    connector: DatabaseConnector,
    tables: List[str],
    selection_method: str,
    semantic_layer: Optional[Dict[str, Any]] = None,
) -> Optional[List[str]]:
    """Select tables using manual list or NLP-based selection."""
    method = (selection_method or "manual").lower()

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

    def __init__(self, query: str, data_context: Dict[str, Any]):
        super().__init__()
        self.query = query
        self.data_context = data_context
        self.agent = AIAgent()

    def run(self):
        try:
            logger.debug(
                f"QueryWorker starting execution for query: {self.query[:100]}..."
            )
            # Run async agent methods in a new event loop
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            # Use the new orchestrated execute_query method
            tables = self.data_context.get("tables", [])
            logger.debug(
                f"Executing query asynchronously (tables: {tables})"
            )
            result = loop.run_until_complete(
                self.agent.execute_query(self.query, self.data_context)
            )
            logger.info(
                f"Query execution completed successfully (result length: {len(result)} chars)"
            )
            self.result_signal.emit(result)

            loop.close()
        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}", exc_info=True)
            self.error_signal.emit(f"Error: {str(e)}")


class DataWorkspaceGUI(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("AI Data Workspace")
        self.setGeometry(100, 100, 1400, 800)
        self.windowIcon = QIcon("icon.svg")
        self.setWindowIcon(self.windowIcon)

        # Create menu bar
        menu_bar = self.menuBar()

        # ===== File Menu =====
        file_menu = menu_bar.addMenu("File")

        new_project_action = QAction("New Project", self)
        new_project_action.setShortcut("Ctrl+N")
        new_project_action.triggered.connect(self.new_project)
        file_menu.addAction(new_project_action)

        load_project_action = QAction("Load Project...", self)
        load_project_action.setShortcut("Ctrl+O")
        load_project_action.triggered.connect(self.load_project_menu)
        file_menu.addAction(load_project_action)

        save_project_action = QAction("Save Project", self)
        save_project_action.setShortcut("Ctrl+S")
        save_project_action.triggered.connect(self.save_project)
        file_menu.addAction(save_project_action)

        file_menu.addSeparator()

        connect_data_action = QAction("Connect Data Source...", self)
        connect_data_action.triggered.connect(self.connect_data_source)
        file_menu.addAction(connect_data_action)

        connect_additional_action = QAction("Connect Additional Data Source...", self)
        connect_additional_action.triggered.connect(self.connect_additional_data_source)
        file_menu.addAction(connect_additional_action)

        api_settings_action = QAction("API Settings...", self)
        api_settings_action.triggered.connect(self.change_api_settings)
        file_menu.addAction(api_settings_action)

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
        conversation_scroll = QScrollArea()
        conversation_scroll.setWidgetResizable(True)
        conversation_scroll.setWidget(self.conversation_display)
        content_layout.addWidget(conversation_scroll, 1)

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
        self.current_theme = "system"
        self.font_point_size = QApplication.instance().font().pointSize()

        # Load saved theme preference or use system theme
        config = ConfigManager.load_config()
        saved_theme = config.get("theme", "system")
        if saved_theme in ["dark", "light", "system"]:
            self.current_theme = saved_theme

        # Apply theme on startup
        self._apply_theme(self.current_theme)

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

        action = menu.exec(self.chat_list.mapToGlobal(position))

        if action == clear_action:
            self.clear_chat_action(item)
        elif action == delete_action:
            self.delete_chat_action(item)

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
                        self.conversation_display.clear()
                        self.conversation_display.setHtml(
                            markdown_to_html(
                                "Chat cleared. Start typing to begin a new conversation."
                            )
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
                history = self.backend.get_chat_history()
                if history:
                    chat_history = self._format_chat_history(history)
                    self.conversation_display.setHtml(markdown_to_html(chat_history))
                else:
                    self.conversation_display.setHtml(
                        markdown_to_html(
                            "No chat history yet. Start typing to begin the conversation."
                        )
                    )
            else:
                logger.warning(f"Failed to load chat session: {self.chat_id}")
                self.conversation_display.setHtml(
                    markdown_to_html("Failed to load chat.")
                )

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
        # Refresh chat list and select the new chat
        self.refresh_chat_list()

        # Select the new chat
        if chat_id:
            self.chat_id = chat_id
            self.conversation_display.clear()
            self.conversation_display.setHtml(
                markdown_to_html(
                    "New chat created. Start typing to begin the conversation."
                )
            )

    def _format_chat_history(self, messages: List[Dict[str, str]]) -> str:
        """Format chat messages as Markdown"""
        parts: List[str] = []
        for msg in messages:
            role = msg.get("role", "unknown").capitalize()
            content = msg.get("content", "")
            formatted_content = self.backend.markdown_to_qt(content)
            parts.append(f"**{role}:** {formatted_content}")
        return "\n\n".join(parts)

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

        if self.data_context is None:
            logger.warning("Query submitted but no data loaded")
            QMessageBox.warning(
                self, "No Data", "No data loaded. Please restart and load data first."
            )
            return

        logger.info(f"Submitting query: {query[:100]}...")  # Log first 100 chars

        # Change button to Stop
        self.submit_button.setText("Stop")
        self.is_running = True

        # Display user message
        current_md = self.conversation_display.toMarkdown()
        user_message_md = f"**You:** {query}"
        processing_md = self.processing_marker
        combined = "\n\n".join(
            [
                segment
                for segment in [current_md.strip(), user_message_md, processing_md]
                if segment
            ]
        )
        self.conversation_display.setHtml(markdown_to_html(combined))

        # Scroll to bottom
        scroll_bar = self.conversation_display.verticalScrollBar()
        if scroll_bar:
            scroll_bar.setValue(scroll_bar.maximum())

        self.query_input.clear()

        # Add user message to chat history
        self.add_message_to_chat("user", query)

        # Create and start worker thread
        logger.debug("Creating query worker for active SQL context")
        self.worker = QueryWorker(query, self.data_context)
        self.worker.result_signal.connect(self.display_result)
        self.worker.error_signal.connect(self.display_error)
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
            current_md = self.conversation_display.toMarkdown()
            cancelled_md = "**Status:** _Query cancelled by user._"
            if self.processing_marker in current_md:
                current_md = current_md.replace(self.processing_marker, cancelled_md)
                self.conversation_display.setHtml(markdown_to_html(current_md))

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
        current_md = self.conversation_display.toMarkdown()
        result_md = f"**Assistant:**\n{formatted_result}"

        if self.processing_marker in current_md:
            current_md = current_md.replace(self.processing_marker, result_md)
            self.conversation_display.setHtml(markdown_to_html(current_md))
        else:
            combined = "\n\n".join(
                [segment for segment in [current_md.strip(), result_md] if segment]
            )
            self.conversation_display.setHtml(markdown_to_html(combined))

        # Scroll to bottom
        scroll_bar = self.conversation_display.verticalScrollBar()
        if scroll_bar:
            scroll_bar.setValue(scroll_bar.maximum())

        # Add to chat history
        self.add_message_to_chat("assistant", result)

    def display_error(self, error: str):
        """Display error message"""
        logger.error(f"Query error: {error}")
        error_md = f"**Error:**\n{error}"
        current_md = self.conversation_display.toMarkdown()

        if self.processing_marker in current_md:
            current_md = current_md.replace(self.processing_marker, error_md)
            self.conversation_display.setHtml(markdown_to_html(current_md))
        else:
            combined = "\n\n".join(
                [segment for segment in [current_md.strip(), error_md] if segment]
            )
            self.conversation_display.setHtml(markdown_to_html(combined))

    def clear_fields(self):
        """Clear conversation"""
        logger.debug(f"Clearing chat fields for chat_id: {self.chat_id}")
        self.conversation_display.clear()
        self.query_input.clear()
        self.backend.clear_session()
        logger.info("Chat fields cleared successfully")

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
                        if self.backend.active_project:
                            chats = self.backend.active_project.get_all_chats()
                            if chats:
                                self.chat_id = chats[0].session_id
                                logger.debug(f"Loaded initial chat: {self.chat_id}")
                                # Load the first chat session and display its history
                                success, _ = self.backend.load_chat_session(
                                    self.chat_id
                                )
                                if success:
                                    history = self.backend.get_chat_history()
                                    if history:
                                        chat_history = self._format_chat_history(
                                            history
                                        )
                                        self.conversation_display.setHtml(
                                            markdown_to_html(chat_history)
                                        )
                                    else:
                                        self.conversation_display.setHtml(
                                            markdown_to_html(
                                                "No chat history yet. Start typing to begin the conversation."
                                            )
                                        )

                    self.refresh_project_list()
                    # Select the first chat in the list
                    if self.chat_list.count() > 0:
                        self.chat_list.setCurrentRow(0)
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
                    if self.backend.active_project:
                        logger.debug(
                            f"Loaded project: {self.backend.active_project.title}"
                        )
                        chats = self.backend.active_project.get_all_chats()
                        if chats:
                            self.chat_id = chats[0].session_id
                            logger.debug(
                                f"Loaded {len(chats)} chat(s), active chat: {self.chat_id}"
                            )
                            # Load the first chat session and display its history
                            success, _ = self.backend.load_chat_session(self.chat_id)
                            if success:
                                history = self.backend.get_chat_history()
                                if history:
                                    chat_history = self._format_chat_history(history)
                                    self.conversation_display.setHtml(
                                        markdown_to_html(chat_history)
                                    )
                                else:
                                    self.conversation_display.setHtml(
                                        markdown_to_html(
                                            "No chat history yet. Start typing to begin the conversation."
                                        )
                                    )

                self.refresh_project_list()
                # Select the first chat in the list
                if self.chat_list.count() > 0:
                    self.chat_list.setCurrentRow(0)
                logger.info("Project load successful, UI refreshed")
            else:
                logger.info("User cancelled project load")
        except Exception as e:
            logger.error(f"Error loading project: {str(e)}", exc_info=True)
            QMessageBox.critical(self, "Error", f"Failed to load project: {str(e)}")

    def connect_data_source(self):
        """Open dialog to connect to a data source"""
        logger.info("User initiated data source connection")
        
        # Loop until data is successfully loaded or user cancels
        while True:
            source_dialog = DataSourceDialog(self)
            if source_dialog.exec() == QDialog.DialogCode.Accepted:
                source_type = source_dialog.data_source_type
                source_config = source_dialog.data_source_config
                logger.info(f"Data source type selected: {source_type}")

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

                                if tables:
                                    selected_tables = select_tables_with_method(
                                        self,
                                        connector,
                                        tables,
                                        selection_method,
                                        semantic_layer,
                                    )
                                    connector.close()

                                    if selected_tables is None:
                                        logger.info("User cancelled table selection, returning to data source selection")
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
                                                db_type, selected_tables, data_context, status
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
                                                    "semantic_layer": semantic_layer,
                                                }
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
                                continue

                        elif source_type == "file":
                            # File load flow
                            file_paths = source_config.get("file_paths", [])
                            logger.debug(f"Loading {len(file_paths)} file(s): {file_paths}")
                            if file_paths:
                                data_context, welcome_msg = self.backend.load_file_data_with_ui(
                                    file_paths
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
                                        self, "Data Loaded", "Files loaded successfully."
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
            else:
                logger.info("User cancelled data source connection")
                return

    def connect_additional_data_source(self):
        """Connect additional data sources without overwriting existing data."""
        logger.info("User initiated additional data source connection")

        if self.data_context is None:
            QMessageBox.information(
                self,
                "No Data Loaded",
                "No data is currently loaded. Use Connect Data Source first.",
            )
            self.connect_data_source()
            return

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

                if self.data_context.get("source_type") != "database":
                    QMessageBox.warning(
                        self,
                        "Not Supported",
                        "Additional database sources can only be added to an existing database workspace.",
                    )
                    return

                if (
                    self.data_context.get("db_type") != db_type
                    or self.data_context.get("credentials") != credentials
                ):
                    QMessageBox.warning(
                        self,
                        "Not Supported",
                        "Adding tables from a different database connection is not supported in SQL-first mode.",
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
                        "semantic_layer": semantic_layer,
                    }

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

                updated_context, status = add_files_to_sqlite(self.data_context, file_paths)
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

    def change_api_settings(self):
        """Open API settings dialog"""
        logger.info("User opened API settings dialog")
        try:
            api_dialog = APIKeyConfigDialog(self)
            if api_dialog.exec() == QDialog.DialogCode.Accepted:
                logger.info("API settings updated successfully")
                QMessageBox.information(
                    self, "API Settings", "API settings updated successfully."
                )
            else:
                logger.info("User cancelled API settings change")
        except Exception as e:
            logger.error(f"Error changing API settings: {str(e)}", exc_info=True)
            QMessageBox.critical(
                self, "Error", f"Failed to update API settings: {str(e)}"
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
            "<h2>AI Data Workspace</h2>"
            "<p>Version 1.0.0</p>"
            "<p>An intelligent application for data analysis and visualization with AI assistance.</p>"
            "<p><b>Features:</b></p>"
            "<ul>"
            "<li>Multi-source data loading (CSV, Excel, Database)</li>"
            "<li>AI-powered data analysis and insights</li>"
            "<li>Project and chat session management</li>"
            "<li>Multi-database support</li>"
            "</ul>"
            "<p>© 2026 AI Data Workspace. All rights reserved.</p>"
        )
        QMessageBox.about(self, "About AI Data Workspace", about_text)


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
    """Ensure API keys are configured. Shows dialog if needed. Returns True if configured."""
    if not ConfigManager.has_any_api_key():
        logger.warning("No API keys configured, prompting user for setup")
        api_config_dialog = APIKeyConfigDialog()
        if api_config_dialog.exec() != QDialog.DialogCode.Accepted:
            logger.error("API key setup cancelled by user, application cannot start")
            QMessageBox.warning(
                None,
                "API Key Required",
                "An API key is required to use this application.\n"
                "Please configure your API key and try again.",
            )
            return False
        logger.info("API key configured successfully")
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
        if window.backend.active_project:
            logger.debug(f"Active project: {window.backend.active_project.title}")
            chats = window.backend.active_project.get_all_chats()
            if chats:
                window.chat_id = chats[0].session_id
                success, _ = window.backend.load_chat_session(window.chat_id)
                logger.info(f"Loaded {len(chats)} chat(s), active chat: {window.chat_id}")
                if success:
                    history = window.backend.get_chat_history()
                    if history:
                        chat_history = window._format_chat_history(history)
                        window.conversation_display.setHtml(markdown_to_html(chat_history))

    window.refresh_project_list()
    if window.chat_list.count() > 0:
        window.chat_list.setCurrentRow(0)


def _display_loaded_project_data(window: DataWorkspaceGUI) -> None:
    """Display welcome message for data that was already loaded from project."""
    if window.backend.data_context is None:
        return

    data_context = window.backend.data_context
    logger.debug("Loaded SQL context from project")
    
    if window.backend.active_project and window.backend.active_project.data_source:
        ds = window.backend.active_project.data_source
        if ds.get("db_type"):
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
            elif source_type == "file":
                if _load_file_data(window, source_config):
                    return True
            else:
                logger.error(f"Unknown data source type: {source_type}")
                QMessageBox.critical(window, "Error", f"Unknown source type: {source_type}")
                continue
        except Exception as e:
            logger.error(f"Error loading data: {str(e)}", exc_info=True)
            error_msg = f"### Error Loading Data\n{str(e)}\nPlease restart and try again."
            window.conversation_display.setHtml(markdown_to_html(error_msg))
            continue


def _load_database_data(window: DataWorkspaceGUI, source_config: Dict[str, Any]) -> bool:
    """
    Load data from a database. Returns True if successful, False if user wants to retry.
    """
    db_type: str = source_config["db_type"]
    credentials: Dict[str, Any] = source_config["credentials"]
    selection_method = source_config.get(
        "table_selection_method",
        ConfigManager.get_table_selection_method(),
    )
    semantic_layer = source_config.get("semantic_layer")
    logger.info(f"Connecting to {db_type} database...")

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
            db_dialog = DatabaseConnectionDialog()
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
        QMessageBox.critical(None, "No Tables Found", "The database does not contain any tables.")
        connector.close()
        return False

    selected_tables = select_tables_with_method(
        window,
        connector,
        tables,
        selection_method,
        semantic_layer,
    )
    connector.close()

    if selected_tables is None:
        logger.info("User cancelled table selection, returning to data source selection")
        return False

    table_value: Any = selected_tables if len(selected_tables) > 1 else selected_tables[0]
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
                "semantic_layer": semantic_layer,
            }
        
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


"""if __name__ == "__main__":
    start_application()
"""
