import sys
import asyncio
import pandas as pd
from PyQt6.QtCore import Qt, pyqtSignal, QThread
from PyQt6.QtGui import QFont, QKeyEvent
from PyQt6.QtWidgets import (
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
    QSplitter,
    QScrollArea,
    QDialog,
    QFileDialog,
    QLineEdit,
    QComboBox,
    QFormLayout,
    QDialogButtonBox,
    QMessageBox,
)
from gui_backend import DataWorkspaceBackend
from agents import AIAgent
from processing import load_data
from connector import DatabaseConnector
from config import ConfigManager
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple
import random
import os
from constants import PLACEHOLDER_PROJECT_NAMES, PLACEHOLDER_PROJECT_DESCRIPTIONS


class MessageTextEdit(QTextEdit):
    """Custom QTextEdit that submits on Enter and adds newline on Shift+Enter"""

    submit_signal = pyqtSignal()

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
        self.api_key_input.clear()
        self.key_is_visible = False
        self.toggle_visibility_btn.setText("Show Key")
        if self.api_key_input.echoMode() == QLineEdit.EchoMode.Normal:
            self.api_key_input.setEchoMode(QLineEdit.EchoMode.Password)

    def toggle_key_visibility(self):
        """Toggle visibility of API key."""
        self.key_is_visible = not self.key_is_visible
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

        if not api_key:
            QMessageBox.warning(
                self, "Empty API Key", f"Please enter a valid {provider} API key."
            )
            return

        # Save to config
        success, message = ConfigManager.set_api_key(provider.lower(), api_key)

        if success:
            # Set as default API if it's the first one
            if (
                not ConfigManager.has_any_api_key()
                or ConfigManager.get_default_api() == "openai"
            ):
                ConfigManager.set_default_api(provider.lower())

            QMessageBox.information(
                self, "Success", f"{provider} API key configured successfully!"
            )
            self.accept()
        else:
            QMessageBox.critical(self, "Error", f"Failed to save API key: {message}")


class CreateProjectDialog(QDialog):
    """Dialog to create a new project or load an existing one"""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Create or Load Project")
        self.setModal(True)
        self.setMinimumWidth(480)

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

        random_placeholder_index = random.randint(0, 9)

        self.project_name_input = QLineEdit()
        self.project_name_input.setPlaceholderText(
            PLACEHOLDER_PROJECT_NAMES[random_placeholder_index]
        )
        form_layout.addRow("Project Name:", self.project_name_input)

        self.project_desc_input = QTextEdit()
        self.project_desc_input.setPlaceholderText(
            PLACEHOLDER_PROJECT_DESCRIPTIONS[random_placeholder_index]
        )
        self.project_desc_input.setMaximumHeight(100)
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
        files = self.backend.list_saved_projects()
        projects_dir = os.path.abspath("projects")
        if not files:
            QMessageBox.information(
                self, "No Projects", f"No saved projects found in {projects_dir}"
            )
            return

        proj_dialog = ProjectLoadDialog(files, self)
        if proj_dialog.exec() != QDialog.DialogCode.Accepted:
            return

        file_name = proj_dialog.get_selected_file()
        if not file_name:
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

            connector.close()

            if not tables:
                QMessageBox.critical(
                    self, "No Tables Found", "The database does not contain any tables."
                )
                return

            # If the project previously stored a table selection, try to reuse it
            saved_table = ds.get("table")
            if saved_table:
                selected_tables = (
                    saved_table if isinstance(saved_table, list) else [saved_table]
                )
            else:
                table_dialog = TableSelectionDialog(tables)
                if table_dialog.exec() != QDialog.DialogCode.Accepted:
                    return
                selected_tables = table_dialog.get_selected_tables()

            source_config = {
                "db_type": new_config["db_type"],
                "credentials": new_config["credentials"],
                "table": selected_tables,
            }
            merged_dataframe, status = load_data("database", source_config)

            if merged_dataframe is not None:
                self.backend.loaded_dataframe = merged_dataframe
                # Update project data_source to reflect the used config (save host/port but don't store password)
                credentials_to_store = new_config["credentials"].copy()
                if "password" in credentials_to_store:
                    credentials_to_store["password"] = ""
                project.data_source = {
                    "db_type": new_config["db_type"],
                    "credentials": credentials_to_store,
                    "table": selected_tables,
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
            merged_dataframe, welcome_msg = self.backend.load_file_data_with_ui(
                file_paths
            )
            if merged_dataframe is not None:
                self.backend.loaded_dataframe = merged_dataframe
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
        project_name = self.project_name_input.text().strip()
        description = self.project_desc_input.toPlainText().strip()

        if not project_name:
            QMessageBox.warning(self, "Validation Error", "Project name is required!")
            return

        success, message, project_id = self.backend.create_project(
            project_name, description
        )

        if not success:
            QMessageBox.critical(self, "Project Creation Failed", message)
            return

        # Create an initial chat for the new project
        success, msg, chat_id = self.backend.create_chat_session("Chat 1")
        if not success:
            QMessageBox.warning(
                self,
                "Warning",
                f"Project created but failed to create initial chat: {msg}",
            )

        # Attempt to persist project to ./projects
        saved, save_msg = self.backend.save_project_to_disk(project_id)
        if not saved:
            QMessageBox.warning(
                self,
                "Project Saved (Memory Only)",
                f"Project created but failed to save to disk: {save_msg}",
            )
        else:
            QMessageBox.information(self, "Project Saved", save_msg)

        self.project_id = project_id
        # Make sure the saved project shows up in the sidebar immediately
        try:
            # If parent has a refresh method (main window), call it; otherwise rely on main to refresh
            parent = self.parent()
            if parent and hasattr(parent, "refresh_project_list"):
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
        db_dialog = DatabaseConnectionDialog(self)
        if db_dialog.exec() == QDialog.DialogCode.Accepted:
            self.data_source_type = "database"
            self.data_source_config = db_dialog.get_config()
            self.accept()

    def select_files(self):
        """Show file selection dialog"""
        files, _ = QFileDialog.getOpenFileNames(
            self,
            "Select Data File(s)",
            "",
            "Data Files (*.csv *.xlsx *.xls);;CSV Files (*.csv);;Excel Files (*.xlsx *.xls);;All Files (*.*)",
        )

        if files:
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

        layout = QVBoxLayout(self)
        form_layout = QFormLayout()

        # Database type selection
        self.db_type_combo = QComboBox()
        self.db_type_combo.addItems(
            ["sqlite", "mysql", "mariadb", "postgresql", "sqlserver", "oracle", "odbc"]
        )
        self.db_type_combo.currentTextChanged.connect(self.on_db_type_changed)
        form_layout.addRow("Database Type:", self.db_type_combo)

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

    def on_db_type_changed(self, db_type):
        """Show/hide fields based on database type"""
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

    def validate_and_accept(self):
        """Validate inputs before accepting"""
        db_type = self.db_type_combo.currentText()
        database = self.database_input.text().strip()

        if not database:
            QMessageBox.warning(self, "Validation Error", "Database field is required!")
            return

        if db_type != "sqlite":
            host = self.host_input.text().strip()
            if not host:
                QMessageBox.warning(self, "Validation Error", "Host is required!")
                return

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

        return {"db_type": db_type, "credentials": credentials}


class TableSelectionDialog(QDialog):
    """Dialog to select one or more database tables."""

    def __init__(self, table_names: List[str], parent=None):
        super().__init__(parent)
        self.setWindowTitle("Select Tables")
        self.setModal(True)
        self.setMinimumWidth(400)

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
        for table_name in table_names:
            self.table_list.addItem(QListWidgetItem(table_name))
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
            self.file_list.addItem(QListWidgetItem(f))
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


class QueryWorker(QThread):
    """Worker thread to handle long-running queries without blocking UI"""

    result_signal = pyqtSignal(str)
    error_signal = pyqtSignal(str)

    def __init__(self, query: str, df: pd.DataFrame):
        super().__init__()
        self.query = query
        self.df = df
        self.agent = AIAgent()

    def run(self):
        try:
            # Run async agent methods in a new event loop
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            # Use the new orchestrated execute_query method
            result = loop.run_until_complete(
                self.agent.execute_query(self.query, self.df)
            )
            self.result_signal.emit(result)

            loop.close()
        except Exception as e:
            self.error_signal.emit(f"Error: {str(e)}")


class DataWorkspaceGUI(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("AI Data Workspace")
        self.setGeometry(100, 100, 1400, 800)

        # Create menu bar
        menu_bar = self.menuBar()

        # File menu
        file_menu = menu_bar.addMenu("File")

        # TODO: Add actions for New Project, Load Project, Save Project, Connect Additional Data Sources, Change API Host/Keys, etc. in the File menu

        # Edit menu
        edit_menu = menu_bar.addMenu("Edit")

        # TODO: Add actions for Clear Conversation, Reset Workspace, etc. in the Edit menu

        # View menu
        view_menu = menu_bar.addMenu("View")

        # TODO: Theme toggle (dark,light,system), font size, layout options, etc. in the View menu

        # Help menu
        help_menu = menu_bar.addMenu("Help")

        # TODO: Add actions for Documentation (link to online docs)

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

        # Conversation area (scrollable), supports HTML
        conversation_title = QLabel("Conversation")
        conversation_title.setFont(QFont("Roboto", 14, QFont.Weight.Bold))
        content_layout.addWidget(conversation_title)

        self.conversation_display = QTextEdit()
        self.conversation_display.setReadOnly(True)
        self.conversation_display.setAcceptRichText(True)
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
        self.dataframe: Optional[pd.DataFrame] = None
        self.is_running = False

    def on_chat_selected(self, item: QListWidgetItem):
        """Handle chat selection"""
        self.chat_id = item.data(Qt.ItemDataRole.UserRole)
        if self.chat_id:
            success, _ = self.backend.load_chat_session(self.chat_id)
            if success:
                history = self.backend.get_chat_history()
                if history:
                    chat_history = self._format_chat_history(history)
                    self.conversation_display.setHtml(chat_history)
                else:
                    self.conversation_display.setHtml(
                        f"<p>No chat history yet. Start typing to begin the conversation.</p>"
                    )
            else:
                self.conversation_display.setHtml(f"<p>Failed to load chat.</p>")

        # Highlight the selected chat
        try:
            idx = self.chat_list.row(item)
            self.chat_list.setCurrentRow(idx)
        except Exception:
            pass

    def create_new_chat(self):
        """Create a new chat in the active project"""
        if self.backend.active_project is None:
            QMessageBox.warning(self, "No Project", "Please load a project first.")
            return

        # Auto-generate chat name
        chat_num = len(self.backend.active_project.chats) + 1
        success, msg, chat_id = self.backend.create_chat_session(f"Chat {chat_num}")

        if not success:
            QMessageBox.warning(self, "Error", f"Failed to create chat: {msg}")
            return

        # Refresh chat list and select the new chat
        self.refresh_chat_list()

        # Select the new chat
        if chat_id:
            self.chat_id = chat_id
            self.conversation_display.clear()
            self.conversation_display.setHtml(
                f"<p>New chat created. Start typing to begin the conversation.</p>"
            )

    def _format_chat_history(self, messages: List[Dict[str, str]]) -> str:
        """Format chat messages as HTML with proper code display"""
        html_parts = []
        for msg in messages:
            role = msg.get("role", "unknown").capitalize()
            content = msg.get("content", "")

            # Format the content using backend markdown to HTML converter
            formatted_content = self.backend.markdown_to_html(content)

            if role == "User":
                html_parts.append(
                    f'<div style="margin: 10px 0;"><b style="color: #0066cc;">{role}:</b> {formatted_content}</div>'
                )
            else:
                html_parts.append(
                    f'<div style="margin: 10px 0;"><b style="color: #009900;">{role}:</b> {formatted_content}</div>'
                )

        return "".join(html_parts)

    def submit_query(self):
        """Handle query submission or stop running query"""
        # If currently running, stop the query
        if self.is_running:
            self.stop_query()
            return

        query = self.query_input.toPlainText().strip()

        if not query:
            return

        if self.dataframe is None:
            QMessageBox.warning(
                self, "No Data", "No data loaded. Please restart and load data first."
            )
            return

        # Change button to Stop
        self.submit_button.setText("Stop")
        self.is_running = True

        # Display user message (escape HTML to prevent injection)
        current_html = self.conversation_display.toHtml()
        escaped_query = (
            query.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
            .replace("'", "&#39;")
        )
        user_message_html = f'<div style="margin: 10px 0;"><b style="color: #0066cc;">You:</b> {escaped_query}</div>'
        processing_html = f'<div style="margin: 10px 0;"><b style="color: #009900;">Agent:</b> <i>Processing...</i></div>'
        self.conversation_display.setHtml(
            f"{current_html}{user_message_html}{processing_html}"
        )

        # Scroll to bottom
        scroll_bar = self.conversation_display.verticalScrollBar()
        if scroll_bar:
            scroll_bar.setValue(scroll_bar.maximum())

        self.query_input.clear()

        # Add user message to chat history
        self.add_message_to_chat("user", query)

        # Create and start worker thread
        self.worker = QueryWorker(query, self.dataframe)
        self.worker.result_signal.connect(self.display_result)
        self.worker.error_signal.connect(self.display_error)
        self.worker.finished.connect(self.on_query_finished)
        self.worker.start()

    def stop_query(self):
        """Stop the currently running query"""
        if self.worker and self.worker.isRunning():
            self.worker.terminate()
            self.worker.wait()

            # Display cancellation message
            current_html = self.conversation_display.toHtml()
            if "<i>Processing...</i>" in current_html:
                cancelled_html = '<div style="margin: 10px 0; color: #ff6600;"><b>Status:</b> <i>Query cancelled by user.</i></div>'
                current_html = current_html.replace(
                    '<div style="margin: 10px 0;"><b style="color: #009900;">Agent:</b> <i>Processing...</i></div>',
                    cancelled_html,
                )
                self.conversation_display.setHtml(current_html)

        self.is_running = False
        self.submit_button.setText("Send")

    def on_query_finished(self):
        """Handle query worker finishing"""
        self.is_running = False
        self.submit_button.setText("Send")

    def display_result(self, result: str):
        """Display query result with proper formatting for code"""
        # Format the result content using backend markdown to HTML converter
        formatted_result = self.backend.markdown_to_html(result)

        # Replace the "Processing..." message with the actual result
        current_html = self.conversation_display.toHtml()

        # Remove the processing message and add the formatted result
        if "<i>Processing...</i>" in current_html:
            result_html = f'<div style="margin: 10px 0;"><b style="color: #009900;">Agent:</b> {formatted_result}</div>'
            current_html = current_html.replace(
                f'<div style="margin: 10px 0;"><b style="color: #009900;">Agent:</b> <i>Processing...</i></div>',
                result_html,
            )
            self.conversation_display.setHtml(current_html)
        else:
            result_html = f'<div style="margin: 10px 0;"><b style="color: #009900;">Agent:</b> {formatted_result}</div>'
            self.conversation_display.setHtml(f"{current_html}{result_html}")

        # Scroll to bottom
        scroll_bar = self.conversation_display.verticalScrollBar()
        if scroll_bar:
            scroll_bar.setValue(scroll_bar.maximum())

        # Add to chat history
        self.add_message_to_chat("assistant", result)

    def display_error(self, error: str):
        """Display error message"""
        escaped_error = (
            error.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
            .replace("'", "&#39;")
        )
        error_html = f'<div style="margin: 10px 0; color: #cc0000;"><b>Error:</b> {escaped_error}</div>'
        current_html = self.conversation_display.toHtml()

        if "<i>Processing...</i>" in current_html:
            current_html = current_html.replace(
                f'<div style="margin: 10px 0;"><b style="color: #009900;">Agent:</b> <i>Processing...</i></div>',
                error_html,
            )
            self.conversation_display.setHtml(current_html)
        else:
            self.conversation_display.setHtml(f"{current_html}{error_html}")

    def clear_fields(self):
        """Clear conversation"""
        self.conversation_display.clear()
        self.query_input.clear()
        self.backend.clear_session()

    def save_project(self):
        """Save current project with all chats"""
        if self.project_id is None:
            QMessageBox.warning(self, "No Project", "No project is currently loaded.")
            return

        if self.project_id in self.backend.projects:
            success, msg = self.backend.save_project_to_disk(self.project_id)
            if success:
                QMessageBox.information(self, "Project Saved", msg)
            else:
                QMessageBox.warning(self, "Save Failed", msg)
        else:
            QMessageBox.warning(self, "No Project", "Project not found.")
            QMessageBox.warning(
                self, "Project Not Found", "Could not find project to save."
            )

    def refresh_chat_list(self):
        """Refresh the chat list in the sidebar for the active project."""
        self.chat_list.clear()

        if self.backend.active_project is None:
            return

        # Populate chat list with all chats in the active project
        for chat in self.backend.active_project.get_all_chats():
            item = QListWidgetItem(chat.title)
            item.setData(Qt.ItemDataRole.UserRole, chat.session_id)
            self.chat_list.addItem(item)

    def refresh_project_list(self):
        """Refresh the entire UI when a project is loaded."""
        # Update project name label
        if self.backend.active_project:
            self.project_name_label.setText(
                f"Project: {self.backend.active_project.title}"
            )
        else:
            self.project_name_label.setText("No Project Loaded")

        # Refresh chat list
        self.refresh_chat_list()

    def add_message_to_chat(self, role: str, content: str):
        """Add a message to the active chat"""
        if self.chat_id and self.backend.active_chat:
            self.backend.add_message_to_session(role, content)


def main():
    """Main application entry point."""
    app = QApplication(sys.argv)

    # Check if API keys are configured, if not prompt user to set them up
    if not ConfigManager.has_any_api_key():
        # Show API key configuration dialog
        api_config_dialog = APIKeyConfigDialog()
        if api_config_dialog.exec() != QDialog.DialogCode.Accepted:
            # User cancelled API key setup
            QMessageBox.warning(
                None,
                "API Key Required",
                "An API key is required to use this application.\n"
                "Please configure your API key and try again.",
            )
            return

    project_dialog = CreateProjectDialog()
    if project_dialog.exec() != QDialog.DialogCode.Accepted:
        return

    window = DataWorkspaceGUI()
    window.backend = project_dialog.backend
    window.project_id = project_dialog.project_id

    # Load the project and setup the UI
    if window.project_id is not None:
        window.backend.load_project(window.project_id)
        # Load the first chat from the project
        if window.backend.active_project:
            chats = window.backend.active_project.get_all_chats()
            if chats:
                window.backend.load_chat_session(chats[0].session_id)
                window.chat_id = chats[0].session_id

    # Refresh the UI with project and chat info
    window.refresh_project_list()

    # Check if data is already loaded from project (e.g., loading saved project with data source)
    data_already_loaded = window.backend.loaded_dataframe is not None

    # Only ask for data source if no data is already loaded
    if data_already_loaded:
        # Data was already loaded from project, generate welcome message
        if window.backend.loaded_dataframe is not None:
            df = window.backend.loaded_dataframe
            # Check what type of source was used
            if (
                window.backend.active_project
                and window.backend.active_project.data_source
            ):
                ds = window.backend.active_project.data_source
                if ds.get("db_type"):
                    # Database source
                    selected_tables = ds.get("table", [])
                    if not isinstance(selected_tables, list):
                        selected_tables = [selected_tables]
                    welcome_msg = window.backend.format_database_welcome_message(
                        ds.get("db_type"),
                        selected_tables,
                        df,
                        "Data loaded from project",
                    )
                    window.conversation_display.setHtml(welcome_msg)
                elif ds.get("file_paths"):
                    # File source
                    welcome_msg = window.backend.load_file_data_with_ui(
                        ds.get("file_paths", [])
                    )[1]
                    window.conversation_display.setHtml(welcome_msg)
                    window.dataframe = df
            window.dataframe = df
    else:
        # No data loaded yet, ask for data source
        source_dialog = DataSourceDialog()
        if source_dialog.exec() != QDialog.DialogCode.Accepted:
            return

        source_type = source_dialog.data_source_type
        source_config = source_dialog.data_source_config

        try:
            if source_type == "database":
                db_type: str = source_config["db_type"]
                credentials: Dict[str, Any] = source_config["credentials"]

                connector = DatabaseConnector()
                while True:
                    success, message = connector.connect(db_type, credentials)
                    if success:
                        break

                    retry = QMessageBox.question(
                        None,
                        "Database Connection Failed",
                        f"{message}\n\nWould you like to try again?",
                        QMessageBox.StandardButton.Retry
                        | QMessageBox.StandardButton.Cancel,
                    )

                    if retry == QMessageBox.StandardButton.Retry:
                        db_dialog = DatabaseConnectionDialog()
                        if db_dialog.exec() != QDialog.DialogCode.Accepted:
                            return
                        source_config = db_dialog.get_config()
                        db_type = source_config["db_type"]
                        credentials = source_config["credentials"]
                    else:
                        return

                try:
                    tables = connector.get_tables()
                except Exception as e:
                    QMessageBox.critical(None, "Table Discovery Failed", str(e))
                    return
                finally:
                    connector.close()

                if not tables:
                    QMessageBox.critical(
                        None,
                        "No Tables Found",
                        "The database does not contain any tables.",
                    )
                    return

                table_dialog = TableSelectionDialog(tables)
                if table_dialog.exec() != QDialog.DialogCode.Accepted:
                    return

                selected_tables = table_dialog.get_selected_tables()
                table_value: Any = (
                    selected_tables if len(selected_tables) > 1 else selected_tables[0]
                )
                source_config["table"] = table_value

                merged_dataframe, status = load_data("database", source_config)

                if merged_dataframe is not None:
                    window.dataframe = merged_dataframe
                    window.backend.loaded_dataframe = merged_dataframe
                    # Store data source in the project
                    if window.backend.active_project:
                        creds_to_store = credentials.copy()
                        if "password" in creds_to_store:
                            creds_to_store["password"] = ""
                        window.backend.active_project.data_source = {
                            "db_type": db_type,
                            "credentials": creds_to_store,
                            "table": selected_tables,
                        }
                    welcome_msg = window.backend.format_database_welcome_message(
                        db_type, selected_tables, merged_dataframe, status
                    )
                    window.conversation_display.setHtml(welcome_msg)
                else:
                    window.conversation_display.setHtml(
                        f"<p>Connected to {db_type} database, but no data loaded.</p>"
                        f"<p>{status}</p><p>You can still ask questions!</p>"
                    )

            elif source_type == "file":
                file_paths = source_config["file_paths"]
                merged_dataframe, welcome_msg = window.backend.load_file_data_with_ui(
                    file_paths
                )

                if merged_dataframe is not None:
                    window.dataframe = merged_dataframe
                    # Store file paths in the project
                    if window.backend.active_project:
                        window.backend.active_project.data_source = {
                            "file_paths": file_paths
                        }
                    window.conversation_display.setHtml(welcome_msg)
                else:
                    window.conversation_display.setHtml(welcome_msg)
                    QMessageBox.critical(
                        window, "Data Loading Error", "Failed to load any files."
                    )

            else:
                QMessageBox.critical(
                    window, "Error", f"Unknown source type: {source_type}"
                )
                return

        except Exception as e:
            error_msg = f"""<h3 style="color: #cc0000;">Error Loading Data</h3>
            <p>{str(e)}</p>
            <p>Please restart and try again.</p>"""
            window.conversation_display.setHtml(error_msg)

    window.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
