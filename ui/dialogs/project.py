"""Project management dialogs (create, load)."""
import os
import json
import random
from datetime import datetime
from typing import Optional, Dict, Any, List
from PySide6.QtCore import Qt
from PySide6.QtGui import QFont, QIcon
from PySide6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QFormLayout, QDialogButtonBox,
    QLabel, QLineEdit, QComboBox, QPushButton, QMessageBox, QListWidget,
    QListWidgetItem, QTextEdit, QWidget,
)
from core.config import ConfigManager
from core.constants import PLACEHOLDER_PROJECT_NAMES, PLACEHOLDER_PROJECT_DESCRIPTIONS
from core.logger import get_logger
from ui.backend import DataWorkspaceBackend
from db.connector import DatabaseConnector
from db.processing import load_data
from ui.dialogs.data_source import DatabaseConnectionDialog, select_tables_with_method

logger = get_logger(__name__)


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
            if parent and hasattr(parent, "refresh_project_list"):
                parent.refresh_project_list()
        except Exception:
            pass

        self.accept()
