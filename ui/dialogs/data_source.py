"""Data source connection and table selection dialogs."""
import os
import json
import random
from typing import Optional, Dict, Any, List
from PySide6.QtCore import Qt
from PySide6.QtGui import QFont, QIcon
from PySide6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QFormLayout, QDialogButtonBox,
    QLabel, QLineEdit, QComboBox, QPushButton, QCheckBox, QSpinBox,
    QMessageBox, QListWidget, QListWidgetItem, QFileDialog, QGroupBox,
    QWidget,
)
from core.config import ConfigManager
from core.constants import NLP_PLACEHOLDER_TEXT
from core.logger import get_logger
from db.connector import DatabaseConnector
from db.nlp_selector import NLPTableSelector
from agents import AIAgent
logger = get_logger(__name__)


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
        self.form_layout = form_layout

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
        # Default to manual table selection
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
        self.sqlite_browse_button = QPushButton("Browse...")
        self.sqlite_browse_button.setFixedWidth(80)
        self.sqlite_browse_button.clicked.connect(self._browse_sqlite_file)
        self.sqlite_browse_button.setVisible(False)
        db_row_widget = QWidget()
        db_row_layout = QHBoxLayout(db_row_widget)
        db_row_layout.setContentsMargins(0, 0, 0, 0)
        db_row_layout.addWidget(self.database_input)
        db_row_layout.addWidget(self.sqlite_browse_button)
        form_layout.addRow("Database:", db_row_widget)

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
        self.semantic_layer_button.setToolTip(
            "Optionally import a semantic layer mapping from a JSON file.\n"
            "This can help the system understand domain-specific terminology\n"
            "and relationships in your database for better automatic table\n"
            "selection and query generation."
        )
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

        # Hide host/port/user/password rows (label + field) for SQLite
        self.form_layout.setRowVisible(self.host_input, not is_sqlite)
        self.form_layout.setRowVisible(self.port_input, not is_sqlite)
        self.form_layout.setRowVisible(self.user_input, not is_sqlite)
        self.form_layout.setRowVisible(self.password_input, not is_sqlite)

        # Update placeholder and browse button for database field
        if is_sqlite:
            self.database_input.setPlaceholderText("Path to .db, .sqlite, or .sql file")
            self.sqlite_browse_button.setVisible(True)
        else:
            self.database_input.setPlaceholderText("Database name")
            self.sqlite_browse_button.setVisible(False)

    def _browse_sqlite_file(self):
        """Open a file picker for SQLite .db/.sqlite/.sql files."""
        path, _ = QFileDialog.getOpenFileName(
            self,
            "Select SQLite Database",
            self.database_input.text() or "",
            "SQLite / SQL Files (*.db *.sqlite *.sql);;All Files (*)",
        )
        if path:
            self.database_input.setText(path)

    def on_selection_method_changed(self, method_text):
        """Keep semantic layer controls available for all database workflows."""
        logger.debug(f"Table selection method changed to: {method_text}")
        self.semantic_layer_container.setVisible(True)

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

        semantic_layer = config.get("semantic_layer")
        if semantic_layer:
            self.semantic_layer = semantic_layer
            self.semantic_layer_label.setText("Semantic layer restored from config")

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
        display = f"[{alias}]  {config['db_type']} \u2014 {config['credentials'].get('database', '?')}"
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
