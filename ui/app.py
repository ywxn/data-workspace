"""Application startup and initialization."""
import sys
import os
import json
from typing import Optional, Dict, Any
from PySide6.QtWidgets import QApplication, QMessageBox, QDialog
from PySide6.QtGui import QPalette
from core.config import ConfigManager
from core.constants import DARK_THEME_STYLESHEET, LIGHT_THEME_STYLESHEET
from core.markdown import markdown_to_html
from core.logger import get_logger
from db.connector import DatabaseConnector
from db.processing import load_data
from db.nlp_selector import NLPTableSelector
from ui.main_window import DataWorkspaceGUI
from ui.dialogs.interaction import InteractionModeDialog
from ui.dialogs.settings import APIKeyDialog
from ui.dialogs.llm_host import AIHostConfigDialog
from ui.dialogs.data_source import (
    DataSourceDialog, DatabaseConnectionDialog,
    MultiDatabaseConnectionDialog, select_tables_with_method,
)
from ui.dialogs.project import CreateProjectDialog
logger = get_logger(__name__)


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

        window._autosave_project()
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

        window._autosave_project()
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
    from db.processing import load_multi_database

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
    window._autosave_project()
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
