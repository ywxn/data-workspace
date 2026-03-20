"""Main application window."""
import sys
import asyncio
import webbrowser
import json
import os
import random
from datetime import datetime
from typing import Optional, Dict, Any, List
from PySide6.QtCore import Qt, Signal, QTimer
from PySide6.QtGui import QFont, QAction, QActionGroup, QIcon, QPalette
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QTextEdit, QPushButton, QLabel, QListWidget, QListWidgetItem,
    QFileDialog, QMessageBox, QMenu, QComboBox, QDialog,
)
from core.config import ConfigManager
from core.constants import (
    NLP_PLACEHOLDER_TEXT, PLACEHOLDER_PROJECT_NAMES,
    PLACEHOLDER_PROJECT_DESCRIPTIONS, DARK_THEME_STYLESHEET,
    LIGHT_THEME_STYLESHEET,
)
from core.markdown import markdown_to_html
from core.logger import get_logger
from db.connector import DatabaseConnector
from db.processing import load_data, add_files_to_sqlite
from db.nlp import NLPTableSelector
from agents import AIAgent
from ui.backend import DataWorkspaceBackend
from ui.widgets import MessageTextEdit
from ui.workers import QueryWorker
from ui.dialogs.interaction import InteractionModeDialog
from ui.dialogs.settings import APIKeyDialog, ModelSettingsDialog, MemoryRetentionDialog
from ui.dialogs.llm_host import AIHostConfigDialog, LocalLLMSettingsDialog
from ui.dialogs.data_source import (
    DataSourceDialog, DatabaseConnectionDialog,
    MultiDatabaseConnectionDialog, TableSelectionDialog,
    NLPPromptDialog, select_tables_with_method,
)
from ui.dialogs.project import CreateProjectDialog, ProjectLoadDialog
logger = get_logger(__name__)


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

        memory_retention_action = QAction("Memory Retention Policy", self)
        memory_retention_action.triggered.connect(
            self.change_memory_retention_settings
        )
        settings_menu.addAction(memory_retention_action)

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
                        from db.processing import load_multi_database

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

    def change_memory_retention_settings(self):
        """Open memory retention policy settings dialog."""
        logger.info("User opened memory retention settings dialog")
        try:
            retention_dialog = MemoryRetentionDialog(self)
            if retention_dialog.exec() == QDialog.DialogCode.Accepted:
                logger.info("Memory retention settings updated successfully")
            else:
                logger.info("User cancelled memory retention settings change")
        except Exception as e:
            logger.error(
                f"Error changing memory retention settings: {str(e)}", exc_info=True
            )
            QMessageBox.critical(
                self,
                "Error",
                f"Failed to update memory retention settings: {str(e)}",
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
        """Delete persisted query memory for global index and active project."""
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        cache_file = os.path.join(
            base_dir,
            "data",
            "query_memory_index.jsonl",
        )
        project_cache_file = (
            os.path.join(base_dir, "projects", f"{self.project_id}_memory.jsonl")
            if self.project_id
            else None
        )

        reply = QMessageBox.warning(
            self,
            "Clear Query Cache",
            "This will permanently delete query cache records.\n\n"
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
            deleted_targets = []

            if os.path.isfile(cache_file):
                os.remove(cache_file)
                logger.info(f"Query cache index deleted: {cache_file}")
                deleted_targets.append("global index")
            else:
                logger.info(f"Query cache index not found: {cache_file}")

            if project_cache_file and os.path.isfile(project_cache_file):
                os.remove(project_cache_file)
                logger.info(f"Project query cache deleted: {project_cache_file}")
                deleted_targets.append(f"project cache ({self.project_id})")
            elif project_cache_file:
                logger.info(f"Project query cache not found: {project_cache_file}")

            if deleted_targets:
                QMessageBox.information(
                    self,
                    "Query Cache Cleared",
                    "Deleted: " + ", ".join(deleted_targets) + ".",
                )
            else:
                QMessageBox.information(
                    self,
                    "Nothing To Clear",
                    "No query cache files were found for the global index or active project.",
                )
        except Exception as e:
            logger.error(f"Failed to clear query cache: {e}", exc_info=True)
            QMessageBox.critical(
                self, "Error", f"Failed to clear query cache: {e}"
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
