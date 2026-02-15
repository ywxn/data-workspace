"""Backend logic for the AI Data Workspace GUI (Qt Markdown version)."""

from typing import Any, Dict, List, Tuple, Optional
import pandas as pd
from dataclasses import dataclass, field
from datetime import datetime
import re
import os
import json
from connector import DatabaseConnector
from processing import load_data
from logger import get_logger

logger = get_logger(__name__)


@dataclass
class ChatSession:
    """Represents a chat session with metadata."""

    session_id: str
    title: str
    created_at: datetime
    messages: List[Dict[str, str]]
    data_source: Optional[Dict[str, Any]] = None

    def add_message(self, role: str, content: str) -> None:
        """Add a message to the session."""
        if not role or not content:
            return
        self.messages.append({"role": role, "content": content})

    def get_history(self) -> List[Dict[str, str]]:
        """Return full message history."""
        return list(self.messages)

    def get_last_n(self, n: int = 10) -> List[Dict[str, str]]:
        """Return the last n messages."""
        if n <= 0:
            return []
        return self.messages[-n:]

    def clear_messages(self) -> None:
        """Clear all messages in the session."""
        self.messages.clear()


@dataclass
class Project:
    """Represents a project that contains multiple chat sessions."""

    project_id: str
    title: str
    description: str
    created_at: datetime
    chats: Dict[str, ChatSession] = field(default_factory=dict)
    data_source: Optional[Dict[str, Any]] = None

    def add_chat(self, chat: ChatSession) -> None:
        """Add a chat session to the project."""
        self.chats[chat.session_id] = chat

    def get_chat(self, chat_id: str) -> Optional[ChatSession]:
        """Get a chat session by ID."""
        return self.chats.get(chat_id)

    def get_all_chats(self) -> List[ChatSession]:
        """Get all chat sessions in the project."""
        return list(self.chats.values())

    def delete_chat(self, chat_id: str) -> bool:
        """Delete a chat session from the project."""
        if chat_id in self.chats:
            del self.chats[chat_id]
            return True
        return False


class DataWorkspaceBackend:
    """Backend manager for the AI Data Workspace GUI."""

    def __init__(self):
        """Initialize the backend."""
        self.active_project: Optional[Project] = None
        self.active_chat: Optional[ChatSession] = None
        self.projects: Dict[str, Project] = {}
        self.loaded_dataframe: Optional[pd.DataFrame] = None
        self.schema_metadata: Optional[Dict[str, Any]] = None

    @staticmethod
    def markdown_to_qt(text: str) -> str:
        """Return markdown text for Qt rendering."""
        return text

    # ==================== Project Management ====================

    def create_project(
        self, project_name: str, description: str
    ) -> Tuple[bool, str, Optional[str]]:
        """
        Create a new project workspace.

        Args:
            project_name: Name of the project
            description: Project description

        Returns:
            Tuple of (success: bool, message: str, project_id: str or None)
        """
        # Ensure uniqueness by title
        if any(p.title == project_name for p in self.projects.values()):
            logger.warning(
                f"Attempted to create project with duplicate name: {project_name}"
            )
            return False, "Project name already exists.", None

        project_id = str(len(self.projects) + 1)
        project = Project(
            project_id=project_id,
            title=project_name,
            description=description,
            created_at=datetime.now(),
        )
        self.projects[project_id] = project
        self.active_project = project

        logger.info(f"Project created: {project_name} (ID: {project_id})")
        return True, "Project created successfully.", project_id

    def save_project_to_disk(self, project_id: str) -> Tuple[bool, str]:
        """
        Save a project and all its chats to the ./projects directory as a JSON file.

        Args:
            project_id: ID of the project to save

        Returns:
            Tuple of (success: bool, message: str)
        """
        if project_id not in self.projects:
            logger.warning(f"Attempted to save non-existent project: {project_id}")
            return False, "Project not found."

        project = self.projects[project_id]

        try:
            os.makedirs("projects", exist_ok=True)
            safe_title = re.sub(r"[^A-Za-z0-9_-]", "_", project.title)[:50]
            filename = f"{project_id}_{safe_title}.json"
            path = os.path.join("projects", filename)

            # Serialize all chats in the project
            chats_data = []
            for chat in project.get_all_chats():
                chats_data.append(
                    {
                        "session_id": chat.session_id,
                        "title": chat.title,
                        "created_at": chat.created_at.isoformat(),
                        "messages": chat.messages,
                        "data_source": chat.data_source,
                    }
                )

            data = {
                "project_id": project.project_id,
                "title": project.title,
                "description": project.description,
                "created_at": project.created_at.isoformat(),
                "chats": chats_data,
                "data_source": project.data_source,
                "file_name": filename,
            }

            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

            logger.info(f"Project saved: {project.title} to {path}")
            return True, f"Project saved to {os.path.abspath(path)}"
        except Exception as e:
            logger.error(f"Error saving project {project_id}: {str(e)}", exc_info=True)
            return False, str(e)

    def list_saved_projects(self) -> List[str]:
        """List saved project files under ./projects."""
        try:
            if not os.path.isdir("projects"):
                return []
            files = [
                f
                for f in os.listdir("projects")
                if os.path.isfile(os.path.join("projects", f))
            ]
            return files
        except Exception:
            return []

    def load_project_from_disk(
        self, file_name: str
    ) -> Tuple[bool, str, Optional[Project]]:
        """
        Load a project from the ./projects directory and restore it.

        Args:
            file_name: Name of the project file to load

        Returns:
            Tuple of (success: bool, message: str, project: Project or None)
        """
        path = os.path.join("projects", file_name)
        if not os.path.isfile(path):
            logger.warning(f"Project file not found: {path}")
            return False, "Project file not found.", None

        try:
            logger.info(f"Loading project from {path}")
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)

            project_id = data.get("project_id", str(len(self.projects) + 1))
            title = data.get("title", f"Project {project_id}")
            description = data.get("description", "")

            created_at_str = data.get("created_at")
            try:
                created_at = (
                    datetime.fromisoformat(created_at_str)
                    if created_at_str
                    else datetime.now()
                )
            except Exception:
                created_at = datetime.now()

            data_source = data.get("data_source") or {}
            data_source["file_name"] = file_name

            project = Project(
                project_id=project_id,
                title=title,
                description=description,
                created_at=created_at,
                data_source=data_source,
            )

            # Load all chats from the project
            chats_data = data.get("chats", [])
            for chat_data in chats_data:
                chat = ChatSession(
                    session_id=chat_data.get("session_id"),
                    title=chat_data.get("title"),
                    created_at=datetime.fromisoformat(
                        chat_data.get("created_at", datetime.now().isoformat())
                    ),
                    messages=chat_data.get("messages", []),
                    data_source=chat_data.get("data_source"),
                )
                project.add_chat(chat)

            self.projects[project_id] = project
            self.active_project = project

            # Load the first chat if available
            chats = project.get_all_chats()
            if chats:
                self.active_chat = chats[0]

            logger.info(f"Project loaded successfully: {title}")
            return True, f"Loaded project '{title}'", project
        except Exception as e:
            logger.error(f"Error loading project {file_name}: {str(e)}", exc_info=True)
            return False, str(e), None

    def save_project(self) -> Tuple[bool, str]:
        """Save the current project."""
        if self.active_project is None:
            return False, "No active project to save."

        return self.save_project_to_disk(self.active_project.project_id)

    def load_project(self, project_id: str) -> Tuple[bool, str]:
        """Load an existing project."""
        if project_id not in self.projects:
            return False, "Project not found."

        self.active_project = self.projects[project_id]
        # Load the first chat as active
        chats = self.active_project.get_all_chats()
        if chats:
            self.active_chat = chats[0]
        return True, "Project loaded successfully."

    def get_project_info(self) -> Optional[Dict[str, Any]]:
        """Get information about the current project."""
        if self.active_project is None:
            return None

        return {
            "project_id": self.active_project.project_id,
            "title": self.active_project.title,
            "description": self.active_project.description,
            "created_at": self.active_project.created_at,
            "chats_count": len(self.active_project.chats),
        }

    # ==================== Data Source Management ====================

    def connect_data_source(
        self, source_type: str, config: Dict[str, Any]
    ) -> Tuple[bool, str]:
        """Connect to a data source (database, CSV, Excel)."""
        try:
            if source_type == "database":
                db_type = config.get("db_type")
                credentials = config.get("credentials")
                # Assuming DatabaseConnector is properly implemented
                connector = DatabaseConnector()
                success, message = connector.connect(db_type, credentials)
                return success, message
            elif source_type in ["csv", "excel"]:
                # For CSV and Excel, you might just validate the file path
                file_path = config.get("file_path")
                if not file_path:
                    return False, "File path is required."
                return True, "File path validated."
            else:
                return False, "Unsupported source type."
        except Exception as e:
            return False, str(e)

    def validate_connection(self) -> Tuple[bool, str]:
        """Validate the current data source connection."""
        if self.loaded_dataframe is not None:
            return True, "Connection is valid."
        return False, "No active connection."

    def load_schema(self) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
        """Load schema metadata from the connected data source."""
        if self.loaded_dataframe is not None:
            schema = {
                "columns": list(self.loaded_dataframe.columns),
                "dtypes": self.loaded_dataframe.dtypes.astype(str).to_dict(),
            }
            return True, "Schema loaded successfully.", schema
        return False, "No data loaded.", None

    def get_available_tables(self) -> List[str]:
        """Get list of available tables from the data source."""
        if self.loaded_dataframe is not None:
            name = self.loaded_dataframe.name or "table"
            return [str(name)]
        return []

    # ==================== Chat Session Management ====================

    def create_chat_session(
        self, title: Optional[str] = None
    ) -> Tuple[bool, str, Optional[str]]:
        """Create a new chat session in the active project."""
        if self.active_project is None:
            return False, "No active project.", None

        chat_title = title or f"Chat {len(self.active_project.chats) + 1}"
        chat_id = str(len(self.active_project.chats) + 1)

        chat = ChatSession(
            session_id=chat_id,
            title=chat_title,
            created_at=datetime.now(),
            messages=[],
            data_source=None,
        )

        self.active_project.add_chat(chat)
        self.active_chat = chat
        return True, "Chat session created successfully.", chat_id

    def get_chat_sessions(self) -> List[Dict[str, Any]]:
        """Get list of all chat sessions in the active project."""
        if self.active_project is None:
            return []

        return [
            {
                "session_id": chat.session_id,
                "title": chat.title,
                "created_at": chat.created_at,
                "messages_count": len(chat.messages),
            }
            for chat in self.active_project.get_all_chats()
        ]

    def load_chat_session(self, chat_id: str) -> Tuple[bool, str]:
        """Load a specific chat session from the active project."""
        if self.active_project is None:
            return False, "No active project."

        chat = self.active_project.get_chat(chat_id)
        if chat is None:
            return False, "Chat session not found."

        self.active_chat = chat
        return True, "Chat session loaded successfully."

    def delete_chat_session(self, chat_id: str) -> Tuple[bool, str]:
        """Delete a chat session from the active project."""
        if self.active_project is None:
            return False, "No active project."

        if not self.active_project.delete_chat(chat_id):
            return False, "Chat session not found."

        if self.active_chat and self.active_chat.session_id == chat_id:
            # Switch to another chat if available
            remaining_chats = self.active_project.get_all_chats()
            self.active_chat = remaining_chats[0] if remaining_chats else None

        return True, "Chat session deleted successfully."

    def get_chat_history(self, chat_id: Optional[str] = None) -> List[Dict[str, str]]:
        """Get chat history for a session."""
        if chat_id:
            if self.active_project is None:
                return []
            chat = self.active_project.get_chat(chat_id)
            return chat.get_history() if chat else []

        if self.active_chat is None:
            return []
        return self.active_chat.get_history()

    # ==================== Message Management ====================

    def add_message_to_session(self, role: str, content: str) -> Tuple[bool, str]:
        """Add a message to the active chat session."""
        if self.active_chat is None:
            return False, "No active chat session."
        self.active_chat.add_message(role, content)
        return True, "Message added successfully."

    def get_last_n_messages(self, n: int = 10) -> List[Dict[str, str]]:
        """Get the last n messages from active session."""
        if self.active_chat is None:
            return []
        return self.active_chat.get_last_n(n)

    # ==================== Error Handling ====================

    def handle_error(
        self, error_message: str, context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Handle and format error messages for user."""

        return {
            "error": error_message,
            "context": context,
            "timestamp": datetime.now().isoformat(),
        }

    def get_error_suggestions(self, error: str) -> List[str]:
        """Get suggestions to fix an error."""
        raise NotImplementedError()

    # ==================== Data Export ====================

    def export_results(
        self, data: pd.DataFrame, format: str, file_path: str
    ) -> Tuple[bool, str]:
        """Export query results to file (CSV, Excel, JSON)."""
        raise NotImplementedError()

    def export_chat_session(
        self, session_id: str, format: str
    ) -> Tuple[bool, str, Optional[str]]:
        """Export chat session to file."""
        raise NotImplementedError()

    # ==================== Utility Functions ====================

    def get_data_preview(self, limit: int = 5) -> Optional[pd.DataFrame]:
        """Get preview of loaded data."""
        if self.loaded_dataframe is not None:
            return self.loaded_dataframe.head(limit)
        return None

    def get_column_info(self) -> Optional[Dict[str, Any]]:
        """Get information about columns in loaded data."""
        if self.loaded_dataframe is not None:
            return {
                "columns": list(self.loaded_dataframe.columns),
                "dtypes": self.loaded_dataframe.dtypes.astype(str).to_dict(),
                "non_null_counts": self.loaded_dataframe.count().to_dict(),
            }
        return None

    def clear_session(self) -> Tuple[bool, str]:
        """Clear the active chat session."""
        if self.active_chat is None:
            return False, "No active chat session."
        self.active_chat.clear_messages()
        return True, "Session cleared."

    # ==================== Data Loading ====================

    def load_file_data_with_ui(
        self, file_paths: List[str]
    ) -> Tuple[Optional[pd.DataFrame], str]:
        """Load and merge data from multiple files, returning dataframe and markdown welcome message."""
        from processing import load_data, merge_dataframes

        dataframes = []
        file_info = []
        errors = []

        for file_path in file_paths:
            try:
                if file_path.lower().endswith(".csv"):
                    df, status = load_data("csv", {"file_path": file_path})
                elif file_path.lower().endswith((".xlsx", ".xls")):
                    df, status = load_data("excel", {"file_path": file_path})
                else:
                    errors.append(f"Unsupported file type: {file_path}")
                    continue

                if df is not None:
                    dataframes.append(df)
                    file_info.append(
                        {
                            "name": file_path.split("/")[-1].split("\\")[-1],
                            "rows": len(df),
                            "columns": len(df.columns),
                        }
                    )
                else:
                    errors.append(f"Failed to load {file_path}: {status}")
            except Exception as e:
                errors.append(f"Error loading {file_path}: {str(e)}")

        if not dataframes:
            error_detail = "\n".join([f"- {err}" for err in errors]) if errors else "- Unknown error"
            return (
                None,
                f"Failed to load any files:\n{error_detail}",
            )

        merged_dataframe, merge_strategy = merge_dataframes(dataframes)
        self.loaded_dataframe = merged_dataframe

        all_columns = merged_dataframe.columns.tolist()
        if len(all_columns) <= 50:
            columns_list = ", ".join(all_columns)
        else:
            first_cols = ", ".join(all_columns[:30])
            last_cols = ", ".join(all_columns[-20:])
            columns_list = (
                f"{first_cols}, ... ({len(all_columns) - 50} more) ..., {last_cols}"
            )

        files_detail = "\n".join(
            [
                f"- {info['name']}: {info['rows']} rows, {info['columns']} columns"
                for info in file_info
            ]
        )

        merge_info = (
            f"**Merge Strategy:** {merge_strategy}\n" if merge_strategy else ""
        )
        file_word = "file" if len(file_info) == 1 else "files"

        welcome_msg = (
            "### Data Loaded Successfully\n"
            f"**Loaded {len(file_info)} {file_word}:**\n{files_detail}\n"
            f"{merge_info}"
            f"**Combined Shape:** {len(merged_dataframe)} rows, {len(merged_dataframe.columns)} columns\n"
            f"**Columns:** {columns_list}\n"
        )

        if errors:
            error_detail = "\n".join([f"- {err}" for err in errors])
            welcome_msg += f"\n**Warnings:**\n{error_detail}\n"

        welcome_msg += (
            "\nReady to analyze your data! Try asking questions like:\n"
            "- What insights can you find in this data?\n"
            "- Show me a summary of the data\n"
            "- What trends are visible?"
        )

        return merged_dataframe, welcome_msg

    def format_database_welcome_message(
        self,
        db_type: str,
        selected_tables: Any,
        merged_dataframe: pd.DataFrame,
        status: str,
    ) -> str:
        """Format welcome message for database data loading."""
        all_columns = merged_dataframe.columns.tolist()
        if len(all_columns) <= 50:
            columns_list = ", ".join(all_columns)
        else:
            first_cols = ", ".join(all_columns[:30])
            last_cols = ", ".join(all_columns[-20:])
            columns_list = (
                f"{first_cols}, ... ({len(all_columns) - 50} more) ..., {last_cols}"
            )

        if isinstance(selected_tables, list):
            tables_detail = "\n".join([f"- {table}" for table in selected_tables])
            merge_info = ""
            if "Merge strategy:" in status:
                merge_strategy = status.split("Merge strategy:")[1].strip()
                merge_info = f"**Merge Strategy:** {merge_strategy}\n"

            table_word = "table" if len(selected_tables) == 1 else "tables"
            welcome_msg = (
                "### Data Loaded Successfully\n"
                f"**Loaded from {db_type} database ({len(selected_tables)} {table_word}):**\n{tables_detail}\n"
                f"{merge_info}"
                f"**Combined Shape:** {len(merged_dataframe)} rows, {len(merged_dataframe.columns)} columns\n"
                f"**Columns:** {columns_list}\n"
                "\nReady to analyze your data! Try asking questions like:\n"
                "- What insights can you find in this data?\n"
                "- Show me a summary of the data\n"
                "- What trends are visible?"
            )
        else:
            welcome_msg = (
                "### Data Loaded Successfully\n"
                f"**Loaded from {db_type} database**\n"
                f"**Table:** {selected_tables}\n"
                f"**Shape:** {len(merged_dataframe)} rows, {len(merged_dataframe.columns)} columns\n"
                f"**Columns:** {columns_list}\n"
                "\nReady to analyze your data! Try asking questions like:\n"
                "- What insights can you find in this data?\n"
                "- Show me a summary of the data\n"
                "- What trends are visible?"
            )

        return welcome_msg
