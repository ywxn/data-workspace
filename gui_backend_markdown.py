"""Backend logic for the AI Data Workspace GUI (Qt Markdown version)."""

from typing import Any, Dict, List, Tuple, Optional
from dataclasses import dataclass, field
from datetime import datetime
import re
import os
import json
import shutil
import tempfile
import uuid
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
    runtime_context: Optional[Dict[str, Any]] = None

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
    semantic_layer: Optional[Dict[str, Any]] = None

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
        self.data_context: Optional[Dict[str, Any]] = None
        self.schema_metadata: Optional[Dict[str, Any]] = None

    @staticmethod
    def markdown_to_qt(text: str) -> str:
        """Return markdown text for Qt rendering."""
        return text

    @staticmethod
    def _join_markdown_blocks(blocks: List[str]) -> str:
        """Join markdown blocks with blank lines to avoid list bleed-through."""
        cleaned = [block.strip() for block in blocks if block and block.strip()]
        return "\n\n".join(cleaned)

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

        # Generate unique project ID using UUID
        project_id = str(uuid.uuid4())
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
            self._persist_chart_assets(project)
            os.makedirs("projects", exist_ok=True)
            safe_title = re.sub(r"[^A-Za-z0-9_-]", "_", project.title)[:50]
            filename = f"{safe_title}.json"
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
                "semantic_layer": project.semantic_layer,
                "file_name": filename,
            }

            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

            logger.info(f"Project saved: {project.title} to {path}")
            return True, f"Project saved to {os.path.abspath(path)}"
        except Exception as e:
            logger.error(f"Error saving project {project_id}: {str(e)}", exc_info=True)
            return False, str(e)

    @staticmethod
    def _get_temp_chart_dir() -> str:
        return os.path.abspath(
            os.path.join(tempfile.gettempdir(), "ai_data_workspace_charts")
        )

    def _persist_chart_assets(self, project: Project) -> None:
        graph_dir = os.path.abspath("graph")
        os.makedirs(graph_dir, exist_ok=True)
        temp_dir = self._get_temp_chart_dir()
        pattern = re.compile(r"!\[([^\]]*)\]\(([^)]+)\)")
        allowed_ext = {".svg", ".png", ".jpg", ".jpeg", ".gif"}

        logger.debug(f"Persisting chart assets for project: {project.project_id}")
        for chat in project.get_all_chats():
            for message in chat.messages:
                content = message.get("content", "")
                if not content:
                    continue
                updated = self._rewrite_chart_paths(
                    content, pattern, temp_dir, graph_dir, allowed_ext
                )
                if updated != content:
                    logger.debug(
                        f"Chart paths rewritten in message: {message.get('role')}"
                    )
                    message["content"] = updated

    @staticmethod
    def _rewrite_chart_paths(
        content: str,
        pattern: re.Pattern,
        temp_dir: str,
        graph_dir: str,
        allowed_ext: set,
    ) -> str:
        def replace(match: re.Match) -> str:
            alt_text = match.group(1)
            raw_path = match.group(2).strip().strip('"').strip("'")
            lower = raw_path.lower()
            if lower.startswith("http://") or lower.startswith("https://"):
                logger.debug(f"Skipping remote URL: {raw_path}")
                return match.group(0)

            path_candidate = raw_path
            if not os.path.isabs(path_candidate):
                path_candidate = os.path.abspath(path_candidate)

            ext = os.path.splitext(path_candidate)[1].lower()
            if ext not in allowed_ext:
                logger.debug(f"Unsupported file extension: {ext}")
                return match.group(0)

            if not os.path.exists(path_candidate):
                logger.warning(f"Chart file not found: {path_candidate}")
                return match.group(0)

            try:
                common = os.path.commonpath([path_candidate, temp_dir])
            except ValueError:
                common = ""

            if common != temp_dir:
                try:
                    common_graph = os.path.commonpath([path_candidate, graph_dir])
                except ValueError:
                    common_graph = ""
                if common_graph == graph_dir:
                    rel_path = os.path.relpath(path_candidate, os.getcwd())
                    rel_path = rel_path.replace("\\", "/")
                    logger.debug(f"Using relative path from graph dir: {rel_path}")
                    return f"![{alt_text}]({rel_path})"
                logger.debug(
                    f"Chart path outside temp/graph directories: {path_candidate}"
                )
                return match.group(0)

            base_name = os.path.basename(path_candidate)
            target = os.path.join(graph_dir, base_name)
            if os.path.exists(target):
                if os.path.getsize(target) != os.path.getsize(path_candidate):
                    name, ext_name = os.path.splitext(base_name)
                    counter = 1
                    while os.path.exists(target):
                        target = os.path.join(graph_dir, f"{name}_{counter}{ext_name}")
                        counter += 1
                    logger.debug(
                        f"File exists with different size, using new name: {target}"
                    )

            try:
                shutil.copy2(path_candidate, target)
                logger.info(f"Chart asset copied: {path_candidate} -> {target}")
            except Exception as e:
                logger.error(
                    f"Failed to copy chart asset: {path_candidate}", exc_info=True
                )
                return match.group(0)

            rel_path = os.path.relpath(target, os.getcwd())
            rel_path = rel_path.replace("\\", "/")
            return f"![{alt_text}]({rel_path})"

        return pattern.sub(replace, content)

    def list_saved_projects(self) -> List[str]:
        """List saved project files under ./projects.

        Only returns JSON files that contain valid project_id fields.
        """
        try:
            if not os.path.isdir("projects"):
                return []
            valid_projects = []

            for f in os.listdir("projects"):
                file_path = os.path.join("projects", f)

                # Only consider JSON files
                if not (os.path.isfile(file_path) and f.endswith(".json")):
                    continue

                # Validate that file contains a project_id
                try:
                    with open(file_path, "r", encoding="utf-8") as fp:
                        data = json.load(fp)
                        if isinstance(data, dict) and "project_id" in data:
                            valid_projects.append(f)
                except (json.JSONDecodeError, IOError):
                    # Skip files that can't be parsed or read
                    continue

            return valid_projects
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

            # Load or generate project ID, ensuring uniqueness
            project_id = data.get("project_id")
            if not project_id or project_id in self.projects:
                project_id = str(uuid.uuid4())
                logger.info(f"Generated new unique project ID: {project_id}")

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

            # Load semantic layer from project data
            semantic_layer = data.get("semantic_layer")

            project = Project(
                project_id=project_id,
                title=title,
                description=description,
                created_at=created_at,
                data_source=data_source,
                semantic_layer=semantic_layer,
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
        if self.data_context is not None:
            return True, "Connection is valid."
        return False, "No active connection."

    def load_schema(self) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
        """Load schema metadata from the connected data source."""
        if self.data_context is not None:
            table_info = self.data_context.get("table_info", {})
            schema = {
                "tables": self.data_context.get("tables", []),
                "columns": {
                    table: info.get("columns", []) for table, info in table_info.items()
                },
                "dtypes": {
                    table: info.get("column_types", {})
                    for table, info in table_info.items()
                },
            }
            return True, "Schema loaded successfully.", schema
        return False, "No data loaded.", None

    def get_available_tables(self) -> List[str]:
        """Get list of available tables from the data source."""
        if self.data_context is not None:
            return list(self.data_context.get("tables", []))
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
        """Get suggestions to fix an error based on common error patterns."""
        error_lower = error.lower()
        suggestions: List[str] = []

        # Connection / network errors
        if any(
            kw in error_lower
            for kw in [
                "connection",
                "connect",
                "refused",
                "timeout",
                "timed out",
                "unreachable",
            ]
        ):
            suggestions.extend(
                [
                    "Check that the database host and port are correct.",
                    "Verify that the database server is running.",
                    "Check your network connection and firewall settings.",
                ]
            )

        # Authentication errors
        if any(
            kw in error_lower
            for kw in [
                "authentication",
                "password",
                "credentials",
                "access denied",
                "login",
            ]
        ):
            suggestions.extend(
                [
                    "Double-check your database username and password.",
                    "Ensure the user has the required permissions.",
                ]
            )

        # API key errors
        if any(
            kw in error_lower
            for kw in [
                "api key",
                "api_key",
                "unauthorized",
                "401",
                "invalid key",
                "authentication_error",
            ]
        ):
            suggestions.extend(
                [
                    "Reconfigure your API key via File \u2192 API Settings.",
                    "Verify your API key has not expired or been revoked.",
                    "Check that you selected the correct AI provider.",
                ]
            )

        # Rate limit errors
        if any(
            kw in error_lower
            for kw in ["rate limit", "429", "too many requests", "quota"]
        ):
            suggestions.extend(
                [
                    "Wait a moment and try again.",
                    "Consider upgrading your API plan for higher limits.",
                    "Try a shorter or simpler query.",
                ]
            )

        # SQL syntax errors
        if any(
            kw in error_lower
            for kw in [
                "syntax error",
                "sql",
                "no such table",
                "no such column",
                "unknown column",
            ]
        ):
            suggestions.extend(
                [
                    "Check that the referenced table and column names exist.",
                    "Try rephrasing your question with different terms.",
                    "Use the schema viewer to see available tables and columns.",
                ]
            )

        # File-related errors
        if any(
            kw in error_lower
            for kw in [
                "file not found",
                "no such file",
                "permission denied",
                "encoding",
                "decode",
            ]
        ):
            suggestions.extend(
                [
                    "Verify the file path is correct and the file exists.",
                    "Ensure the file is a supported format (CSV, Excel).",
                    "Try re-saving the file with UTF-8 encoding.",
                ]
            )

        # Model / LLM errors
        if any(
            kw in error_lower
            for kw in ["model", "llama", "llm", "context length", "token"]
        ):
            suggestions.extend(
                [
                    "Try a shorter query to reduce token usage.",
                    "Check that your local LLM server is running (Settings \u2192 Local LLM Settings).",
                ]
            )

        # Fallback
        if not suggestions:
            suggestions.extend(
                [
                    "Try rephrasing your question.",
                    "Check the application logs for more details.",
                    "Restart the application and try again.",
                ]
            )

        return suggestions

    # ==================== Data Export ====================

    def export_results(
        self, data: Dict[str, Any], format: str, file_path: str
    ) -> Tuple[bool, str]:
        """Export query results to file (CSV, Excel, JSON).

        Args:
            data: Dict with 'columns' (list of str) and 'rows' (list of lists)
                  or any dict serialisable to a DataFrame.
            format: One of 'csv', 'excel', 'json'.
            file_path: Destination file path.

        Returns:
            Tuple of (success, message).
        """
        try:
            import pandas as pd  # local import to keep module light

            # Build DataFrame from structured data or raw dict
            if "columns" in data and "rows" in data:
                df = pd.DataFrame(data["rows"], columns=data["columns"])
            else:
                df = pd.DataFrame(data)

            fmt = format.lower()
            if fmt == "csv":
                df.to_csv(file_path, index=False)
            elif fmt in ("excel", "xlsx"):
                df.to_excel(file_path, index=False, engine="openpyxl")
            elif fmt == "json":
                df.to_json(file_path, orient="records", indent=2)
            else:
                return False, f"Unsupported export format: {format}"

            logger.info(f"Exported results to {file_path} ({fmt})")
            return True, f"Results exported to {file_path}"

        except ImportError:
            return False, "pandas is required for export but is not installed."
        except Exception as e:
            logger.error(f"Export failed: {e}")
            return False, f"Export failed: {str(e)}"

    def export_chat_session(
        self, session_id: str, format: str
    ) -> Tuple[bool, str, Optional[str]]:
        """Export a chat session's messages to a string.

        Args:
            session_id: ID of the chat session to export.
            format: One of 'json', 'markdown', 'txt'.

        Returns:
            Tuple of (success, message, content_string_or_None).
        """
        if self.active_project is None:
            return False, "No active project.", None

        chat = self.active_project.get_chat(session_id)
        if chat is None:
            return False, f"Chat session '{session_id}' not found.", None

        messages = chat.get_history()
        fmt = format.lower()

        try:
            if fmt == "json":
                content = json.dumps(
                    {
                        "session_id": chat.session_id,
                        "title": chat.title,
                        "created_at": chat.created_at.isoformat(),
                        "messages": messages,
                    },
                    indent=2,
                )
            elif fmt in ("markdown", "md"):
                lines = [f"# {chat.title}", ""]
                for msg in messages:
                    role = msg.get("role", "unknown").capitalize()
                    body = msg.get("content", "")
                    lines.append(f"**{role}:**\n{body}\n")
                content = "\n".join(lines)
            elif fmt in ("txt", "text"):
                lines = [chat.title, "=" * len(chat.title), ""]
                for msg in messages:
                    role = msg.get("role", "unknown").capitalize()
                    body = msg.get("content", "")
                    lines.append(f"{role}:\n{body}\n")
                content = "\n".join(lines)
            else:
                return False, f"Unsupported format: {format}", None

            logger.info(f"Exported chat '{chat.title}' as {fmt}")
            return True, "Chat exported successfully.", content

        except Exception as e:
            logger.error(f"Chat export failed: {e}")
            return False, f"Export failed: {str(e)}", None

    # ==================== Utility Functions ====================

    def get_data_preview(self, limit: int = 5) -> Optional[Dict[str, Any]]:
        """Get preview of loaded data."""
        if self.data_context is None:
            return None

        tables = self.data_context.get("tables", [])
        if not tables:
            return None

        primary_table = tables[0]
        info = self.data_context.get("table_info", {}).get(primary_table, {})
        rows = info.get("sample_rows", [])[:limit]
        return {"table": primary_table, "rows": rows}

    def get_column_info(self) -> Optional[Dict[str, Any]]:
        """Get information about columns in loaded data."""
        if self.data_context is None:
            return None

        table_info = self.data_context.get("table_info", {})
        return {
            "tables": self.data_context.get("tables", []),
            "columns": {
                table: info.get("columns", []) for table, info in table_info.items()
            },
            "dtypes": {
                table: info.get("column_types", {})
                for table, info in table_info.items()
            },
            "row_counts": {
                table: info.get("row_count", 0) for table, info in table_info.items()
            },
        }

    def clear_session(self) -> Tuple[bool, str]:
        """Clear the active chat session."""
        if self.active_chat is None:
            return False, "No active chat session."
        self.active_chat.clear_messages()
        return True, "Session cleared."

    # ==================== Data Loading ====================

    def load_file_data_with_ui(
        self, file_paths: List[str]
    ) -> Tuple[Optional[Dict[str, Any]], str]:
        """Load data from multiple files, returning SQL context and markdown message."""
        errors = []

        try:
            context, status = load_data("file", {"file_paths": file_paths})
        except Exception as e:
            context = None
            status = str(e)

        if context is None:
            error_detail = (
                "\n".join([f"- {err}" for err in errors])
                if errors
                else "- Unknown error"
            )
            return None, f"Failed to load any files:\n{error_detail}\n{status}"

        self.data_context = context
        welcome_msg = self.format_file_welcome_message(file_paths, context, status)

        return context, welcome_msg

    def format_database_welcome_message(
        self,
        db_type: str,
        selected_tables: Any,
        data_context: Dict[str, Any],
        status: str,
    ) -> str:
        """Format welcome message for database data loading with compact bullets."""

        table_info = data_context.get("table_info", {})
        qualified_columns: List[str] = []
        for table, info in table_info.items():
            qualified_columns.extend(
                [f"{table}.{col}" for col in info.get("columns", [])]
            )

        if len(qualified_columns) <= 50:
            columns_list = ", ".join(qualified_columns)
        else:
            first_cols = ", ".join(qualified_columns[:30])
            last_cols = ", ".join(qualified_columns[-20:])
            columns_list = f"{first_cols}, ... ({len(qualified_columns) - 50} more) ..., {last_cols}"

        # Prepare example questions list (compact bullets)
        example_questions = [
            "What insights can you find in this data?",
            "Show me a summary of the data",
            "What trends are visible?",
        ]
        example_list = "\n".join([f"- {q}" for q in example_questions])

        if isinstance(selected_tables, list):
            tables_detail = "\n".join(
                [
                    f"- {table}: {table_info.get(table, {}).get('row_count', 0)} rows, "
                    f"{len(table_info.get(table, {}).get('columns', []))} columns"
                    for table in selected_tables
                ]
            )

            table_word = "table" if len(selected_tables) == 1 else "tables"
            welcome_msg = self._join_markdown_blocks(
                [
                    "### Data Loaded Successfully",
                    f"**Loaded from {db_type} database ({len(selected_tables)} {table_word}):**\n\n{tables_detail}",
                    f"**Columns:** {columns_list}",
                    f"Ready to analyze your data! Try asking questions like:\n\n{example_list}",
                ]
            )
        else:
            table = str(selected_tables)
            info = table_info.get(table, {})
            welcome_msg = self._join_markdown_blocks(
                [
                    "### Data Loaded Successfully",
                    f"**Loaded from {db_type} database**",
                    f"**Table:** {table}",
                    f"**Rows:** {info.get('row_count', 0)}",
                    f"**Columns:** {columns_list}",
                    f"Ready to analyze your data! Try asking questions like:\n\n{example_list}",
                ]
            )

        return welcome_msg

    def format_file_welcome_message(
        self,
        file_paths: List[str],
        data_context: Dict[str, Any],
        status: str,
    ) -> str:
        """Format welcome message for file data loading with compact bullets."""

        table_info = data_context.get("table_info", {})
        qualified_columns: List[str] = []
        for table, info in table_info.items():
            qualified_columns.extend(
                [f"{table}.{col}" for col in info.get("columns", [])]
            )

        if len(qualified_columns) <= 50:
            columns_list = ", ".join(qualified_columns)
        else:
            first_cols = ", ".join(qualified_columns[:30])
            last_cols = ", ".join(qualified_columns[-20:])
            columns_list = f"{first_cols}, ... ({len(qualified_columns) - 50} more) ..., {last_cols}"

        # Prepare example questions list (compact bullets)
        example_questions = [
            "What insights can you find in this data?",
            "Show me a summary of the data",
            "What trends are visible?",
        ]
        example_list = "\n".join([f"- {q}" for q in example_questions])

        file_count = len(file_paths)
        file_word = "file" if file_count == 1 else "files"
        files_detail = "\n".join([f"- {os.path.basename(fp)}" for fp in file_paths])

        table_detail = "\n".join(
            [
                f"- {table}: {info.get('row_count', 0)} rows, {len(info.get('columns', []))} columns"
                for table, info in table_info.items()
            ]
        )

        welcome_msg = self._join_markdown_blocks(
            [
                "### Data Loaded Successfully",
                f"**Loaded {file_count} {file_word}:**\n\n{files_detail}",
                f"**Tables:**\n\n{table_detail}",
                f"**Columns:** {columns_list}",
                f"Ready to analyze your data! Try asking questions like:\n\n{example_list}",
            ]
        )

        return welcome_msg
