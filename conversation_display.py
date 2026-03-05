"""
Custom conversation display widget that supports both Markdown and interactive visualizations.

This widget provides a scrollable area that can display:
- Formatted markdown messages
- Interactive PyQtGraph charts
- Interactive data tables
"""

from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QTextEdit, QScrollArea, QFrame
)
from PySide6.QtCore import Qt
from PySide6.QtGui import QFont
from typing import List, Dict, Any, Optional

from markdown_converter import markdown_to_html
from logger import get_logger

logger = get_logger(__name__)


class ConversationDisplayWidget(QWidget):
    """Custom widget for displaying conversations with markdown + interactive widgets."""
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setup_ui()
        self.message_widgets: List[QWidget] = []
    
    def setup_ui(self):
        """Initialize the UI."""
        layout = QVBoxLayout(self)
        layout.setSpacing(8)
        layout.setContentsMargins(0, 0, 0, 0)
        
        # Create a scroll area to contain messages
        self.scroll_area = QScrollArea()
        self.scroll_area.setWidgetResizable(True)
        self.scroll_area.setStyleSheet("QScrollArea { border: none; }")
        
        # Container for messages
        self.message_container = QWidget()
        self.message_layout = QVBoxLayout(self.message_container)
        self.message_layout.setSpacing(12)
        self.message_layout.setContentsMargins(8, 8, 8, 8)
        
        self.scroll_area.setWidget(self.message_container)
        layout.addWidget(self.scroll_area)
        
        # Add stretch to keep messages at the top
        self.message_layout.addStretch()
    
    def add_message(self, role: str, content: str, widget: Optional[QWidget] = None):
        """
        Add a message to the conversation display.
        
        Args:
            role: "user" or "assistant"
            content: Markdown text content
            widget: Optional interactive widget (chart, table, etc.)
        """
        try:
            # Remove the stretch item temporarily
            if self.message_layout.count() > 0:
                self.message_layout.removeItem(self.message_layout.itemAt(self.message_layout.count() - 1))
            
            # Create message frame
            message_frame = QFrame()
            message_frame.setStyleSheet(
                f"QFrame {{ border-left: 3px solid {'#4CAF50' if role == 'assistant' else '#2196F3'}; "
                f"padding: 8px; background-color: {'#f5f5f5' if role == 'assistant' else '#e3f2fd'}; }}"
            )
            frame_layout = QVBoxLayout(message_frame)
            frame_layout.setSpacing(4)
            frame_layout.setContentsMargins(8, 8, 8, 8)
            
            # Add role label
            role_label = f"**{role.capitalize()}**"
            
            # Add content as HTML
            content_text = QTextEdit()
            content_text.setReadOnly(True)
            content_text.setHtml(markdown_to_html(f"{role_label}\n{content}"))
            content_text.setMaximumHeight(200)
            content_text.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
            frame_layout.addWidget(content_text)
            
            # Add widget if provided
            if widget:
                widget.setMaximumHeight(400)
                frame_layout.addWidget(widget)
            
            self.message_layout.addWidget(message_frame)
            self.message_widgets.append(message_frame)
            
            # Re-add stretch
            self.message_layout.addStretch()
            
            # Scroll to bottom
            self.scroll_area.verticalScrollBar().setValue(
                self.scroll_area.verticalScrollBar().maximum()
            )
            
            logger.debug(f"Added message from {role} (widget={widget is not None})")
        
        except Exception as e:
            logger.error(f"Failed to add message to display: {e}", exc_info=True)
    
    def clear(self):
        """Clear all messages from the display."""
        try:
            # Clear layout
            while self.message_layout.count() > 1:  # Keep the stretch
                item = self.message_layout.takeAt(0)
                if item.widget():
                    item.widget().deleteLater()
            
            self.message_widgets.clear()
            logger.debug("Conversation display cleared")
        except Exception as e:
            logger.error(f"Failed to clear conversation display: {e}", exc_info=True)
    
    def get_markdown(self) -> str:
        """
        Get all messages as markdown text (for export/save purposes).
        
        Returns:
            Markdown-formatted conversation history
        """
        markdown_parts = []
        for widget in self.message_widgets:
            # Extract text from the message frame
            try:
                text_edits = widget.findChildren(QTextEdit)
                if text_edits:
                    markdown_parts.append(text_edits[0].toPlainText())
            except Exception:
                pass
        
        return "\n\n".join(markdown_parts)
    
    def set_html_content(self, html: str):
        """
        Set the conversation display from HTML content.
        
        This is for backward compatibility with the old markdown_to_html rendering.
        
        Args:
            html: HTML content to display
        """
        try:
            self.clear()
            
            # Create a single text edit to display the HTML
            content_text = QTextEdit()
            content_text.setReadOnly(True)
            content_text.setHtml(html)
            content_text.setStyleSheet("QTextEdit { border: none; }")
            
            self.message_layout.removeItem(self.message_layout.itemAt(self.message_layout.count() - 1))
            self.message_layout.addWidget(content_text)
            self.message_layout.addStretch()
            
            logger.debug("Set conversation display from HTML")
        except Exception as e:
            logger.error(f"Failed to set HTML content: {e}", exc_info=True)
