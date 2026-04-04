"""Custom widgets for the UI."""

from PySide6.QtCore import Qt, QMimeData
from PySide6.QtGui import QKeyEvent
from PySide6.QtWidgets import QTextEdit
from PySide6.QtCore import Signal


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

    def insertFromMimeData(self, source: QMimeData) -> None:
        """Paste only plain text to prevent external rich-text formatting."""
        if source and source.hasText():
            self.insertPlainText(source.text())
            return
        super().insertFromMimeData(source)
