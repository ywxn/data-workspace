"""Interaction mode selection dialog."""

from typing import Optional
from PySide6.QtCore import Qt
from PySide6.QtGui import QFont, QIcon
from PySide6.QtWidgets import QDialog, QVBoxLayout, QHBoxLayout, QPushButton, QLabel


class InteractionModeDialog(QDialog):
    """Dialog to select interaction mode at startup (CxO or Analyst)."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Select Interaction Mode")
        self.setModal(True)
        self.setMinimumWidth(480)
        self.windowIcon = QIcon("icon.ico")
        self.setWindowIcon(self.windowIcon)

        layout = QVBoxLayout(self)

        # Title
        title = QLabel("Choose Your Interaction Mode")
        title.setFont(QFont("Roboto", 14, QFont.Weight.Bold))
        title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(title)

        layout.addSpacing(10)

        # Description
        description = QLabel("Select how you'd like to interact with your data:\n")
        description.setWordWrap(True)
        layout.addWidget(description)

        layout.addSpacing(5)

        # CxO mode button
        self.cxo_btn = QPushButton("CxO Mode")
        self.cxo_btn.setFont(QFont("Roboto", 11, QFont.Weight.Bold))
        self.cxo_btn.setMinimumHeight(60)
        self.cxo_btn.setToolTip(
            "Executive-friendly: concise insights and charts.\n"
            "No SQL or technical details shown."
        )
        self.cxo_btn.clicked.connect(self._select_cxo)
        layout.addWidget(self.cxo_btn)

        cxo_desc = QLabel(
            "  \u2022 One-step insights written for executives\n"
            "  \u2022 Charts and visualizations without technical detail\n"
            "  \u2022 No SQL or intermediate steps shown"
        )
        cxo_desc.setStyleSheet("color: gray;")
        cxo_desc.setWordWrap(True)
        layout.addWidget(cxo_desc)

        layout.addSpacing(10)

        # Analyst mode button
        self.analyst_btn = QPushButton("Analyst Mode")
        self.analyst_btn.setFont(QFont("Roboto", 11, QFont.Weight.Bold))
        self.analyst_btn.setMinimumHeight(60)
        self.analyst_btn.setToolTip(
            "Full detail: SQL queries, intermediate results,\nand detailed analysis."
        )
        self.analyst_btn.clicked.connect(self._select_analyst)
        layout.addWidget(self.analyst_btn)

        analyst_desc = QLabel(
            "  \u2022 Full data analysis workflow with SQL visibility\n"
            "  \u2022 Intermediate results and detailed breakdowns\n"
            "  \u2022 Technical details available for deeper exploration"
        )
        analyst_desc.setStyleSheet("color: gray;")
        analyst_desc.setWordWrap(True)
        layout.addWidget(analyst_desc)

        layout.addSpacing(15)

        self.selected_mode: Optional[str] = None

    def _select_cxo(self):
        self.selected_mode = "cxo"
        self.accept()

    def _select_analyst(self):
        self.selected_mode = "analyst"
        self.accept()

    def get_selected_mode(self) -> str:
        return self.selected_mode or "analyst"
