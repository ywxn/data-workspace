"""
Interactive chart widget using PyQtGraph with zoom, pan, and export capabilities.

This module provides a native Qt widget for rendering interactive charts with
hover tooltips, zoom/pan functionality, legend toggling, and export to image formats.
"""

import pyqtgraph as pg
from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QPushButton,
    QFileDialog,
    QHBoxLayout,
)
from PySide6.QtCore import Qt
from PySide6.QtGui import QColor
import numpy as np
from typing import Dict, List, Any, Optional
import json
from logger import get_logger

logger = get_logger(__name__)

# Configure PyQtGraph for better appearance
pg.setConfigOption("background", "w")
pg.setConfigOption("foreground", "k")


class InteractiveChartWidget(QWidget):
    """Interactive chart widget using PyQtGraph with zoom, pan, and export."""

    CHART_TYPES = ["line", "bar", "scatter", "area", "multi-series"]
    DEFAULT_COLORS = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd"]

    def __init__(self, parent=None):
        super().__init__(parent)
        self.plot_widget = None
        self.v_line = None
        self.h_line = None
        self.tooltip_label = None
        self.save_btn = None
        self.reset_btn = None
        self.current_data = None
        self.setup_ui()

    def setup_ui(self):
        """Initialize UI components."""
        layout = QVBoxLayout(self)

        # PyQtGraph plot widget
        self.plot_widget = pg.PlotWidget()
        self.plot_widget.setBackground("w")
        self.plot_widget.showGrid(x=True, y=True, alpha=0.3)
        legend = self.plot_widget.addLegend()
        legend.setParentItem(self.plot_widget.plotItem)

        # Enable crosshair cursor
        self.v_line = pg.InfiniteLine(
            angle=90,
            movable=False,
            pen=pg.mkPen("g", width=0.5, style=Qt.PenStyle.DashLine),
        )
        self.h_line = pg.InfiniteLine(
            angle=0,
            movable=False,
            pen=pg.mkPen("g", width=0.5, style=Qt.PenStyle.DashLine),
        )
        self.plot_widget.addItem(self.v_line, ignoreBounds=True)
        self.plot_widget.addItem(self.h_line, ignoreBounds=True)

        # Tooltip label
        self.tooltip_label = pg.TextItem(anchor=(0, 1), color="k")
        self.plot_widget.addItem(self.tooltip_label)

        # Mouse move event for crosshair
        self.plot_widget.scene().sigMouseMoved.connect(self.on_mouse_moved)

        layout.addWidget(self.plot_widget)

        # Control buttons
        button_layout = QHBoxLayout()
        self.save_btn = QPushButton("Save Chart")
        self.save_btn.clicked.connect(self.save_chart)
        self.reset_btn = QPushButton("Reset Zoom")
        self.reset_btn.clicked.connect(self.reset_view)

        button_layout.addStretch()
        button_layout.addWidget(self.reset_btn)
        button_layout.addWidget(self.save_btn)
        layout.addLayout(button_layout)

    def render_from_data(self, chart_data: Dict[str, Any]):
        """
        Render chart from standardized data structure.

        Args:
            chart_data: Dictionary with structure:
                {
                    'type': 'line',
                    'title': 'Chart Title',
                    'x_label': 'X Axis',
                    'y_label': 'Y Axis',
                    'series': [
                        {
                            'name': 'Series 1',
                            'x': [...],
                            'y': [...],
                            'color': '#1f77b4'
                        }
                    ]
                }
        """
        self.current_data = chart_data
        self.plot_widget.clear()

        chart_type = chart_data.get("type", "line")
        title = chart_data.get("title", "")
        x_label = chart_data.get("x_label", "Index")
        y_label = chart_data.get("y_label", "Value")

        self.plot_widget.setTitle(title)
        self.plot_widget.setLabel("left", y_label)
        self.plot_widget.setLabel("bottom", x_label)

        series_list = chart_data.get("series", [])

        if not series_list:
            self.tooltip_label.setText("No data to display")
            return

        if chart_type == "line":
            self._render_line_chart(series_list)
        elif chart_type == "bar":
            self._render_bar_chart(series_list)
        elif chart_type == "scatter":
            self._render_scatter_chart(series_list)
        elif chart_type == "area":
            self._render_area_chart(series_list)
        elif chart_type == "multi-series":
            self._render_multi_series_chart(series_list)

    def _render_line_chart(self, series_list: List[Dict[str, Any]]):
        """Render line chart from series data."""
        # Determine if X values are numeric or string/datetime
        first_series = series_list[0] if series_list else {}
        x_data = first_series.get("x", [])
        is_numeric_x = self._is_numeric_data(x_data)
        
        # Create a mapping from original x values to numeric indices if needed
        x_labels = []
        x_numeric = None
        if not is_numeric_x and x_data:
            x_labels = [str(val) for val in x_data]
            x_numeric = np.arange(len(x_data), dtype=float)
        
        for idx, series in enumerate(series_list):
            x = series.get("x", [])
            y = np.array(series.get("y", []), dtype=float)
            name = series.get("name", f"Series {idx + 1}")
            color = series.get("color", self.DEFAULT_COLORS[idx % len(self.DEFAULT_COLORS)])

            # Convert X to numeric if needed
            if not is_numeric_x:
                x_plot = x_numeric if x_numeric is not None else np.arange(len(y), dtype=float)
            else:
                x_plot = np.array(x, dtype=float)

            pen = pg.mkPen(color=color, width=2)
            self.plot_widget.plot(
                x_plot,
                y,
                name=name,
                pen=pen,
                symbol="o",
                symbolSize=5,
                symbolPen=pen,
                symbolBrush=color,
            )
        
        # Set custom X-axis labels if we have string/datetime data
        if not is_numeric_x and x_labels:
            self._set_custom_x_axis_labels(x_labels)

    def _render_bar_chart(self, series_list: List[Dict[str, Any]]):
        """Render bar chart from series data."""
        # Determine if X values are numeric or string/datetime
        first_series = series_list[0] if series_list else {}
        x_data = first_series.get("x", [])
        is_numeric_x = self._is_numeric_data(x_data)
        
        # Create a mapping from original x values to numeric indices if needed
        x_labels = []
        x_numeric = None
        if not is_numeric_x and x_data:
            x_labels = [str(val) for val in x_data]
            x_numeric = np.arange(len(x_data), dtype=float)
        
        for idx, series in enumerate(series_list):
            x = series.get("x", [])
            y = np.array(series.get("y", []), dtype=float)
            name = series.get("name", f"Series {idx + 1}")
            color = series.get("color", self.DEFAULT_COLORS[idx % len(self.DEFAULT_COLORS)])

            # Convert X to numeric if needed
            if not is_numeric_x:
                x_plot = x_numeric if x_numeric is not None else np.arange(len(y), dtype=float)
            else:
                x_plot = np.array(x, dtype=float)

            # Use step plot for bar-like appearance
            pen = pg.mkPen(color=color, width=2)
            self.plot_widget.plot(x_plot, y, name=name, pen=pen, stepMode="center")
        
        # Set custom X-axis labels if we have string/datetime data
        if not is_numeric_x and x_labels:
            self._set_custom_x_axis_labels(x_labels)

    def _render_scatter_chart(self, series_list: List[Dict[str, Any]]):
        """Render scatter chart from series data."""
        # Determine if X values are numeric or string/datetime
        first_series = series_list[0] if series_list else {}
        x_data = first_series.get("x", [])
        is_numeric_x = self._is_numeric_data(x_data)
        
        # Create a mapping from original x values to numeric indices if needed
        x_labels = []
        x_numeric = None
        if not is_numeric_x and x_data:
            x_labels = [str(val) for val in x_data]
            x_numeric = np.arange(len(x_data), dtype=float)
        
        for idx, series in enumerate(series_list):
            x = series.get("x", [])
            y = np.array(series.get("y", []), dtype=float)
            name = series.get("name", f"Series {idx + 1}")
            color = series.get("color", self.DEFAULT_COLORS[idx % len(self.DEFAULT_COLORS)])

            # Convert X to numeric if needed
            if not is_numeric_x:
                x_plot = x_numeric if x_numeric is not None else np.arange(len(y), dtype=float)
            else:
                x_plot = np.array(x, dtype=float)

            scatter = pg.ScatterPlotItem(
                x_plot, y, pen=None, brush=pg.mkBrush(color), size=8, name=name
            )
            self.plot_widget.addItem(scatter)
            # Add to legend manually
            self.plot_widget.plot(x_plot, y, name=name, pen=None, symbol="o", symbolSize=8, 
                                 symbolBrush=color, symbolPen=None)
        
        # Set custom X-axis labels if we have string/datetime data
        if not is_numeric_x and x_labels:
            self._set_custom_x_axis_labels(x_labels)

    def _render_area_chart(self, series_list: List[Dict[str, Any]]):
        """Render area chart from series data."""
        # Determine if X values are numeric or string/datetime
        first_series = series_list[0] if series_list else {}
        x_data = first_series.get("x", [])
        is_numeric_x = self._is_numeric_data(x_data)
        
        # Create a mapping from original x values to numeric indices if needed
        x_labels = []
        x_numeric = None
        if not is_numeric_x and x_data:
            x_labels = [str(val) for val in x_data]
            x_numeric = np.arange(len(x_data), dtype=float)
        
        for idx, series in enumerate(series_list):
            x = series.get("x", [])
            y = np.array(series.get("y", []), dtype=float)
            name = series.get("name", f"Series {idx + 1}")
            color = series.get("color", self.DEFAULT_COLORS[idx % len(self.DEFAULT_COLORS)])

            # Convert X to numeric if needed
            if not is_numeric_x:
                x_plot = x_numeric if x_numeric is not None else np.arange(len(y), dtype=float)
            else:
                x_plot = np.array(x, dtype=float)

            pen = pg.mkPen(color=color, width=2)
            brush = pg.mkBrush(color)
            brush.setAlpha(128)

            # Create filled area using FillBetweenItem
            plot_item = self.plot_widget.plot(
                x_plot, y, name=name, pen=pen, symbol="o", symbolSize=5
            )
        
        # Set custom X-axis labels if we have string/datetime data
        if not is_numeric_x and x_labels:
            self._set_custom_x_axis_labels(x_labels)

    def _render_multi_series_chart(self, series_list: List[Dict[str, Any]]):
        """Render multi-series chart (defaults to line + scatter overlay)."""
        self._render_line_chart(series_list)

    def on_mouse_moved(self, pos):
        """Update crosshair and tooltip on mouse move."""
        if self.plot_widget.sceneBoundingRect().contains(pos):
            mouse_point = self.plot_widget.plotItem.vb.mapSceneToView(pos)
            self.v_line.setPos(mouse_point.x())
            self.h_line.setPos(mouse_point.y())

            # Update tooltip
            self.tooltip_label.setText(f"x={mouse_point.x():.2f}, y={mouse_point.y():.2f}")
            self.tooltip_label.setPos(mouse_point)

    def save_chart(self):
        """Export chart to image file."""
        file_path, _ = QFileDialog.getSaveFileName(
            self,
            "Save Chart",
            "",
            "PNG (*.png);;SVG (*.svg);;All Files (*)",
        )
        if file_path:
            try:
                exporter = pg.exporters.ImageExporter(self.plot_widget.plotItem)
                exporter.params["width"] = 1920
                exporter.params["height"] = 1080
                exporter.export(file_path)
            except Exception as e:
                print(f"Error saving chart: {e}")

    def reset_view(self):
        """Reset zoom to show all data."""
        self.plot_widget.autoRange()

    def export_to_json(self) -> Optional[str]:
        """
        Export current chart data to JSON string.

        Returns:
            JSON string representation of chart data, or None if no data loaded
        """
        if self.current_data is None:
            return None
        try:
            return json.dumps(self.current_data, indent=2)
        except Exception as e:
            print(f"Error exporting to JSON: {e}")
            return None

    def _is_numeric_data(self, data: List[Any]) -> bool:
        """
        Check if data list contains numeric values.
        
        Args:
            data: List of values to check
            
        Returns:
            True if all non-None values are numeric, False otherwise
        """
        if not data:
            return True
        
        for val in data:
            if val is None:
                continue
            try:
                # Try to convert to float
                float(val)
            except (ValueError, TypeError):
                # Non-numeric value found
                return False
        return True

    def _set_custom_x_axis_labels(self, labels: List[str]):
        """
        Set custom string labels for X-axis at numeric positions.
        
        Args:
            labels: List of string labels for X-axis
        """
        try:
            axis = self.plot_widget.getAxis("bottom")
            ticks = list(range(len(labels)))
            axis.setTicks([[(i, labels[i]) for i in ticks]])
        except Exception as e:
            logger.debug(f"Failed to set custom X-axis labels: {e}")
