"""
Integration layer for embedding interactive visualization widgets in the GUI conversation display.

This module provides helper functions to detect chart and table data in agent responses
and render them as interactive PyQtGraph/Qt widgets.
"""

import json
import re
from typing import Dict, Any, Optional, List, Tuple
from pathlib import Path

from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel,
    QPushButton, QScrollArea, QFrame
)
from PySide6.QtCore import Qt
from PySide6.QtGui import QFont

from chart_widget import InteractiveChartWidget
from table_widget import InteractiveTableWidget
from config import ConfigManager
from logger import get_logger

logger = get_logger(__name__)


def should_use_interactive_widgets() -> bool:
    """Check if interactive widgets are enabled in config."""
    config = ConfigManager.load_config()
    viz_config = config.get("visualization", {})
    return viz_config.get("use_interactive_charts", True) or viz_config.get(
        "use_interactive_tables", True
    )


def create_chart_widget_from_data(chart_data: Dict[str, Any]) -> Optional[InteractiveChartWidget]:
    """
    Create an interactive chart widget from chart data.
    
    Args:
        chart_data: Dictionary with chart configuration:
            {
                'type': 'line',
                'title': 'Chart Title',
                'x_label': 'X Axis',
                'y_label': 'Y Axis',
                'series': [...]
            }
    
    Returns:
        InteractiveChartWidget instance or None if data is invalid
    """
    if not chart_data or not isinstance(chart_data, dict):
        return None
    
    config = ConfigManager.load_config()
    viz_config = config.get("visualization", {})
    
    if not viz_config.get("use_interactive_charts", True):
        logger.debug("Interactive charts disabled in config")
        return None
    
    try:
        widget = InteractiveChartWidget()
        widget.render_from_data(chart_data)
        logger.info(f"Created interactive chart widget: {chart_data.get('type', 'line')}")
        return widget
    except Exception as e:
        logger.error(f"Failed to create chart widget: {e}", exc_info=True)
        return None


def create_table_widget_from_data(table_data: Dict[str, Any]) -> Optional[InteractiveTableWidget]:
    """
    Create an interactive table widget from table data.
    
    Args:
        table_data: Dictionary with table configuration:
            {
                'headers': ['col1', 'col2', ...],
                'rows': [
                    ['val1', 'val2', ...],
                    ...
                ]
            }
    
    Returns:
        InteractiveTableWidget instance or None if data is invalid
    """
    if not table_data or not isinstance(table_data, dict):
        return None
    
    config = ConfigManager.load_config()
    viz_config = config.get("visualization", {})
    
    if not viz_config.get("use_interactive_tables", True):
        logger.debug("Interactive tables disabled in config")
        return None
    
    try:
        widget = InteractiveTableWidget()
        headers = table_data.get("headers", [])
        rows = table_data.get("rows", [])
        
        if not headers or not rows:
            logger.warning("Table data missing headers or rows")
            return None
        
        widget.load_data(headers, rows)
        logger.info(f"Created interactive table widget with {len(rows)} rows")
        return widget
    except Exception as e:
        logger.error(f"Failed to create table widget: {e}", exc_info=True)
        return None


def extract_visualization_data_from_response(response: str) -> Tuple[str, Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """
    Extract visualization data from agent response and return cleaned text.
    
    Looks for JSON blocks containing chart or table data.
    
    Args:
        response: Agent response text (markdown)
    
    Returns:
        Tuple of (cleaned_text, chart_data_dict, table_data_dict)
    """
    chart_data = None
    table_data = None
    cleaned_response = response
    
    try:
        # Look for [[CHART_DATA_START]] ... [[CHART_DATA_END]] blocks
        chart_pattern = r"\[\[CHART_DATA_START\]\](.*?)\[\[CHART_DATA_END\]\]"
        chart_matches = re.findall(chart_pattern, response, re.DOTALL)
        
        if chart_matches:
            try:
                chart_json = chart_matches[0].strip()
                chart_data = json.loads(chart_json)
                # Remove the data block from response
                cleaned_response = re.sub(chart_pattern, "", cleaned_response, flags=re.DOTALL)
                logger.debug("Extracted chart data from response")
            except json.JSONDecodeError as e:
                logger.warning(f"Failed to parse chart data JSON: {e}")
        
        # Look for [[TABLE_DATA_START]] ... [[TABLE_DATA_END]] blocks
        table_pattern = r"\[\[TABLE_DATA_START\]\](.*?)\[\[TABLE_DATA_END\]\]"
        table_matches = re.findall(table_pattern, cleaned_response, re.DOTALL)
        
        if table_matches:
            try:
                table_json = table_matches[0].strip()
                table_data = json.loads(table_json)
                # Remove the data block from response
                cleaned_response = re.sub(table_pattern, "", cleaned_response, flags=re.DOTALL)
                logger.debug("Extracted table data from response")
            except json.JSONDecodeError as e:
                logger.warning(f"Failed to parse table data JSON: {e}")
        
        # Clean up extra whitespace
        cleaned_response = cleaned_response.strip()
        
    except Exception as e:
        logger.error(f"Error extracting visualization data: {e}")
    
    return cleaned_response, chart_data, table_data


def create_widget_container(
    chart_widget: Optional[InteractiveChartWidget] = None,
    table_widget: Optional[InteractiveTableWidget] = None,
    title: Optional[str] = None,
) -> Optional[QWidget]:
    """
    Create a container widget for visualizations with optional title.
    
    Args:
        chart_widget: Optional interactive chart widget
        table_widget: Optional interactive table widget
        title: Optional title for the container
    
    Returns:
        Container QWidget or None if no widgets provided
    """
    if not chart_widget and not table_widget:
        return None
    
    try:
        container = QFrame()
        container.setStyleSheet("QFrame { border: 1px solid #ddd; border-radius: 4px; }")
        layout = QVBoxLayout(container)
        layout.setSpacing(8)
        layout.setContentsMargins(8, 8, 8, 8)
        
        # Add title if provided
        if title:
            title_label = QLabel(title)
            title_label.setFont(QFont("Roboto", 11, QFont.Weight.Bold))
            layout.addWidget(title_label)
        
        # Add widgets
        if chart_widget:
            chart_widget.setMinimumHeight(350)
            layout.addWidget(chart_widget)
        
        if table_widget:
            table_widget.setMinimumHeight(300)
            layout.addWidget(table_widget)
        
        return container
    except Exception as e:
        logger.error(f"Failed to create widget container: {e}", exc_info=True)
        return None
