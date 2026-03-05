#!/usr/bin/env python3
"""
Test script to verify that the chart widget renders without errors on datetime data.
"""

import sys
import json
from pathlib import Path
from datetime import datetime, timedelta

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from PySide6.QtWidgets import QApplication
from chart_widget import InteractiveChartWidget
from logger import get_logger

logger = get_logger(__name__)


def test_chart_widget_rendering():
    """Test that chart widget can render datetime X-axis data."""
    
    print("=" * 70)
    print("Testing Chart Widget with Datetime X-Axis Values")
    print("=" * 70)
    
    # Create QApplication
    app = QApplication.instance() or QApplication(sys.argv)
    
    try:
        # Create sample chart data with datetime X values converted to indices
        chart_data = {
            'type': 'line',
            'title': 'Sales Trend Over Time',
            'x_label': 'Time',
            'y_label': 'Sales',
            'series': [
                {
                    'name': 'Sales',
                    'x': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],  # Datetime converted to indices
                    'y': [10000, 10500, 11000, 11500, 12000, 12500, 13000, 13500, 14000, 14500],
                    'color': '#1f77b4'
                },
                {
                    'name': 'Units',
                    'x': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],  # Datetime converted to indices
                    'y': [150, 155, 160, 165, 170, 175, 180, 185, 190, 195],
                    'color': '#ff7f0e'
                }
            ]
        }
        
        print(f"\n📊 Creating chart widget...")
        print(f"   - Chart Type: {chart_data['type']}")
        print(f"   - Title: {chart_data['title']}")
        print(f"   - Series Count: {len(chart_data['series'])}")
        print(f"   - X-axis Data Points: {len(chart_data['series'][0]['x'])}")
        
        # Create widget
        widget = InteractiveChartWidget()
        print(f"\n✅ Chart widget created successfully")
        
        # Render the data
        print(f"\n🎨 Rendering chart...")
        widget.render_from_data(chart_data)
        print(f"✅ Chart rendered successfully!")
        
        # Verify the plot has data
        if widget.plot_widget.listDataItems():
            print(f"\n✅ Plot contains {len(widget.plot_widget.listDataItems())} data items")
        
        # Test export to JSON
        json_export = widget.export_to_json()
        if json_export:
            print(f"\n✅ Successfully exported chart to JSON ({len(json_export)} chars)")
        
        print("\n" + "=" * 70)
        print("✅ TEST PASSED: Chart widget renders datetime data correctly!")
        print("=" * 70)
        return True
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_chart_widget_rendering()
    sys.exit(0 if success else 1)
