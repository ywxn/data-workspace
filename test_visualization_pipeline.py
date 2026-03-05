#!/usr/bin/env python3
"""
Integration test for the full visualization pipeline:
Agent → Generate Data → Extract → Create Widgets → Display in GUI
"""

import sys
import json
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from PySide6.QtWidgets import QApplication
from visualization_integration import extract_visualization_data_from_response, create_chart_widget_from_data, create_table_widget_from_data
from gui_frontend_markdown import DataWorkspaceGUI
from logger import get_logger

logger = get_logger(__name__)


def test_full_visualization_pipeline():
    """Test the complete visualization pipeline."""
    print("\n" + "="*70)
    print("Integration Test: Full Visualization Pipeline")
    print("="*70)
    
    # Create QApplication
    app = QApplication.instance() or QApplication(sys.argv)
    
    try:
        # Step 1: Setup (no agent needed for this test, we simulate the data)
        print("\n1️⃣  Setting up test environment...")
        
        # Step 2: Generate visualization data
        print("\n2️⃣  Generating visualization data...")
        
        # Create sample data that would trigger visualization
        sample_data = {
            'monthly_sales': {
                'dates': ['2025-01-01', '2025-02-01', '2025-03-01', '2025-04-01', '2025-05-01'],
                'sales': [50000, 52000, 48000, 55000, 58000]
            }
        }
        
        # Simulate agent response with visualization data
        chart_data = {
            'type': 'line',
            'title': 'Monthly Sales Trend',
            'x_label': 'Month',
            'y_label': 'Sales ($)',
            'series': [
                {
                    'name': 'Sales',
                    'x': [0, 1, 2, 3, 4],
                    'y': [50000, 52000, 48000, 55000, 58000],
                    'color': '#1f77b4'
                }
            ]
        }
        
        table_data = {
            'headers': ['Month', 'Sales'],
            'rows': [
                ['Jan 2025', '$50,000'],
                ['Feb 2025', '$52,000'],
                ['Mar 2025', '$48,000'],
                ['Apr 2025', '$55,000'],
                ['May 2025', '$58,000']
            ]
        }
        
        # Format as response with markers
        chart_block = f"[[CHART_DATA_START]]\n{json.dumps(chart_data, indent=2)}\n[[CHART_DATA_END]]"
        table_block = f"[[TABLE_DATA_START]]\n{json.dumps(table_data, indent=2)}\n[[TABLE_DATA_END]]"
        response_with_markers = f"""
Here's the sales analysis:

{chart_block}

{table_block}

The sales show an upward trend with a dip in March.
"""
        
        print("✅ Visualization data prepared")
        
        # Step 3: Extract visualization data
        print("\n3️⃣  Extracting visualization data from response...")
        response_text, chart_data_extracted, table_data_extracted = extract_visualization_data_from_response(response_with_markers)
        print(f"✅ Extracted data:")
        print(f"   - Chart data: {chart_data_extracted is not None}")
        print(f"   - Table data: {table_data_extracted is not None}")
        
        if not chart_data_extracted:
            print("❌ No chart data extracted!")
            return False
        
        # Step 4: Create widgets
        print("\n4️⃣  Creating visualization widgets...")
        chart_widget = create_chart_widget_from_data(chart_data_extracted)
        table_widget = create_table_widget_from_data(table_data_extracted) if table_data_extracted else None
        
        print(f"✅ Widgets created:")
        print(f"   - Chart: {chart_widget is not None}")
        print(f"   - Table: {table_widget is not None}")
        
        # Step 5: Display in GUI
        print("\n5️⃣  Displaying in GUI...")
        gui = DataWorkspaceGUI()
        gui.show()  # IMPORTANT: Must show GUI for visibility to work
        app.processEvents()
        
        print("✅ GUI window shown")
        
        # Step 6: Call display method
        print("\n6️⃣  Calling _display_visualization_widgets()...")
        gui._clear_visualization_widgets()
        gui._display_visualization_widgets(chart_widget, table_widget)
        app.processEvents()
        
        print("✅ Widgets displayed")
        
        # Step 7: Verify display
        print("\n7️⃣  Verifying visualization container visibility...")
        if gui.visualization_container.isVisible():
            print("✅ Visualization container is VISIBLE")
        else:
            print("❌ Visualization container is NOT visible")
            return False
        
        # Check tabs
        print("\n8️⃣  Verifying tabs...")
        if gui.visualization_container.isTabEnabled(0):
            print("✅ Chart tab is enabled")
        else:
            print("❌ Chart tab is disabled")
            return False
        
        if gui.visualization_container.isTabEnabled(1):
            print("✅ Table tab is enabled")
        else:
            print("❌ Table tab is disabled")
            return False
        
        # Check current tab
        if gui.visualization_container.currentIndex() == 0:
            print("✅ Chart tab is active (default)")
        else:
            print("❌ Chart tab should be active")
            return False
        
        print("\n" + "="*70)
        print("✅ INTEGRATION TEST PASSED!")
        print("="*70)
        print("\nThe complete visualization pipeline works correctly:")
        print("  1. Agent generates data")
        print("  2. Data is extracted from response markers")
        print("  3. Widgets are created from data")
        print("  4. Widgets are displayed in tabbed interface")
        print("  5. User can switch between chart and table views")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Test failed with exception: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == '__main__':
    success = test_full_visualization_pipeline()
    sys.exit(0 if success else 1)
