#!/usr/bin/env python3
"""
Test script to verify the tabbed visualization interface displays correctly.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from PySide6.QtWidgets import QApplication
from gui_frontend_markdown import DataWorkspaceGUI
from chart_widget import InteractiveChartWidget
from table_widget import InteractiveTableWidget
from logger import get_logger

logger = get_logger(__name__)


def test_tabbed_visualization():
    """Test that the tabbed visualization interface works correctly."""
    
    print("=" * 70)
    print("Testing Tabbed Visualization Interface")
    print("=" * 70)
    
    # Create QApplication
    app = QApplication.instance() or QApplication(sys.argv)
    
    try:
        # Create the GUI
        gui = DataWorkspaceGUI()
        print(f"\n✅ DataWorkspaceGUI created successfully")
        print(f"🎯 GUI visible: {gui.isVisible()}")
        
        # Show the GUI window
        gui.show()
        print(f"🎯 After show() - GUI visible: {gui.isVisible()}")
        app.processEvents()
        
        # Create sample widgets
        print(f"\n📊 Creating sample chart widget...")
        chart_data = {
            'type': 'line',
            'title': 'Sales Trend',
            'x_label': 'Time',
            'y_label': 'Sales',
            'series': [
                {
                    'name': 'Sales',
                    'x': [0, 1, 2, 3, 4, 5],
                    'y': [10000, 12000, 11000, 13000, 14000, 15000],
                    'color': '#1f77b4'
                }
            ]
        }
        chart_widget = InteractiveChartWidget()
        chart_widget.render_from_data(chart_data)
        print(f"✅ Chart widget created")
        
        print(f"\n📋 Creating sample table widget...")
        table_widget = InteractiveTableWidget()
        table_widget.load_data(
            ['ID', 'Name', 'Value'],
            [[1, 'Item A', 100], [2, 'Item B', 200], [3, 'Item C', 300]]
        )
        print(f"✅ Table widget created")
        
        # Clear any existing widgets
        print(f"\n🧹 Clearing visualization widgets...")
        gui._clear_visualization_widgets()
        
        # Check that visualization is hidden
        if not gui.visualization_container.isVisible():
            print(f"✅ Visualization container hidden after clear")
        else:
            print(f"❌ Visualization container should be hidden")
            return False
        
        # Display both widgets
        print(f"\n🎨 Displaying visualization widgets...")
        print(f"  - chart_widget: {chart_widget}")
        print(f"  - table_widget: {table_widget}")
        gui._display_visualization_widgets(chart_widget, table_widget)
        app.processEvents()  # Process pending events
        print(f"  - visualization_container visible: {gui.visualization_container.isVisible()}")
        
        # Check that visualization is now visible
        if gui.visualization_container.isVisible():
            print(f"✅ Visualization container is now visible")
        else:
            print(f"❌ Visualization container should be visible")
            return False
        
        # Check that both tabs are enabled
        if gui.visualization_container.isTabEnabled(0):
            print(f"✅ Chart tab is enabled")
        else:
            print(f"❌ Chart tab should be enabled")
            return False
        
        if gui.visualization_container.isTabEnabled(1):
            print(f"✅ Table tab is enabled")
        else:
            print(f"❌ Table tab should be enabled")
            return False
        
        # Check that chart tab is default
        if gui.visualization_container.currentIndex() == 0:
            print(f"✅ Chart tab is selected by default")
        else:
            print(f"❌ Chart tab should be selected by default")
            return False
        
        # Test switching to table tab
        print(f"\n📂 Testing tab switching...")
        gui.visualization_container.setCurrentIndex(1)
        if gui.visualization_container.currentIndex() == 1:
            print(f"✅ Successfully switched to table tab")
        else:
            print(f"❌ Failed to switch to table tab")
            return False
        
        # Test switching back to chart tab
        gui.visualization_container.setCurrentIndex(0)
        if gui.visualization_container.currentIndex() == 0:
            print(f"✅ Successfully switched back to chart tab")
        else:
            print(f"❌ Failed to switch back to chart tab")
            return False
        
        # Test with chart only (no table)
        print(f"\n🔄 Testing chart-only visualization...")
        gui._clear_visualization_widgets()
        gui._display_visualization_widgets(chart_widget, None)
        
        if gui.visualization_container.isVisible():
            print(f"✅ Visualization container visible with chart only")
        else:
            print(f"❌ Visualization container should be visible")
            return False
        
        if gui.visualization_container.isTabEnabled(0):
            print(f"✅ Chart tab is enabled")
        else:
            print(f"❌ Chart tab should be enabled")
            return False
        
        if not gui.visualization_container.isTabEnabled(1):
            print(f"✅ Table tab is disabled (no table)")
        else:
            print(f"❌ Table tab should be disabled")
            return False
        
        # Test with table only (no chart)
        print(f"\n🔄 Testing table-only visualization...")
        gui._clear_visualization_widgets()
        gui._display_visualization_widgets(None, table_widget)
        
        if gui.visualization_container.isVisible():
            print(f"✅ Visualization container visible with table only")
        else:
            print(f"❌ Visualization container should be visible")
            return False
        
        if not gui.visualization_container.isTabEnabled(0):
            print(f"✅ Chart tab is disabled (no chart)")
        else:
            print(f"❌ Chart tab should be disabled")
            return False
        
        if gui.visualization_container.isTabEnabled(1):
            print(f"✅ Table tab is enabled")
        else:
            print(f"❌ Table tab should be enabled")
            return False
        
        if gui.visualization_container.currentIndex() == 1:
            print(f"✅ Table tab selected when chart unavailable")
        else:
            print(f"❌ Table tab should be selected")
            return False
        
        print("\n" + "=" * 70)
        print("✅ TEST PASSED: Tabbed visualization interface works correctly!")
        print("=" * 70)
        return True
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_tabbed_visualization()
    sys.exit(0 if success else 1)
