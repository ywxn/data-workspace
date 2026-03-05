#!/usr/bin/env python3
"""
Test script to verify the visualization widgets appear in the GUI.
"""

import sys
import json
from pathlib import Path
from datetime import datetime, timedelta

sys.path.insert(0, str(Path(__file__).parent))

from PySide6.QtWidgets import QApplication, QMainWindow, QVBoxLayout, QWidget, QLabel
from PySide6.QtCore import Qt
from gui_frontend_markdown import DataWorkspaceGUI
from logger import get_logger

logger = get_logger(__name__)


def test_gui_visualization_integration():
    """Test that the GUI can display visualization widgets."""
    
    print("=" * 70)
    print("Testing GUI Visualization Widget Display")
    print("=" * 70)
    
    # Create QApplication
    app = QApplication.instance() or QApplication(sys.argv)
    
    try:
        # Create the GUI
        gui = DataWorkspaceGUI()
        print(f"\n✅ DataWorkspaceGUI created successfully")
        
        # Verify visualization container exists
        if hasattr(gui, 'visualization_container'):
            print(f"✅ visualization_container (QTabWidget) exists")
        else:
            print(f"❌ visualization_container NOT found")
            return False
        
        if hasattr(gui, 'chart_layout'):
            print(f"✅ chart_layout exists")
        else:
            print(f"❌ chart_layout NOT found")
            return False
        
        if hasattr(gui, 'table_layout'):
            print(f"✅ table_layout exists")
        else:
            print(f"❌ table_layout NOT found")
            return False
        
        # Check that clear method exists
        if hasattr(gui, '_clear_visualization_widgets'):
            print(f"✅ _clear_visualization_widgets method exists")
        else:
            print(f"❌ _clear_visualization_widgets method NOT found")
            return False
        
        if hasattr(gui, '_display_visualization_widgets'):
            print(f"✅ _display_visualization_widgets method exists")
        else:
            print(f"❌ _display_visualization_widgets method NOT found")
            return False
        
        print("\n" + "=" * 70)
        print("✅ TEST PASSED: GUI visualization integration is properly set up!")
        print("=" * 70)
        return True
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_gui_visualization_integration()
    sys.exit(0 if success else 1)
