#!/usr/bin/env python3
"""
Test script to verify that the chart widget properly handles datetime X-axis values.
"""

import asyncio
import json
import re
import pandas as pd
import sys
from pathlib import Path
from datetime import datetime, timedelta

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from agents import AIAgent
from config import ConfigManager


async def test_visualization_with_datetime():
    """Test that visualization handles datetime X-axis values correctly."""
    
    print("=" * 70)
    print("Testing Visualization Agent with Datetime X-Axis Values")
    print("=" * 70)
    
    # Initialize agent
    agent = AIAgent()
    
    # Create sample query result with datetime column
    start_date = datetime(2025, 9, 8, 14, 39, 44)
    dates = [start_date + timedelta(hours=i) for i in range(10)]
    
    sample_result = {
        'columns': ['timestamp', 'sales', 'units'],
        'rows': [
            [str(d), 10000 + i*500, 150 + i*5]
            for i, d in enumerate(dates)
        ]
    }
    
    user_query = "Show me the sales trend over time"
    plan = {
        'analysis_focus': ['temporal analysis', 'trend identification']
    }
    
    print(f"\n📊 Input Query: {user_query}")
    print(f"📈 Sample Data Points: {len(sample_result['rows'])}")
    print(f"📋 Columns: {', '.join(sample_result['columns'])}")
    print(f"🕐 First timestamp: {sample_result['rows'][0][0]}")
    
    # Call visualization agent
    print("\n🔄 Calling visualization_agent...")
    try:
        response = await agent.visualization_agent(sample_result, plan, user_query)
        
        if response is None:
            print("❌ FAILED: visualization_agent returned None")
            return False
        
        print(f"\n✅ Response received ({len(response)} characters)")
        
        # Check for chart data blocks
        chart_pattern = r"\[\[CHART_DATA_START\]\](.*?)\[\[CHART_DATA_END\]\]"
        chart_matches = re.findall(chart_pattern, response, re.DOTALL)
        
        if chart_matches:
            print(f"\n✅ Chart Data Block Found!")
            chart_json = chart_matches[0].strip()
            
            try:
                chart_data = json.loads(chart_json)
                print(f"   - Chart Type: {chart_data.get('type', 'unknown')}")
                print(f"   - Title: {chart_data.get('title', 'N/A')}")
                print(f"   - Series Count: {len(chart_data.get('series', []))}")
                
                # Check X-axis data type
                for i, series in enumerate(chart_data.get('series', []), 1):
                    x_values = series.get('x', [])
                    if x_values:
                        first_x = x_values[0]
                        print(f"   - Series {i}: {series.get('name', 'Unnamed')}")
                        print(f"     - X values type: {type(first_x).__name__}")
                        print(f"     - X values are numeric: {isinstance(first_x, (int, float))}")
                        print(f"     - First X value: {first_x}")
                        
                        if not isinstance(first_x, (int, float)):
                            print(f"     ❌ ERROR: X values should be numeric but got {type(first_x)}")
                            return False
                        else:
                            print(f"     ✅ X values are properly numeric!")
            except json.JSONDecodeError as e:
                print(f"   ❌ Failed to parse chart JSON: {e}")
                return False
        else:
            print(f"\n❌ No Chart Data Block Found!")
            return False
        
        print("\n" + "=" * 70)
        print("✅ TEST PASSED: Datetime X-axis values handled correctly!")
        print("=" * 70)
        return True
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    # Configure config for testing
    try:
        config = ConfigManager.load_config()
        config.setdefault('visualization', {})['use_interactive_charts'] = True
        config.setdefault('visualization', {})['use_interactive_tables'] = True
    except:
        pass
    
    # Run test
    success = asyncio.run(test_visualization_with_datetime())
    sys.exit(0 if success else 1)
