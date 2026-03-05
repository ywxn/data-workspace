#!/usr/bin/env python3
"""
Test script to verify that the visualization_agent produces [[CHART_DATA]] blocks.
"""

import asyncio
import json
import re
import pandas as pd
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from agents import AIAgent
from config import ConfigManager


async def test_visualization_agent():
    """Test that visualization_agent outputs interactive chart data blocks."""
    
    print("=" * 70)
    print("Testing Visualization Agent with New Interactive Format")
    print("=" * 70)
    
    # Initialize agent
    agent = AIAgent()
    
    # Create sample query result
    sample_result = {
        'columns': ['date', 'sales', 'units'],
        'rows': [
            ['2024-01-01', 10000, 150],
            ['2024-01-02', 12000, 160],
            ['2024-01-03', 11500, 155],
            ['2024-01-04', 13000, 175],
            ['2024-01-05', 14500, 190],
            ['2024-01-06', 13200, 180],
            ['2024-01-07', 15000, 200],
            ['2024-01-08', 14800, 195],
            ['2024-01-09', 16000, 210],
            ['2024-01-10', 16500, 215],
        ]
    }
    
    user_query = "Show me the sales trend over the past week"
    plan = {
        'analysis_focus': ['temporal analysis', 'trend identification']
    }
    
    print(f"\n📊 Input Query: {user_query}")
    print(f"📈 Sample Data Points: {len(sample_result['rows'])}")
    print(f"📋 Columns: {', '.join(sample_result['columns'])}")
    
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
                print(f"   - X-Axis: {chart_data.get('x_label', 'N/A')}")
                print(f"   - Y-Axis: {chart_data.get('y_label', 'N/A')}")
                
                # Verify series structure
                for i, series in enumerate(chart_data.get('series', []), 1):
                    print(f"   - Series {i}: {series.get('name', 'Unnamed')} ({len(series.get('x', []))} points)")
            except json.JSONDecodeError as e:
                print(f"   ❌ Failed to parse chart JSON: {e}")
                return False
        else:
            print(f"\n❌ No Chart Data Block Found!")
            return False
        
        # Check for table data blocks
        table_pattern = r"\[\[TABLE_DATA_START\]\](.*?)\[\[TABLE_DATA_END\]\]"
        table_matches = re.findall(table_pattern, response, re.DOTALL)
        
        if table_matches:
            print(f"\n✅ Table Data Block Found!")
            table_json = table_matches[0].strip()
            
            try:
                table_data = json.loads(table_json)
                print(f"   - Headers: {len(table_data.get('headers', []))} columns")
                print(f"   - Rows: {table_data.get('row_count', len(table_data.get('rows', [])))}")
            except json.JSONDecodeError as e:
                print(f"   ❌ Failed to parse table JSON: {e}")
        
        # Check for description
        if "Generated interactive" in response:
            print(f"\n✅ Interactive Visualization Description Found!")
        
        # Print section of response (without full data)
        print(f"\n📄 Response Preview (cleaned):")
        response_cleaned = re.sub(chart_pattern, "[CHART_DATA_BLOCK]", response, flags=re.DOTALL)
        response_cleaned = re.sub(table_pattern, "[TABLE_DATA_BLOCK]", response_cleaned, flags=re.DOTALL)
        lines = response_cleaned.split('\n')
        for line in lines[:20]:  # Print first 20 lines
            if line.strip():
                print(f"   {line}")
        
        print("\n" + "=" * 70)
        print("✅ TEST PASSED: visualization_agent using new interactive format!")
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
    success = asyncio.run(test_visualization_agent())
    sys.exit(0 if success else 1)
