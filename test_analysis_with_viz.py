#!/usr/bin/env python3
"""
Test to verify analysis is included with visualization responses.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from visualization_integration import extract_visualization_data_from_response
import json


def test_analysis_with_visualization():
    """Test that analysis is included when visualization is present."""
    print("\n" + "="*70)
    print("Test: Analysis Included With Visualization")
    print("="*70)
    
    # Simulate a response with both visualization and analysis
    chart_data = {
        'type': 'line',
        'title': 'Sales Trend',
        'x_label': 'Month',
        'y_label': 'Sales ($)',
        'series': [{
            'name': 'Sales',
            'x': [0, 1, 2, 3, 4],
            'y': [50000, 52000, 48000, 55000, 58000],
            'color': '#1f77b4'
        }]
    }
    
    table_data = {
        'headers': ['Month', 'Sales'],
        'rows': [
            ['Jan', '$50,000'],
            ['Feb', '$52,000'],
            ['Mar', '$48,000'],
            ['Apr', '$55000'],
            ['May', '$58,000']
        ]
    }
    
    test_analysis = """The sales data shows a strong upward trend with the following patterns:

- **Overall Growth**: Sales increased by 16% from January to May
- **Dip in March**: A notable decline of $4,000 in March, likely due to seasonal factors
- **Recovery in April**: Strong recovery with $7,000 increase month-over-month
- **Peak Performance**: May achieved the highest sales at $58,000

**Recommendations**: 
- Investigate March decline to identify root causes and prevent recurrence
- Capitalize on April-May momentum with targeted marketing initiatives
- Plan for sustained Q2 growth with increased inventory"""
    
    # Build response as it would come from agent
    chart_block = f"[[CHART_DATA_START]]\n{json.dumps(chart_data, indent=2)}\n[[CHART_DATA_END]]"
    table_block = f"[[TABLE_DATA_START]]\n{json.dumps(table_data, indent=2)}\n[[TABLE_DATA_END]]"
    
    response = f"""{chart_block}

{table_block}

### Analysis:
{test_analysis}"""
    
    print("\n1️⃣ Checking response structure...")
    
    # Verify response contains both visualization and analysis
    if "[[CHART_DATA_START]]" not in response:
        print("❌ Response missing CHART_DATA block")
        return False
    print("✅ CHART_DATA block present")
    
    if "[[TABLE_DATA_START]]" not in response:
        print("❌ Response missing TABLE_DATA block")
        return False
    print("✅ TABLE_DATA block present")
    
    if "### Analysis:" not in response:
        print("❌ Response missing Analysis section")
        return False
    print("✅ Analysis section present")
    
    # Extract and verify each component
    print("\n2️⃣ Extracting components...")
    response_text, extracted_chart, extracted_table = extract_visualization_data_from_response(response)
    
    if extracted_chart:
        print("✅ Chart data extracted")
    else:
        print("❌ Failed to extract chart data")
        return False
    
    if extracted_table:
        print("✅ Table data extracted")
    else:
        print("❌ Failed to extract table data")
        return False
    
    # Check that response_text contains analysis (after extraction removes markers)
    print("\n3️⃣ Verifying analysis in extracted response...")
    
    if "Analysis" in response_text:
        print("✅ Analysis present in extracted response text")
    else:
        print("❌ Analysis missing from extracted response text")
        return False
    
    if "Overall Growth" in response_text:
        print("✅ Analysis details preserved")
    else:
        print("❌ Analysis details not found")
        return False
    
    if "Recommendations" in response_text:
        print("✅ Recommendations section present")
    else:
        print("❌ Recommendations missing")
        return False
    
    print("\n" + "="*70)
    print("✅ TEST PASSED: Analysis is properly included with visualizations")
    print("="*70)
    print("\nResponse flow:")
    print("  1. Visualization data blocks extracted for widget creation")
    print("  2. Analysis text preserved in response for user reading")
    print("  3. Both components available simultaneously")
    
    return True


if __name__ == '__main__':
    success = test_analysis_with_visualization()
    sys.exit(0 if success else 1)
