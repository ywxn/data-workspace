"""
Automated Testing Script for AI Data Analysis

This script runs a dictionary of prompts against specified database tables,
generates AI analysis and visualizations, and outputs a comprehensive markdown report.

Example test_suite structure:
{
    "Processing": [
        {
            "prompt": "What is the average processing time?",
            "tables": ["orders", "processing_logs"]
        },
        {
            "prompt": "Show trend of processing over time",
            "tables": ["processing_logs"]
        }
    ],
    "Sales": [
        {
            "prompt": "Total revenue by region",
            "tables": ["sales", "regions"]
        }
    ]
}
"""

import asyncio
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Tuple, Optional
import os
import sys
sys.path.append(str(Path(__file__).parent.parent))  # to import from parent dir

from connector import DatabaseConnector
import processing
from agent_orchestrator import AIAgent
from logger import get_logger

logger = get_logger(__name__)


class TestRunner:
    """Runs AI analysis tests and generates markdown reports."""

    def __init__(self, db_config: Dict[str, Any], output_file: Optional[str] = None):
        """
        Initialize the test runner.

        Args:
            db_config: Database configuration dictionary with:
                - db_type: Database type (e.g., 'mysql', 'postgresql')
                - credentials: Dict with connection credentials
            output_file: Path to save the markdown report (default: auto-generated)
        """
        self.db_config = db_config
        self.agent = AIAgent()
        self.results: List[Dict[str, Any]] = []
        self.output_file = output_file or f"test_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"

    async def run_test_suite(
        self, test_suite: Dict[str, List[Dict[str, Any]]]
    ) -> None:
        """
        Run all tests in the test suite.
        Writes results incrementally to markdown file after each test.

        Args:
            test_suite: Dictionary mapping category names to lists of test cases.
                Each test case is a dict with 'prompt' and 'tables' keys.
        """
        logger.info("Starting test suite execution")
        logger.info(f"Output file: {self.output_file}")
        
        # Initialize the markdown file with header
        self._initialize_markdown_report(test_suite)

        for category, tests in test_suite.items():
            logger.info(f"Processing category: {category}")

            for idx, test_case in enumerate(tests, 1):
                prompt = test_case.get("prompt", "")
                tables = test_case.get("tables", [])

                logger.info(
                    f"Running test {idx}/{len(tests)} in {category}: {prompt}"
                )

                try:
                    # Load data from specified tables
                    context, load_message = self._load_data(tables)

                    if context is None:
                        result = {
                            "category": category,
                            "test_number": idx,
                            "prompt": prompt,
                            "tables": tables,
                            "status": "error",
                            "output": f"Failed to load data: {load_message}",
                            "error": load_message,
                        }
                    else:
                        # Execute query with AI agent
                        output = await self.agent.execute_query(prompt, context)

                        table_info = context.get("table_info", {})
                        row_counts = {
                            table: info.get("row_count", 0)
                            for table, info in table_info.items()
                        }

                        result = {
                            "category": category,
                            "test_number": idx,
                            "prompt": prompt,
                            "tables": tables,
                            "status": "success",
                            "output": output,
                            "row_counts": row_counts,
                            "load_message": load_message,
                        }

                except Exception as e:
                    logger.error(f"Error running test: {str(e)}", exc_info=True)
                    result = {
                        "category": category,
                        "test_number": idx,
                        "prompt": prompt,
                        "tables": tables,
                        "status": "error",
                        "output": None,
                        "error": str(e),
                    }

                self.results.append(result)
                logger.info(f"Completed test {idx} in {category}")
                
                # Write results to markdown file immediately after each test
                self.generate_markdown_report(self.output_file)
                print(f"  [{len(self.results)} completed] {prompt[:60]}...")

        logger.info("Test suite execution complete")

    def _load_data(self, tables: List[str]) -> Tuple[Optional[Dict[str, Any]], str]:
        """
        Load data from specified tables.

        Args:
            tables: List of table names to load

        Returns:
            Tuple of (context or None, status message)
        """
        if not tables:
            return None, "No tables specified"

        try:
            source_config = {
                "db_type": self.db_config.get("db_type"),
                "credentials": self.db_config.get("credentials", {}),
                "table": tables if len(tables) > 1 else tables[0],
            }

            context, message = processing.load_data("database", source_config)
            return context, message

        except Exception as e:
            error_msg = f"Error loading data: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return None, error_msg

    def _initialize_markdown_report(self, test_suite: Dict[str, List[Dict[str, Any]]]) -> None:
        """
        Initialize the markdown report file with header information.

        Args:
            test_suite: The test suite being executed
        """
        total_tests = sum(len(tests) for tests in test_suite.values())
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        initial_content = [
            "# AI Data Analysis Test Report",
            "",
            f"**Started:** {timestamp}",
            f"**Total Planned Tests:** {total_tests}",
            f"**Status:** In Progress...",
            "",
            "---",
            "",
            "*Report updates automatically after each test completes.*",
            "",
            "---",
            "",
        ]
        
        try:
            Path(self.output_file).write_text("\n".join(initial_content), encoding="utf-8")
            print(f"\n📝 Report file initialized: {self.output_file}")
            print(f"   Updates will be written after each test completes.\n")
        except Exception as e:
            logger.error(f"Error initializing report: {str(e)}", exc_info=True)

    def generate_markdown_report(self, output_path: str = "test_report.md") -> None:
        """
        Generate a markdown report from test results.

        Args:
            output_path: Path to save the markdown file
        """
        logger.info(f"Generating markdown report: {output_path}")

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        completed = len(self.results)
        successful = sum(1 for r in self.results if r['status'] == 'success')
        failed = sum(1 for r in self.results if r['status'] == 'error')

        # Build markdown content
        md_lines = [
            "# AI Data Analysis Test Report",
            "",
            f"**Last Updated:** {timestamp}",
            f"**Tests Completed:** {completed}",
            f"**Successful:** {successful} | **Failed:** {failed}",
            "",
            "---",
            "",
        ]

        # Group results by category
        categories: Dict[str, List[Dict[str, Any]]] = {}
        for result in self.results:
            cat = result["category"]
            if cat not in categories:
                categories[cat] = []
            categories[cat].append(result)

        # Generate content for each category
        for category, tests in categories.items():
            md_lines.append(f"## {category}")
            md_lines.append("")

            for test in tests:
                test_num = test["test_number"]
                prompt = test["prompt"]
                tables = test["tables"]
                status = test["status"]

                md_lines.append(f"### {test_num}. {prompt}")
                md_lines.append("")
                md_lines.append(f"**Tables:** {', '.join(tables)}")
                md_lines.append(f"**Status:** {'✓ Success' if status == 'success' else '✗ Error'}")

                if status == "success":
                    if "row_counts" in test:
                        counts = ", ".join(
                            [f"{table}: {count:,} rows" for table, count in test["row_counts"].items()]
                        )
                        md_lines.append(f"**Row Counts:** {counts}")
                    if "load_message" in test:
                        md_lines.append(f"**Load Info:** {test['load_message']}")

                    md_lines.append("")
                    md_lines.append("#### Results")
                    md_lines.append("")
                    md_lines.append(test["output"])
                else:
                    md_lines.append("")
                    md_lines.append("#### Error")
                    md_lines.append(f"```")
                    md_lines.append(test.get("error", "Unknown error"))
                    md_lines.append("```")

                md_lines.append("")
                md_lines.append("---")
                md_lines.append("")

        # Write to file
        markdown_content = "\n".join(md_lines)

        try:
            Path(output_path).write_text(markdown_content, encoding="utf-8")
            logger.info(f"Report updated: {output_path}")
        except Exception as e:
            logger.error(f"Error writing report: {str(e)}", exc_info=True)
            print(f"\n✗ Error updating report: {str(e)}")


async def main():
    """
    Main function demonstrating test suite execution.

    Customize the test_suite dictionary and db_config for your use case.
    """
    # Example database configuration
    db_config = {
        "db_type": "mysql",  # Change to your database type
        "credentials": {
            "host": "localhost",
            "port": 3306,
            "user": "appuser",
            "password": "testing",
            "database": "pmsuite_uat",
        },
    }

    # Comprehensive test suite for procurement and inventory management
    test_suite = {
        "A. Procurement & Purchase Monitoring": [
            {
                "prompt": "Pending Requisitions Awaiting Approval",
                "tables": ["requisition"],
            },
            {
                "prompt": "Requisition vs Purchase Order Conversion Rate",
                "tables": ["requisition", "requisition_dtl", "purchase_order_dtl"],
            },
            {
                "prompt": "Top 10 Items Frequently Requested",
                "tables": ["requisition", "requisition_dtl", "mst_item"],
            },
            {
                "prompt": "Vendor-wise Purchase Value (Monthly/Yearly)",
                "tables": ["mst_supplier", "purchase_order", "purchase_order_dtl"],
            },
            {
                "prompt": "Pending Purchase Orders (Not Yet Inwarded)",
                "tables": ["purchase_order", "purchase_order_dtl"],
            },
            {
                "prompt": "Average Purchase Lead Time (Requisition → PO → Inward)",
                "tables": ["requisition", "purchase_order", "inward_dtl"],
            },
        ],
        "B. Inventory & Stock Control": [
            {
                "prompt": "Project-wise Current Stock Summary",
                "tables": ["project", "stock"],
            },
            {
                "prompt": "Location-wise Stock Value",
                "tables": ["mst_godown", "stock"],
            },
            {
                "prompt": "Slow-Moving & Non-Moving Inventory (ind 10 only)",
                "tables": ["mst_item", "stock"],
            },
            {
                "prompt": "Items Below Reorder Level",
                "tables": ["mst_item", "stock"],
            },
            {
                "prompt": "Available Stock Comparison",
                "tables": ["mst_item", "stock"],
            },
            {
                "prompt": "Negative Stock Occurrence Report",
                "tables": ["mst_item", "stock"],
            },
        ],
        "C. Material Movement Tracking": [
            {
                "prompt": "Inward vs Outward Quantity Trend (Monthly)",
                "tables": ["inward_hdr", "inward_dtl", "mst_item", "outward_hdr", "outward_dtl"],
            },
            {
                "prompt": "Inter-Project Stock Transfer Summary",
                "tables": ["stock", "project"],
            },
            {
                "prompt": "Top 10 Most Issued Items",
                "tables": ["mst_item", "outward_hdr", "outward_dtl"],
            },
            {
                "prompt": "Stock Adjustment / Manual Correction Log",
                "tables": ["stock", "mst_item"],
            },
        ],
        "D. Cost & Budget Control": [
            {
                "prompt": "Project-wise Material Consumption Cost",
                "tables": ["project", "mst_item", "outward_hdr", "outward_dtl"],
            },
            {
                "prompt": "Budget vs Actual Procurement Cost",
                "tables": ["project", "mst_item", "outward_hdr", "outward_dtl"],
            },
            {
                "prompt": "Purchase Price Variance (Item-wise)",
                "tables": ["mst_supplier", "mst_item", "purchase_order", "purchase_order_dtl"],
            },
            {
                "prompt": "High-Value Purchase Transactions (Above Threshold)",
                "tables": ["mst_supplier", "purchase_order", "purchase_order_dtl"],
            },
        ],
        "E. User & Approval Monitoring": [
            {
                "prompt": "Pending Approvals by Approver (Attendance, Expense, Loan, Purchase Order)",
                "tables": ["mst_employee", "employee_attendance_project", "employee_expense_book", "employee_expense_book_dtl", "loan_advance_hdr", "loan_advance_dtl", "mst_supplier", "purchase_order"],
            },
            {
                "prompt": "Average Approval Turnaround Time (created date to approved date)",
                "tables": ["mst_employee", "employee_attendance_project", "employee_expense_book", "employee_expense_book_dtl", "loan_advance_hdr", "loan_advance_dtl", "mst_supplier", "purchase_order"],
            },
            {
                "prompt": "User-wise Requisition Creation Count",
                "tables": ["mst_user", "requisition"],
            },
            {
                "prompt": "Unauthorized / Rejected Transactions Report",
                "tables": ["mst_employee", "employee_attendance_project", "employee_expense_book", "employee_expense_book_dtl", "loan_advance_hdr", "loan_advance_dtl"],
            },
        ],
        "F. Performance & Exception Reports": [
            {
                "prompt": "Delayed Inward Report (PO vs GRN Delay)",
                "tables": ["purchase_order", "purchase_order_dtl", "inward_hdr", "inward_dtl", "mst_item"],
            },
        ],
    }

    print("=" * 70)
    print("AI Data Analysis Test Runner")
    print("=" * 70)
    print(f"\nCategories: {len(test_suite)}")
    total_tests = sum(len(tests) for tests in test_suite.values())
    print(f"Total tests: {total_tests}\n")

    # Create test runner and execute
    output_file = f"test_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
    runner = TestRunner(db_config, output_file=output_file)

    try:
        print("Starting test execution...")
        print("(You can stop at any time with Ctrl+C - results are saved after each test)\n")
        await runner.run_test_suite(test_suite)

        print(f"\n✓ All tests completed!")
        print(f"Success: {sum(1 for r in runner.results if r['status'] == 'success')}")
        print(f"Failed: {sum(1 for r in runner.results if r['status'] == 'error')}")
        print(f"\nFinal report: {output_file}")

    except Exception as e:
        logger.error(f"Error in main: {str(e)}", exc_info=True)
        print(f"\n✗ Fatal error: {str(e)}")
        return 1

    print("\n" + "=" * 70)
    print("Test execution complete!")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)
