"""
Simplified NLP Table Selection Test Runner
"""

from pathlib import Path
from datetime import datetime
from pyexpat import model
from typing import Dict, List, Any
import asyncio
import json
import logging
import sys
import time
sys.path.append(str(Path(__file__).parent.parent))  # to import from parent dir

from agent_orchestrator import AIAgent
from connector import DatabaseConnector
from nlp_table_selector import NLPTableSelector
from logger import get_logger

logger = get_logger(__name__)


# ---------- Metrics ----------

def calc_metrics(predicted: List[str], expected: List[str]) -> Dict[str, Any]:
    p = {t.lower() for t in predicted}
    e = {t.lower() for t in expected}

    tp = len(p & e)
    fp = len(p - e)
    fn = len(e - p)

    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0

    return {
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "precision": round(precision, 3),
        "recall": round(recall, 3),
        "f1": round(f1, 3),
        "match": tp == len(e),  # Pass if all expected tables found (extras allowed)
    }


# ---------- Runner ----------

class NLPTestRunner:
    def __init__(self, db_config: Dict[str, Any], semantic_layer_path: str):
        self.connector = DatabaseConnector(db_type=db_config["db_type"])
        ok, msg = self.connector.connect(db_config["db_type"], db_config["credentials"])
        if not ok:
            raise RuntimeError(msg)

        semantic = json.loads(Path(semantic_layer_path).read_text(encoding="utf-8"))
        self.model_name = "all-MiniLM-L6-v2"
        confidence_threshold = 0.50

        self.selector = NLPTableSelector(
            db_connector=self.connector,
            model_name=self.model_name,
            confidence_threshold=confidence_threshold,
            semantic_layer=semantic,
        )

        self.semantic = semantic
        self.tables = self.connector.get_tables()

        # Prompt-expansion agent (uses the configured LLM provider)
        try:
            self.agent = AIAgent()
            self.use_expansion = False
            logger.info("Prompt expansion agent initialised")
        except Exception as e:
            logger.warning(f"Prompt expansion unavailable, running without: {e}")
            self.agent = None
            self.use_expansion = False

        self.results: List[Dict[str, Any]] = []

        # Seconds to wait between prompts to avoid API rate limits
        self.rate_limit_delay = 3.0

    # ---------- Test execution ----------

    def _expand_prompt(self, prompt: str) -> str:
        """Run the LLM prompt-expansion agent synchronously."""
        if not self.use_expansion or self.agent is None:
            return prompt
        try:
            schema_meta = {"tables": self.tables}
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                expanded = loop.run_until_complete(
                    self.agent.prompt_expansion_agent(
                        prompt, schema_meta, self.semantic
                    )
                )
            finally:
                loop.close()
            if expanded and len(expanded) > len(prompt):
                logger.info(f"Expanded: {expanded[:200]}")
                return expanded
        except Exception as e:
            logger.warning(f"Prompt expansion failed, using original: {e}")
        return prompt

    def run(self, test_suite: Dict[str, List[Dict[str, Any]]]) -> None:
        total = sum(len(t) for t in test_suite.values())
        test_num = 0

        for category, tests in test_suite.items():
            for i, t in enumerate(tests, 1):
                test_num += 1
                prompt = t["prompt"]
                expected = t["expected_tables"]

                # Expand the prompt via LLM middleman
                effective_prompt = self._expand_prompt(prompt)

                # Rate-limit delay (skip before the very first call)
                if test_num > 1 and self.use_expansion:
                    logger.debug(f"Rate-limit pause ({self.rate_limit_delay}s)...")
                    time.sleep(self.rate_limit_delay)

                try:
                    r = self.selector.select_tables(effective_prompt, top_k=5)
                    metrics = calc_metrics(r.tables, expected)

                    self.results.append({
                        "category": category,
                        "idx": i,
                        "prompt": prompt,
                        "expanded_prompt": effective_prompt if effective_prompt != prompt else None,
                        "expected": expected,
                        "predicted": r.tables,
                        "conf": r.confidences,
                        "status": r.status,
                        "metrics": metrics,
                        "error": None,
                    })

                except Exception as e:
                    logger.exception("Test failed")
                    self.results.append({
                        "category": category,
                        "idx": i,
                        "prompt": prompt,
                        "expanded_prompt": effective_prompt if effective_prompt != prompt else None,
                        "expected": expected,
                        "predicted": [],
                        "conf": {},
                        "status": "error",
                        "metrics": {},
                        "error": str(e),
                    })

    # ---------- Report ----------

    def write_markdown(self, path: str) -> None:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        model = self.model_name
        confidence_threshold = self.selector.confidence_threshold

        tp = sum(r["metrics"].get("tp", 0) for r in self.results)
        fp = sum(r["metrics"].get("fp", 0) for r in self.results)
        fn = sum(r["metrics"].get("fn", 0) for r in self.results)
        matches = sum(1 for r in self.results if r["metrics"].get("match"))

        prec = tp / (tp + fp) if tp + fp else 0.0
        rec = tp / (tp + fn) if tp + fn else 0.0
        f1 = 2 * prec * rec / (prec + rec) if prec + rec else 0.0

        md = [
            "# NLP Table Selection Test Report",
            "",
            f"**Generated:** {ts}",
            f"**Model:** {model}",
            f"**Confidence Threshold:** {confidence_threshold}",
            f"**Prompt Expansion:** {'Enabled' if self.use_expansion else 'Disabled'}",
            f"**Tests:** {len(self.results)}",
            f"**Exact Matches:** {matches}/{len(self.results)}",
            "",
            "## Overall Metrics",
            f"- Precision: {prec:.3f}",
            f"- Recall: {rec:.3f}",
            f"- F1: {f1:.3f}",
            f"- TP: {tp}  FP: {fp}  FN: {fn}",
            "",
            "---",
        ]

        # group by category
        cats: Dict[str, List[Dict[str, Any]]] = {}
        for r in self.results:
            cats.setdefault(r["category"], []).append(r)

        for cat, tests in cats.items():
            md.append(f"\n## {cat}\n")
            for r in tests:
                m = r["metrics"]
                match = "✓" if m.get("match") else "✗"

                pred = ", ".join(
                    f"{t} ({r['conf'].get(t,0):.3f})"
                    for t in r["predicted"]
                ) or "(none)"

                md += [
                    f"### {match} {r['idx']}. {r['prompt']}",
                ]
                if r.get("expanded_prompt"):
                    md.append(f"- Expanded: *{r['expanded_prompt']}*")
                md += [
                    f"- Expected: `{', '.join(r['expected'])}`",
                    f"- Predicted: `{pred}`",
                    f"- Precision: {m.get('precision',0):.3f}  "
                    f"Recall: {m.get('recall',0):.3f}  "
                    f"F1: {m.get('f1',0):.3f}",
                    "",
                ]

        Path(path).write_text("\n".join(md), encoding="utf-8")

    def close(self):
        self.connector.close()



# ---------- Example usage ----------

def main():
    db_config = {
        "db_type": "mysql",
        "credentials": {
            "host": "localhost",
            "port": 3306,
            "user": "appuser",
            "password": "testing",
            "database": "pmsuite_uat",
        },
    }

    test_suite = {
        "Procurement & Purchase": [
            {"prompt": "Pending Requisitions Awaiting Approval", "expected_tables": ["requisition"]},
            {"prompt": "Requisition vs Purchase Order Conversion Rate", "expected_tables": ["requisition", "requisition_dtl", "purchase_order_dtl"]},
            {"prompt": "Top 10 Items Frequently Requested", "expected_tables": ["requisition", "requisition_dtl", "mst_item"]},
            {"prompt": "Vendor-wise Purchase Value", "expected_tables": ["mst_supplier", "purchase_order", "purchase_order_dtl"]},
            {"prompt": "Pending Purchase Orders Not Yet Inwarded", "expected_tables": ["purchase_order", "purchase_order_dtl"]},
            {"prompt": "Average Purchase Lead Time from Requisition to Inward", "expected_tables": ["requisition", "purchase_order", "inward_dtl"]},
        ],
        "Inventory & Stock": [
            {"prompt": "Project-wise Current Stock Summary", "expected_tables": ["project", "stock"]},
            {"prompt": "Location-wise Stock Value", "expected_tables": ["mst_godown", "stock"]},
            {"prompt": "Slow-Moving and Non-Moving Inventory", "expected_tables": ["mst_item", "stock"]},
            {"prompt": "Items Below Reorder Level", "expected_tables": ["mst_item", "stock"]},
            {"prompt": "Available Stock Comparison", "expected_tables": ["mst_item", "stock"]},
            {"prompt": "Negative Stock Occurrence Report", "expected_tables": ["mst_item", "stock"]},
        ],
        "Material Movement": [
            {"prompt": "Inward vs Outward Quantity Trend Monthly", "expected_tables": ["inward_hdr", "inward_dtl", "mst_item", "outward_hdr", "outward_dtl"]},
            {"prompt": "Inter-Project Stock Transfer Summary", "expected_tables": ["stock", "project"]},
            {"prompt": "Top 10 Most Issued Items", "expected_tables": ["mst_item", "outward_hdr", "outward_dtl"]},
            {"prompt": "Stock Adjustment and Manual Correction Log", "expected_tables": ["stock", "mst_item"]},
        ],
        "Cost & Budget": [
            {"prompt": "Project-wise Material Consumption Cost", "expected_tables": ["project", "mst_item", "outward_hdr", "outward_dtl"]},
            {"prompt": "Budget vs Actual Procurement Cost", "expected_tables": ["project", "mst_item", "outward_hdr", "outward_dtl"]},
            {"prompt": "Purchase Price Variance by Item", "expected_tables": ["mst_supplier", "mst_item", "purchase_order", "purchase_order_dtl"]},
            {"prompt": "High-Value Purchase Transactions Above Threshold", "expected_tables": ["mst_supplier", "purchase_order", "purchase_order_dtl"]},
        ],
        "Performance & Exception Reports": [
            {"prompt": "Delayed Inward Report (PO vs GRN Delay)", "expected_tables": ["purchase_order", "purchase_order_dtl", "inward_hdr", "inward_dtl", "mst_item"]},
        ],
    }

    semantic_path = "D:/Projects/data/semantic_layer.json"
    output = f"nlp_test_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"

    runner = None
    try:
        runner = NLPTestRunner(db_config, semantic_path)
        print(f"Running {sum(len(t) for t in test_suite.values())} tests...")
        runner.run(test_suite)
        runner.write_markdown(output)
        
        matches = sum(1 for r in runner.results if r["metrics"].get("match"))
        print(f"✓ Complete. {matches}/{len(runner.results)} exact matches → {output}")
    except Exception as e:
        logger.exception("Failed")
        print(f"✗ Error: {e}")
    finally:
        if runner:
            runner.close()


if __name__ == "__main__":
    main()
