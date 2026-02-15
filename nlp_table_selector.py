import logging
import hashlib
from typing import Dict, List, Tuple, Optional, Any
import numpy as np
from connector import DatabaseConnector

logger = logging.getLogger(__name__)


class NLPTableSelector:
    """
    Production-grade NLP-based table selector using semantic embeddings.

    Architecture:
    - Builds semantic table documents (table name + columns + synonyms)
    - Embeds tables once at init (cached)
    - Encodes user prompts at runtime
    - Scores via cosine similarity + softmax
    - Applies confidence thresholds and tie detection
    """

    def __init__(
        self,
        db_connector: DatabaseConnector,
        model_name: str = "all-MiniLM-L6-v2",
        confidence_threshold: float = 0.55,
        tie_threshold: float = 0.10,
        table_synonyms: Optional[Dict[str, List[str]]] = None,
        column_comments: Optional[Dict[str, Dict[str, str]]] = None,
    ):
        """
        Initialize the NLP table selector.

        Args:
            db_connector: DatabaseConnector instance
            model_name: SentenceTransformer model (default: all-MiniLM-L6-v2)
            confidence_threshold: Min confidence to accept a selection (0-1)
            tie_threshold: Max difference between top 2 scores to flag as ambiguous
            table_synonyms: Dict mapping table_name → list of business synonyms
            column_comments: Dict mapping table_name → {column_name → comment}
        """
        try:
            from sentence_transformers import SentenceTransformer
        except ImportError:
            raise ImportError(
                "sentence-transformers not installed. "
                "Install with: pip install sentence-transformers"
            )

        self.db_connector = db_connector
        self.model = SentenceTransformer(model_name)
        self.model_name = model_name
        self.confidence_threshold = confidence_threshold
        self.tie_threshold = tie_threshold
        self.table_synonyms = table_synonyms or {}
        self.column_comments = column_comments or {}

        # Build and embed table documents
        self.table_docs = self._build_table_documents()
        self.table_embeddings: Dict[str, np.ndarray] = {}
        self.schema_hash = ""
        self._embed_tables()

        logger.info(
            f"NLPTableSelector initialized with {len(self.table_docs)} tables. "
            f"Model: {model_name}, Threshold: {confidence_threshold}"
        )

    def _build_table_documents(self) -> Dict[str, str]:
        """
        Build semantic documents for each table.

        Document includes:
        - Table name
        - Column names
        - Column comments (if available)
        - Business synonyms (if provided)

        Returns:
            Dict mapping table_name → semantic document string
        """
        table_docs = {}
        tables = self.db_connector.get_tables()

        for table in tables:
            doc_parts = [table]  # Start with table name

            # Add business synonyms if available
            if table in self.table_synonyms:
                doc_parts.extend(self.table_synonyms[table])

            # Add column names and comments
            columns = self.db_connector.get_columns(table)
            doc_parts.extend(columns)

            if table in self.column_comments:
                for col in columns:
                    if col in self.column_comments[table]:
                        doc_parts.append(self.column_comments[table][col])

            # Join into single document string
            document = " ".join(doc_parts)
            table_docs[table] = document
            logger.debug(f"Built document for table '{table}': {document[:100]}...")

        return table_docs

    def _embed_tables(self):
        """Embed all table documents and compute schema hash."""
        try:
            for table_name, doc in self.table_docs.items():
                embedding = self.model.encode(doc, normalize_embeddings=True)
                self.table_embeddings[table_name] = embedding

            # Compute schema hash for versioning
            schema_str = "|".join(sorted(self.table_docs.keys()))
            self.schema_hash = hashlib.sha256(schema_str.encode()).hexdigest()[:16]

            logger.info(
                f"Embedded {len(self.table_embeddings)} tables. "
                f"Schema hash: {self.schema_hash}"
            )
        except Exception as e:
            logger.error(f"Failed to embed tables: {str(e)}")
            raise

    def _softmax(self, scores: np.ndarray) -> np.ndarray:
        """
        Compute softmax probabilities from scores.

        Args:
            scores: Array of similarity scores

        Returns:
            Softmax probabilities (sum to 1.0)
        """
        e = np.exp(scores - np.max(scores))
        return e / e.sum()

    def select_tables(
        self,
        prompt: str,
        top_k: int = 3,
        return_all_scores: bool = False,
    ) -> Dict[str, Any]:
        """
        Select the most relevant table(s) for a user prompt.

        Args:
            prompt: User's natural language prompt
            top_k: Number of top candidates to return if ambiguous
            return_all_scores: If True, return scores for all tables

        Returns:
            Dict with keys:
            - status: "success", "ambiguous", or "no_match"
            - tables: Selected table name(s) (str or list)
            - confidences: Dict mapping table_name → confidence (0-1)
            - reason: Why this decision was made (for ambiguous/no_match)
            - top_candidates: Top k alternatives (if ambiguous)
            - metadata: Debug info (prompt, embeddings shapes, schema_hash)
        """
        logger.debug(f"Processing prompt: {prompt}")

        try:
            # Encode prompt
            prompt_embedding = self.model.encode(prompt, normalize_embeddings=True)

            # Compute cosine similarity scores
            scores = []
            tables = list(self.table_embeddings.keys())

            for table in tables:
                score = float(np.dot(prompt_embedding, self.table_embeddings[table]))
                scores.append(score)

            scores = np.array(scores)
            confidences_raw = self._softmax(scores)

            # Map to table names
            confidence_dict = {
                table: float(conf) for table, conf in zip(tables, confidences_raw)
            }

            # Sort by confidence
            sorted_results = sorted(
                confidence_dict.items(), key=lambda x: x[1], reverse=True
            )
            top_table, top_confidence = sorted_results[0]

            # Decision logic with guardrails
            result = {
                "metadata": {
                    "prompt": prompt,
                    "model": self.model_name,
                    "schema_hash": self.schema_hash,
                    "embedding_shape": prompt_embedding.shape,
                }
            }

            if return_all_scores:
                result["all_confidences"] = confidence_dict

            # Check: confidence threshold
            if top_confidence < self.confidence_threshold:
                logger.warning(
                    f"Low confidence: {top_table}={top_confidence:.3f} < {self.confidence_threshold}"
                )
                result.update(
                    {
                        "status": "ambiguous",
                        "reason": "low_confidence",
                        "confidences": confidence_dict,
                        "top_candidates": [t for t, _ in sorted_results[:top_k]],
                    }
                )
                return result

            # Check: tie detection (multiple plausible tables)
            if len(sorted_results) > 1:
                second_table, second_confidence = sorted_results[1]
                margin = top_confidence - second_confidence

                if margin < self.tie_threshold:
                    logger.warning(
                        f"Tie detected: {top_table}={top_confidence:.3f} vs "
                        f"{second_table}={second_confidence:.3f}, margin={margin:.3f}"
                    )
                    # Return top k candidates
                    top_candidates = [t for t, _ in sorted_results[:top_k]]
                    result.update(
                        {
                            "status": "ambiguous",
                            "reason": "multiple_tables",
                            "confidences": confidence_dict,
                            "top_candidates": top_candidates,
                        }
                    )
                    return result

            # Success: single clear winner
            logger.info(
                f"Selected table: {top_table} (confidence={top_confidence:.3f})"
            )
            result.update(
                {
                    "status": "success",
                    "tables": top_table,
                    "confidences": confidence_dict,
                }
            )
            return result

        except Exception as e:
            logger.error(f"Error selecting tables: {str(e)}", exc_info=True)
            return {
                "status": "error",
                "reason": str(e),
                "tables": None,
                "metadata": {"prompt": prompt},
            }

    def set_synonyms(self, table_synonyms: Dict[str, List[str]]):
        """
        Update table synonyms and re-embed (schema changed).

        Args:
            table_synonyms: Dict mapping table_name → list of business synonyms
        """
        self.table_synonyms = table_synonyms
        self.table_docs = self._build_table_documents()
        self._embed_tables()
        logger.info("Synonyms updated and tables re-embedded.")

    def set_column_comments(self, column_comments: Dict[str, Dict[str, str]]):
        """
        Update column comments and re-embed (schema changed).

        Args:
            column_comments: Dict mapping table_name → {column_name → comment}
        """
        self.column_comments = column_comments
        self.table_docs = self._build_table_documents()
        self._embed_tables()
        logger.info("Column comments updated and tables re-embedded.")

    def refresh_schema(self):
        """Re-fetch schema and re-embed (call after schema changes in DB)."""
        logger.info("Refreshing schema from database...")
        self.table_docs = self._build_table_documents()
        self._embed_tables()
        logger.info("Schema refreshed and tables re-embedded.")
