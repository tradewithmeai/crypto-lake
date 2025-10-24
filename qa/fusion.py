"""
Fusion Scoring

Combines schema validation and AI detection results into fusion scores and verdicts.

Scoring formula:
    fusion_score = 0.7 * schema_score + 0.2 * detector_score + 0.1 * ai_confidence

Verdict logic:
    - PASS: score >= 0.85 AND no critical violations
    - REVIEW: 0.65 <= score < 0.85 OR has major violations
    - FAIL: score < 0.65 OR has critical violations
"""

import json
import os
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

import pandas as pd
from loguru import logger

from qa.schema_validator import SEVERITY_MAP


def load_violations(violations_path: str) -> List[Dict[str, Any]]:
    """Load schema violations from JSONL file."""
    violations = []

    if not os.path.exists(violations_path):
        logger.warning(f"Violations file not found: {violations_path}")
        return violations

    try:
        with open(violations_path, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    violations.append(json.loads(line))
    except Exception as e:
        logger.error(f"Failed to load violations: {e}")

    return violations


def load_anomalies(anomalies_path: str) -> List[Dict[str, Any]]:
    """Load AI anomalies from JSONL file."""
    anomalies = []

    if not os.path.exists(anomalies_path):
        logger.warning(f"Anomalies file not found: {anomalies_path}")
        return anomalies

    try:
        with open(anomalies_path, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    anomalies.append(json.loads(line))
    except Exception as e:
        logger.error(f"Failed to load anomalies: {e}")

    return anomalies


def compute_fusion_scores(
    violations: List[Dict[str, Any]],
    anomalies: List[Dict[str, Any]],
    all_symbols: List[str],
    date_str: str
) -> pd.DataFrame:
    """
    Compute fusion scores for all symbols.

    Args:
        violations: List of schema violations
        anomalies: List of AI anomalies
        all_symbols: All symbols to score
        date_str: Date being processed (YYYY-MM-DD)

    Returns:
        DataFrame with columns: ts, symbol, verdict, score, metadata
    """
    # Group violations by symbol
    violations_by_symbol = defaultdict(list)
    for v in violations:
        violations_by_symbol[v["symbol"]].append(v)

    # Group anomalies by symbol
    anomalies_by_symbol = defaultdict(list)
    for a in anomalies:
        anomalies_by_symbol[a["symbol"]].append(a)

    # Compute scores per symbol
    rows = []

    for symbol in all_symbols:
        symbol_violations = violations_by_symbol.get(symbol, [])
        symbol_anomalies = anomalies_by_symbol.get(symbol, [])

        # Compute component scores
        schema_score = _compute_schema_score(symbol_violations)
        detector_score = _compute_detector_score(symbol_anomalies)
        ai_confidence = _compute_ai_confidence(symbol_anomalies)

        # Fusion score
        fusion_score = 0.7 * schema_score + 0.2 * detector_score + 0.1 * ai_confidence

        # Determine verdict
        verdict, has_critical, has_major = _compute_verdict(
            fusion_score, symbol_violations
        )

        # Metadata
        metadata = {
            "violations_count": len(symbol_violations),
            "anomalies_count": len(symbol_anomalies),
            "critical_count": sum(1 for v in symbol_violations if v["severity"] == "critical"),
            "major_count": sum(1 for v in symbol_violations if v["severity"] == "major"),
            "minor_count": sum(1 for v in symbol_violations if v["severity"] == "minor"),
            "schema_score": round(schema_score, 4),
            "detector_score": round(detector_score, 4),
            "ai_confidence": round(ai_confidence, 4),
        }

        # Create row
        row = {
            "ts": pd.Timestamp(f"{date_str}T00:00:00Z"),
            "symbol": symbol,
            "verdict": verdict,
            "score": round(fusion_score, 4),
            "metadata": json.dumps(metadata)
        }

        rows.append(row)

    df = pd.DataFrame(rows)
    logger.info(f"Computed fusion scores for {len(df)} symbols")

    return df


def _compute_schema_score(violations: List[Dict[str, Any]]) -> float:
    """
    Compute schema score from violations.

    Returns score in [0, 1] where 1 = perfect (no violations).
    """
    if not violations:
        return 1.0

    # Count violations by severity
    critical_count = sum(1 for v in violations if v["severity"] == "critical")
    major_count = sum(1 for v in violations if v["severity"] == "major")
    minor_count = sum(1 for v in violations if v["severity"] == "minor")

    # Penalty weights
    critical_penalty = 0.5
    major_penalty = 0.2
    minor_penalty = 0.05

    # Compute total penalty
    penalty = (
        critical_count * critical_penalty +
        major_count * major_penalty +
        minor_count * minor_penalty
    )

    # Score = 1 - penalty (clamped to [0, 1])
    score = max(0.0, 1.0 - penalty)

    return score


def _compute_detector_score(anomalies: List[Dict[str, Any]]) -> float:
    """
    Compute detector score from anomalies.

    Returns score in [0, 1] where 1 = perfect (no anomalies).
    """
    if not anomalies:
        return 1.0

    # Simple penalty: each anomaly reduces score
    # Assume ~10 anomalies per day is "normal" noise
    normal_anomaly_count = 10
    anomaly_ratio = len(anomalies) / normal_anomaly_count

    # Score decays logarithmically
    score = 1.0 / (1.0 + anomaly_ratio)

    return max(0.0, score)


def _compute_ai_confidence(anomalies: List[Dict[str, Any]]) -> float:
    """
    Compute average AI confidence from anomalies.

    Returns average confidence in [0, 1].
    """
    if not anomalies:
        return 1.0  # No anomalies = high confidence in data quality

    # Average confidence from labeled anomalies
    confidences = [a.get("confidence", 0.5) for a in anomalies]
    avg_confidence = sum(confidences) / len(confidences)

    # Invert: high anomaly confidence = low data quality confidence
    score = 1.0 - avg_confidence

    return max(0.0, min(1.0, score))


def _compute_verdict(
    fusion_score: float,
    violations: List[Dict[str, Any]]
) -> Tuple[str, bool, bool]:
    """
    Compute verdict from fusion score and violations.

    Returns:
        (verdict, has_critical, has_major) tuple
    """
    has_critical = any(v["severity"] == "critical" for v in violations)
    has_major = any(v["severity"] == "major" for v in violations)

    # Verdict logic
    if has_critical or fusion_score < 0.65:
        verdict = "FAIL"
    elif has_major or fusion_score < 0.85:
        verdict = "REVIEW"
    else:
        verdict = "PASS"

    return verdict, has_critical, has_major


def get_summary_stats(fusion_df: pd.DataFrame) -> Dict[str, Any]:
    """
    Get summary statistics from fusion results.

    Args:
        fusion_df: Fusion DataFrame

    Returns:
        Dictionary with summary statistics
    """
    if fusion_df.empty:
        return {
            "total_symbols": 0,
            "pass_count": 0,
            "review_count": 0,
            "fail_count": 0,
            "avg_score": 0.0,
        }

    verdict_counts = fusion_df["verdict"].value_counts().to_dict()

    stats = {
        "total_symbols": len(fusion_df),
        "pass_count": verdict_counts.get("PASS", 0),
        "review_count": verdict_counts.get("REVIEW", 0),
        "fail_count": verdict_counts.get("FAIL", 0),
        "avg_score": fusion_df["score"].mean(),
        "min_score": fusion_df["score"].min(),
        "max_score": fusion_df["score"].max(),
    }

    return stats
