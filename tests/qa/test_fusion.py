"""
Fast tests for fusion scoring logic.

No DuckDB, no file I/O - pure in-memory unit tests.
"""

import pytest

from qa.fusion import (
    _compute_ai_confidence,
    _compute_detector_score,
    _compute_schema_score,
    _compute_verdict,
    compute_fusion_scores,
)


def test_compute_schema_score_no_violations():
    """Test schema score with no violations."""
    violations = []
    score = _compute_schema_score(violations)
    assert score == 1.0


def test_compute_schema_score_critical():
    """Test schema score with critical violations."""
    violations = [
        {"severity": "critical"},
        {"severity": "critical"},
    ]
    score = _compute_schema_score(violations)
    # 2 * 0.5 = 1.0 penalty, score = 0.0
    assert score == 0.0


def test_compute_schema_score_major():
    """Test schema score with major violations."""
    violations = [
        {"severity": "major"},
        {"severity": "major"},
    ]
    score = _compute_schema_score(violations)
    # 2 * 0.2 = 0.4 penalty, score = 0.6
    assert score == 0.6


def test_compute_schema_score_minor():
    """Test schema score with minor violations."""
    violations = [
        {"severity": "minor"},
        {"severity": "minor"},
    ]
    score = _compute_schema_score(violations)
    # 2 * 0.05 = 0.1 penalty, score = 0.9
    assert score == 0.9


def test_compute_detector_score_no_anomalies():
    """Test detector score with no anomalies."""
    anomalies = []
    score = _compute_detector_score(anomalies)
    assert score == 1.0


def test_compute_detector_score_few_anomalies():
    """Test detector score with few anomalies."""
    anomalies = [{"detector": "ZSCORE"} for _ in range(5)]
    score = _compute_detector_score(anomalies)
    # 5 anomalies -> ratio = 0.5, score = 1/(1+0.5) = 0.666...
    assert 0.6 < score < 0.7


def test_compute_ai_confidence_no_anomalies():
    """Test AI confidence with no anomalies."""
    anomalies = []
    confidence = _compute_ai_confidence(anomalies)
    assert confidence == 1.0


def test_compute_ai_confidence_high_confidence_anomalies():
    """Test AI confidence with high-confidence anomalies."""
    anomalies = [
        {"confidence": 0.9},
        {"confidence": 0.95},
    ]
    confidence = _compute_ai_confidence(anomalies)
    # avg = 0.925, inverted = 1 - 0.925 = 0.075
    assert 0.07 < confidence < 0.08


def test_compute_verdict_pass():
    """Test PASS verdict."""
    violations = []
    verdict, has_critical, has_major = _compute_verdict(0.9, violations)
    assert verdict == "PASS"
    assert not has_critical
    assert not has_major


def test_compute_verdict_fail_critical():
    """Test FAIL verdict with critical violation."""
    violations = [{"severity": "critical"}]
    verdict, has_critical, has_major = _compute_verdict(0.9, violations)
    assert verdict == "FAIL"
    assert has_critical


def test_compute_verdict_fail_low_score():
    """Test FAIL verdict with low score."""
    violations = []
    verdict, has_critical, has_major = _compute_verdict(0.5, violations)
    assert verdict == "FAIL"
    assert not has_critical


def test_compute_verdict_review_major():
    """Test REVIEW verdict with major violation."""
    violations = [{"severity": "major"}]
    verdict, has_critical, has_major = _compute_verdict(0.9, violations)
    assert verdict == "REVIEW"
    assert not has_critical
    assert has_major


def test_compute_verdict_review_score():
    """Test REVIEW verdict with borderline score."""
    violations = []
    verdict, has_critical, has_major = _compute_verdict(0.75, violations)
    assert verdict == "REVIEW"


def test_compute_fusion_scores_empty():
    """Test fusion scoring with no violations or anomalies."""
    violations = []
    anomalies = []
    symbols = ["BTCUSDT", "ETHUSDT"]
    date_str = "2025-10-23"

    df = compute_fusion_scores(violations, anomalies, symbols, date_str)

    assert len(df) == 2
    assert all(df["score"] == 1.0)
    assert all(df["verdict"] == "PASS")


def test_compute_fusion_scores_with_violations():
    """Test fusion scoring with violations."""
    violations = [
        {"symbol": "BTCUSDT", "severity": "major", "rule": "R3_ASK_GTE_BID"},
        {"symbol": "BTCUSDT", "severity": "minor", "rule": "R7_KLINE_PARITY"},
    ]
    anomalies = []
    symbols = ["BTCUSDT"]
    date_str = "2025-10-23"

    df = compute_fusion_scores(violations, anomalies, symbols, date_str)

    assert len(df) == 1
    assert df.iloc[0]["symbol"] == "BTCUSDT"
    assert df.iloc[0]["score"] < 1.0
    assert df.iloc[0]["verdict"] in ["REVIEW", "FAIL"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
