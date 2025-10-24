"""
Tests for anomaly clustering functionality.
"""

import pytest
from datetime import datetime, timezone, timedelta

from qa.run_ai import cluster_anomalies, _flush_bucket


def test_cluster_anomalies_basic():
    """Test basic clustering with synthetic data."""
    # Create synthetic anomalies at t, t+1s, t+3s, t+9s
    base_ts = datetime(2025, 10, 24, 12, 0, 0, tzinfo=timezone.utc)

    anomalies = [
        {
            "symbol": "TESTSYM",
            "detector": "JUMP",
            "ts": base_ts.isoformat(),
            "features": {"z_score": 7.5},
            "label": "anomaly",
            "rationale": "Test 1"
        },
        {
            "symbol": "TESTSYM",
            "detector": "JUMP",
            "ts": (base_ts + timedelta(seconds=1)).isoformat(),
            "features": {"z_score": 8.0},
            "label": "anomaly",
            "rationale": "Test 2"
        },
        {
            "symbol": "TESTSYM",
            "detector": "JUMP",
            "ts": (base_ts + timedelta(seconds=3)).isoformat(),
            "features": {"z_score": 6.5},
            "label": "anomaly",
            "rationale": "Test 3"
        },
        {
            "symbol": "TESTSYM",
            "detector": "JUMP",
            "ts": (base_ts + timedelta(seconds=9)).isoformat(),
            "features": {"z_score": 9.0},
            "label": "anomaly",
            "rationale": "Test 4"
        },
    ]

    # Cluster with 5-second cooldown
    # Expected: first three together (0s, 1s, 3s within 5s window)
    #           last one alone (9s is >5s from previous)
    clustered = cluster_anomalies(anomalies, cooldown_seconds=5)

    # Should have 2 clusters
    assert len(clustered) == 2

    # First cluster should have count=3
    first_cluster = clustered[0]
    assert "cluster" in first_cluster
    assert first_cluster["cluster"]["count"] == 3
    assert first_cluster["cluster"]["start_ts"] == base_ts.isoformat()
    assert first_cluster["cluster"]["end_ts"] == (base_ts + timedelta(seconds=3)).isoformat()
    assert first_cluster["cluster"]["max_abs_z"] == 8.0  # Max of 7.5, 8.0, 6.5

    # Second cluster should be a single anomaly (no cluster field)
    second_cluster = clustered[1]
    assert "cluster" not in second_cluster or second_cluster.get("cluster") is None
    assert second_cluster["ts"] == (base_ts + timedelta(seconds=9)).isoformat()


def test_cluster_anomalies_different_symbols():
    """Test that clustering respects symbol boundaries."""
    base_ts = datetime(2025, 10, 24, 12, 0, 0, tzinfo=timezone.utc)

    anomalies = [
        {
            "symbol": "SYM1",
            "detector": "JUMP",
            "ts": base_ts.isoformat(),
            "features": {"z_score": 7.0},
            "label": "anomaly"
        },
        {
            "symbol": "SYM2",
            "detector": "JUMP",
            "ts": (base_ts + timedelta(seconds=1)).isoformat(),
            "features": {"z_score": 7.5},
            "label": "anomaly"
        },
    ]

    # Even though timestamps are within cooldown, different symbols should not cluster
    clustered = cluster_anomalies(anomalies, cooldown_seconds=5)

    # Should have 2 separate anomalies (no clustering)
    assert len(clustered) == 2
    assert clustered[0]["symbol"] == "SYM1"
    assert clustered[1]["symbol"] == "SYM2"
    assert "cluster" not in clustered[0]
    assert "cluster" not in clustered[1]


def test_cluster_anomalies_different_detectors():
    """Test that clustering respects detector boundaries."""
    base_ts = datetime(2025, 10, 24, 12, 0, 0, tzinfo=timezone.utc)

    anomalies = [
        {
            "symbol": "TEST",
            "detector": "JUMP",
            "ts": base_ts.isoformat(),
            "features": {"z_score": 7.0},
            "label": "anomaly"
        },
        {
            "symbol": "TEST",
            "detector": "ZSCORE",
            "ts": (base_ts + timedelta(seconds=1)).isoformat(),
            "features": {"z_score": 7.5},
            "label": "anomaly"
        },
    ]

    # Even though timestamps are within cooldown, different detectors should not cluster
    clustered = cluster_anomalies(anomalies, cooldown_seconds=5)

    # Should have 2 separate anomalies
    assert len(clustered) == 2
    assert clustered[0]["detector"] == "JUMP"
    assert clustered[1]["detector"] == "ZSCORE"
    assert "cluster" not in clustered[0]
    assert "cluster" not in clustered[1]


def test_cluster_anomalies_zero_cooldown():
    """Test that cooldown=0 disables clustering."""
    base_ts = datetime(2025, 10, 24, 12, 0, 0, tzinfo=timezone.utc)

    anomalies = [
        {
            "symbol": "TEST",
            "detector": "JUMP",
            "ts": base_ts.isoformat(),
            "features": {"z_score": 7.0},
            "label": "anomaly"
        },
        {
            "symbol": "TEST",
            "detector": "JUMP",
            "ts": (base_ts + timedelta(seconds=1)).isoformat(),
            "features": {"z_score": 7.5},
            "label": "anomaly"
        },
    ]

    # With cooldown=0, should return input unchanged
    clustered = cluster_anomalies(anomalies, cooldown_seconds=0)

    assert len(clustered) == 2
    assert clustered == anomalies


def test_flush_bucket_single():
    """Test that single-item bucket returns record unchanged."""
    record = {
        "symbol": "TEST",
        "ts": "2025-10-24T12:00:00+00:00",
        "features": {"z_score": 7.0}
    }

    flushed = _flush_bucket([record])

    # Should return same record without cluster field
    assert flushed == record
    assert "cluster" not in flushed


def test_flush_bucket_multiple():
    """Test that multi-item bucket adds cluster metadata."""
    base_ts = datetime(2025, 10, 24, 12, 0, 0, tzinfo=timezone.utc)

    bucket = [
        {
            "symbol": "TEST",
            "ts": base_ts.isoformat(),
            "features": {"z_score": 7.0}
        },
        {
            "symbol": "TEST",
            "ts": (base_ts + timedelta(seconds=2)).isoformat(),
            "features": {"z_score": -8.5}
        },
        {
            "symbol": "TEST",
            "ts": (base_ts + timedelta(seconds=4)).isoformat(),
            "features": {"z_score": 6.2}
        },
    ]

    flushed = _flush_bucket(bucket)

    # Should use first record as base
    assert flushed["symbol"] == "TEST"
    assert flushed["ts"] == base_ts.isoformat()

    # Should have cluster metadata
    assert "cluster" in flushed
    assert flushed["cluster"]["count"] == 3
    assert flushed["cluster"]["start_ts"] == base_ts.isoformat()
    assert flushed["cluster"]["end_ts"] == (base_ts + timedelta(seconds=4)).isoformat()
    assert flushed["cluster"]["max_abs_z"] == 8.5  # Max absolute value
