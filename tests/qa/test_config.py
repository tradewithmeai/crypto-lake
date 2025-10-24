"""
Fast tests for QA configuration.

No file I/O - pure in-memory unit tests.
"""

import copy

import pytest

from qa.config import DEFAULT_QA_CONFIG, _validate_qa_config, load_qa_config


def test_default_qa_config():
    """Test default QA configuration structure."""
    assert DEFAULT_QA_CONFIG["enable_ai"] is True
    assert DEFAULT_QA_CONFIG["hourly_window_min"] == 90
    assert DEFAULT_QA_CONFIG["daily_run_utc"] == "00:15"
    assert DEFAULT_QA_CONFIG["ai_labeler"] == "rules"
    assert "iforest" in DEFAULT_QA_CONFIG
    assert "zscore" in DEFAULT_QA_CONFIG
    assert "jump" in DEFAULT_QA_CONFIG


def test_load_qa_config_empty():
    """Test loading QA config when qa block is missing."""
    config = {}
    qa_config = load_qa_config(config)

    # Should apply defaults
    assert qa_config["enable_ai"] is True
    assert qa_config["hourly_window_min"] == 90


def test_load_qa_config_partial():
    """Test loading QA config with partial configuration."""
    config = {
        "qa": {
            "enable_ai": False,
            "hourly_window_min": 120,
        }
    }
    qa_config = load_qa_config(config)

    # Should merge with defaults
    assert qa_config["enable_ai"] is False
    assert qa_config["hourly_window_min"] == 120
    assert qa_config["daily_run_utc"] == "00:15"  # From default


def test_load_qa_config_nested():
    """Test loading QA config with nested overrides."""
    config = {
        "qa": {
            "iforest": {
                "contamination": 0.01,
            }
        }
    }
    qa_config = load_qa_config(config)

    # Should merge nested dicts
    assert qa_config["iforest"]["contamination"] == 0.01
    assert qa_config["iforest"]["n_estimators"] == 200  # From default
    assert qa_config["iforest"]["random_state"] == 42  # From default


def test_validate_qa_config_valid():
    """Test validation with valid configuration."""
    qa_config = copy.deepcopy(DEFAULT_QA_CONFIG)
    # Should not raise
    _validate_qa_config(qa_config)


def test_validate_qa_config_invalid_enable_ai():
    """Test validation with invalid enable_ai."""
    qa_config = copy.deepcopy(DEFAULT_QA_CONFIG)
    qa_config["enable_ai"] = "true"  # Should be bool

    with pytest.raises(ValueError, match="enable_ai must be boolean"):
        _validate_qa_config(qa_config)


def test_validate_qa_config_invalid_window():
    """Test validation with invalid window."""
    qa_config = copy.deepcopy(DEFAULT_QA_CONFIG)
    qa_config["hourly_window_min"] = -10

    with pytest.raises(ValueError, match="hourly_window_min must be positive"):
        _validate_qa_config(qa_config)


def test_validate_qa_config_invalid_labeler():
    """Test validation with invalid labeler."""
    qa_config = copy.deepcopy(DEFAULT_QA_CONFIG)
    qa_config["ai_labeler"] = "invalid"

    with pytest.raises(ValueError, match="ai_labeler must be one of"):
        _validate_qa_config(qa_config)


def test_validate_qa_config_invalid_contamination():
    """Test validation with invalid contamination."""
    qa_config = copy.deepcopy(DEFAULT_QA_CONFIG)
    qa_config["iforest"]["contamination"] = 1.5  # Must be in (0, 1)

    with pytest.raises(ValueError, match="contamination must be in"):
        _validate_qa_config(qa_config)


def test_validate_qa_config_invalid_zscore_k():
    """Test validation with invalid zscore k."""
    qa_config = copy.deepcopy(DEFAULT_QA_CONFIG)
    qa_config["zscore"]["k"] = -3.0

    with pytest.raises(ValueError, match="zscore.k must be positive"):
        _validate_qa_config(qa_config)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
