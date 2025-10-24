"""
QA Configuration

Loads and validates QA configuration with sane defaults.
"""

from typing import Any, Dict

from loguru import logger


# Default QA configuration
DEFAULT_QA_CONFIG = {
    "enable_ai": True,
    "hourly_window_min": 90,
    "daily_run_utc": "00:15",
    "ai_labeler": "rules",
    "iforest": {
        "n_estimators": 200,
        "contamination": 0.005,
        "random_state": 42
    },
    "zscore": {
        "window": "1h",
        "k": 5.0
    },
    "jump": {
        "k_sigma": 6.0,
        "spread_stable_bps": 50
    }
}


def load_qa_config(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Load QA configuration with defaults.

    Args:
        config: Main configuration dictionary (from config.yml)

    Returns:
        QA configuration dictionary with defaults applied
    """
    qa_config = config.get("qa", {})

    # Apply defaults for missing keys
    resolved = DEFAULT_QA_CONFIG.copy()
    resolved.update(qa_config)

    # Ensure nested dicts are merged properly
    for key in ["iforest", "zscore", "jump"]:
        if key in qa_config:
            resolved[key] = {**DEFAULT_QA_CONFIG[key], **qa_config[key]}

    # Validate configuration
    _validate_qa_config(resolved)

    # Log resolved configuration
    logger.info(f"Resolved QA config: enable_ai={resolved['enable_ai']}, "
                f"hourly_window_min={resolved['hourly_window_min']}, "
                f"daily_run_utc={resolved['daily_run_utc']}, "
                f"ai_labeler={resolved['ai_labeler']}")
    logger.debug(f"IForest: {resolved['iforest']}")
    logger.debug(f"ZScore: {resolved['zscore']}")
    logger.debug(f"Jump: {resolved['jump']}")

    return resolved


def _validate_qa_config(qa_config: Dict[str, Any]) -> None:
    """
    Validate QA configuration.

    Args:
        qa_config: QA configuration dictionary

    Raises:
        ValueError: If configuration is invalid
    """
    if not isinstance(qa_config.get("enable_ai"), bool):
        raise ValueError("qa.enable_ai must be boolean")

    if not isinstance(qa_config.get("hourly_window_min"), (int, float)):
        raise ValueError("qa.hourly_window_min must be numeric")

    if qa_config["hourly_window_min"] <= 0:
        raise ValueError("qa.hourly_window_min must be positive")

    if not isinstance(qa_config.get("daily_run_utc"), str):
        raise ValueError("qa.daily_run_utc must be string (HH:MM format)")

    valid_labelers = ["rules", "llm", "hybrid"]
    if qa_config.get("ai_labeler") not in valid_labelers:
        raise ValueError(f"qa.ai_labeler must be one of {valid_labelers}")

    # Validate nested configs
    iforest = qa_config.get("iforest", {})
    if iforest.get("contamination", 0) <= 0 or iforest.get("contamination", 0) >= 1:
        raise ValueError("qa.iforest.contamination must be in (0, 1)")

    zscore = qa_config.get("zscore", {})
    if zscore.get("k", 0) <= 0:
        raise ValueError("qa.zscore.k must be positive")

    jump = qa_config.get("jump", {})
    if jump.get("k_sigma", 0) <= 0:
        raise ValueError("qa.jump.k_sigma must be positive")

    logger.debug("QA configuration validated successfully")
