"""
Environment-aware configuration loader.

Wraps the existing tools.common.load_config() and adds environment variable overrides
for cloud deployment scenarios. This allows secrets to be injected via env vars rather
than stored in config.yml.

Environment variables (override config.yml values):
- CRYPTO_DB_URL: Database connection string
- GCS_BUCKET_NAME: Google Cloud Storage bucket name
- GCP_PROJECT_ID: Google Cloud Platform project ID
- GCP_REGION: GCP region (e.g., europe-west2)
"""

from pathlib import Path
import os
import yaml


def load_config(path: str = "config.yml") -> dict:
    """
    Load configuration from YAML file with environment variable overrides.

    Args:
        path: Path to config.yml file (default: config.yml)

    Returns:
        Configuration dictionary with env vars applied
    """
    cfg = {}
    p = Path(path)
    if p.exists():
        with open(p, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}

    # ENV overrides (env-first approach for cloud deployments)
    cfg.setdefault("database", {})
    cfg["database"]["url"] = os.getenv("CRYPTO_DB_URL", cfg["database"].get("url", ""))

    cfg.setdefault("gcs", {})
    cfg["gcs"]["bucket_name"] = os.getenv("GCS_BUCKET_NAME", cfg["gcs"].get("bucket_name", ""))

    cfg.setdefault("gcp", {})
    cfg["gcp"]["project_id"] = os.getenv("GCP_PROJECT_ID", cfg["gcp"].get("project_id", ""))
    cfg["gcp"]["region"] = os.getenv("GCP_REGION", cfg["gcp"].get("region", ""))

    return cfg
