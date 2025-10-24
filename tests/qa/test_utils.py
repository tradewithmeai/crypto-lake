"""
Fast tests for QA utilities.

Minimal file I/O, use temp directories.
"""

import json
import os
import tempfile
from datetime import datetime, timezone

import pandas as pd
import pytest

from qa.utils import (
    atomic_write_jsonl,
    atomic_write_parquet,
    atomic_write_text,
    format_duration,
    get_qa_ai_path,
    get_qa_fusion_path,
    get_qa_report_path,
    get_qa_schema_path,
    parse_date_args,
    to_iso8601_utc,
)


def test_parse_date_args_day():
    """Test parsing --day argument."""
    start, end = parse_date_args(None, None, "2025-10-23")
    assert start == "2025-10-23"
    assert end == "2025-10-23"


def test_parse_date_args_from_to():
    """Test parsing --from and --to arguments."""
    start, end = parse_date_args("2025-10-21", "2025-10-23", None)
    assert start == "2025-10-21"
    assert end == "2025-10-23"


def test_parse_date_args_from_only():
    """Test parsing --from only."""
    start, end = parse_date_args("2025-10-21", None, None)
    assert start == "2025-10-21"
    assert end == "2025-10-21"


def test_parse_date_args_invalid():
    """Test invalid date arguments."""
    with pytest.raises(ValueError, match="Cannot use --day with --from or --to"):
        parse_date_args("2025-10-21", None, "2025-10-23")


def test_to_iso8601_utc():
    """Test ISO8601 UTC conversion."""
    dt = datetime(2025, 10, 23, 12, 34, 56, tzinfo=timezone.utc)
    iso_str = to_iso8601_utc(dt)
    assert "2025-10-23T12:34:56" in iso_str
    assert "+00:00" in iso_str or "Z" in iso_str


def test_format_duration_seconds():
    """Test duration formatting for seconds."""
    assert format_duration(45.7) == "45.7s"


def test_format_duration_minutes():
    """Test duration formatting for minutes."""
    result = format_duration(125.5)
    assert "2m" in result
    assert "5.5s" in result


def test_get_qa_paths():
    """Test QA path getters."""
    base = "D:/Test"
    date = "2025-10-23"

    assert "schema" in get_qa_schema_path(base, date)
    assert "ai" in get_qa_ai_path(base, date)
    assert "fusion" in get_qa_fusion_path(base, date)
    assert "reports" in get_qa_report_path(base, date)


def test_atomic_write_jsonl():
    """Test atomic JSONL writing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "test.jsonl")
        records = [
            {"key": "value1"},
            {"key": "value2"},
        ]

        atomic_write_jsonl(records, path)

        # Verify file exists and content
        assert os.path.exists(path)

        with open(path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            assert len(lines) == 2
            assert json.loads(lines[0])["key"] == "value1"
            assert json.loads(lines[1])["key"] == "value2"


def test_atomic_write_parquet():
    """Test atomic Parquet writing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "test.parquet")
        df = pd.DataFrame({
            "col1": [1, 2, 3],
            "col2": ["a", "b", "c"],
        })

        atomic_write_parquet(df, path)

        # Verify file exists
        assert os.path.exists(path)

        # Read back
        df_read = pd.read_parquet(path)
        assert len(df_read) == 3
        assert list(df_read.columns) == ["col1", "col2"]


def test_atomic_write_text():
    """Test atomic text writing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "test.txt")
        content = "Hello, World!\nLine 2"

        atomic_write_text(content, path)

        # Verify file exists and content
        assert os.path.exists(path)

        with open(path, 'r', encoding='utf-8') as f:
            read_content = f.read()
            assert read_content == content


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
