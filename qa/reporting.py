"""
QA Reporting

Generates daily QA reports in Markdown format with:
- Summary statistics
- Violations by symbol
- Anomalies by detector
- Fusion scores and verdicts
- Both UTC and Europe/London timestamps
"""

import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List

import pandas as pd
import pytz
from loguru import logger

from qa.fusion import load_anomalies, load_violations
from qa.schema_validator import SEVERITY_MAP


def generate_daily_report(
    date_str: str,
    violations_path: str,
    anomalies_path: str,
    fusion_path: str,
    output_path: str,
    anomalies_top_n: int = 10
) -> None:
    """
    Generate daily QA report.

    Args:
        date_str: Date being reported (YYYY-MM-DD)
        violations_path: Path to violations JSONL
        anomalies_path: Path to anomalies JSONL
        fusion_path: Path to fusion Parquet
        output_path: Output path for report
    """
    logger.info(f"Generating QA report for {date_str}")

    # Load data
    violations = load_violations(violations_path)
    anomalies = load_anomalies(anomalies_path)

    if os.path.exists(fusion_path):
        fusion_df = pd.read_parquet(fusion_path)
    else:
        fusion_df = pd.DataFrame()

    # Generate report sections
    sections = []

    # Header
    sections.append(_generate_header(date_str))

    # Summary
    sections.append(_generate_summary(violations, anomalies, fusion_df))

    # Clustered Anomaly Summary (new section)
    sections.append(_generate_clustered_anomaly_summary(anomalies, anomalies_top_n))

    # Fusion Scores
    if not fusion_df.empty:
        sections.append(_generate_fusion_table(fusion_df))

    # Anomalies (detailed by detector)
    if anomalies:
        sections.append(_generate_anomalies_section(anomalies))

    # Footer
    sections.append(_generate_footer())

    # Combine and write
    report_content = "\n\n".join(sections)

    # Atomic write
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    tmp_path = output_path + '.tmp'

    try:
        with open(tmp_path, 'w', encoding='utf-8', newline='\n') as f:
            f.write(report_content)
        os.replace(tmp_path, output_path)
        logger.info(f"Report written to {output_path}")
    except Exception as e:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        raise e


def _generate_header(date_str: str) -> str:
    """Generate report header with timestamps."""
    now_utc = datetime.now(timezone.utc)
    london_tz = pytz.timezone("Europe/London")
    now_london = now_utc.astimezone(london_tz)

    utc_str = now_utc.strftime("%Y-%m-%d %H:%M:%S UTC")
    london_str = now_london.strftime("%Y-%m-%d %H:%M:%S %Z")

    header = f"""# Crypto Lake QA Report - {date_str}

**Generated:** {utc_str} ({london_str})

---
"""
    return header


def _generate_summary(
    violations: List[Dict[str, Any]],
    anomalies: List[Dict[str, Any]],
    fusion_df: pd.DataFrame
) -> str:
    """Generate summary statistics."""
    # Count violations by severity
    critical_count = sum(1 for v in violations if v["severity"] == "critical")
    major_count = sum(1 for v in violations if v["severity"] == "major")
    minor_count = sum(1 for v in violations if v["severity"] == "minor")

    # Count fusion verdicts
    if not fusion_df.empty:
        verdict_counts = fusion_df["verdict"].value_counts().to_dict()
        pass_count = verdict_counts.get("PASS", 0)
        review_count = verdict_counts.get("REVIEW", 0)
        fail_count = verdict_counts.get("FAIL", 0)
        avg_score = fusion_df["score"].mean()
    else:
        pass_count = review_count = fail_count = 0
        avg_score = 0.0

    summary = f"""## Summary

| Metric | Value |
|--------|-------|
| **Total Violations** | {len(violations)} |
| Critical | {critical_count} |
| Major | {major_count} |
| Minor | {minor_count} |
| **Total Anomalies** | {len(anomalies)} |
| **Fusion Scores** | |
| PASS | {pass_count} |
| REVIEW | {review_count} |
| FAIL | {fail_count} |
| Average Score | {avg_score:.4f} |
"""
    return summary


def _generate_clustered_anomaly_summary(anomalies: List[Dict[str, Any]], top_n: int = 10) -> str:
    """
    Generate clustered anomaly summary with top symbols.

    Args:
        anomalies: List of anomaly dictionaries (may include cluster metadata)
        top_n: Number of top symbols to display

    Returns:
        Markdown section for clustered anomaly summary
    """
    if not anomalies:
        return """## Anomaly Summary (clustered)

No anomalies recorded."""

    # Count by detector
    detector_counts = {}
    for a in anomalies:
        detector = a.get("detector", "unknown")
        detector_counts[detector] = detector_counts.get(detector, 0) + 1

    # Count by symbol (with cluster expansion)
    symbol_counts = {}
    symbol_details = {}  # symbol -> {time_range, example_rationale, example_label}

    for a in anomalies:
        symbol = a.get("symbol", "unknown")
        cluster_info = a.get("cluster")

        # If clustered, count represents multiple anomalies
        if cluster_info:
            count = cluster_info.get("count", 1)
        else:
            count = 1

        symbol_counts[symbol] = symbol_counts.get(symbol, 0) + count

        # Track details for this symbol
        if symbol not in symbol_details:
            ts_str = a.get("ts", "")
            symbol_details[symbol] = {
                "min_ts": ts_str,
                "max_ts": ts_str,
                "rationale": a.get("rationale", "N/A"),
                "label": a.get("label", "anomaly")
            }
        else:
            # Update time range
            ts_str = a.get("ts", "")
            if ts_str < symbol_details[symbol]["min_ts"]:
                symbol_details[symbol]["min_ts"] = ts_str
            if ts_str > symbol_details[symbol]["max_ts"]:
                symbol_details[symbol]["max_ts"] = ts_str

    # Sort symbols by count
    sorted_symbols = sorted(symbol_counts.items(), key=lambda x: -x[1])[:top_n]

    # Build section
    sections = ["## Anomaly Summary (clustered)", ""]

    # Detector breakdown
    detector_str = ", ".join(f"{det}={cnt}" for det, cnt in sorted(detector_counts.items()))
    sections.append(f"**Total clustered anomalies:** {len(anomalies)}")
    sections.append(f"**By detector:** {detector_str}")
    sections.append("")

    # Top symbols table
    if sorted_symbols:
        sections.append(f"**Top {min(top_n, len(sorted_symbols))} symbols by anomaly count:**")
        sections.append("")
        sections.append("| Symbol | Anomalies | Time Range (UTC) | Example |")
        sections.append("|--------|-----------|------------------|---------|")

        for symbol, count in sorted_symbols:
            details = symbol_details[symbol]
            min_ts = details["min_ts"]
            max_ts = details["max_ts"]
            rationale = details["rationale"]

            # Parse timestamps for display
            try:
                min_obj = datetime.fromisoformat(min_ts.replace('Z', '+00:00'))
                max_obj = datetime.fromisoformat(max_ts.replace('Z', '+00:00'))
                time_range = f"{min_obj.strftime('%H:%M:%S')} → {max_obj.strftime('%H:%M:%S')}"
            except:
                time_range = "N/A"

            # Truncate rationale
            if len(rationale) > 50:
                rationale = rationale[:47] + "..."

            sections.append(f"| {symbol} | {count} | {time_range} | {rationale} |")

    return "\n".join(sections)


def _generate_fusion_table(fusion_df: pd.DataFrame) -> str:
    """Generate fusion scores table."""
    # Sort by score ascending (worst first)
    fusion_sorted = fusion_df.sort_values("score").copy()

    # Parse metadata
    fusion_sorted["metadata_parsed"] = fusion_sorted["metadata"].apply(json.loads)

    rows = []
    rows.append("## Fusion Scores by Symbol")
    rows.append("")
    rows.append("| Symbol | Verdict | Score | Critical | Major | Minor | Anomalies |")
    rows.append("|--------|---------|-------|----------|-------|-------|-----------|")

    for _, row in fusion_sorted.iterrows():
        meta = row["metadata_parsed"]
        symbol = row["symbol"]
        verdict = row["verdict"]
        score = row["score"]
        critical = meta.get("critical_count", 0)
        major = meta.get("major_count", 0)
        minor = meta.get("minor_count", 0)
        anomalies = meta.get("anomalies_count", 0)

        # Add emoji for verdict
        if verdict == "PASS":
            verdict_display = "✓ PASS"
        elif verdict == "REVIEW":
            verdict_display = "⚠ REVIEW"
        else:
            verdict_display = "✗ FAIL"

        rows.append(
            f"| {symbol} | {verdict_display} | {score:.4f} | {critical} | {major} | {minor} | {anomalies} |"
        )

    return "\n".join(rows)


def _generate_violations_section(violations: List[Dict[str, Any]]) -> str:
    """Generate violations section."""
    # Group by rule
    by_rule = {}
    for v in violations:
        rule = v["rule"]
        if rule not in by_rule:
            by_rule[rule] = []
        by_rule[rule].append(v)

    # Sort by severity (critical first)
    severity_order = {"critical": 0, "major": 1, "minor": 2}
    sorted_rules = sorted(
        by_rule.items(),
        key=lambda x: (
            severity_order.get(SEVERITY_MAP.get(x[0], "major"), 1),
            -len(x[1])
        )
    )

    sections = ["## Violations by Rule", ""]

    for rule, rule_violations in sorted_rules:
        severity = SEVERITY_MAP.get(rule, "major")
        count = len(rule_violations)

        sections.append(f"### {rule} ({severity.upper()}) - {count} violations")
        sections.append("")

        # Show up to 10 examples
        examples = rule_violations[:10]

        sections.append("| Symbol | Timestamp (UTC) | Detail |")
        sections.append("|--------|-----------------|--------|")

        for v in examples:
            symbol = v["symbol"]
            ts = v["ts"]
            detail = v["detail"]

            # Parse timestamp
            try:
                ts_obj = datetime.fromisoformat(ts.replace('Z', '+00:00'))
                ts_display = ts_obj.strftime("%Y-%m-%d %H:%M:%S")
            except:
                ts_display = ts

            sections.append(f"| {symbol} | {ts_display} | {detail} |")

        if len(rule_violations) > 10:
            sections.append("")
            sections.append(f"*... and {len(rule_violations) - 10} more violations*")

        sections.append("")

    return "\n".join(sections)


def _generate_anomalies_section(anomalies: List[Dict[str, Any]]) -> str:
    """Generate anomalies section."""
    # Group by detector
    by_detector = {}
    for a in anomalies:
        detector = a["detector"]
        if detector not in by_detector:
            by_detector[detector] = []
        by_detector[detector].append(a)

    # Sort by count descending
    sorted_detectors = sorted(by_detector.items(), key=lambda x: -len(x[1]))

    sections = ["## Anomalies by Detector", ""]

    for detector, detector_anomalies in sorted_detectors:
        count = len(detector_anomalies)

        sections.append(f"### {detector} - {count} anomalies")
        sections.append("")

        # Show up to 10 examples
        examples = detector_anomalies[:10]

        sections.append("| Symbol | Timestamp (UTC) | Rationale |")
        sections.append("|--------|-----------------|-----------|")

        for a in examples:
            symbol = a["symbol"]
            ts = a["ts"]
            rationale = a.get("rationale", "N/A")

            # Parse timestamp
            try:
                ts_obj = datetime.fromisoformat(ts.replace('Z', '+00:00'))
                ts_display = ts_obj.strftime("%Y-%m-%d %H:%M:%S")
            except:
                ts_display = ts

            # Truncate rationale if too long
            if len(rationale) > 80:
                rationale = rationale[:77] + "..."

            sections.append(f"| {symbol} | {ts_display} | {rationale} |")

        if len(detector_anomalies) > 10:
            sections.append("")
            sections.append(f"*... and {len(detector_anomalies) - 10} more anomalies*")

        sections.append("")

    return "\n".join(sections)


def _generate_footer() -> str:
    """Generate report footer."""
    footer = """---

*Report generated by Crypto Lake QA Sidecar*
"""
    return footer


def add_age_and_status(report_content: str, last_seen_utc: datetime = None) -> str:
    """
    Add age and status to report header.

    Args:
        report_content: Existing report content
        last_seen_utc: Last seen data timestamp (UTC)

    Returns:
        Updated report content
    """
    if last_seen_utc is None:
        return report_content

    now_utc = datetime.now(timezone.utc)
    age_seconds = (now_utc - last_seen_utc).total_seconds()

    # Format last seen with both UTC and London time
    london_tz = pytz.timezone("Europe/London")
    last_seen_london = last_seen_utc.astimezone(london_tz)

    utc_str = last_seen_utc.strftime("%Y-%m-%d %H:%M:%S UTC")
    london_str = last_seen_london.strftime("%Y-%m-%d %H:%M:%S %Z")

    # Determine status
    if age_seconds < 300:  # < 5 minutes
        status = "FRESH"
    elif age_seconds < 3600:  # < 1 hour
        status = "RECENT"
    else:
        status = "STALE"

    # Insert after "Generated:" line
    age_line = f"**Last Seen:** {utc_str} ({london_str}) | Age: {int(age_seconds)}s | Status: {status}"

    lines = report_content.split('\n')
    for i, line in enumerate(lines):
        if line.startswith("**Generated:**"):
            lines.insert(i + 1, age_line)
            break

    return '\n'.join(lines)
