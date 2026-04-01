#!/usr/bin/env python3
"""
Analyze tool failures across multiple RCA incidents.

This CLI tool aggregates and analyzes tool call failures from the
tool_failures.jsonl log file to identify patterns and reliability issues.

Usage:
    python -m sim.rca.analyze_failures --log-file outputs/tool_failures.jsonl
    python -m sim.rca.analyze_failures  # Uses default path
"""

import argparse
import json
from collections import Counter, defaultdict
from pathlib import Path


def load_failures(log_file: Path) -> list[dict]:
    """Load failure records from JSONL file."""
    if not log_file.exists():
        return []

    records = []
    with open(log_file) as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    return records


def analyze_failures(records: list[dict]) -> dict:
    """Analyze failure patterns across all records."""
    if not records:
        return {"error": "No failure records found"}

    # Aggregate stats
    total_incidents = len(records)
    total_calls = sum(r.get("total_calls", 0) for r in records)
    all_failures = []
    for r in records:
        all_failures.extend(r.get("failures", []))

    total_failures = len(all_failures)

    # Group by tool
    failures_by_tool = defaultdict(list)
    for f in all_failures:
        failures_by_tool[f["tool"]].append(f)

    # Group by error type
    failures_by_error_type = Counter(f.get("error_type", "unknown") for f in all_failures)

    # Find common error messages
    error_messages = Counter(f.get("error", "unknown")[:100] for f in all_failures)

    return {
        "summary": {
            "total_incidents": total_incidents,
            "total_tool_calls": total_calls,
            "total_failures": total_failures,
            "overall_success_rate": round((total_calls - total_failures) / total_calls * 100, 1) if total_calls > 0 else 100.0,
        },
        "failures_by_tool": {
            tool: {
                "count": len(failures),
                "percentage": round(len(failures) / total_failures * 100, 1) if total_failures > 0 else 0,
                "error_types": Counter(f.get("error_type", "unknown") for f in failures),
            }
            for tool, failures in sorted(failures_by_tool.items(), key=lambda x: -len(x[1]))
        },
        "failures_by_error_type": dict(failures_by_error_type.most_common()),
        "common_errors": [
            {"message": msg, "count": count}
            for msg, count in error_messages.most_common(10)
        ],
    }


def print_report(analysis: dict) -> None:
    """Print a formatted report to stdout."""
    if "error" in analysis:
        print(f"Error: {analysis['error']}")
        return

    summary = analysis["summary"]

    print("=" * 60)
    print(f"Tool Failure Summary (across {summary['total_incidents']} incidents)")
    print("=" * 60)
    print(f"Total tool calls: {summary['total_tool_calls']}")
    print(f"Failed calls: {summary['total_failures']} ({100 - summary['overall_success_rate']:.1f}%)")
    print(f"Success rate: {summary['overall_success_rate']}%")
    print()

    print("By Tool:")
    for tool, stats in analysis["failures_by_tool"].items():
        print(f"  {tool}: {stats['count']} failures ({stats['percentage']:.1f}%)")
        for error_type, count in stats["error_types"].items():
            print(f"    - {error_type}: {count}")
    print()

    print("By Error Type:")
    for error_type, count in analysis["failures_by_error_type"].items():
        print(f"  {error_type}: {count}")
    print()

    print("Common Error Patterns:")
    for i, error in enumerate(analysis["common_errors"], 1):
        msg = error["message"]
        if len(msg) > 60:
            msg = msg[:57] + "..."
        print(f"  {i}. \"{msg}\" ({error['count']} occurrences)")


def main():
    parser = argparse.ArgumentParser(
        description="Analyze tool failures across RCA incidents"
    )
    parser.add_argument(
        "--log-file",
        type=Path,
        default=Path("sim/output/tool_failures.jsonl"),
        help="Path to tool_failures.jsonl log file (default: sim/output/tool_failures.jsonl)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output raw JSON instead of formatted report",
    )

    args = parser.parse_args()

    records = load_failures(args.log_file)
    analysis = analyze_failures(records)

    if args.json:
        print(json.dumps(analysis, indent=2, default=str))
    else:
        print_report(analysis)


if __name__ == "__main__":
    main()
