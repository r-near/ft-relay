#!/usr/bin/env python3
"""Parse BENCHMARK_METRIC lines into a Markdown matrix table."""

from __future__ import annotations

import os
import re
import sys
from collections import OrderedDict


METRIC_ORDER = [
    "Benchmark",
    "HTTP duration (s)",
    "HTTP throughput (req/s)",
    "HTTP success (%)",
    "Settlement loop (s)",
    "Settlement total (s)",
    "Settlement lag (s)",
    "Chain throughput (tx/s)",
    "Post-HTTP throughput (tx/s)",
    "On-chain success (%)",
]

LINE_RE = re.compile(r"^BENCHMARK_METRIC\|([^|]+)\|([^|]+)\|(.*)$")


def format_env(env: str) -> str:
    return env.capitalize()


def format_value(metric: str, value: str) -> str:
    value = value.strip()
    if not value:
        return "-"
    if metric == "Chain throughput (tx/s)":
        return f"**{value}**"
    return value


def parse_metrics(path: str) -> tuple[list[str], dict[str, dict[str, str]]]:
    data: "OrderedDict[str, dict[str, str]]" = OrderedDict()

    with open(path, encoding="utf-8", errors="ignore") as source:
        for raw in source:
            match = LINE_RE.match(raw.strip())
            if not match:
                continue
            env, metric, value = match.groups()
            env = env.strip()
            metric = metric.strip()
            value = value.strip()
            data.setdefault(env, {})[metric] = value

    envs = list(data.keys())
    return envs, data


def build_table(envs: list[str], data: dict[str, dict[str, str]]) -> str:
    header = ["Metric"] + [format_env(env) for env in envs]
    lines = ["| " + " | ".join(header) + " |", "| " + " | ".join(["---"] * len(header)) + " |"]

    for metric in METRIC_ORDER:
        row = [metric]
        for env in envs:
            row.append(format_value(metric, data.get(env, {}).get(metric, "")))
        lines.append("| " + " | ".join(row) + " |")

    return "\n".join(lines)


def main() -> int:
    if len(sys.argv) != 2:
        print("Usage: benchmark_matrix.py <logfile>", file=sys.stderr)
        return 1

    logfile = sys.argv[1]
    envs, data = parse_metrics(logfile)
    if not envs:
        print("No benchmark metrics were captured.", file=sys.stderr)
        return 1

    table = build_table(envs, data)

    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with open(summary_path, "a", encoding="utf-8") as summary:
            summary.write("## Benchmark Results\n\n")
            summary.write(table)
            summary.write("\n\n")
    else:
        print(table)

    return 0


if __name__ == "__main__":
    sys.exit(main())
