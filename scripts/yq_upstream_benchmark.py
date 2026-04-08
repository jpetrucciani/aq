#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import os
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

from jq_upstream_report import (
    DEFAULT_CASE_ENV,
    DEFAULT_SOURCES,
    UpstreamCase,
    case_requires_module_fixtures,
    fetch_text,
    harvest_success_cases,
    materialize_module_fixtures,
    semantically_matches_expected,
    write_text,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Benchmark harvested jq upstream direct success cases against mikefarah/yq "
            "and aq, comparing only the overlap where yq matches the jq upstream expectation."
        )
    )
    parser.add_argument(
        "--source-url",
        action="append",
        dest="source_urls",
        default=[],
        help="Upstream jq test file URL. Repeatable. Defaults to the current harvested jq success-case sources.",
    )
    parser.add_argument(
        "--aq-binary",
        default="target/release/aq",
        help="Path to the aq binary to execute, relative to the repo root.",
    )
    parser.add_argument(
        "--yq-binary",
        default="yq",
        help="Path to the mikefarah/yq binary to execute.",
    )
    parser.add_argument(
        "--warmup-runs",
        type=int,
        default=1,
        help="Warmup runs per tool per case.",
    )
    parser.add_argument(
        "--measured-runs",
        type=int,
        default=3,
        help="Measured runs per tool per case.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=10.0,
        help="Per-run execution timeout in seconds.",
    )
    parser.add_argument(
        "--comparison-tolerance",
        type=float,
        default=0.05,
        help="Relative tolerance for calling two medians 'roughly equal'.",
    )
    parser.add_argument(
        "--case-limit",
        type=int,
        help="Optional cap on harvested cases, useful for smoke tests.",
    )
    parser.add_argument(
        "--json-out",
        default="benchmarks/yq-upstream-benchmark.json",
        help="Path for the JSON report, relative to the repo root.",
    )
    parser.add_argument(
        "--markdown-out",
        default="benchmarks/yq-upstream-benchmark.md",
        help="Path for the Markdown report, relative to the repo root.",
    )
    return parser.parse_args()


def ensure_aq_binary(repo_root: Path, binary_path: Path) -> None:
    if path_display(binary_path, repo_root) == "target/release/aq":
        subprocess.run(
            ["cargo", "build", "--release", "--quiet"],
            cwd=repo_root,
            check=True,
        )
        return

    if binary_path.exists():
        return

    raise FileNotFoundError(f"aq binary not found at {binary_path}")


def read_version(command: list[str], cwd: Path | None) -> str:
    result = subprocess.run(
        command + ["--version"],
        cwd=cwd,
        check=False,
        capture_output=True,
        text=True,
    )
    version = (result.stdout or result.stderr).strip()
    return version or "unknown"


def run_once(
    command: list[str],
    input_text: str,
    expected_stdout: str,
    timeout_seconds: float,
    cwd: Path | None,
) -> dict[str, Any]:
    start = time.perf_counter_ns()
    try:
        env = dict(os.environ)
        env.update(DEFAULT_CASE_ENV)
        result = subprocess.run(
            command,
            input=input_text,
            text=True,
            capture_output=True,
            cwd=cwd,
            env=env,
            check=False,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired as error:
        end = time.perf_counter_ns()
        return {
            "status": "timeout",
            "elapsed_seconds": (end - start) / 1_000_000_000,
            "stdout": error.stdout or "",
            "stderr": error.stderr or "",
            "returncode": None,
        }

    end = time.perf_counter_ns()
    status = (
        "passed"
        if semantically_matches_expected(result.stdout, expected_stdout)
        else "failed"
    )
    return {
        "status": status,
        "elapsed_seconds": (end - start) / 1_000_000_000,
        "stdout": result.stdout,
        "stderr": result.stderr,
        "returncode": result.returncode,
    }


def benchmark_tool(
    tool_name: str,
    command: list[str],
    case: UpstreamCase,
    timeout_seconds: float,
    warmup_runs: int,
    measured_runs: int,
    cwd: Path | None,
) -> dict[str, Any]:
    for _ in range(warmup_runs):
        outcome = run_once(
            command, case.input_text, case.expected_stdout, timeout_seconds, cwd
        )
        if outcome["status"] != "passed":
            return {
                "tool": tool_name,
                "status": outcome["status"],
                "samples_seconds": [],
                "median_seconds": None,
                "mean_seconds": None,
                "min_seconds": None,
                "max_seconds": None,
                "stdout": outcome["stdout"],
                "stderr": outcome["stderr"],
                "returncode": outcome["returncode"],
            }

    samples: list[float] = []
    stdout = ""
    stderr = ""
    returncode: int | None = None
    for _ in range(measured_runs):
        outcome = run_once(
            command, case.input_text, case.expected_stdout, timeout_seconds, cwd
        )
        if outcome["status"] != "passed":
            return {
                "tool": tool_name,
                "status": outcome["status"],
                "samples_seconds": samples,
                "median_seconds": None,
                "mean_seconds": None,
                "min_seconds": None,
                "max_seconds": None,
                "stdout": outcome["stdout"],
                "stderr": outcome["stderr"],
                "returncode": outcome["returncode"],
            }
        samples.append(outcome["elapsed_seconds"])
        stdout = outcome["stdout"]
        stderr = outcome["stderr"]
        returncode = outcome["returncode"]

    return {
        "tool": tool_name,
        "status": "passed",
        "samples_seconds": samples,
        "median_seconds": statistics.median(samples),
        "mean_seconds": statistics.fmean(samples),
        "min_seconds": min(samples),
        "max_seconds": max(samples),
        "stdout": stdout,
        "stderr": stderr,
        "returncode": returncode,
    }


def compare_case(
    aq_result: dict[str, Any],
    yq_result: dict[str, Any],
    tolerance: float,
) -> dict[str, Any]:
    if aq_result["status"] != "passed" or yq_result["status"] != "passed":
        return {
            "status": "uncomparable",
            "aq_over_yq_ratio": None,
            "faster_tool": None,
        }

    aq_median = aq_result["median_seconds"]
    yq_median = yq_result["median_seconds"]
    if (
        not isinstance(aq_median, float)
        or not isinstance(yq_median, float)
        or yq_median <= 0
    ):
        return {
            "status": "uncomparable",
            "aq_over_yq_ratio": None,
            "faster_tool": None,
        }

    ratio = aq_median / yq_median
    if ratio > 1 + tolerance:
        faster_tool = "yq"
    elif ratio < 1 - tolerance:
        faster_tool = "aq"
    else:
        faster_tool = "roughly_equal"

    return {
        "status": "compared",
        "aq_over_yq_ratio": ratio,
        "faster_tool": faster_tool,
    }


def format_case_label(case: dict[str, Any]) -> str:
    return f"{case['source']}#{case['source_case_number']}"


def geometric_mean_ratio(ratios: list[float]) -> float | None:
    if not ratios:
        return None
    return math.exp(statistics.fmean(math.log(ratio) for ratio in ratios))


def path_display(path: Path, repo_root: Path) -> str:
    try:
        return str(path.relative_to(repo_root))
    except ValueError:
        return str(path)


def summarize_cases(cases: list[dict[str, Any]], tolerance: float) -> dict[str, Any]:
    compared_cases = [
        case for case in cases if case["comparison"]["status"] == "compared"
    ]
    ratios = [
        case["comparison"]["aq_over_yq_ratio"]
        for case in compared_cases
        if isinstance(case["comparison"]["aq_over_yq_ratio"], float)
    ]

    aq_faster_cases = [
        case for case in compared_cases if case["comparison"]["faster_tool"] == "aq"
    ]
    yq_faster_cases = [
        case for case in compared_cases if case["comparison"]["faster_tool"] == "yq"
    ]
    roughly_equal_cases = [
        case
        for case in compared_cases
        if case["comparison"]["faster_tool"] == "roughly_equal"
    ]
    yq_success_cases = [case for case in cases if case["yq"]["status"] == "passed"]
    aq_success_cases = [case for case in cases if case["aq"]["status"] == "passed"]
    uncomparable_cases = [
        case for case in cases if case["comparison"]["status"] != "compared"
    ]

    heaviest_cases = sorted(
        compared_cases,
        key=lambda case: (case["aq"]["median_seconds"] or 0.0)
        + (case["yq"]["median_seconds"] or 0.0),
        reverse=True,
    )
    slowest_relative_cases = sorted(
        yq_faster_cases,
        key=lambda case: case["comparison"]["aq_over_yq_ratio"] or 0.0,
        reverse=True,
    )
    fastest_relative_cases = sorted(
        aq_faster_cases,
        key=lambda case: case["comparison"]["aq_over_yq_ratio"] or float("inf"),
    )

    aq_total = sum(case["aq"]["median_seconds"] or 0.0 for case in compared_cases)
    yq_total = sum(case["yq"]["median_seconds"] or 0.0 for case in compared_cases)

    return {
        "compared_cases": len(compared_cases),
        "aq_faster_cases": len(aq_faster_cases),
        "yq_faster_cases": len(yq_faster_cases),
        "roughly_equal_cases": len(roughly_equal_cases),
        "uncomparable_cases": len(uncomparable_cases),
        "aq_success_cases": len(aq_success_cases),
        "yq_success_cases": len(yq_success_cases),
        "aq_total_median_seconds": aq_total,
        "yq_total_median_seconds": yq_total,
        "median_aq_over_yq_ratio": statistics.median(ratios) if ratios else None,
        "geometric_mean_aq_over_yq_ratio": geometric_mean_ratio(ratios),
        "comparison_tolerance": tolerance,
        "top_aq_slower_cases": slowest_relative_cases[:15],
        "top_aq_faster_cases": fastest_relative_cases[:15],
        "top_heaviest_cases": heaviest_cases[:15],
        "top_yq_failures": uncomparable_cases[:20],
    }


def render_ratio(ratio: float | None) -> str:
    if ratio is None:
        return "n/a"
    return f"{ratio:.2f}x"


def render_case_timing_line(case: dict[str, Any]) -> str:
    aq_median = case["aq"]["median_seconds"]
    yq_median = case["yq"]["median_seconds"]
    ratio = case["comparison"]["aq_over_yq_ratio"]
    return (
        f"`{format_case_label(case)}` `{case['program']}`"
        f", yq `{yq_median * 1000:.2f}ms`, aq `{aq_median * 1000:.2f}ms`, aq/yq `{render_ratio(ratio)}`"
    )


def render_failure_line(case: dict[str, Any]) -> str:
    return (
        f"`{format_case_label(case)}` `{case['program']}`"
        f", yq `{case['yq']['status']}`, aq `{case['aq']['status']}`"
    )


def render_markdown(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "## yq vs aq Upstream Benchmark",
        "",
        f"- Sources: `{', '.join(report['sources'])}`",
        f"- Harvested direct success cases: `{report['harvested_cases']}`",
        f"- Warmup runs: `{report['warmup_runs']}`",
        f"- Measured runs: `{report['measured_runs']}`",
        f"- Per-run timeout: `{report['timeout_seconds']}s`",
        f"- yq binary: `{report['yq_binary']}`",
        f"- yq version: `{report['yq_version']}`",
        f"- aq binary: `{report['aq_binary']}`",
        f"- aq version: `{report['aq_version']}`",
        f"- Benchmark wall time: `{report['benchmark_wall_seconds']:.1f}s`",
        f"- aq success cases: `{summary['aq_success_cases']}`",
        f"- yq success cases: `{summary['yq_success_cases']}`",
        f"- Compared overlap cases: `{summary['compared_cases']}`",
        f"- aq faster cases: `{summary['aq_faster_cases']}`",
        f"- yq faster cases: `{summary['yq_faster_cases']}`",
        f"- Roughly equal cases: `{summary['roughly_equal_cases']}`",
        f"- yq-incompatible or otherwise uncomparable cases: `{summary['uncomparable_cases']}`",
        f"- Sum of yq medians: `{summary['yq_total_median_seconds']:.3f}s`",
        f"- Sum of aq medians: `{summary['aq_total_median_seconds']:.3f}s`",
        f"- Median aq/yq ratio: `{render_ratio(summary['median_aq_over_yq_ratio'])}`",
        f"- Geometric mean aq/yq ratio: `{render_ratio(summary['geometric_mean_aq_over_yq_ratio'])}`",
        "",
        "Interpretation: aq/yq ratios above `1.00x` mean aq is slower, below `1.00x` mean aq is faster.",
        "",
        "Important: mikefarah/yq is not jq-compatible. This report benchmarks the overlap where `yq`"
        " happens to satisfy the jq upstream expected stdout. It is not a whole-language comparison.",
    ]

    if summary["top_aq_slower_cases"]:
        lines.extend(["", "### Biggest aq Slowdowns", ""])
        for case in summary["top_aq_slower_cases"]:
            lines.append(f"- {render_case_timing_line(case)}")

    if summary["top_aq_faster_cases"]:
        lines.extend(["", "### Biggest aq Speedups", ""])
        for case in summary["top_aq_faster_cases"]:
            lines.append(f"- {render_case_timing_line(case)}")

    if summary["top_heaviest_cases"]:
        lines.extend(["", "### Heaviest Overlap Cases", ""])
        for case in summary["top_heaviest_cases"]:
            lines.append(f"- {render_case_timing_line(case)}")

    if summary["top_yq_failures"]:
        lines.extend(["", "### Sample yq Incompatibilities", ""])
        for case in summary["top_yq_failures"]:
            lines.append(f"- {render_failure_line(case)}")

    return "\n".join(lines) + "\n"


def main() -> int:
    args = parse_args()
    repo_root = Path(__file__).resolve().parent.parent
    aq_binary = repo_root / args.aq_binary
    ensure_aq_binary(repo_root, aq_binary)

    source_urls = args.source_urls or DEFAULT_SOURCES
    harvested_cases: list[UpstreamCase] = []
    for url in source_urls:
        text = fetch_text(url)
        cases, _ = harvest_success_cases(url, text)
        harvested_cases.extend(cases)

    if args.case_limit is not None:
        harvested_cases = harvested_cases[: args.case_limit]

    yq_version = read_version([args.yq_binary], repo_root)
    aq_version = read_version([str(aq_binary)], repo_root)

    benchmark_cases: list[dict[str, Any]] = []
    benchmark_start = time.perf_counter()
    with tempfile.TemporaryDirectory(prefix="aq-yq-bench-modules-") as module_dir_raw:
        module_dir = Path(module_dir_raw)
        materialize_module_fixtures(module_dir)
        total_cases = len(harvested_cases)
        for index, case in enumerate(harvested_cases, start=1):
            if index == 1 or index % 25 == 0 or index == total_cases:
                print(
                    f"[{index}/{total_cases}] benchmarking {case.source}#{case.source_case_number}",
                    file=sys.stderr,
                    flush=True,
                )
            case_cwd = module_dir if case_requires_module_fixtures(case) else None
            yq_result = benchmark_tool(
                tool_name="yq",
                command=[
                    args.yq_binary,
                    "-p=json",
                    "-o=json",
                    "-I=0",
                    case.program,
                ],
                case=case,
                timeout_seconds=args.timeout_seconds,
                warmup_runs=args.warmup_runs,
                measured_runs=args.measured_runs,
                cwd=case_cwd,
            )
            aq_result = benchmark_tool(
                tool_name="aq",
                command=[
                    str(aq_binary),
                    "--input-format",
                    "json",
                    "--compact",
                    "--",
                    case.program,
                ],
                case=case,
                timeout_seconds=args.timeout_seconds,
                warmup_runs=args.warmup_runs,
                measured_runs=args.measured_runs,
                cwd=case_cwd,
            )
            comparison = compare_case(aq_result, yq_result, args.comparison_tolerance)
            benchmark_cases.append(
                {
                    "source": case.source,
                    "source_case_number": case.source_case_number,
                    "program": case.program,
                    "input_text": case.input_text,
                    "requires_module_fixtures": case_requires_module_fixtures(case),
                    "yq": yq_result,
                    "aq": aq_result,
                    "comparison": comparison,
                }
            )
    benchmark_wall_seconds = time.perf_counter() - benchmark_start

    report = {
        "sources": source_urls,
        "harvested_cases": len(harvested_cases),
        "warmup_runs": args.warmup_runs,
        "measured_runs": args.measured_runs,
        "timeout_seconds": args.timeout_seconds,
        "yq_binary": args.yq_binary,
        "yq_version": yq_version,
        "aq_binary": path_display(aq_binary, repo_root),
        "aq_version": aq_version,
        "benchmark_wall_seconds": benchmark_wall_seconds,
        "cases": benchmark_cases,
    }
    report["summary"] = summarize_cases(benchmark_cases, args.comparison_tolerance)

    markdown = render_markdown(report)
    print(markdown, end="")
    write_text(repo_root / args.json_out, json.dumps(report, indent=2) + "\n")
    write_text(repo_root / args.markdown_out, markdown)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
