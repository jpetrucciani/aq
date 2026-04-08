#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from jq_upstream_report import (
    DEFAULT_CASE_ENV,
    DEFAULT_SOURCES,
    append_step_summary,
    ensure_binary,
    fetch_text,
    materialize_module_fixtures,
    write_text,
)


@dataclass
class UpstreamFailureCase:
    source: str
    source_case_number: int
    directive: str
    program: str
    expected_stderr: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run aq against jq upstream %%FAIL directive blocks."
    )
    parser.add_argument(
        "--source-url",
        action="append",
        dest="source_urls",
        default=[],
        help="Upstream jq test file URL. Repeatable. Defaults to the current harvested jq sources.",
    )
    parser.add_argument(
        "--binary",
        default="target/debug/aq",
        help="Path to the aq binary to execute, relative to the repo root.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=3.0,
        help="Per-case execution timeout in seconds.",
    )
    parser.add_argument(
        "--max-failures",
        type=int,
        default=20,
        help="Maximum number of failing cases to include in the report.",
    )
    parser.add_argument(
        "--json-out",
        help="Optional path for a JSON report, relative to the repo root.",
    )
    parser.add_argument(
        "--markdown-out",
        help="Optional path for a Markdown report, relative to the repo root.",
    )
    return parser.parse_args()


def harvest_failure_cases(source: str, text: str) -> list[UpstreamFailureCase]:
    lines = text.splitlines()
    cases: list[UpstreamFailureCase] = []
    index = 0
    source_case_number = 0

    while index < len(lines):
        current = lines[index].strip()
        if not current.startswith("%%FAIL"):
            index += 1
            continue

        directive = current[2:].strip() or "FAIL"
        index += 1
        while index < len(lines) and (
            not lines[index].strip() or lines[index].lstrip().startswith("#")
        ):
            index += 1
        if index >= len(lines):
            break

        program = lines[index]
        index += 1

        expected_lines: list[str] = []
        while index < len(lines):
            current_line = lines[index]
            stripped = current_line.strip()
            if not stripped or stripped.startswith("%%"):
                break
            expected_lines.append(current_line)
            index += 1

        source_case_number += 1
        cases.append(
            UpstreamFailureCase(
                source=source,
                source_case_number=source_case_number,
                directive=directive,
                program=program,
                expected_stderr="\n".join(expected_lines) + "\n",
            )
        )

        while index < len(lines) and not lines[index].strip():
            index += 1

    return cases


def normalize_error_text(text: str) -> str:
    return re.sub(r"[^a-z0-9%]+", " ", text.lower()).strip()


def expected_error_fragment(expected_stderr: str) -> str:
    for line in expected_stderr.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("jq: error:"):
            stripped = stripped[len("jq: error:") :].strip()
        for marker in (" at <top-level>", " at tests/", " at /"):
            if marker in stripped:
                stripped = stripped.split(marker, 1)[0].strip()
        stripped = re.sub(r"\s+\(while parsing .*?\)$", "", stripped).strip()
        return normalize_error_text(stripped)
    return ""


def failure_case_requires_module_fixtures(case: UpstreamFailureCase) -> bool:
    return (
        "import " in case.program
        or "include " in case.program
        or "modulemeta" in case.program
    )


def run_case(
    binary_path: Path,
    case: UpstreamFailureCase,
    timeout_seconds: float,
    cwd: Path | None,
) -> dict[str, Any]:
    command = [str(binary_path), "-n", "--", case.program]
    try:
        env = os.environ.copy()
        env.update(DEFAULT_CASE_ENV)
        result = subprocess.run(
            command,
            text=True,
            capture_output=True,
            cwd=cwd,
            env=env,
            check=False,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired as error:
        return {
            "status": "timeout",
            "source": case.source,
            "source_case_number": case.source_case_number,
            "directive": case.directive,
            "program": case.program,
            "expected_stderr": case.expected_stderr,
            "timeout_seconds": timeout_seconds,
            "stdout": error.stdout or "",
            "stderr": error.stderr or "",
        }

    expected_fragment = expected_error_fragment(case.expected_stderr)
    normalized_stderr = normalize_error_text(result.stderr)
    message_matches = (
        True
        if case.directive == "FAIL IGNORE MSG"
        else bool(expected_fragment and expected_fragment in normalized_stderr)
    )
    status = (
        "passed" if result.returncode != 0 and message_matches else "failed"
    )
    return {
        "status": status,
        "source": case.source,
        "source_case_number": case.source_case_number,
        "directive": case.directive,
        "program": case.program,
        "expected_stderr": case.expected_stderr,
        "expected_fragment": expected_fragment,
        "stdout": result.stdout,
        "stderr": result.stderr,
        "returncode": result.returncode,
    }


def render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "## jq Upstream Failure-Contract Report",
        "",
        f"- Sources: `{', '.join(report['sources'])}`",
        f"- Harvested failure blocks: `{report['harvested_cases']}`",
        f"- Per-case timeout: `{report['timeout_seconds']}s`",
        f"- Binary: `{report['binary']}`",
        f"- Passed: `{report['passed_cases']}`",
        f"- Failed: `{report['failed_cases']}`",
        f"- Timed out: `{report['timed_out_cases']}`",
        f"- Failure-contract compliance: `{report['compliance_rate']:.1f}%`",
        "",
        "A case passes when `aq` exits non-zero and, for plain `%%FAIL` blocks, its stderr contains the normalized upstream core error fragment.",
    ]

    failures = report["failures"]
    if failures:
        lines.extend(["", "### Sample Failures", ""])
        for failure in failures:
            lines.extend(
                [
                    f"- `{failure['source']}#{failure['source_case_number']}` `{failure['directive']}` `{failure['program']}`",
                    "```text",
                    f"status: {failure['status']}",
                    f"expected fragment: {failure.get('expected_fragment', '')}",
                    f"stdout: {failure['stdout'].rstrip()}",
                    f"stderr: {failure['stderr'].rstrip()}",
                    "```",
                ]
            )
    return "\n".join(lines) + "\n"


def main() -> int:
    args = parse_args()
    repo_root = Path(__file__).resolve().parent.parent
    source_urls = args.source_urls or DEFAULT_SOURCES
    binary_path = repo_root / args.binary
    ensure_binary(repo_root, binary_path)

    harvested_cases: list[UpstreamFailureCase] = []
    for url in source_urls:
        harvested_cases.extend(harvest_failure_cases(url, fetch_text(url)))

    passed_cases = 0
    timed_out_cases = 0
    failures: list[dict[str, Any]] = []
    with tempfile.TemporaryDirectory(prefix="aq-jq-fail-modules-") as module_dir_raw:
        module_dir = Path(module_dir_raw)
        materialize_module_fixtures(module_dir)
        for case in harvested_cases:
            case_cwd = module_dir if failure_case_requires_module_fixtures(case) else None
            outcome = run_case(binary_path, case, args.timeout_seconds, case_cwd)
            status = outcome["status"]
            if status == "passed":
                passed_cases += 1
            elif status == "timeout":
                timed_out_cases += 1
                if len(failures) < args.max_failures:
                    failures.append(outcome)
            else:
                if len(failures) < args.max_failures:
                    failures.append(outcome)

    failed_cases = len(harvested_cases) - passed_cases - timed_out_cases
    compliance_rate = (
        (passed_cases / len(harvested_cases)) * 100 if harvested_cases else 0.0
    )
    report = {
        "sources": source_urls,
        "binary": str(binary_path.relative_to(repo_root)),
        "timeout_seconds": args.timeout_seconds,
        "harvested_cases": len(harvested_cases),
        "passed_cases": passed_cases,
        "failed_cases": failed_cases,
        "timed_out_cases": timed_out_cases,
        "compliance_rate": compliance_rate,
        "failures": failures,
    }

    markdown = render_markdown(report)
    print(markdown, end="")
    append_step_summary(markdown)
    if args.json_out:
        write_text(repo_root / args.json_out, json.dumps(report, indent=2) + "\n")
    if args.markdown_out:
        write_text(repo_root / args.markdown_out, markdown)
    return 0


if __name__ == "__main__":
    sys.exit(main())
