#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any

DEFAULT_SOURCES = [
    "https://raw.githubusercontent.com/jqlang/jq/master/tests/base64.test",
    "https://raw.githubusercontent.com/jqlang/jq/master/tests/jq.test",
    "https://raw.githubusercontent.com/jqlang/jq/master/tests/man.test",
    "https://raw.githubusercontent.com/jqlang/jq/master/tests/manonig.test",
    "https://raw.githubusercontent.com/jqlang/jq/master/tests/onig.test",
    "https://raw.githubusercontent.com/jqlang/jq/master/tests/optional.test",
    "https://raw.githubusercontent.com/jqlang/jq/master/tests/uri.test",
]
MODULE_TREE_URL = "https://api.github.com/repos/jqlang/jq/git/trees/master?recursive=1"
MODULE_PREFIX = "tests/modules/"
DEFAULT_CASE_ENV = {"PAGER": "less"}


@dataclass
class UpstreamCase:
    source: str
    source_case_number: int
    program: str
    input_text: str
    expected_stdout: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run aq against direct success cases harvested from jq upstream tests."
    )
    parser.add_argument(
        "--source-url",
        action="append",
        dest="source_urls",
        default=[],
        help="Upstream jq test file URL. Repeatable. Defaults to the current harvested jq success-case sources.",
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


def fetch_text(url: str) -> str:
    request = urllib.request.Request(
        url, headers={"User-Agent": "aq-jq-upstream-report"}
    )
    with urllib.request.urlopen(request) as response:
        return response.read().decode("utf-8")


def fetch_json(url: str) -> Any:
    request = urllib.request.Request(
        url, headers={"User-Agent": "aq-jq-upstream-report"}
    )
    with urllib.request.urlopen(request) as response:
        return json.load(response)


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def append_step_summary(markdown: str) -> None:
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not summary_path:
        return
    with Path(summary_path).open("a", encoding="utf-8") as handle:
        handle.write(markdown)


def harvest_success_cases(
    source: str, text: str
) -> tuple[list[UpstreamCase], list[dict[str, Any]]]:
    lines = text.splitlines()
    cases: list[UpstreamCase] = []
    skipped_directives: list[dict[str, Any]] = []
    index = 0
    source_case_number = 0

    while index < len(lines):
        current = lines[index].strip()
        if not current or current.startswith("#"):
            index += 1
            continue

        if current.startswith("%%"):
            directive = current[2:].strip() or "UNKNOWN"
            block = [lines[index]]
            index += 1
            while index < len(lines) and lines[index].strip():
                block.append(lines[index])
                index += 1
            skipped_directives.append(
                {
                    "source": source,
                    "directive": directive,
                    "block": block,
                }
            )
            continue

        program = lines[index]
        index += 1
        while index < len(lines) and (
            not lines[index].strip() or lines[index].lstrip().startswith("#")
        ):
            index += 1
        if index >= len(lines):
            break

        input_text = lines[index]
        index += 1

        expected_lines: list[str] = []
        while index < len(lines):
            current_line = lines[index]
            stripped = current_line.strip()
            if not stripped or stripped.startswith("#") or stripped.startswith("%%"):
                break
            expected_lines.append(current_line)
            index += 1

        source_case_number += 1
        if expected_lines:
            cases.append(
                UpstreamCase(
                    source=source,
                    source_case_number=source_case_number,
                    program=program,
                    input_text=input_text,
                    expected_stdout="\n".join(expected_lines) + "\n",
                )
            )

        while index < len(lines) and not lines[index].strip():
            index += 1

    return cases, skipped_directives


def ensure_binary(repo_root: Path, binary_path: Path) -> None:
    try:
        binary_display = str(binary_path.relative_to(repo_root))
    except ValueError:
        binary_display = str(binary_path)

    command = ["cargo", "build", "--quiet"]
    if binary_display == "target/release/aq":
        command.insert(2, "--release")
    subprocess.run(command, cwd=repo_root, check=True)


def case_requires_module_fixtures(case: UpstreamCase) -> bool:
    return (
        "import " in case.program
        or "include " in case.program
        or "modulemeta" in case.program
    )


def materialize_module_fixtures(root: Path) -> None:
    tree = fetch_json(MODULE_TREE_URL)["tree"]
    for item in tree:
        if item.get("type") != "blob":
            continue
        path = item["path"]
        if not path.startswith(MODULE_PREFIX):
            continue
        relative_path = Path(path.removeprefix(MODULE_PREFIX))
        write_text(
            root / relative_path,
            fetch_text(f"https://raw.githubusercontent.com/jqlang/jq/master/{path}"),
        )


def run_case(
    binary_path: Path,
    case: UpstreamCase,
    timeout_seconds: float,
    cwd: Path | None,
) -> dict[str, Any]:
    command = [
        str(binary_path),
        "--input-format",
        "json",
        "--compact",
        "--",
        case.program,
    ]
    try:
        env = os.environ.copy()
        env.update(DEFAULT_CASE_ENV)
        result = subprocess.run(
            command,
            input=case.input_text,
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
            "program": case.program,
            "input": case.input_text,
            "expected_stdout": case.expected_stdout,
            "timeout_seconds": timeout_seconds,
            "stdout": error.stdout or "",
            "stderr": error.stderr or "",
        }

    status = (
        "passed"
        if semantically_matches_expected(result.stdout, case.expected_stdout)
        else "failed"
    )
    return {
        "status": status,
        "source": case.source,
        "source_case_number": case.source_case_number,
        "program": case.program,
        "input": case.input_text,
        "expected_stdout": case.expected_stdout,
        "stdout": result.stdout,
        "stderr": result.stderr,
        "returncode": result.returncode,
    }


def semantically_matches_expected(stdout: str, expected_stdout: str) -> bool:
    if stdout == expected_stdout:
        return True

    stdout_lines = [line for line in stdout.split("\n") if line]
    expected_lines = [line for line in expected_stdout.split("\n") if line]
    if len(stdout_lines) != len(expected_lines):
        return False

    try:
        stdout_values = [json.loads(line) for line in stdout_lines]
        expected_values = [json.loads(line) for line in expected_lines]
    except json.JSONDecodeError:
        return False

    return stdout_values == expected_values


def render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "## jq Upstream Compatibility Report",
        "",
        f"- Sources: `{', '.join(report['sources'])}`",
        f"- Harvested direct success cases: `{report['harvested_cases']}`",
        f"- Skipped directive blocks: `{report['skipped_directives']}`",
        f"- Per-case timeout: `{report['timeout_seconds']}s`",
        f"- Binary: `{report['binary']}`",
        f"- Passed: `{report['passed_cases']}`",
        f"- Failed: `{report['failed_cases']}`",
        f"- Timed out: `{report['timed_out_cases']}`",
        f"- Compliance over harvested cases: `{report['compliance_rate']:.1f}%`",
    ]

    failures = report["failures"]
    if failures:
        lines.extend(["", "### Sample Failures", ""])
        for failure in failures:
            lines.extend(
                [
                    f"- `{failure['source']}#{failure['source_case_number']}` `{failure['program']}`",
                    "```text",
                    f"status: {failure['status']}",
                    f"input: {failure['input']}",
                    f"expected: {failure['expected_stdout'].rstrip()}",
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

    harvested_cases: list[UpstreamCase] = []
    skipped_directives: list[dict[str, Any]] = []
    for url in source_urls:
        text = fetch_text(url)
        cases, directives = harvest_success_cases(url, text)
        harvested_cases.extend(cases)
        skipped_directives.extend(directives)

    passed_cases = 0
    timed_out_cases = 0
    failures: list[dict[str, Any]] = []
    with tempfile.TemporaryDirectory(prefix="aq-jq-modules-") as module_dir_raw:
        module_dir = Path(module_dir_raw)
        materialize_module_fixtures(module_dir)
        for case in harvested_cases:
            case_cwd = module_dir if case_requires_module_fixtures(case) else None
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
        "skipped_directives": len(skipped_directives),
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
