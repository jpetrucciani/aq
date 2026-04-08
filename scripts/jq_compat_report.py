#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the curated jq compatibility suite and emit a report."
    )
    parser.add_argument(
        "--suite-path",
        default="tests/fixtures/jq_compat_suite.json",
        help="Path to the jq compatibility suite fixture, relative to the repo root.",
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


def load_json(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, dict):
        raise ValueError(f"expected JSON object in {path}")
    return data


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "## jq Compatibility Report",
        "",
        f"- Suite: `{report['suite_name']}`",
        f"- Sources: `{', '.join(report['sources'])}`",
        f"- Total cases: `{report['total_cases']}`",
        f"- Target pass rate: `{report['target_pass_rate']}%`",
        f"- Command: `{' '.join(report['command'])}`",
    ]
    if report["status"] == "passed":
        lines.extend(
            [
                f"- Result: passed `{report['passed_cases']}` / `{report['total_cases']}`",
                f"- Pass rate: `{report['pass_rate']:.0f}%`",
            ]
        )
    else:
        lines.extend(
            [
                "- Result: failed",
                "- Pass rate: unavailable because the current suite runs as one aggregated test.",
            ]
        )
    if report["stderr"]:
        lines.extend(["", "### stderr", "", "```text", report["stderr"], "```"])
    return "\n".join(lines) + "\n"


def append_step_summary(markdown: str) -> None:
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not summary_path:
        return
    with Path(summary_path).open("a", encoding="utf-8") as handle:
        handle.write(markdown)


def main() -> int:
    args = parse_args()
    repo_root = Path(__file__).resolve().parent.parent
    suite_path = repo_root / args.suite_path
    suite = load_json(suite_path)
    cases = suite.get("cases", [])
    if not isinstance(cases, list):
        raise ValueError("suite cases must be a list")

    command = [
        "cargo",
        "test",
        "supports_upstream_jq_case_subset",
        "--",
        "--exact",
    ]
    result = subprocess.run(
        command,
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=False,
    )

    report: dict[str, Any] = {
        "suite_name": suite.get("suite_name", "jq_compat"),
        "sources": suite.get("sources", []),
        "target_pass_rate": suite.get("target_pass_rate", 100),
        "total_cases": len(cases),
        "passed_cases": len(cases) if result.returncode == 0 else None,
        "pass_rate": 100.0 if result.returncode == 0 else None,
        "status": "passed" if result.returncode == 0 else "failed",
        "command": command,
        "stdout": result.stdout.strip(),
        "stderr": result.stderr.strip(),
    }

    markdown = render_markdown(report)
    print(markdown, end="")
    append_step_summary(markdown)

    if args.json_out:
        write_text(repo_root / args.json_out, json.dumps(report, indent=2) + "\n")
    if args.markdown_out:
        write_text(repo_root / args.markdown_out, markdown)

    return result.returncode


if __name__ == "__main__":
    sys.exit(main())
