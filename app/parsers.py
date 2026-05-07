from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class CricketTimeResult:
    mismatch_count: int
    rows: list[dict[str, str]]
    summary: dict[str, str]
    failure_message: str = ""
    root_error: str = ""


SUMMARY_LABELS = (
    "Scrape Status",
    "Betfair Fixtures",
    "Decimal Fixtures",
    "Matched Fixtures",
    "Unmatched Betfair Fixtures",
    "Unmatched Decimal Fixtures",
)


def _parse_summary_line(line: str) -> tuple[str, str] | None:
    stripped = line.strip()
    for label in SUMMARY_LABELS:
        if not stripped.startswith(label):
            continue
        remainder = stripped[len(label) :].strip()
        if not remainder:
            return None
        return label, re.sub(r"\s{2,}|\t+", " ", remainder)
    return None


def parse_cricket_time_check_output(output_lines: list[str]) -> CricketTimeResult:
    summary: dict[str, str] = {}
    for line in output_lines:
        if line.startswith("STDERR:"):
            continue
        parsed = _parse_summary_line(line)
        if parsed is None:
            continue
        label, value = parsed
        if label == "Scrape Status":
            parts = value.split(maxsplit=1)
            if len(parts) == 2:
                summary[f"{parts[0].lower()}_status"] = parts[1]
            continue
        summary[label.lower().replace(" ", "_")] = value

    failure_line = next(
        (
            line.strip()
            for line in output_lines
            if line.strip() == "Decimal fixture scrape failed; comparison not reliable."
            or line.strip().startswith("Failure")
        ),
        "",
    )
    root_error_line = next((line.strip() for line in output_lines if line.strip().startswith("Root Error")), "")
    if failure_line:
        root_error = re.sub(r"^Root Error:?\s*", "", root_error_line).strip()
        failure_message = "Decimal fixture scrape failed; comparison not reliable."
        return CricketTimeResult(
            mismatch_count=0,
            rows=[],
            summary=summary,
            failure_message=failure_message,
            root_error=root_error,
        )

    summary_line = next((line.strip() for line in output_lines if line.strip().startswith("Not Matching")), None)
    if summary_line is None:
        raise ValueError("Could not find mismatch summary line.")

    match = re.match(r"^Not Matching\s+(\d+)$", summary_line)
    if not match:
        raise ValueError("Could not parse mismatch summary line.")

    header_index = next(
        (index for index, line in enumerate(output_lines) if line.strip().startswith("Status")),
        None,
    )
    if header_index is None:
        raise ValueError("Could not find cricket output header.")

    rows: list[dict[str, str]] = []
    for line in output_lines[header_index + 1 :]:
        stripped = line.strip()
        if not stripped or stripped.startswith("STDERR:"):
            continue
        parts = re.split(r"\s{2,}", stripped, maxsplit=3)
        if len(parts) == 4:
            rows.append(
                {
                    "status": parts[0],
                    "match": parts[1],
                    "betfair_time": parts[2],
                    "decimal_time": parts[3],
                }
            )

    return CricketTimeResult(mismatch_count=int(match.group(1)), rows=rows, summary=summary)
