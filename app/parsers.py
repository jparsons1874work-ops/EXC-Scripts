from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class CricketTimeResult:
    mismatch_count: int
    rows: list[dict[str, str]]


def parse_cricket_time_check_output(output_lines: list[str]) -> CricketTimeResult:
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

    return CricketTimeResult(mismatch_count=int(match.group(1)), rows=rows)
