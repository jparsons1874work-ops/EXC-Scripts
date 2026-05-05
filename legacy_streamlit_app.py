from __future__ import annotations

import base64
import os
import queue
import re
import signal
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, time as dt_time
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import streamlit as st


APP_ROOT = Path(__file__).resolve().parent
SCRIPTS_ROOT = APP_ROOT / "scripts"
RUNTIME_DIR = APP_ROOT / "runtime"
LOG_DIR = RUNTIME_DIR / "logs"
OUTPUT_DIR = RUNTIME_DIR / "output"
SECRET_RUNTIME_DIR = RUNTIME_DIR / "secrets"
UK_TZ = ZoneInfo("Europe/London")

SECRET_KEYS = (
    "BETFAIR_USERNAME",
    "BETFAIR_PASSWORD",
    "BETFAIR_APP_KEY",
    "BETFAIR_CERT_FILE",
    "BETFAIR_CERT_B64",
    "BETFAIR_KEY_FILE",
    "BETFAIR_KEY_B64",
    "DECIMAL_USERNAME",
    "DECIMAL_PASSWORD",
    "SLACK_BOT_TOKEN",
    "SLACK_CHANNEL",
    "SLACK_WEBHOOK_URL",
    "DG_API_KEY",
)


@dataclass(frozen=True)
class RunWindow:
    start: str
    end: str
    timezone: str = "Europe/London"


@dataclass(frozen=True)
class ScriptSpec:
    name: str
    category: str
    description: str
    relative_path: str
    default_args: tuple[str, ...] = ()
    long_running: bool = False
    needs_parameters: bool = False
    parsed_output: bool = False
    allowed_window: RunWindow | None = None


SCRIPT_REGISTRY: tuple[ScriptSpec, ...] = (
    ScriptSpec(
        "Golf - Non-Runner Check",
        "Golf",
        "Checks Betfair Exchange golf markets for potential non-runner issues.",
        "scripts/Golf_Exchange_NR_Checks.py",
    ),
    ScriptSpec(
        "Cricket - Decimal Fixture Scrape",
        "Cricket",
        "Scrapes Decimal cricket fixtures and writes the latest output workbook.",
        "scripts/Decimal_Cricket_Scrape_Auto.py",
    ),
    ScriptSpec(
        "SAMM - Selection Name Check",
        "SAMM",
        "Extracts selection names and probabilities from Polymarket or Kalshi.",
        "scripts/Signal_Selection_Extractor.py",
        needs_parameters=True,
    ),
    ScriptSpec(
        "Betfair - Duplicate Match Check",
        "Betfair",
        "Monitors Betfair Exchange fixtures for duplicate match listings.",
        "scripts/Betfair_Duplicate_Match_Check.py",
        ("--repeat-minutes", "30", "--send-startup-message", "--send-shutdown-message"),
        long_running=True,
        allowed_window=RunWindow("07:00", "23:00"),
    ),
    ScriptSpec(
        "Betfair - Duplicate Market Check",
        "Betfair",
        "Checks Betfair Exchange football events for duplicate market names.",
        "scripts/Betfair_Duplicate_Market_Check.py",
        long_running=True,
        allowed_window=RunWindow("07:00", "23:00"),
    ),
    ScriptSpec(
        "Tennis - Integrity Check",
        "Tennis",
        "Runs the integrity scanner against Betfair tennis markets.",
        "scripts/Integrity-Scanner/start_scanner.py",
        long_running=True,
        allowed_window=RunWindow("07:00", "23:00"),
    ),
    ScriptSpec(
        "Cricket - Time Check Today",
        "Cricket",
        "Compares today's Betfair and Decimal cricket fixture start times.",
        "scripts/exc-cric-time-check/betfair_decimal_time_checker.py",
        ("--today", "--pretty"),
        parsed_output=True,
    ),
    ScriptSpec(
        "Cricket - Time Check Tomorrow",
        "Cricket",
        "Compares tomorrow's Betfair and Decimal cricket fixture start times.",
        "scripts/exc-cric-time-check/betfair_decimal_time_checker.py",
        ("--tomorrow", "--pretty"),
        parsed_output=True,
    ),
)


def ensure_runtime_dirs() -> None:
    for path in (LOG_DIR, OUTPUT_DIR, SECRET_RUNTIME_DIR):
        path.mkdir(parents=True, exist_ok=True)


def get_secret(name: str, default: str = "") -> str:
    value = os.getenv(name, "")
    if value:
        return value
    try:
        raw = st.secrets.get(name, default)
    except Exception:
        raw = default
    return str(raw or "")


def materialize_b64_secret(secret_name: str, file_name: str) -> str:
    encoded = get_secret(secret_name)
    if not encoded:
        return ""
    target = SECRET_RUNTIME_DIR / file_name
    target.write_bytes(base64.b64decode(encoded))
    return str(target)


def child_environment() -> dict[str, str]:
    env = os.environ.copy()
    for key in SECRET_KEYS:
        value = get_secret(key)
        if value:
            env[key] = value

    cert_file = env.get("BETFAIR_CERT_FILE") or materialize_b64_secret("BETFAIR_CERT_B64", "client-2048.crt")
    key_file = env.get("BETFAIR_KEY_FILE") or materialize_b64_secret("BETFAIR_KEY_B64", "client-2048.key")
    if cert_file:
        env["BETFAIR_CERT_FILE"] = cert_file
        env.setdefault("BETFAIR_CERTS_DIR", str(Path(cert_file).parent))
        env.setdefault("BF_CERTS_DIR", str(Path(cert_file).parent))
    if key_file:
        env["BETFAIR_KEY_FILE"] = key_file

    env.setdefault("SCRIPT_OUTPUT_DIR", str(OUTPUT_DIR))
    env.setdefault("CHROME_PROFILE_DIR", str(OUTPUT_DIR / "chrome_profile"))
    env.setdefault("PYTHONUNBUFFERED", "1")
    return env


def configure_page() -> None:
    st.set_page_config(page_title="Betfair Scripts Hub", layout="wide")
    st.markdown(
        """
        <style>
        .stApp { background: #071323; color: #eef3f8; }
        [data-testid="stSidebar"] { background: #0b1b31; border-right: 1px solid #1a2e4a; }
        h1, h2, h3 { color: #ffffff; letter-spacing: 0; }
        .hub-subtext { color: #aab8ca; font-size: 1.02rem; margin-top: -0.6rem; }
        .script-card {
            border: 1px solid #203750;
            background: #0d1f36;
            border-radius: 8px;
            padding: 1.1rem 1.2rem;
            margin: 0.75rem 0 1rem;
        }
        .status-pill {
            display: inline-block;
            border: 1px solid #f6c343;
            color: #f6c343;
            background: rgba(246, 195, 67, 0.08);
            border-radius: 999px;
            padding: 0.18rem 0.7rem;
            font-weight: 700;
            font-size: 0.9rem;
        }
        .console {
            background: #050b14;
            border: 1px solid #1c324e;
            border-radius: 8px;
            padding: 1rem;
            min-height: 320px;
            max-height: 560px;
            overflow: auto;
            white-space: pre-wrap;
            font-family: Consolas, Monaco, monospace;
            font-size: 0.9rem;
            color: #dce7f5;
        }
        .stButton > button[kind="primary"] { background: #f6c343; color: #071323; border-color: #f6c343; }
        </style>
        """,
        unsafe_allow_html=True,
    )


def init_state() -> None:
    defaults: dict[str, Any] = {
        "authenticated": False,
        "process": None,
        "output_queue": None,
        "output_lines": [],
        "active_script": "",
        "active_log_path": "",
        "status": "Idle",
        "return_code": None,
        "stopping": False,
    }
    for key, value in defaults.items():
        st.session_state.setdefault(key, value)


def password_gate() -> bool:
    configured_password = get_secret("APP_PASSWORD")
    if not configured_password:
        st.warning("APP_PASSWORD is not configured. Local development access is currently open.")
        st.session_state.authenticated = True
        return True

    if st.session_state.authenticated:
        return True

    st.title("Betfair Scripts Hub")
    password = st.text_input("Password", type="password")
    if st.button("Sign in", type="primary"):
        if password == configured_password:
            st.session_state.authenticated = True
            st.rerun()
        st.error("Incorrect password.")
    return False


def parse_hhmm(value: str) -> dt_time:
    hour, minute = value.split(":", 1)
    return dt_time(int(hour), int(minute))


def window_status(spec: ScriptSpec) -> tuple[bool, str]:
    if not spec.allowed_window:
        return True, ""
    tz = ZoneInfo(spec.allowed_window.timezone)
    now = datetime.now(tz)
    start = parse_hhmm(spec.allowed_window.start)
    end = parse_hhmm(spec.allowed_window.end)
    now_time = now.time().replace(second=0, microsecond=0)
    if start <= end:
        allowed = start <= now_time <= end
    else:
        allowed = now_time >= start or now_time <= end
    label = f"{spec.allowed_window.start}-{spec.allowed_window.end} {spec.allowed_window.timezone}"
    return allowed, label


def enqueue_stream(stream: Any, output_queue: queue.Queue[str], log_path: Path, prefix: str = "") -> None:
    try:
        with log_path.open("a", encoding="utf-8", errors="replace") as log_file:
            for line in iter(stream.readline, ""):
                text = f"{prefix}{line.rstrip()}"
                output_queue.put(text)
                log_file.write(text + "\n")
                log_file.flush()
    finally:
        stream.close()


def drain_output() -> None:
    output_queue = st.session_state.get("output_queue")
    if output_queue is None:
        return
    while True:
        try:
            st.session_state.output_lines.append(output_queue.get_nowait())
        except queue.Empty:
            break


def stop_process() -> None:
    process = st.session_state.get("process")
    if process is None or process.poll() is not None:
        st.session_state.status = "Idle"
        return
    st.session_state.status = "Stopping"
    st.session_state.stopping = True
    try:
        if os.name == "nt":
            process.send_signal(signal.CTRL_BREAK_EVENT)
        else:
            process.terminate()
    except Exception as exc:
        st.session_state.output_lines.append(f"Could not stop process gracefully: {exc}")
        process.terminate()


def script_args(spec: ScriptSpec, signal_source: str, identifier: str) -> list[str]:
    if spec.needs_parameters:
        return [signal_source, identifier]
    return list(spec.default_args)


def start_script(spec: ScriptSpec, args: list[str]) -> None:
    allowed, label = window_status(spec)
    if spec.long_running and not allowed:
        st.session_state.status = "Blocked by time window"
        st.session_state.output_lines = [f"Start blocked. Allowed window: {label}."]
        return

    ensure_runtime_dirs()
    safe_name = re.sub(r"[^A-Za-z0-9_.-]+", "_", spec.name).strip("_")
    log_path = LOG_DIR / f"{safe_name}.log"
    log_path.write_text("", encoding="utf-8")

    st.session_state.output_lines = []
    st.session_state.output_queue = queue.Queue()
    st.session_state.active_script = spec.name
    st.session_state.active_log_path = str(log_path)
    st.session_state.return_code = None
    st.session_state.stopping = False
    st.session_state.status = "Running"

    script_path = APP_ROOT / spec.relative_path
    cmd = [sys.executable, str(script_path), *args]
    creationflags = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0) if os.name == "nt" else 0
    process = subprocess.Popen(
        cmd,
        cwd=str(APP_ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
        env=child_environment(),
        creationflags=creationflags,
    )
    st.session_state.process = process

    threading.Thread(
        target=enqueue_stream,
        args=(process.stdout, st.session_state.output_queue, log_path),
        daemon=True,
    ).start()
    threading.Thread(
        target=enqueue_stream,
        args=(process.stderr, st.session_state.output_queue, log_path, "STDERR: "),
        daemon=True,
    ).start()


def monitor_active_script(registry_by_name: dict[str, ScriptSpec]) -> None:
    process = st.session_state.get("process")
    if process is None:
        return

    active = registry_by_name.get(st.session_state.active_script)
    if active and active.long_running and st.session_state.status == "Running":
        allowed, label = window_status(active)
        if not allowed:
            st.session_state.output_lines.append(f"Allowed window ended ({label}). Attempting clean stop.")
            stop_process()

    drain_output()
    return_code = process.poll()
    if return_code is None:
        return

    process.wait(timeout=1)
    drain_output()
    st.session_state.return_code = return_code
    st.session_state.process = None
    st.session_state.output_queue = None
    st.session_state.stopping = False
    st.session_state.status = "Complete" if return_code == 0 else "Failed"


def parse_cricket_time_check_output(output_lines: list[str]) -> tuple[int, pd.DataFrame]:
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

    rows: list[list[str]] = []
    for line in output_lines[header_index + 1 :]:
        stripped = line.strip()
        if not stripped or stripped.startswith("STDERR:"):
            continue
        parts = re.split(r"\s{2,}", stripped, maxsplit=3)
        if len(parts) == 4:
            rows.append(parts)
    return int(match.group(1)), pd.DataFrame(rows, columns=["Status", "Match", "Betfair Time", "Decimal Time"])


def render_output(spec: ScriptSpec) -> None:
    lines = st.session_state.output_lines
    if spec.parsed_output and st.session_state.status in {"Complete", "Failed"}:
        try:
            mismatch_count, dataframe = parse_cricket_time_check_output(lines)
            st.metric("Not Matching", mismatch_count)
            st.dataframe(dataframe, use_container_width=True, hide_index=True)
        except Exception:
            pass
    st.markdown(f"<div class='console'>{escape_console(lines)}</div>", unsafe_allow_html=True)


def escape_console(lines: list[str]) -> str:
    text = "\n".join(lines[-500:]) if lines else "No output yet."
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def main() -> None:
    ensure_runtime_dirs()
    configure_page()
    init_state()
    if not password_gate():
        return

    registry_by_name = {spec.name: spec for spec in SCRIPT_REGISTRY if (APP_ROOT / spec.relative_path).exists()}
    categories = sorted({spec.category for spec in registry_by_name.values()})
    monitor_active_script(registry_by_name)

    st.title("Betfair Scripts Hub")
    st.markdown(
        "<p class='hub-subtext'>Run operational checks, scrapers and monitoring tools from one place.</p>",
        unsafe_allow_html=True,
    )

    with st.sidebar:
        st.header("Scripts")
        category = st.selectbox("Category", categories)
        options = [spec.name for spec in registry_by_name.values() if spec.category == category]
        selected_name = st.selectbox("Script", options)

    spec = registry_by_name[selected_name]
    allowed, window_label = window_status(spec)

    st.markdown("<div class='script-card'>", unsafe_allow_html=True)
    st.subheader(spec.name)
    st.write(spec.description)
    st.caption(f"Path: {spec.relative_path}")
    if spec.long_running and window_label:
        st.caption(f"Allowed window: {window_label}")
    st.markdown(f"<span class='status-pill'>{st.session_state.status}</span>", unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)

    signal_source = "polymarket"
    identifier = ""
    if spec.needs_parameters:
        p1, p2 = st.columns([1, 2])
        with p1:
            signal_source = st.selectbox("Signal source", ["polymarket", "kalshi"])
        with p2:
            label = "Polymarket event slug" if signal_source == "polymarket" else "Kalshi identifier or URL"
            identifier = st.text_input(label)

    c1, c2, c3 = st.columns([1, 1, 4])
    process_running = st.session_state.get("process") is not None
    with c1:
        start_clicked = st.button("Start", type="primary", use_container_width=True, disabled=process_running)
    with c2:
        stop_clicked = st.button("Stop", use_container_width=True, disabled=not process_running)
    with c3:
        if spec.long_running and not allowed:
            st.warning(f"Outside allowed run window: {window_label}")

    if start_clicked:
        if spec.needs_parameters and not identifier.strip():
            st.error("Enter the required identifier before starting.")
        else:
            start_script(spec, script_args(spec, signal_source, identifier.strip()))
            st.rerun()

    if stop_clicked:
        stop_process()
        st.rerun()

    render_output(registry_by_name.get(st.session_state.active_script, spec))

    if st.session_state.get("process") is not None:
        time.sleep(1)
        st.rerun()


if __name__ == "__main__":
    main()
