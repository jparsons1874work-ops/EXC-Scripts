from __future__ import annotations

import os
import re
import signal
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path

from app.config import LOG_DIR, PROJECT_ROOT, child_environment, ensure_runtime_dirs
from app.registry import SCRIPTS_BY_ID, ScriptSpec
from app.scheduler import window_status


IDLE = "Idle"
RUNNING = "Running"
STOPPING = "Stopping"
COMPLETE = "Complete"
FAILED = "Failed"
BLOCKED = "Blocked by time window"


@dataclass
class ScriptRunState:
    status: str = IDLE
    output_lines: list[str] = field(default_factory=list)
    process: subprocess.Popen[str] | None = None
    return_code: int | None = None
    log_path: Path | None = None
    started_at: float | None = None
    message: str = ""
    command: list[str] = field(default_factory=list)


class ScriptRunner:
    def __init__(self) -> None:
        self._states = {script_id: ScriptRunState() for script_id in SCRIPTS_BY_ID}
        self._lock = threading.RLock()

    def get_state(self, script_id: str) -> ScriptRunState:
        self._refresh(script_id)
        return self._states[script_id]

    def start(self, script_id: str, args: list[str]) -> ScriptRunState:
        spec = SCRIPTS_BY_ID[script_id]
        with self._lock:
            state = self._states[script_id]
            self._refresh_locked(script_id)
            if state.process is not None and state.process.poll() is None:
                state.message = "Script is already running."
                return state

            allowed, window_label = window_status(spec)
            if spec.long_running and not allowed:
                state.status = BLOCKED
                state.output_lines = [f"Start blocked. Allowed window: {window_label}."]
                state.message = state.output_lines[0]
                state.return_code = None
                state.process = None
                return state

            ensure_runtime_dirs()
            safe_name = re.sub(r"[^A-Za-z0-9_.-]+", "_", spec.name).strip("_")
            log_path = LOG_DIR / f"{safe_name}.log"
            log_path.write_text("", encoding="utf-8")

            script_path = (PROJECT_ROOT / spec.relative_path).resolve()
            command = [sys.executable, str(script_path), *args]
            creationflags = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0) if os.name == "nt" else 0

            process = subprocess.Popen(
                command,
                cwd=str(PROJECT_ROOT),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
                env=child_environment(),
                creationflags=creationflags,
            )

            state.status = RUNNING
            state.output_lines = []
            state.process = process
            state.return_code = None
            state.log_path = log_path
            state.started_at = time.time()
            state.message = ""
            state.command = command

            threading.Thread(target=self._read_stream, args=(script_id, process.stdout, ""), daemon=True).start()
            threading.Thread(target=self._read_stream, args=(script_id, process.stderr, "STDERR: "), daemon=True).start()
            return state

    def stop(self, script_id: str) -> ScriptRunState:
        with self._lock:
            state = self._states[script_id]
            process = state.process
            if process is None or process.poll() is not None:
                self._refresh_locked(script_id)
                return state

            state.status = STOPPING
            state.message = "Stopping script..."
            try:
                if os.name == "nt":
                    process.send_signal(signal.CTRL_BREAK_EVENT)
                else:
                    process.terminate()
            except Exception as exc:
                self._append_locked(script_id, f"Could not stop gracefully: {exc}")
                process.terminate()

            threading.Thread(target=self._kill_later, args=(script_id, process), daemon=True).start()
            return state

    def stop_expired_windows(self) -> None:
        for script_id, spec in SCRIPTS_BY_ID.items():
            if not spec.long_running:
                continue
            state = self.get_state(script_id)
            if state.status != RUNNING:
                continue
            allowed, window_label = window_status(spec)
            if not allowed:
                with self._lock:
                    self._append_locked(script_id, f"Allowed window ended ({window_label}). Attempting clean stop.")
                self.stop(script_id)

    def _read_stream(self, script_id: str, stream, prefix: str) -> None:
        if stream is None:
            return
        try:
            for line in iter(stream.readline, ""):
                self._append(script_id, f"{prefix}{line.rstrip()}")
        finally:
            stream.close()

    def _append(self, script_id: str, line: str) -> None:
        with self._lock:
            self._append_locked(script_id, line)

    def _append_locked(self, script_id: str, line: str) -> None:
        state = self._states[script_id]
        state.output_lines.append(line)
        if len(state.output_lines) > 1000:
            state.output_lines = state.output_lines[-1000:]
        if state.log_path:
            with state.log_path.open("a", encoding="utf-8", errors="replace") as handle:
                handle.write(line + "\n")

    def _refresh(self, script_id: str) -> None:
        with self._lock:
            self._refresh_locked(script_id)

    def _refresh_locked(self, script_id: str) -> None:
        state = self._states[script_id]
        process = state.process
        if process is None:
            return
        return_code = process.poll()
        if return_code is None:
            return
        state.return_code = return_code
        state.process = None
        state.status = COMPLETE if return_code == 0 else FAILED
        state.message = "Script completed." if return_code == 0 else f"Script exited with code {return_code}."

    def _kill_later(self, script_id: str, process: subprocess.Popen[str]) -> None:
        time.sleep(8)
        if process.poll() is None:
            with self._lock:
                self._append_locked(script_id, "Graceful stop timed out. Killing process.")
            process.kill()


def default_args_for(spec: ScriptSpec, form: dict[str, str]) -> list[str]:
    if spec.needs_parameters:
        return [form.get("signal_source", "polymarket"), form.get("identifier", "").strip()]
    return list(spec.default_args)


runner = ScriptRunner()
