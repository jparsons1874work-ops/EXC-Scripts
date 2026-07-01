from __future__ import annotations

import os
import re
import signal
import subprocess
import sys
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from app.config import LOG_DIR, OUTPUT_DIR, PROJECT_ROOT, SCRIPTS_ROOT, child_environment, ensure_runtime_dirs
from app.registry import SCRIPTS_BY_ID, ScriptSpec
from app.scheduler import window_status


IDLE = "idle"
RUNNING = "running"
STOPPING = "stopping"
SUCCESS = "success"
FAILED = "failed"
TIMEOUT = "timeout"
KILLED = "killed"
ALREADY_RUNNING = "already_running"
BLOCKED = "blocked"

DEFAULT_TIMEOUT_SECONDS = 10 * 60
MAX_TAIL_LINES = 1000
MAX_TAIL_BYTES = 2 * 1024 * 1024
MAX_LINE_CHARS = 16 * 1024
STOP_GRACE_SECONDS = 8
FORCE_WAIT_SECONDS = 5


@dataclass
class ScriptRunState:
    status: str = IDLE
    output_lines: list[str] = field(default_factory=list)
    stdout_tail: list[str] = field(default_factory=list)
    stderr_tail: list[str] = field(default_factory=list)
    process: subprocess.Popen[str] | None = None
    return_code: int | None = None
    exit_code: int | None = None
    log_path: Path | None = None
    started_at: float | None = None
    finished_at: float | None = None
    message: str = ""
    command: list[str] = field(default_factory=list)
    job_id: str = ""
    script_name: str = ""
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS
    pid: int | None = None
    process_group_id: int | None = None
    last_error: str = ""
    duplicate_attempts: int = 0


class ScriptRunner:
    def __init__(self) -> None:
        self._states = {
            script_id: ScriptRunState(script_name=spec.name, timeout_seconds=spec.timeout_seconds)
            for script_id, spec in SCRIPTS_BY_ID.items()
        }
        self._lock = threading.RLock()
        self._last_startup_cleanup: list[str] = []

    def get_state(self, script_id: str) -> ScriptRunState:
        with self._lock:
            self._refresh_locked(script_id)
            return self._states[script_id]

    def start(self, script_id: str, args: list[str]) -> ScriptRunState:
        spec = SCRIPTS_BY_ID[script_id]
        with self._lock:
            state = self._states[script_id]
            self._refresh_locked(script_id)
            if not spec.allow_concurrent and self._is_running_locked(state):
                state.duplicate_attempts += 1
                state.message = "Script is already running; duplicate start ignored."
                state.last_error = ALREADY_RUNNING
                self._append_locked(script_id, "Start ignored: script is already running.")
                return state

            allowed, window_label = window_status(spec)
            if spec.long_running and not allowed:
                self._reset_state_for_start_locked(script_id, spec, args)
                state = self._states[script_id]
                state.status = BLOCKED
                state.finished_at = time.time()
                state.output_lines = [f"Start blocked. Allowed window: {window_label}."]
                state.message = state.output_lines[0]
                state.last_error = BLOCKED
                return state

            return self._launch_locked(script_id, spec, args)

    def stop(self, script_id: str) -> ScriptRunState:
        with self._lock:
            state = self._states[script_id]
            process = state.process
            job_id = state.job_id
            if process is None or process.poll() is not None:
                self._refresh_locked(script_id)
                return state

            state.status = STOPPING
            state.message = "Stopping script..."
            self._append_locked(script_id, "Stop requested. Terminating process group.", job_id=job_id)

        threading.Thread(
            target=self._terminate_job,
            args=(script_id, job_id, process, KILLED, "Script was stopped by user."),
            daemon=True,
        ).start()
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
                    self._append_locked(
                        script_id,
                        f"Allowed window ended ({window_label}). Attempting clean stop.",
                        job_id=state.job_id,
                    )
                self.stop(script_id)

    def startup_cleanup(self) -> None:
        lines = self._cleanup_orphaned_script_processes()
        with self._lock:
            self._last_startup_cleanup = lines

    def health_snapshot(self) -> dict[str, Any]:
        with self._lock:
            for script_id in self._states:
                self._refresh_locked(script_id)
            running_jobs = [
                self._state_snapshot(script_id, state)
                for script_id, state in self._states.items()
                if state.status in {RUNNING, STOPPING}
            ]
            last_error = next(
                (
                    state.message or state.last_error
                    for state in sorted(
                        self._states.values(),
                        key=lambda item: item.started_at or 0,
                        reverse=True,
                    )
                    if state.last_error or state.status in {FAILED, TIMEOUT, KILLED, BLOCKED}
                ),
                "",
            )
            return {
                "hub": "alive",
                "running_jobs_count": len(running_jobs),
                "running_jobs": running_jobs,
                "last_job_error": last_error,
                "memory_mb": self._current_memory_mb(),
                "startup_cleanup": self._last_startup_cleanup,
            }

    def _launch_locked(self, script_id: str, spec: ScriptSpec, args: list[str]) -> ScriptRunState:
        state = self._reset_state_for_start_locked(script_id, spec, args)
        ensure_runtime_dirs()
        safe_name = re.sub(r"[^A-Za-z0-9_.-]+", "_", spec.name).strip("_")
        log_path = LOG_DIR / f"{safe_name}.log"
        log_path.write_text("", encoding="utf-8")

        script_path = (PROJECT_ROOT / spec.relative_path).resolve()
        command = [sys.executable, str(script_path), *args]
        env = child_environment()

        if script_id in {
            "cricket-time-check-today",
            "cricket-time-check-tomorrow",
            "cricket-decimal-fixture-scrape",
        }:
            profile_dir = OUTPUT_DIR / "chrome_profiles" / f"{script_id}_{int(time.time())}"
            profile_dir.mkdir(parents=True, exist_ok=True)
            env["CHROME_PROFILE_DIR"] = str(profile_dir)
            if not env.get("CHROME_BINARY") and Path("/usr/bin/google-chrome").exists():
                env["CHROME_BINARY"] = "/usr/bin/google-chrome"

        popen_kwargs: dict[str, Any] = {}
        if os.name == "nt":
            popen_kwargs["creationflags"] = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
        else:
            popen_kwargs["start_new_session"] = True

        try:
            process = subprocess.Popen(
                command,
                cwd=str(PROJECT_ROOT),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
                env=env,
                **popen_kwargs,
            )
        except Exception as exc:
            state.status = FAILED
            state.finished_at = time.time()
            state.message = f"Failed to start script: {exc}"
            state.last_error = state.message
            state.log_path = log_path
            self._append_locked(script_id, state.message, job_id=state.job_id)
            return state

        state.status = RUNNING
        state.process = process
        state.log_path = log_path
        state.started_at = time.time()
        state.command = command
        state.pid = process.pid
        state.process_group_id = self._process_group_id(process)
        state.message = f"Script started as job {state.job_id}."
        self._append_locked(script_id, f"Started job {state.job_id} with timeout {state.timeout_seconds} seconds.", job_id=state.job_id)

        threading.Thread(target=self._read_stream, args=(script_id, state.job_id, process.stdout, "stdout"), daemon=True).start()
        threading.Thread(target=self._read_stream, args=(script_id, state.job_id, process.stderr, "stderr"), daemon=True).start()
        threading.Thread(target=self._watch_process, args=(script_id, state.job_id, process, state.timeout_seconds), daemon=True).start()
        return state

    def _reset_state_for_start_locked(self, script_id: str, spec: ScriptSpec, args: list[str]) -> ScriptRunState:
        timeout_seconds = spec.timeout_seconds or DEFAULT_TIMEOUT_SECONDS
        state = self._states[script_id]
        state.status = IDLE
        state.output_lines = []
        state.stdout_tail = []
        state.stderr_tail = []
        state.process = None
        state.return_code = None
        state.exit_code = None
        state.log_path = None
        state.started_at = time.time()
        state.finished_at = None
        state.message = ""
        state.command = [sys.executable, str((PROJECT_ROOT / spec.relative_path).resolve()), *args]
        state.job_id = uuid.uuid4().hex[:12]
        state.script_name = spec.name
        state.timeout_seconds = timeout_seconds
        state.pid = None
        state.process_group_id = None
        state.last_error = ""
        state.duplicate_attempts = 0
        return state

    def _is_running_locked(self, state: ScriptRunState) -> bool:
        process = state.process
        return process is not None and process.poll() is None and state.status in {RUNNING, STOPPING}

    def _read_stream(self, script_id: str, job_id: str, stream, stream_name: str) -> None:
        if stream is None:
            return
        try:
            for line in iter(stream.readline, ""):
                self._append(script_id, line.rstrip(), stream_name=stream_name, job_id=job_id)
        finally:
            try:
                stream.close()
            except Exception:
                pass

    def _watch_process(
        self,
        script_id: str,
        job_id: str,
        process: subprocess.Popen[str],
        timeout_seconds: int,
    ) -> None:
        deadline = time.monotonic() + timeout_seconds
        while True:
            try:
                return_code = process.wait(timeout=0.5)
            except subprocess.TimeoutExpired:
                if time.monotonic() >= deadline:
                    self._terminate_job(
                        script_id,
                        job_id,
                        process,
                        TIMEOUT,
                        f"Script timed out after {timeout_seconds} seconds and was killed.",
                    )
                    return
                continue

            with self._lock:
                self._finish_process_locked(script_id, job_id, process, return_code)
            return

    def _terminate_job(
        self,
        script_id: str,
        job_id: str,
        process: subprocess.Popen[str],
        final_status: str,
        final_message: str,
    ) -> None:
        with self._lock:
            self._append_locked(script_id, final_message, job_id=job_id)

        self._terminate_process_group(process, force=False)
        try:
            return_code = process.wait(timeout=STOP_GRACE_SECONDS)
        except subprocess.TimeoutExpired:
            with self._lock:
                self._append_locked(script_id, "Graceful stop timed out. Force killing process group.", job_id=job_id)
            self._terminate_process_group(process, force=True)
            try:
                return_code = process.wait(timeout=FORCE_WAIT_SECONDS)
            except subprocess.TimeoutExpired:
                return_code = process.poll()

        with self._lock:
            state = self._states[script_id]
            if state.job_id != job_id:
                return
            state.return_code = return_code
            state.exit_code = return_code
            state.process = None
            state.status = final_status
            state.finished_at = time.time()
            state.message = final_message
            if final_status in {FAILED, TIMEOUT, KILLED}:
                state.last_error = final_message

    def _terminate_process_group(self, process: subprocess.Popen[str], force: bool) -> None:
        if process.poll() is not None:
            return
        if os.name == "nt":
            command = ["taskkill", "/PID", str(process.pid), "/T"]
            if force:
                command.append("/F")
            try:
                subprocess.run(
                    command,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    timeout=5,
                )
                return
            except Exception:
                if force:
                    process.kill()
                    return
            try:
                process.send_signal(signal.CTRL_BREAK_EVENT)
            except Exception:
                process.terminate()
            return

        try:
            os.killpg(os.getpgid(process.pid), signal.SIGKILL if force else signal.SIGTERM)
        except ProcessLookupError:
            return
        except Exception:
            if force:
                process.kill()
            else:
                process.terminate()

    def _append(self, script_id: str, line: str, stream_name: str = "stdout", job_id: str | None = None) -> None:
        with self._lock:
            self._append_locked(script_id, line, stream_name=stream_name, job_id=job_id)

    def _append_locked(
        self,
        script_id: str,
        line: str,
        stream_name: str = "stdout",
        job_id: str | None = None,
    ) -> None:
        state = self._states[script_id]
        if job_id and state.job_id != job_id:
            return
        line = self._bounded_line(line)
        display_line = f"STDERR: {line}" if stream_name == "stderr" else line
        state.output_lines.append(display_line)
        if stream_name == "stderr":
            state.stderr_tail.append(line)
            state.stderr_tail = self._trim_tail(state.stderr_tail)
        else:
            state.stdout_tail.append(line)
            state.stdout_tail = self._trim_tail(state.stdout_tail)
        state.output_lines = self._trim_tail(state.output_lines)
        if state.log_path:
            with state.log_path.open("a", encoding="utf-8", errors="replace") as handle:
                handle.write(display_line + "\n")

    def _refresh_locked(self, script_id: str) -> None:
        state = self._states[script_id]
        process = state.process
        if process is None:
            return
        return_code = process.poll()
        if return_code is None:
            return
        self._finish_process_locked(script_id, state.job_id, process, return_code)

    def _finish_process_locked(
        self,
        script_id: str,
        job_id: str,
        process: subprocess.Popen[str],
        return_code: int | None,
    ) -> None:
        state = self._states[script_id]
        if state.job_id != job_id:
            return
        if state.process is not None and state.process is not process:
            return
        state.return_code = return_code
        state.exit_code = return_code
        state.process = None
        state.finished_at = time.time()
        if state.status in {TIMEOUT, KILLED}:
            return
        state.status = SUCCESS if return_code == 0 else FAILED
        state.message = "Script completed." if return_code == 0 else f"Script exited with code {return_code}."
        if state.status == FAILED:
            state.last_error = state.message

    def _process_group_id(self, process: subprocess.Popen[str]) -> int | None:
        if os.name == "nt":
            return process.pid
        try:
            return os.getpgid(process.pid)
        except Exception:
            return None

    def _trim_tail(self, lines: list[str]) -> list[str]:
        trimmed = lines[-MAX_TAIL_LINES:]
        total = 0
        keep_reversed: list[str] = []
        for line in reversed(trimmed):
            total += len(line.encode("utf-8", errors="replace"))
            if total > MAX_TAIL_BYTES:
                break
            keep_reversed.append(line)
        keep_reversed.reverse()
        return keep_reversed

    def _bounded_line(self, line: str) -> str:
        if len(line) <= MAX_LINE_CHARS:
            return line
        return line[:MAX_LINE_CHARS] + "... [line truncated]"

    def _state_snapshot(self, script_id: str, state: ScriptRunState) -> dict[str, Any]:
        elapsed = None
        if state.started_at:
            elapsed = round((state.finished_at or time.time()) - state.started_at, 1)
        return {
            "script_id": script_id,
            "job_id": state.job_id,
            "script_name": state.script_name,
            "command": state.command,
            "started_at": state.started_at,
            "finished_at": state.finished_at,
            "status": state.status,
            "exit_code": state.exit_code,
            "timeout_seconds": state.timeout_seconds,
            "pid": state.pid,
            "process_group_id": state.process_group_id,
            "elapsed_seconds": elapsed,
            "last_message": state.message,
        }

    def _cleanup_orphaned_script_processes(self) -> list[str]:
        if os.name == "nt":
            return ["Startup orphan cleanup skipped on Windows."]
        if os.getenv("BETFAIR_HUB_CLEANUP_ORPHANS", "1").lower() in {"0", "false", "no"}:
            return ["Startup orphan cleanup disabled."]

        script_roots = {str(SCRIPTS_ROOT.resolve()), "/opt/betfair-scripts/scripts"}
        current_pid = os.getpid()
        try:
            completed = subprocess.run(
                ["ps", "-eo", "pid=,ppid=,pgid=,args="],
                capture_output=True,
                text=True,
                timeout=2,
                check=False,
            )
        except Exception as exc:
            return [f"Startup orphan cleanup could not inspect processes: {exc}"]

        stale_processes: list[tuple[int, int, str]] = []
        for raw_line in completed.stdout.splitlines():
            parts = raw_line.strip().split(maxsplit=3)
            if len(parts) < 4:
                continue
            try:
                pid = int(parts[0])
                ppid = int(parts[1])
                pgid = int(parts[2])
            except ValueError:
                continue
            args = parts[3]
            if pid == current_pid:
                continue
            if ppid != 1:
                continue
            if not any(root in args for root in script_roots):
                continue
            stale_processes.append((pid, pgid, args))

        if not stale_processes:
            return ["No orphaned hub child scripts detected."]

        cleaned: list[str] = []
        killed_groups: set[int] = set()
        for pid, pgid, args in stale_processes:
            if pgid in killed_groups:
                continue
            try:
                os.killpg(pgid, signal.SIGTERM)
                killed_groups.add(pgid)
                cleaned.append(f"Sent SIGTERM to stale script process group {pgid} (pid {pid}): {args[:160]}")
            except ProcessLookupError:
                cleaned.append(f"Stale script process already exited (pid {pid}).")
            except Exception as exc:
                cleaned.append(f"Could not terminate stale script pid {pid}: {exc}")

        time.sleep(2)
        for pid, pgid, args in stale_processes:
            if not self._pid_exists(pid):
                continue
            try:
                os.killpg(pgid, signal.SIGKILL)
                cleaned.append(f"Sent SIGKILL to stale script process group {pgid} (pid {pid}): {args[:160]}")
            except ProcessLookupError:
                pass
            except Exception as exc:
                cleaned.append(f"Could not force kill stale script pid {pid}: {exc}")
        return cleaned

    def _pid_exists(self, pid: int) -> bool:
        try:
            os.kill(pid, 0)
            return True
        except ProcessLookupError:
            return False
        except PermissionError:
            return True

    def _current_memory_mb(self) -> float | None:
        try:
            import resource
        except Exception:
            return None
        usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        if sys.platform == "darwin":
            return round(usage / (1024 * 1024), 1)
        return round(usage / 1024, 1)


def default_args_for(spec: ScriptSpec, form: dict[str, str]) -> list[str]:
    if spec.needs_parameters:
        return [form.get("signal_source", "polymarket"), form.get("identifier", "").strip()]
    return list(spec.default_args)


runner = ScriptRunner()
