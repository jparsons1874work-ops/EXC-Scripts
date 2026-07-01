from __future__ import annotations

import codecs
import logging
import os
import queue
import re
import signal
import subprocess
import sys
import threading
import time
import uuid
from dataclasses import dataclass, field, replace
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
TAIL_TRIM_LINES = 900
MAX_TAIL_BYTES = 2 * 1024 * 1024
TAIL_TRIM_BYTES = 1536 * 1024
MAX_STREAM_TAIL_LINES = 250
STREAM_TAIL_TRIM_LINES = 200
MAX_STREAM_TAIL_BYTES = 256 * 1024
STREAM_TAIL_TRIM_BYTES = 192 * 1024
MAX_LINE_CHARS = 16 * 1024
STREAM_CHUNK_BYTES = 8192
LOG_QUEUE_LINES = 128
MAX_LOG_BYTES = 2 * 1024 * 1024
STOP_GRACE_SECONDS = 8
FORCE_WAIT_SECONDS = 5

logger = logging.getLogger("uvicorn.error")


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
    dropped_log_lines: int = 0
    cancel_requested: bool = False
    termination_status: str | None = None
    termination_message: str = ""
    output_bytes: int = field(default=0, repr=False)
    stdout_bytes: int = field(default=0, repr=False)
    stderr_bytes: int = field(default=0, repr=False)

    @property
    def elapsed_seconds(self) -> float | None:
        if self.started_at is None:
            return None
        return round((self.finished_at or time.time()) - self.started_at, 1)

    @property
    def output_tail(self) -> list[str]:
        return self.output_lines


class ScriptRunner:
    def __init__(self) -> None:
        self._states = {
            script_id: ScriptRunState(script_name=spec.name, timeout_seconds=spec.timeout_seconds)
            for script_id, spec in SCRIPTS_BY_ID.items()
        }
        self._state_locks = {script_id: threading.RLock() for script_id in SCRIPTS_BY_ID}
        self._log_queues: dict[str, queue.Queue[tuple[str, Path, str]]] = {
            script_id: queue.Queue(maxsize=LOG_QUEUE_LINES) for script_id in SCRIPTS_BY_ID
        }
        self._log_bytes: dict[tuple[str, str], int] = {}
        self._log_truncated: set[tuple[str, str]] = set()
        self._meta_lock = threading.RLock()
        self._last_startup_cleanup: list[str] = []
        for script_id in SCRIPTS_BY_ID:
            threading.Thread(target=self._write_log_loop, args=(script_id,), daemon=True).start()

    def get_state(self, script_id: str) -> ScriptRunState:
        with self._state_locks[script_id]:
            return self._copy_state_locked(script_id)

    def get_all_states(self) -> dict[str, ScriptRunState]:
        return {script_id: self.get_state(script_id) for script_id in SCRIPTS_BY_ID}

    def start(self, script_id: str, args: list[str]) -> ScriptRunState:
        spec = SCRIPTS_BY_ID[script_id]
        self._event("script_start_requested", script_id)
        with self._state_locks[script_id]:
            state = self._states[script_id]
            if not spec.allow_concurrent and self._is_active_locked(state):
                state.duplicate_attempts += 1
                state.message = "Script is already running; duplicate start ignored."
                state.last_error = ALREADY_RUNNING
                self._append_locked(script_id, "Start ignored: script is already running.")
                self._event("script_already_running", script_id, job_id=state.job_id, pid=state.pid)
                return self._copy_state_locked(script_id)

            allowed, window_label = window_status(spec)
            if spec.long_running and not allowed:
                self._reset_state_for_start_locked(script_id, spec, args)
                state = self._states[script_id]
                state.status = BLOCKED
                state.finished_at = time.time()
                state.message = f"Start blocked. Allowed window: {window_label}."
                state.last_error = BLOCKED
                self._append_locked(script_id, state.message, job_id=state.job_id)
                return self._copy_state_locked(script_id)

            self._reset_state_for_start_locked(script_id, spec, args)
            state = self._states[script_id]
            state.status = RUNNING
            state.message = f"Starting job {state.job_id}..."
            job_id = state.job_id
            snapshot = self._copy_state_locked(script_id)

        threading.Thread(target=self._launch_job, args=(script_id, job_id, spec, list(args)), daemon=True).start()
        return snapshot

    def stop(self, script_id: str) -> ScriptRunState:
        self._event("script_stop_requested", script_id)
        with self._state_locks[script_id]:
            state = self._states[script_id]
            process = state.process
            job_id = state.job_id
            if not self._is_active_locked(state):
                return self._copy_state_locked(script_id)

            state.status = STOPPING
            state.cancel_requested = True
            state.termination_status = KILLED
            state.termination_message = "Script was stopped by user."
            state.message = "Stopping script..."
            self._append_locked(script_id, "Stop requested. Terminating process group.", job_id=job_id)
            snapshot = self._copy_state_locked(script_id)

        if process is not None:
            threading.Thread(
                target=self._terminate_job,
                args=(script_id, job_id, process, KILLED, "Script was stopped by user."),
                daemon=True,
            ).start()
        return snapshot

    def stop_expired_windows(self) -> None:
        for script_id, spec in SCRIPTS_BY_ID.items():
            if not spec.long_running:
                continue
            state = self.get_state(script_id)
            if state.status != RUNNING:
                continue
            allowed, window_label = window_status(spec)
            if not allowed:
                self._append(
                    script_id,
                    f"Allowed window ended ({window_label}). Attempting clean stop.",
                    job_id=state.job_id,
                )
                self.stop(script_id)

    def startup_cleanup(self) -> None:
        for script_id in SCRIPTS_BY_ID:
            with self._state_locks[script_id]:
                state = self._states[script_id]
                if state.status not in {RUNNING, STOPPING}:
                    continue
                if state.process is not None and state.process.poll() is None:
                    continue
                state.status = "stale"
                state.process = None
                state.finished_at = time.time()
                state.message = "Previous running job state was stale at hub startup."
                state.last_error = state.message
        lines = self._cleanup_orphaned_script_processes()
        with self._meta_lock:
            self._last_startup_cleanup = lines

    def health_snapshot(self) -> dict[str, Any]:
        states = self.get_all_states()
        jobs = [self._state_snapshot(script_id, states[script_id]) for script_id in SCRIPTS_BY_ID]
        running_jobs = [job for job in jobs if job["status"] in {RUNNING, STOPPING}]
        last_error = next(
            (
                state.last_error or state.message
                for state in sorted(states.values(), key=lambda item: item.started_at or 0, reverse=True)
                if state.last_error or state.status in {FAILED, TIMEOUT, KILLED, BLOCKED, "stale"}
            ),
            "",
        )
        with self._meta_lock:
            cleanup = list(self._last_startup_cleanup)
        return {
            "hub": "alive",
            "running_jobs_count": len(running_jobs),
            "running_job_names": [job["script_name"] for job in running_jobs],
            "running_jobs": running_jobs,
            "scripts": jobs,
            "last_job_error": last_error,
            "memory_mb": self._current_memory_mb(),
            "startup_cleanup": cleanup,
        }

    def _launch_job(self, script_id: str, job_id: str, spec: ScriptSpec, args: list[str]) -> None:
        ensure_runtime_dirs()
        safe_name = re.sub(r"[^A-Za-z0-9_.-]+", "_", spec.name).strip("_")
        log_path = LOG_DIR / f"{safe_name}.log"
        command = [sys.executable, str((PROJECT_ROOT / spec.relative_path).resolve()), *args]
        try:
            log_path.write_text("", encoding="utf-8")
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

            process = subprocess.Popen(
                command,
                cwd=str(PROJECT_ROOT),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                bufsize=0,
                env=env,
                **popen_kwargs,
            )
        except Exception as exc:
            with self._state_locks[script_id]:
                state = self._states[script_id]
                if state.job_id != job_id:
                    return
                state.status = FAILED
                state.finished_at = time.time()
                state.message = f"Failed to start script: {exc}"
                state.last_error = state.message
                state.log_path = log_path
                self._append_locked(script_id, state.message, job_id=job_id)
            self._event("script_job_finished", script_id, level=logging.ERROR, job_id=job_id, status=FAILED, error=exc)
            return

        with self._state_locks[script_id]:
            state = self._states[script_id]
            if state.job_id != job_id:
                stale_launch = True
                cancel_requested = True
                timeout_seconds = state.timeout_seconds
                pgid = None
            else:
                stale_launch = False
                state.process = process
                state.log_path = log_path
                state.command = command
                state.pid = process.pid
                state.process_group_id = self._process_group_id(process)
                state.message = f"Script started as job {job_id}."
                self._append_locked(
                    script_id,
                    f"Started job {job_id} with timeout {state.timeout_seconds} seconds.",
                    job_id=job_id,
                )
                cancel_requested = state.cancel_requested
                timeout_seconds = state.timeout_seconds
                pgid = state.process_group_id

        if stale_launch:
            self._terminate_process_group(script_id, process, force=True)
            return

        self._event("script_job_started", script_id, job_id=job_id, pid=process.pid, pgid=pgid)
        threading.Thread(target=self._read_stream, args=(script_id, job_id, process.stdout, "stdout"), daemon=True).start()
        threading.Thread(target=self._read_stream, args=(script_id, job_id, process.stderr, "stderr"), daemon=True).start()
        threading.Thread(target=self._watch_process, args=(script_id, job_id, process, timeout_seconds), daemon=True).start()

        if cancel_requested:
            self._terminate_job(script_id, job_id, process, KILLED, "Script was stopped by user.")

    def _reset_state_for_start_locked(self, script_id: str, spec: ScriptSpec, args: list[str]) -> ScriptRunState:
        timeout_seconds = spec.timeout_seconds or DEFAULT_TIMEOUT_SECONDS
        state = self._states[script_id]
        state.status = IDLE
        state.output_lines = []
        state.stdout_tail = []
        state.stderr_tail = []
        state.output_bytes = 0
        state.stdout_bytes = 0
        state.stderr_bytes = 0
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
        state.dropped_log_lines = 0
        state.cancel_requested = False
        state.termination_status = None
        state.termination_message = ""
        self._log_bytes[(script_id, state.job_id)] = 0
        self._log_truncated.discard((script_id, state.job_id))
        return state

    def _is_active_locked(self, state: ScriptRunState) -> bool:
        return state.status in {RUNNING, STOPPING}

    def _read_stream(self, script_id: str, job_id: str, stream, stream_name: str) -> None:
        if stream is None:
            return
        decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
        pending = ""
        try:
            while True:
                chunk = stream.read(STREAM_CHUNK_BYTES)
                if not chunk:
                    break
                pending += decoder.decode(chunk)
                while "\n" in pending:
                    line, pending = pending.split("\n", 1)
                    self._append(script_id, line.rstrip("\r"), stream_name=stream_name, job_id=job_id)
                while len(pending) > MAX_LINE_CHARS:
                    self._append(
                        script_id,
                        pending[:MAX_LINE_CHARS] + "... [line chunked]",
                        stream_name=stream_name,
                        job_id=job_id,
                    )
                    pending = pending[MAX_LINE_CHARS:]
            pending += decoder.decode(b"", final=True)
            if pending:
                self._append(script_id, pending.rstrip("\r"), stream_name=stream_name, job_id=job_id)
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

            with self._state_locks[script_id]:
                finalized = self._finish_process_locked(script_id, job_id, process, return_code)
                state = self._states[script_id]
                final_status = state.status
                final_message = state.message
            if finalized:
                if final_status == KILLED:
                    self._event("script_job_killed", script_id, job_id=job_id, exit_code=return_code)
                self._event(
                    "script_job_finished",
                    script_id,
                    job_id=job_id,
                    status=final_status,
                    exit_code=return_code,
                    message=final_message,
                )
            return

    def _terminate_job(
        self,
        script_id: str,
        job_id: str,
        process: subprocess.Popen[str],
        final_status: str,
        final_message: str,
    ) -> None:
        with self._state_locks[script_id]:
            state = self._states[script_id]
            if state.job_id != job_id:
                return
            state.termination_status = final_status
            state.termination_message = final_message
            state.cancel_requested = True
            state.status = STOPPING
            self._append_locked(script_id, final_message, job_id=job_id)

        if final_status == TIMEOUT:
            self._event("script_job_timeout", script_id, level=logging.WARNING, job_id=job_id, pid=process.pid)

        self._terminate_process_group(script_id, process, force=False)
        try:
            return_code = process.wait(timeout=STOP_GRACE_SECONDS)
        except subprocess.TimeoutExpired:
            self._append(script_id, "Graceful stop timed out. Force killing process group.", job_id=job_id)
            self._terminate_process_group(script_id, process, force=True)
            try:
                return_code = process.wait(timeout=FORCE_WAIT_SECONDS)
            except subprocess.TimeoutExpired:
                return_code = process.poll()

        with self._state_locks[script_id]:
            state = self._states[script_id]
            if state.job_id != job_id:
                return
            already_finalized = state.process is None and state.status == final_status and state.finished_at is not None
            if already_finalized:
                return
            state.return_code = return_code
            state.exit_code = return_code
            state.process = None
            state.status = final_status
            state.finished_at = time.time()
            state.message = final_message
            if final_status in {FAILED, TIMEOUT, KILLED}:
                state.last_error = final_message

        if final_status == KILLED:
            self._event("script_job_killed", script_id, job_id=job_id, exit_code=return_code)
        self._event(
            "script_job_finished",
            script_id,
            job_id=job_id,
            status=final_status,
            exit_code=return_code,
            message=final_message,
        )

    def _terminate_process_group(
        self,
        script_id: str,
        process: subprocess.Popen[str],
        force: bool,
    ) -> None:
        if process.poll() is not None:
            return
        if os.name == "nt":
            try:
                completed = subprocess.run(
                    ["taskkill", "/PID", str(process.pid), "/T", "/F"],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.PIPE,
                    text=True,
                    timeout=5,
                )
                if completed.returncode == 0:
                    return
                self._event(
                    "script_process_tree_kill_failed",
                    script_id,
                    level=logging.WARNING,
                    pid=process.pid,
                    error=completed.stderr.strip(),
                )
            except Exception as exc:
                self._event(
                    "script_process_tree_kill_failed",
                    script_id,
                    level=logging.WARNING,
                    pid=process.pid,
                    error=str(exc),
                )
            finally:
                if process.poll() is None:
                    process.kill()
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
        with self._state_locks[script_id]:
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
        state.output_bytes += self._line_bytes(display_line)
        state.output_bytes = self._trim_buffer(state.output_lines, state.output_bytes)
        if stream_name == "stderr":
            state.stderr_tail.append(line)
            state.stderr_bytes += self._line_bytes(line)
            state.stderr_bytes = self._trim_buffer(
                state.stderr_tail,
                state.stderr_bytes,
                max_lines=MAX_STREAM_TAIL_LINES,
                trim_lines=STREAM_TAIL_TRIM_LINES,
                max_bytes=MAX_STREAM_TAIL_BYTES,
                trim_bytes=STREAM_TAIL_TRIM_BYTES,
            )
        else:
            state.stdout_tail.append(line)
            state.stdout_bytes += self._line_bytes(line)
            state.stdout_bytes = self._trim_buffer(
                state.stdout_tail,
                state.stdout_bytes,
                max_lines=MAX_STREAM_TAIL_LINES,
                trim_lines=STREAM_TAIL_TRIM_LINES,
                max_bytes=MAX_STREAM_TAIL_BYTES,
                trim_bytes=STREAM_TAIL_TRIM_BYTES,
            )
        if state.log_path:
            try:
                self._log_queues[script_id].put_nowait((state.job_id, state.log_path, display_line))
            except queue.Full:
                state.dropped_log_lines += 1

    def _write_log_loop(self, script_id: str) -> None:
        log_queue = self._log_queues[script_id]
        while True:
            job_id, log_path, line = log_queue.get()
            try:
                with self._state_locks[script_id]:
                    if self._states[script_id].job_id != job_id:
                        continue
                key = (script_id, job_id)
                encoded = (line + "\n").encode("utf-8", errors="replace")
                written = self._log_bytes.get(key, 0)
                if written + len(encoded) <= MAX_LOG_BYTES:
                    with log_path.open("ab") as handle:
                        handle.write(encoded)
                    self._log_bytes[key] = written + len(encoded)
                elif key not in self._log_truncated:
                    marker = b"[Hub log capped at 2 MB; latest output remains available in the UI tail.]\n"
                    with log_path.open("ab") as handle:
                        handle.write(marker)
                    self._log_truncated.add(key)
            except Exception as exc:
                self._event("script_log_write_failed", script_id, level=logging.WARNING, error=exc)
            finally:
                log_queue.task_done()

    def _finish_process_locked(
        self,
        script_id: str,
        job_id: str,
        process: subprocess.Popen[str],
        return_code: int | None,
    ) -> bool:
        state = self._states[script_id]
        if state.job_id != job_id or state.process is not process:
            return False
        state.return_code = return_code
        state.exit_code = return_code
        state.process = None
        state.finished_at = time.time()
        if state.termination_status:
            state.status = state.termination_status
            state.message = state.termination_message
        else:
            state.status = SUCCESS if return_code == 0 else FAILED
            state.message = "Script completed." if return_code == 0 else f"Script exited with code {return_code}."
        if state.status in {FAILED, TIMEOUT, KILLED}:
            state.last_error = state.message
        return True

    def _copy_state_locked(self, script_id: str) -> ScriptRunState:
        state = self._states[script_id]
        return replace(
            state,
            output_lines=list(state.output_lines),
            stdout_tail=list(state.stdout_tail),
            stderr_tail=list(state.stderr_tail),
            command=list(state.command),
        )

    def _process_group_id(self, process: subprocess.Popen[str]) -> int | None:
        if os.name == "nt":
            return process.pid
        try:
            return os.getpgid(process.pid)
        except Exception:
            return None

    def _trim_buffer(
        self,
        lines: list[str],
        total_bytes: int,
        max_lines: int = MAX_TAIL_LINES,
        trim_lines: int = TAIL_TRIM_LINES,
        max_bytes: int = MAX_TAIL_BYTES,
        trim_bytes: int = TAIL_TRIM_BYTES,
    ) -> int:
        if len(lines) <= max_lines and total_bytes <= max_bytes:
            return total_bytes
        drop_count = 0
        while drop_count < len(lines) and (
            len(lines) - drop_count > trim_lines or total_bytes > trim_bytes
        ):
            total_bytes -= self._line_bytes(lines[drop_count])
            drop_count += 1
        if drop_count:
            del lines[:drop_count]
        return max(total_bytes, 0)

    def _line_bytes(self, line: str) -> int:
        return len(line.encode("utf-8", errors="replace")) + 1

    def _bounded_line(self, line: str) -> str:
        if len(line) <= MAX_LINE_CHARS:
            return line
        return line[:MAX_LINE_CHARS] + "... [line truncated]"

    def _state_snapshot(self, script_id: str, state: ScriptRunState) -> dict[str, Any]:
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
            "elapsed_seconds": state.elapsed_seconds,
            "last_message": state.message,
            "last_error": state.last_error,
            "dropped_log_lines": state.dropped_log_lines,
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

    def _event(self, event: str, script_id: str, level: int = logging.INFO, **details: Any) -> None:
        spec = SCRIPTS_BY_ID.get(script_id)
        detail_text = " ".join(f"{key}={value!r}" for key, value in details.items())
        logger.log(
            level,
            "%s script_id=%s script_name=%r%s",
            event,
            script_id,
            spec.name if spec else script_id,
            f" {detail_text}" if detail_text else "",
        )


def default_args_for(spec: ScriptSpec, form: dict[str, str]) -> list[str]:
    if spec.needs_parameters:
        return [form.get("signal_source", "polymarket"), form.get("identifier", "").strip()]
    return list(spec.default_args)


runner = ScriptRunner()
