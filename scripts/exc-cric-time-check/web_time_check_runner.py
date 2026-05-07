from __future__ import annotations

import os
import sys
import time
from pathlib import Path


THIS_FILE = Path(__file__).resolve()
SCRIPT_DIR = THIS_FILE.parent
PROJECT_ROOT = THIS_FILE.parents[2]
ORIGINAL = SCRIPT_DIR / "betfair_decimal_time_checker.py"


def load_dotenv_file(env_path: Path, env: dict[str, str]) -> None:
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")

        if key and value and key not in env:
            env[key] = value


def main() -> None:
    env = os.environ.copy()
    load_dotenv_file(PROJECT_ROOT / ".env", env)

    if Path("/usr/bin/google-chrome").exists():
        env["CHROME_BINARY"] = "/usr/bin/google-chrome"

    profile_dir = (
        PROJECT_ROOT
        / "runtime"
        / "output"
        / "chrome_profiles"
        / f"web_cricket_{int(time.time())}_{os.getpid()}"
    )
    profile_dir.mkdir(parents=True, exist_ok=True)
    env["CHROME_PROFILE_DIR"] = str(profile_dir)
    env["PYTHONUNBUFFERED"] = "1"

    args = list(sys.argv[1:])

    if "--debug-decimal" not in args:
        args.append("--debug-decimal")
    if "--verbose" not in args:
        args.append("--verbose")

    print(f"[web-cricket-wrapper] original={ORIGINAL}", flush=True)
    print(f"[web-cricket-wrapper] cwd={PROJECT_ROOT}", flush=True)
    print(f"[web-cricket-wrapper] chrome_binary={env.get('CHROME_BINARY', '')}", flush=True)
    print(f"[web-cricket-wrapper] chrome_profile_dir={env.get('CHROME_PROFILE_DIR', '')}", flush=True)
    print(f"[web-cricket-wrapper] args={' '.join(args)}", flush=True)

    os.chdir(PROJECT_ROOT)
    os.execvpe(sys.executable, [sys.executable, str(ORIGINAL), *args], env)


if __name__ == "__main__":
    main()
