"""
Integrity Scanner Launcher for ScriptManager

This script launches the Integrity Scanner from any location.
It handles setting the correct working directory and Python path.
"""

import sys
import os
from pathlib import Path


def has_tennis_slack_webhook() -> bool:
    return bool(os.getenv("TENNIS_INTEGRITY_SLACK_WEBHOOK_URL") or os.getenv("SLACK_WEBHOOK_URL"))


def main():
    # Get the directory where this launcher script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))

    os.chdir(script_dir)
    project_root = Path(script_dir).parents[1]
    os.environ.setdefault("SCRIPT_OUTPUT_DIR", str(project_root / "runtime" / "output"))

    # Check if credentials exist
    required_env = ["BETFAIR_USERNAME", "BETFAIR_PASSWORD", "BETFAIR_APP_KEY"]
    missing_env = [name for name in required_env if not os.getenv(name)]
    if not has_tennis_slack_webhook():
        missing_env.append("TENNIS_INTEGRITY_SLACK_WEBHOOK_URL")
    creds_path = os.path.join(script_dir, "config", "credentials.json")
    slack_missing = "TENNIS_INTEGRITY_SLACK_WEBHOOK_URL" in missing_env
    if slack_missing or (missing_env and not os.path.exists(creds_path)):
        print("=" * 50)
        print("ERROR: Scanner credentials are not configured.")
        print("=" * 50)
        print()
        print("Set these environment variables or create ignored config/credentials.json:")
        print("  " + ", ".join(missing_env))
        if "TENNIS_INTEGRITY_SLACK_WEBHOOK_URL" in missing_env:
            print("  SLACK_WEBHOOK_URL may be used only as the backwards-compatible tennis Slack fallback.")
        print()
        sys.exit(1)

    # Check if integrity list exists
    list_path = os.path.join(script_dir, 'data', 'integrity_list.xlsx')
    if not os.path.exists(list_path):
        print("=" * 50)
        print("ERROR: Integrity list not found.")
        print("=" * 50)
        print()
        print("Please add integrity_list.xlsx to the data folder.")
        print()
        sys.exit(1)

    # Check if certs exist
    cert_file = os.getenv("BETFAIR_CERT_FILE") or os.path.join(script_dir, "certs", "client-2048.crt")
    key_file = os.getenv("BETFAIR_KEY_FILE") or os.path.join(script_dir, "certs", "client-2048.key")
    if not os.path.exists(cert_file) or not os.path.exists(key_file):
        print("=" * 50)
        print("ERROR: Betfair certificate not found.")
        print("=" * 50)
        print()
        print("Set BETFAIR_CERT_FILE and BETFAIR_KEY_FILE or add ignored cert files locally.")
        print()
        sys.exit(1)

    print("=" * 50)
    print("   Integrity Player Scanner")
    print("=" * 50)
    print()
    print(f"Working directory: {script_dir}")
    print(f"Using Python: {sys.executable}")
    print()
    print("Starting scanner... Press Ctrl+C to stop.")
    print()

    # Run the scanner using the venv Python
    # Use subprocess to run with the venv's Python interpreter
    try:
        from src.main import main as scanner_main

        scanner_main()
    except KeyboardInterrupt:
        print()
        print("Scanner stopped by user.")
        sys.exit(0)
    except Exception as e:
        print(f"Error running scanner: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
