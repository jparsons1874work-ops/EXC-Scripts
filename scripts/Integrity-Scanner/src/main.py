"""Main orchestration entry point for the Integrity Player Scanner."""

from __future__ import annotations

import json
import logging
import os
import signal
import sys
import time

import schedule

from src.betfair_client import BetfairClient
from src.integrity_checker import IntegrityChecker
from src.slack_notifier import SlackNotifier
from src.utils import setup_logging

logger = logging.getLogger(__name__)
main_logger = logging.getLogger("integrity_scanner")

scanner_running: bool = True
alerted_matches: set[tuple[str, str]] = set()
notifier: SlackNotifier | None = None
consecutive_failures: int = 0
persistent_issue_alert_sent: bool = False


def _resolve_path(preferred_relative_path: str, fallback_relative_path: str) -> str:
    """Resolve a project file path whether running from project root or the `src` folder.

    Parameters:
        preferred_relative_path: Path to try relative to the current working directory.
        fallback_relative_path: Path to try relative to this file's parent directory.

    Returns:
        The first existing absolute path, otherwise the preferred absolute candidate.
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    candidates = [
        os.path.abspath(preferred_relative_path),
        os.path.abspath(os.path.join(script_dir, fallback_relative_path)),
    ]

    for candidate in candidates:
        if os.path.exists(candidate):
            return candidate

    return candidates[0]


def load_config(config_path: str = "config/credentials.json") -> dict:
    """Load the scanner configuration from JSON.

    Parameters:
        config_path: Preferred config path to load.

    Returns:
        The parsed configuration dictionary.

    Raises:
        FileNotFoundError: If no config file can be found.
        ValueError: If the config file contains invalid JSON.
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_candidates = [
        os.path.abspath(config_path),
        os.path.abspath(os.path.join(script_dir, "..", config_path)),
    ]

    env_config = {
        "betfair": {
            "username": os.getenv("BETFAIR_USERNAME", ""),
            "password": os.getenv("BETFAIR_PASSWORD", ""),
            "app_key": os.getenv("BETFAIR_APP_KEY", ""),
            "certs_path": os.getenv("BETFAIR_CERTS_DIR", ""),
        },
        "slack": {"webhook_url": os.getenv("SLACK_WEBHOOK_URL", "")},
    }
    if all(env_config["betfair"].values()) and env_config["slack"]["webhook_url"]:
        return env_config

    config_file = next((path for path in config_candidates if os.path.exists(path)), None)
    if config_file is None:
        raise FileNotFoundError(
            "Configuration file not found. Checked: "
            + ", ".join(config_candidates)
        )

    try:
        with open(config_file, encoding="utf-8") as file_handle:
            return json.load(file_handle)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in configuration file '{config_file}': {exc}") from exc


def run_scan(
    betfair_client: BetfairClient,
    integrity_checker: IntegrityChecker,
    slack_notifier: SlackNotifier,
) -> int:
    """Perform one complete scanner cycle.

    Parameters:
        betfair_client: Authenticated Betfair API client.
        integrity_checker: Integrity watchlist matcher.
        slack_notifier: Slack webhook notifier.

    Returns:
        The number of alerts sent during this scan cycle.
    """
    global alerted_matches

    alerts_sent = 0

    try:
        main_logger.info("Starting scan cycle...")
        if not check_betfair_session(betfair_client, slack_notifier):
            run_scan.last_cycle_failed = True
            return 0

        try:
            previous_watchlist_count = len(integrity_checker.watchlist)
            reloaded_watchlist = integrity_checker.load_watchlist()
            if reloaded_watchlist:
                main_logger.info("Reloaded integrity watchlist with %d players.", len(reloaded_watchlist))
            elif previous_watchlist_count > 0:
                main_logger.error("Watchlist reload failed; continuing with existing watchlist.")
                slack_notifier.send_scan_error("Integrity watchlist reload failed. Continuing with existing watchlist.")
            else:
                main_logger.error("Watchlist reload failed and no existing watchlist is available.")
                slack_notifier.send_scan_error("Integrity watchlist reload failed and no existing watchlist is available.")
        except Exception as exc:
            main_logger.exception("Unexpected watchlist reload failure: %s", exc)
            if integrity_checker.watchlist:
                slack_notifier.send_scan_error(
                    f"Integrity watchlist reload failed. Continuing with existing watchlist. Error: {exc}"
                )
            else:
                slack_notifier.send_scan_error(
                    f"Integrity watchlist reload failed and no existing watchlist is available. Error: {exc}"
                )

        try:
            markets = betfair_client.get_tennis_match_odds_markets()
        except Exception as exc:
            main_logger.exception("Betfair market fetch raised an unexpected error: %s", exc)
            run_scan.last_cycle_failed = True
            return 0

        if not markets:
            main_logger.warning("No tennis Match Odds markets returned this cycle.")
            run_scan.last_cycle_failed = False
            return 0

        main_logger.info("Found %d tennis Match Odds markets.", len(markets))

        for market in markets:
            try:
                for runner in market.get("runners", []):
                    runner_name = runner.get("runner_name", "")
                    match_result = integrity_checker.check_player(runner_name)
                    if not match_result:
                        continue

                    dedup_key = (str(market.get("event_id", "")), match_result["watchlist_name"])
                    if dedup_key in alerted_matches:
                        continue

                    alert_data = {
                        "player_name": runner_name,
                        "watchlist_name": match_result["watchlist_name"],
                        "confidence_score": match_result["confidence_score"],
                        "confidence_level": match_result["confidence_level"],
                        "requires_verification": match_result["requires_verification"],
                        "tournament_name": market.get("competition_name", "Unknown"),
                        "match_start_time": market.get("market_start_time"),
                        "event_id": str(market.get("event_id", "")),
                        "event_name": market.get("event_name", "Unknown"),
                    }

                    if slack_notifier.send_integrity_alert(alert_data):
                        alerted_matches.add(dedup_key)
                        alerts_sent += 1
                        main_logger.info(
                            "Integrity alert sent for event %s and watchlist player '%s'.",
                            alert_data["event_id"],
                            alert_data["watchlist_name"],
                        )
            except Exception as exc:
                main_logger.exception(
                    "Failed to process market %s (%s): %s",
                    market.get("market_id", "unknown"),
                    market.get("event_name", "Unknown"),
                    exc,
                )
                continue

        main_logger.info(
            "Scan complete. %d alert(s) sent this cycle. Next scan in 10 minutes.",
            alerts_sent,
        )
        run_scan.last_cycle_failed = False
        return alerts_sent
    except Exception as exc:  # pragma: no cover - orchestration safety net
        main_logger.exception("Scan cycle failed: %s", exc)
        slack_notifier.send_scan_error(str(exc))
        run_scan.last_cycle_failed = True
        return 0


run_scan.last_cycle_failed = False


def check_betfair_session(betfair_client: BetfairClient, slack_notifier: SlackNotifier) -> bool:
    """Check the Betfair session and attempt re-login when needed.

    Parameters:
        betfair_client: Betfair API client to validate.
        slack_notifier: Slack notifier used for error reporting.

    Returns:
        `True` if the session is active or successfully re-established, otherwise `False`.
    """
    if betfair_client.keep_alive():
        return True

    main_logger.warning("Betfair session appears invalid. Attempting re-login.")
    if betfair_client.login():
        main_logger.info("Betfair session re-established successfully.")
        return True

    main_logger.error("Betfair re-login failed.")
    slack_notifier.send_scan_error("Betfair session expired and re-login failed.")
    return False


def execute_scan_cycle(
    betfair_client: BetfairClient,
    integrity_checker: IntegrityChecker,
    slack_notifier: SlackNotifier,
) -> int:
    """Run a scan cycle and track persistent consecutive failures.

    Parameters:
        betfair_client: Authenticated Betfair API client.
        integrity_checker: Integrity watchlist matcher.
        slack_notifier: Slack webhook notifier.

    Returns:
        The number of alerts sent during the cycle.
    """
    global consecutive_failures, persistent_issue_alert_sent

    alerts_sent = run_scan(betfair_client, integrity_checker, slack_notifier)
    if run_scan.last_cycle_failed:
        consecutive_failures += 1
        main_logger.warning("Scan cycle failed completely. Consecutive failures: %d", consecutive_failures)
        if consecutive_failures >= 5 and not persistent_issue_alert_sent:
            slack_notifier.send_scan_error("Scanner has failed completely for 5 consecutive scan cycles.")
            persistent_issue_alert_sent = True
    else:
        consecutive_failures = 0
        persistent_issue_alert_sent = False

    return alerts_sent


def signal_handler(signum, frame):
    """Handle termination signals by stopping the main loop.

    Parameters:
        signum: Received signal number.
        frame: Current execution frame, unused.
    """
    del signum, frame

    global scanner_running

    scanner_running = False
    main_logger.info("Shutdown signal received...")


def main():
    """Initialise scanner components, schedule scans, and run until shutdown."""
    global scanner_running, alerted_matches, notifier, consecutive_failures, persistent_issue_alert_sent

    scanner_running = True
    alerted_matches = set()
    consecutive_failures = 0
    persistent_issue_alert_sent = False
    setup_logging()
    main_logger.info("Integrity Scanner starting...")

    try:
        config = load_config()
    except (FileNotFoundError, ValueError) as exc:
        main_logger.error("%s", exc)
        sys.exit(1)

    betfair_config = config.get("betfair", {})
    slack_config = config.get("slack", {})
    required_betfair_fields = ("username", "password", "app_key", "certs_path")
    missing_betfair_fields = [field for field in required_betfair_fields if not betfair_config.get(field)]
    if missing_betfair_fields:
        main_logger.error(
            "Missing required Betfair config fields: %s",
            ", ".join(missing_betfair_fields),
        )
        sys.exit(1)

    webhook_url = slack_config.get("webhook_url")
    if not webhook_url:
        main_logger.error("Missing required Slack config field: webhook_url")
        sys.exit(1)

    data_path = _resolve_path("data/integrity_list.xlsx", os.path.join("..", "data", "integrity_list.xlsx"))
    certs_path = betfair_config["certs_path"]
    if not os.path.isabs(certs_path):
        project_root = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
        certs_path = os.path.abspath(os.path.join(project_root, certs_path))

    betfair_client = BetfairClient(
        username=betfair_config["username"],
        password=betfair_config["password"],
        app_key=betfair_config["app_key"],
        certs_path=certs_path,
    )
    integrity_checker = IntegrityChecker(data_path)
    notifier = SlackNotifier(webhook_url)

    signal.signal(signal.SIGINT, signal_handler)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, signal_handler)

    if not betfair_client.login():
        main_logger.error("Failed to login to Betfair. Scanner cannot continue.")
        notifier.send_scanner_offline("Betfair login failed")
        sys.exit(1)

    notifier.send_scanner_online()
    main_logger.info("Scanner initialised. Running scan every 10 minutes. Press Ctrl+C to stop.")

    execute_scan_cycle(betfair_client, integrity_checker, notifier)
    schedule.every(10).minutes.do(execute_scan_cycle, betfair_client, integrity_checker, notifier)

    while scanner_running:
        schedule.run_pending()
        time.sleep(1)

    main_logger.info("Shutting down...")
    notifier.send_scanner_offline("Manual shutdown")
    betfair_client.logout()
    main_logger.info("Scanner stopped.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # pragma: no cover - final safety net
        logger.exception("Unhandled scanner exception: %s", exc)
        if notifier is not None:
            notifier.send_scanner_offline(str(exc))
        sys.exit(1)
