"""Slack webhook notification helpers for Integrity Player Scanner."""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timezone
from pathlib import Path

import requests

from src.retry import retry

logger = logging.getLogger(__name__)


class SlackNotifier:
    """Send scanner and integrity alerts to Slack via incoming webhook."""

    SLACK_WEBHOOK_PREFIX = "https://hooks.slack.com/"

    def __init__(self, webhook_url: str):
        """Initialise the Slack notifier.

        Parameters:
            webhook_url: Slack incoming webhook URL.
        """
        self.webhook_url = webhook_url
        if webhook_url and not webhook_url.startswith(self.SLACK_WEBHOOK_PREFIX):
            logger.warning("Slack webhook URL does not look valid.")

    def send_message(self, text: str) -> bool:
        """Send a simple text message to Slack.

        Parameters:
            text: Plain text message to send.

        Returns:
            `True` on HTTP 200 success, otherwise `False`.
        """
        return self._post_to_slack({"text": text})

    def send_integrity_alert(self, alert_data: dict) -> bool:
        """Send a formatted integrity alert to Slack.

        Parameters:
            alert_data: Alert fields describing the player match and market context.

        Returns:
            `True` on HTTP 200 success, otherwise `False`.
        """
        match_start_time = self._format_datetime(alert_data.get("match_start_time"), include_seconds=False)
        player_name = alert_data.get("player_name", "Unknown")
        watchlist_name = alert_data.get("watchlist_name", "Unknown")
        confidence_score = int(alert_data.get("confidence_score", 0))
        confidence_level = str(alert_data.get("confidence_level", "")).lower()
        tournament_name = alert_data.get("tournament_name", "Unknown")
        event_id = str(alert_data.get("event_id", "Unknown"))
        event_name = alert_data.get("event_name", "Unknown")

        is_high_confidence = confidence_level == "high" and not alert_data.get("requires_verification", False)
        if is_high_confidence:
            header_text = "INTEGRITY ALERT"
            confidence_text = f"Confidence: :white_check_mark: Confirmed ({confidence_score}%)"
            action_text = "Action Required: Remove from site"
            intro_line = f"*Player:* {player_name}\n*Matched To:* {watchlist_name}"
        else:
            header_text = "\u26a0\ufe0f INTEGRITY ALERT - REQUIRES VERIFICATION"
            confidence_text = f"Confidence: :warning: Uncertain ({confidence_score}%)"
            action_text = "Action Required: Verify player identity, then remove if confirmed"
            intro_line = f"*Player:* {player_name}\n*Possible Match For:* {watchlist_name}"

        body_text = (
            f"{intro_line}\n"
            f"*{confidence_text}*\n\n"
            f"*Tournament:* {tournament_name}\n"
            f"*Match:* {event_name}\n"
            f"*Start Time:* {match_start_time}\n"
            f"*Betfair Match ID:* {event_id}\n\n"
            f"*{action_text}*\n"
            f"`MATCH:{event_id}`"
        )

        payload = {
            "text": f"{header_text} - {player_name}",
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": header_text,
                        "emoji": True,
                    },
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": body_text,
                    },
                },
            ],
        }
        return self._post_to_slack(payload)

    def send_scanner_online(self) -> bool:
        """Send a notification that the scanner has started.

        Returns:
            `True` on HTTP 200 success, otherwise `False`.
        """
        current_time = self._format_datetime(datetime.now(UTC), include_seconds=True)
        message = (
            "SCANNER ONLINE\n\n"
            "Integrity Scanner has started successfully.\n"
            f"Time: {current_time}\n"
            "Scan interval: Every 10 minutes"
        )
        return self.send_message(message)

    def send_scanner_offline(self, reason: str = "Manual shutdown") -> bool:
        """Send a notification that the scanner has stopped.

        Parameters:
            reason: Human-readable reason for the scanner stopping.

        Returns:
            `True` on HTTP 200 success, otherwise `False`.
        """
        current_time = self._format_datetime(datetime.now(UTC), include_seconds=True)
        message = (
            "SCANNER OFFLINE\n\n"
            "Integrity Scanner has stopped.\n"
            f"Time: {current_time}\n"
            f"Reason: {reason}\n\n"
            "Please restart or investigate."
        )
        return self.send_message(message)

    def send_scan_error(self, error_message: str) -> bool:
        """Send a non-fatal scanner warning to Slack.

        Parameters:
            error_message: Description of the error encountered during scanning.

        Returns:
            `True` on HTTP 200 success, otherwise `False`.
        """
        current_time = self._format_datetime(datetime.now(UTC), include_seconds=True)
        message = (
            "SCANNER WARNING\n\n"
            "An error occurred during scanning:\n"
            f"{error_message}\n\n"
            f"Time: {current_time}\n"
            "Scanner will retry on next cycle."
        )
        return self.send_message(message)

    def _post_to_slack(self, payload: dict) -> bool:
        """Post a JSON payload to the configured Slack webhook.

        Parameters:
            payload: Slack message payload.

        Returns:
            `True` if Slack responds with HTTP 200, otherwise `False`.
        """
        if not self.webhook_url:
            logger.error("Slack webhook URL is not configured.")
            return False

        try:
            @retry(
                max_attempts=3,
                delay_seconds=1.0,
                backoff_multiplier=2.0,
                exceptions=(requests.exceptions.RequestException,),
            )
            def post_request():
                return requests.post(self.webhook_url, json=payload, timeout=10)

            response = post_request()
        except requests.exceptions.RequestException as exc:
            logger.exception("Slack webhook request failed: %s", exc)
            return False

        if response.status_code == 200:
            logger.info("Slack notification sent successfully.")
            return True

        logger.error(
            "Slack notification failed with status %s: %s",
            response.status_code,
            response.text,
        )
        return False

    def _format_datetime(self, value: object, include_seconds: bool) -> str:
        """Format datetimes for Slack messages in UTC.

        Parameters:
            value: Datetime-like object to format.
            include_seconds: Whether to include seconds in the formatted output.

        Returns:
            A UTC datetime string or `"Unknown"` if formatting is not possible.
        """
        if not isinstance(value, datetime):
            return "Unknown"

        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        else:
            value = value.astimezone(timezone.utc)

        format_string = "%Y-%m-%d %H:%M:%S UTC" if include_seconds else "%Y-%m-%d %H:%M UTC"
        return value.strftime(format_string)


if __name__ == "__main__":
    import json
    import os
    from datetime import datetime, timezone

    with Path("config/credentials.json").open(encoding="utf-8") as file_handle:
        creds = json.load(file_handle)

    webhook_url = (
        os.getenv("TENNIS_INTEGRITY_SLACK_WEBHOOK_URL", "").strip()
        or os.getenv("SLACK_WEBHOOK_URL", "").strip()
        or creds.get("slack", {}).get("webhook_url", "")
    )

    if not webhook_url:
        print("Please set TENNIS_INTEGRITY_SLACK_WEBHOOK_URL, or SLACK_WEBHOOK_URL as the fallback.")
    else:
        notifier = SlackNotifier(webhook_url)

        # Test scanner online message
        print("Sending test 'Scanner Online' message...")
        if notifier.send_scanner_online():
            print("Success!")
        else:
            print("Failed to send message")

        # Uncomment below to test integrity alert:
        # test_alert = {
        #     "player_name": "M. Zekic",
        #     "watchlist_name": "Miljan Zekic",
        #     "confidence_score": 87,
        #     "confidence_level": "high",
        #     "requires_verification": False,
        #     "tournament_name": "ATP Challenger Test",
        #     "match_start_time": datetime.now(timezone.utc),
        #     "event_id": "12345678",
        #     "event_name": "Zekic v Test Player"
        # }
        # notifier.send_integrity_alert(test_alert)
