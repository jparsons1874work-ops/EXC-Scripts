#!/usr/bin/env python3
"""Schedule Slack reminders for selected Betfair Exchange events."""

from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import sys
import tempfile
import traceback
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable
from zoneinfo import ZoneInfo

import requests
from betfairlightweight import APIClient
from betfairlightweight.filters import market_filter


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
CONFIG_DIR = PROJECT_ROOT / "config"
CONFIG_PATH = CONFIG_DIR / "betfair_event_reminders_config.json"
EXAMPLE_CONFIG_PATH = CONFIG_DIR / "betfair_event_reminders_config.example.json"
CONFIG_ENV_VAR = "BETFAIR_EVENT_REMINDERS_CONFIG"
STATE_PATH = PROJECT_ROOT / "data" / "betfair_event_reminders_sent.json"
LOG_DIR = PROJECT_ROOT / "logs"

UK_TZ = ZoneInfo("Europe/London")
UTC_TZ = ZoneInfo("UTC")
SCAN_START_TIME_UK = time(7, 0)
REMINDER_LEAD_MINUTES = 5
SLACK_SCHEDULE_LIMIT_PER_BUCKET = 30
SLACK_BUCKET_SECONDS = 5 * 60
SLACK_SCHEDULE_URL = "https://slack.com/api/chat.scheduleMessage"
MATCH_ODDS = "MATCH_ODDS"

PLACEHOLDER_CONFIG: dict[str, str] = {
    "slack_bot_token": "xoxb-NEW-CHANNEL-BOT-TOKEN-PLACEHOLDER",
    "slack_channel_id": "C_NEW_CHANNEL_ID_PLACEHOLDER",
    "slack_channel_name": "#exc_sports_ops",
    "fallback_webhook_url": "https://hooks.slack.com/services/NEW/CHANNEL/WEBHOOK_PLACEHOLDER",
    "betfair_app_key": "BETFAIR_APP_KEY_PLACEHOLDER",
    "betfair_username": "BETFAIR_USERNAME_PLACEHOLDER",
    "betfair_password": "BETFAIR_PASSWORD_PLACEHOLDER",
    "certs_dir": r"C:\BetfairScripts\certs",
}

PLACEHOLDER_MARKERS = ("PLACEHOLDER", "CHANGE_ME", "YOUR_", "TODO", "PASTE_")

SPORT_RULE_ALL = "all"
SPORT_RULE_FIRST = "first"
SPORT_RULE_DARTS_COMPETITION = "darts_first_per_competition"

SPORTS: tuple[dict[str, str], ...] = (
    {"name": "American Football", "rule": SPORT_RULE_ALL, "emoji": ":football:"},
    {"name": "Boxing", "rule": SPORT_RULE_FIRST, "emoji": ":boxing_glove:"},
    {"name": "Darts", "rule": SPORT_RULE_DARTS_COMPETITION, "emoji": ":dart:"},
    {"name": "Gaelic Games", "rule": SPORT_RULE_ALL, "emoji": ":flag-ie:"},
    {"name": "Mixed Martial Arts", "rule": SPORT_RULE_FIRST, "emoji": ":martial_arts_uniform:"},
    {"name": "Rugby League", "rule": SPORT_RULE_ALL, "emoji": ":rugby_football:"},
    {"name": "Rugby Union", "rule": SPORT_RULE_ALL, "emoji": ":rugby_football:"},
    {"name": "Snooker", "rule": SPORT_RULE_FIRST, "emoji": ":8ball:"},
)

# Fallback IDs are deliberately easy to edit. The script first asks Betfair for
# event types in the scan window and only falls back to this mapping when needed.
FALLBACK_EVENT_TYPE_IDS: dict[str, str] = {
    "American Football": "6423",
    "Boxing": "6",
    "Darts": "3503",
    "Gaelic Games": "2152880",
    "Mixed Martial Arts": "26420387",
    "Rugby League": "1477",
    "Rugby Union": "5",
    "Snooker": "6422",
}


@dataclass(frozen=True)
class Config:
    slack_bot_token: str
    slack_channel_id: str
    slack_channel_name: str
    fallback_webhook_url: str
    betfair_app_key: str
    betfair_username: str
    betfair_password: str
    certs_dir: str


@dataclass(frozen=True)
class ScanWindow:
    start_uk: datetime
    end_uk: datetime
    start_utc: datetime
    end_utc: datetime


@dataclass(frozen=True)
class EventReminder:
    sport: str
    emoji: str
    event_type_id: str
    event_id: str
    event_name: str
    competition_id: str
    competition_name: str
    market_id: str
    market_name: str
    event_start_utc: datetime

    @property
    def event_start_uk(self) -> datetime:
        return self.event_start_utc.astimezone(UK_TZ)


@dataclass
class RunStats:
    sports_scanned: int = 0
    raw_markets_found: int = 0
    unique_events_found: int = 0
    reminders_selected: int = 0
    scheduled_in_slack: int = 0
    skipped_duplicates: int = 0
    skipped_past_times: int = 0
    failures: int = 0


class ConfigMissing(RuntimeError):
    pass


class ConfigPlaceholderError(RuntimeError):
    pass


def log(message: str) -> None:
    logging.info(message)
    print(message, flush=True)


def object_get(obj: Any, name: str, default: Any = "") -> Any:
    if isinstance(obj, dict):
        return obj.get(name, default)
    return getattr(obj, name, default)


def parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC_TZ)
    return parsed.astimezone(UTC_TZ)


def format_uk(value: datetime) -> str:
    return value.astimezone(UK_TZ).strftime("%Y-%m-%d %H:%M:%S %Z")


def format_utc(value: datetime) -> str:
    return value.astimezone(UTC_TZ).strftime("%Y-%m-%d %H:%M:%S UTC")


def betfair_time(value: datetime) -> str:
    return value.astimezone(UTC_TZ).strftime("%Y-%m-%dT%H:%M:%SZ")


def is_placeholder(value: str, *, allow_empty: bool = False) -> bool:
    stripped = str(value or "").strip()
    if not stripped:
        return not allow_empty
    return any(marker in stripped for marker in PLACEHOLDER_MARKERS)


def build_scan_window(now_uk: datetime | None = None, lookahead_hours: float = 24, start_now: bool = False) -> ScanWindow:
    now = now_uk.astimezone(UK_TZ) if now_uk else datetime.now(UK_TZ)
    if start_now:
        start_uk = now
    else:
        start_uk = datetime.combine(now.date(), SCAN_START_TIME_UK, tzinfo=UK_TZ)
    end_uk = start_uk + timedelta(hours=lookahead_hours)
    return ScanWindow(start_uk, end_uk, start_uk.astimezone(UTC_TZ), end_uk.astimezone(UTC_TZ))


def reminder_time(event_start: datetime) -> datetime:
    return event_start.astimezone(UK_TZ) - timedelta(minutes=REMINDER_LEAD_MINUTES)


def duplicate_key(reminder: EventReminder, scheduled_post_epoch: int, slack_channel_id: str) -> str:
    return f"{reminder.sport}|{reminder.event_id}|{scheduled_post_epoch}|{slack_channel_id}"


def slack_bucket_epoch(post_epoch: int) -> int:
    return post_epoch - (post_epoch % SLACK_BUCKET_SECONDS)


def slack_bucket_warnings(post_epochs: Iterable[int], limit: int = SLACK_SCHEDULE_LIMIT_PER_BUCKET) -> list[str]:
    counts = Counter(slack_bucket_epoch(epoch) for epoch in post_epochs)
    warnings: list[str] = []
    for bucket, count in sorted(counts.items()):
        if count > limit:
            bucket_dt = datetime.fromtimestamp(bucket, UTC_TZ).astimezone(UK_TZ)
            warnings.append(
                f"WARNING Slack scheduled-message bucket limit would be exceeded: "
                f"{count}/{limit} for {format_uk(bucket_dt)}"
            )
    return warnings


def catalogue_to_reminder(catalogue: Any, sport: str, emoji: str, fallback_event_type_id: str) -> EventReminder | None:
    event = object_get(catalogue, "event", {})
    event_type = object_get(catalogue, "event_type", {})
    competition = object_get(catalogue, "competition", {})
    start_utc = parse_datetime(object_get(catalogue, "market_start_time", None))
    event_id = str(object_get(event, "id", "") or "").strip()
    event_name = str(object_get(event, "name", "") or object_get(catalogue, "market_name", "") or "").strip()
    if not event_id or start_utc is None:
        return None
    return EventReminder(
        sport=sport,
        emoji=emoji,
        event_type_id=str(object_get(event_type, "id", "") or fallback_event_type_id),
        event_id=event_id,
        event_name=event_name,
        competition_id=str(object_get(competition, "id", "") or ""),
        competition_name=str(object_get(competition, "name", "") or ""),
        market_id=str(object_get(catalogue, "market_id", "") or ""),
        market_name=str(object_get(catalogue, "market_name", "") or ""),
        event_start_utc=start_utc,
    )


def dedupe_events(markets: Iterable[EventReminder]) -> list[EventReminder]:
    by_event: dict[str, EventReminder] = {}
    for reminder in sorted(markets, key=lambda item: (item.event_start_utc, item.market_id)):
        if reminder.event_id not in by_event:
            by_event[reminder.event_id] = reminder
    return list(by_event.values())


def select_reminders(events: Iterable[EventReminder], rule: str) -> list[EventReminder]:
    sorted_events = sorted(events, key=lambda item: (item.event_start_utc, item.event_name.casefold(), item.event_id))
    if rule == SPORT_RULE_ALL:
        return sorted_events
    if rule == SPORT_RULE_FIRST:
        return sorted_events[:1]
    if rule == SPORT_RULE_DARTS_COMPETITION:
        first_by_competition: dict[str, EventReminder] = {}
        for event in sorted_events:
            competition_key = event.competition_id or event.competition_name or f"event:{event.event_id}"
            if competition_key not in first_by_competition:
                first_by_competition[competition_key] = event
        return list(first_by_competition.values())
    raise ValueError(f"Unknown selection rule: {rule}")


def resolve_config_path(cli_config_path: str = "") -> Path:
    if cli_config_path.strip():
        return Path(cli_config_path).expanduser()
    env_config_path = os.getenv(CONFIG_ENV_VAR, "").strip()
    if env_config_path:
        return Path(env_config_path).expanduser()
    return CONFIG_PATH


def read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def missing_config_message(path: Path) -> str:
    return "\n".join(
        [
            f"Missing real config file: {path}",
            "Create it on the EC2 server using config/betfair_event_reminders_config.example.json as the template.",
            "Rename it to betfair_event_reminders_config.json, fill in the EC2-only real values, then run --dry-run first.",
            "Do not commit the real config to Git.",
        ]
    )


def load_config(config_path: Path) -> Config:
    if not config_path.exists():
        raise ConfigMissing(
            missing_config_message(config_path)
        )
    data = read_json(config_path)
    if not isinstance(data, dict):
        raise RuntimeError(f"Config must be a JSON object: {config_path}")
    merged = {**PLACEHOLDER_CONFIG, **data}
    return Config(**{key: str(merged.get(key, "") or "") for key in PLACEHOLDER_CONFIG})


def placeholder_fields(config: Config) -> list[str]:
    missing: list[str] = []
    if is_placeholder(config.slack_bot_token):
        missing.append("slack_bot_token")
    if is_placeholder(config.slack_channel_id):
        missing.append("slack_channel_id")
    if is_placeholder(config.betfair_app_key):
        missing.append("betfair_app_key")
    if is_placeholder(config.betfair_username):
        missing.append("betfair_username")
    if is_placeholder(config.betfair_password):
        missing.append("betfair_password")
    return missing


def validate_config(config: Config, config_path: Path) -> None:
    missing = placeholder_fields(config)
    if missing:
        raise ConfigPlaceholderError(
            "\n".join(
                [
                    f"Config still contains placeholder values: {', '.join(missing)}",
                    f"Config path: {config_path}",
                    "Fill in the EC2-only real values, including the new Slack bot token and channel ID.",
                    "fallback_webhook_url is optional only and is not used for scheduled reminders.",
                    "No Betfair or Slack calls were made.",
                ]
            )
        )


def setup_logging() -> Path:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_path = LOG_DIR / f"betfair_event_reminders_{datetime.now(UK_TZ):%Y%m%d}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.FileHandler(log_path, encoding="utf-8")],
        force=True,
    )
    return log_path


def build_client(config: Config) -> APIClient:
    certs_dir = Path(config.certs_dir).resolve()
    cert_file = certs_dir / "client-2048.crt"
    key_file = certs_dir / "client-2048.key"
    log(f"Resolved Betfair cert path: {certs_dir}")
    if not cert_file.exists():
        raise FileNotFoundError(f"Betfair cert file not found: {cert_file}")
    if not key_file.exists():
        raise FileNotFoundError(f"Betfair key file not found: {key_file}")
    client = APIClient(
        username=config.betfair_username,
        password=config.betfair_password,
        app_key=config.betfair_app_key,
        certs=str(certs_dir),
    )
    client.login()
    log("Betfair login success")
    return client


def list_event_type_ids(client: APIClient, window: ScanWindow) -> dict[str, str]:
    event_filter = market_filter(
        market_start_time={"from": betfair_time(window.start_utc), "to": betfair_time(window.end_utc)}
    )
    results = client.betting.list_event_types(filter=event_filter)
    discovered: dict[str, str] = {}
    for result in results:
        event_type = object_get(result, "event_type", {})
        name = str(object_get(event_type, "name", "") or "").strip()
        event_type_id = str(object_get(event_type, "id", "") or "").strip()
        if name and event_type_id:
            discovered[name.casefold()] = event_type_id
    sport_ids: dict[str, str] = {}
    for sport in SPORTS:
        name = sport["name"]
        sport_ids[name] = discovered.get(name.casefold()) or FALLBACK_EVENT_TYPE_IDS[name]
    log(f"Sport event type mapping: {sport_ids}")
    return sport_ids


def list_market_catalogues(client: APIClient, event_type_id: str, window: ScanWindow) -> list[Any]:
    fixture_filter = market_filter(
        event_type_ids=[event_type_id],
        market_type_codes=[MATCH_ODDS],
        market_start_time={"from": betfair_time(window.start_utc), "to": betfair_time(window.end_utc)},
    )
    return client.betting.list_market_catalogue(
        filter=fixture_filter,
        market_projection=["EVENT", "EVENT_TYPE", "COMPETITION", "MARKET_START_TIME"],
        max_results=1000,
        sort="FIRST_TO_START",
    )


def load_state(path: Path = STATE_PATH) -> dict[str, Any]:
    if not path.exists():
        return {"scheduled": []}
    try:
        data = read_json(path)
    except json.JSONDecodeError:
        backup = path.with_name(f"{path.name}.{datetime.now(UK_TZ):%Y%m%d%H%M%S}.bak")
        shutil.copy2(path, backup)
        log(f"State JSON was corrupt; backed up to {backup}")
        return {"scheduled": []}
    if not isinstance(data, dict):
        return {"scheduled": []}
    scheduled = data.get("scheduled", [])
    return {"scheduled": scheduled if isinstance(scheduled, list) else []}


def save_state(state: dict[str, Any], path: Path = STATE_PATH) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(state, indent=2, sort_keys=True) + "\n"
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False, dir=str(path.parent), suffix=".tmp") as handle:
        handle.write(payload)
        temp_path = Path(handle.name)
    temp_path.replace(path)


def scheduled_keys(state: dict[str, Any]) -> set[str]:
    keys: set[str] = set()
    for record in state.get("scheduled", []):
        if isinstance(record, dict) and record.get("duplicate_key"):
            keys.add(str(record["duplicate_key"]))
    return keys


def format_slack_text(reminder: EventReminder) -> str:
    return f"{reminder.emoji} {reminder.event_name} (Event ID: {reminder.event_id})"


def schedule_slack_message(config: Config, reminder: EventReminder, post_epoch: int) -> str:
    response = requests.post(
        SLACK_SCHEDULE_URL,
        headers={"Authorization": f"Bearer {config.slack_bot_token}", "Content-Type": "application/json"},
        json={"channel": config.slack_channel_id, "text": format_slack_text(reminder), "post_at": post_epoch},
        timeout=20,
    )
    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError(f"Slack returned non-JSON response: status={response.status_code} body={response.text}") from exc
    if response.status_code >= 400 or not payload.get("ok"):
        error = str(payload.get("error", "unknown_error"))
        if error == "restricted_too_many":
            raise RuntimeError("Slack chat.scheduleMessage failed: restricted_too_many")
        raise RuntimeError(f"Slack chat.scheduleMessage failed: {error}")
    return str(payload.get("scheduled_message_id") or "")


def state_record(reminder: EventReminder, key: str, post_epoch: int, scheduled_message_id: str) -> dict[str, Any]:
    return {
        "duplicate_key": key,
        "sport": reminder.sport,
        "event_id": reminder.event_id,
        "event_name": reminder.event_name,
        "competition_id": reminder.competition_id,
        "competition_name": reminder.competition_name,
        "market_id": reminder.market_id,
        "market_name": reminder.market_name,
        "event_start_uk": format_uk(reminder.event_start_uk),
        "scheduled_slack_post_uk": format_uk(datetime.fromtimestamp(post_epoch, UTC_TZ)),
        "scheduled_slack_post_epoch": post_epoch,
        "slack_scheduled_message_id": scheduled_message_id,
        "created_at_uk": format_uk(datetime.now(UK_TZ)),
    }


def run_scan(args: argparse.Namespace, config: Config, config_path: Path) -> int:
    log_path = setup_logging()
    log("Betfair Event Reminders starting")
    log(f"Config path: {config_path}")
    log(f"Log path: {log_path}")
    log(f"Slack channel: {config.slack_channel_name} ({config.slack_channel_id})")
    log("Scheduling method: Slack Web API chat.scheduleMessage")
    if config.fallback_webhook_url:
        log("Fallback webhook configured but not used for scheduled reminders.")
    if args.dry_run:
        log("Dry run enabled: no Slack messages will be scheduled and no state records will be written.")

    validate_config(config, config_path)
    window = build_scan_window(lookahead_hours=args.lookahead_hours, start_now=args.start_now)
    log(f"UK scan window: {format_uk(window.start_uk)} -> {format_uk(window.end_uk)}")
    log(f"UTC scan window: {format_utc(window.start_utc)} -> {format_utc(window.end_utc)}")

    stats = RunStats()
    all_selected: list[EventReminder] = []
    client = build_client(config)
    try:
        sport_ids = list_event_type_ids(client, window)
        for sport in SPORTS:
            sport_name = sport["name"]
            stats.sports_scanned += 1
            event_type_id = sport_ids[sport_name]
            log(f"Scanning {sport_name} eventTypeId={event_type_id}")
            raw_catalogues = list_market_catalogues(client, event_type_id, window)
            stats.raw_markets_found += len(raw_catalogues)
            reminders = [
                reminder
                for catalogue in raw_catalogues
                if (reminder := catalogue_to_reminder(catalogue, sport_name, sport["emoji"], event_type_id)) is not None
            ]
            unique_events = dedupe_events(reminders)
            selected = select_reminders(unique_events, sport["rule"])
            stats.unique_events_found += len(unique_events)
            stats.reminders_selected += len(selected)
            all_selected.extend(selected)
            log(
                f"{sport_name}: raw markets={len(raw_catalogues)}, "
                f"unique events={len(unique_events)}, selected reminders={len(selected)}"
            )
    finally:
        try:
            client.logout()
        except Exception:
            pass

    post_epochs = [int(reminder_time(item.event_start_uk).timestamp()) for item in all_selected]
    for warning in slack_bucket_warnings(post_epochs):
        log(warning)

    state = load_state()
    existing_keys = scheduled_keys(state)
    if args.force:
        log("Force mode enabled: duplicate record checks will be ignored.")

    now_uk = datetime.now(UK_TZ)
    for reminder in all_selected:
        post_time_uk = reminder_time(reminder.event_start_uk)
        post_epoch = int(post_time_uk.timestamp())
        readable_remind = (
            f"/remind {config.slack_channel_name} {format_slack_text(reminder)} "
            f"at {post_time_uk.strftime('%H:%M')}"
        )
        log(f"Reminder candidate: {readable_remind}")
        if post_time_uk <= now_uk:
            stats.skipped_past_times += 1
            log(f"SKIP time in past: {reminder.sport} {reminder.event_name} post_at={format_uk(post_time_uk)}")
            continue
        key = duplicate_key(reminder, post_epoch, config.slack_channel_id)
        if not args.force and key in existing_keys:
            stats.skipped_duplicates += 1
            log(f"SKIP duplicate already scheduled: {key}")
            continue
        if args.dry_run:
            log(
                f"DRY RUN would schedule chat.scheduleMessage channel={config.slack_channel_id} "
                f"post_at={post_epoch} text={format_slack_text(reminder)!r}"
            )
            continue
        try:
            scheduled_message_id = schedule_slack_message(config, reminder, post_epoch)
        except Exception as exc:
            stats.failures += 1
            log(f"Slack scheduling failure for event {reminder.event_id}: {exc}")
            continue
        stats.scheduled_in_slack += 1
        log(f"Slack scheduling success: event={reminder.event_id} scheduled_message_id={scheduled_message_id}")
        record = state_record(reminder, key, post_epoch, scheduled_message_id)
        state.setdefault("scheduled", []).append(record)
        existing_keys.add(key)
        save_state(state)

    print_summary(window, stats)
    return 1 if stats.failures else 0


def print_summary(window: ScanWindow, stats: RunStats) -> None:
    lines = [
        "",
        "Betfair Event Reminders Summary",
        f"Scan window UK: {window.start_uk.strftime('%Y-%m-%d %H:%M')} -> {window.end_uk.strftime('%Y-%m-%d %H:%M')}",
        f"Sports scanned: {stats.sports_scanned}",
        f"Raw markets found: {stats.raw_markets_found}",
        f"Unique events found: {stats.unique_events_found}",
        f"Reminders selected: {stats.reminders_selected}",
        f"Scheduled in Slack: {stats.scheduled_in_slack}",
        f"Skipped duplicates: {stats.skipped_duplicates}",
        f"Skipped past times: {stats.skipped_past_times}",
        f"Failures: {stats.failures}",
    ]
    for line in lines:
        log(line)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Schedule Slack reminders for selected Betfair Exchange events.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Production config guidance:\n"
            "  Copy config/betfair_event_reminders_config.example.json on EC2.\n"
            "  Rename it to betfair_event_reminders_config.json and fill in EC2-only real values.\n"
            "  Keep the real config out of Git, then run --dry-run first.\n"
            f"  Linux override: set {CONFIG_ENV_VAR} or pass --config /opt/betfair-scripts/config/betfair_event_reminders_config.json."
        ),
    )
    parser.add_argument("--config", default="", help="Path to the real EC2/local JSON config file.")
    parser.add_argument("--dry-run", action="store_true", help="Scan and print what would be scheduled without Slack/state writes.")
    parser.add_argument("--pause-on-exit", action="store_true", help="Wait for Enter before closing the console.")
    parser.add_argument("--lookahead-hours", type=float, default=24)
    parser.add_argument("--start-now", action="store_true", help="Use current UK time as scan start.")
    parser.add_argument("--force", action="store_true", help="Ignore duplicate record checks.")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging.")
    return parser.parse_args()


def pause_before_exit() -> None:
    print("Betfair Event Reminders finished. Press Enter to close...", flush=True)
    try:
        input()
    except EOFError:
        pass


def main() -> int:
    args = parse_args()
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    try:
        config_path = resolve_config_path(args.config)
        config = load_config(config_path)
        return run_scan(args, config, config_path)
    except (ConfigMissing, ConfigPlaceholderError) as exc:
        print(str(exc), flush=True)
        return 2


if __name__ == "__main__":
    exit_code = 0
    try:
        exit_code = main()
    except KeyboardInterrupt:
        print("Interrupted.", flush=True)
        exit_code = 130
    except Exception as exc:
        print(f"ERROR: {exc}", flush=True)
        traceback.print_exc()
        exit_code = 1
    finally:
        if "--pause-on-exit" in sys.argv:
            pause_before_exit()
    raise SystemExit(exit_code)
