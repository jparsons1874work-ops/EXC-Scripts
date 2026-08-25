#!/usr/bin/env python3
"""Monitor official golf field pages and report confirmed changes to Slack."""

from __future__ import annotations

import argparse
from contextlib import contextmanager
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import time
import unicodedata
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterator, Optional
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

import requests

if TYPE_CHECKING:
    from playwright.sync_api import Browser, BrowserContext, Page


try:
    sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", line_buffering=True)
except AttributeError:
    pass


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
CONFIG_PATH = Path(
    os.getenv(
        "GOLF_FIELD_CONFIG_PATH",
        str(PROJECT_ROOT / "runtime" / "config" / "golf_field_checker.json"),
    )
)
STATE_DIR = Path(
    os.getenv(
        "GOLF_FIELD_STATE_DIR",
        str(PROJECT_ROOT / "runtime" / "output" / "golf_field_checker"),
    )
)

DEFAULT_REPEAT_MINUTES = 5.0
SUSPICIOUS_SHORT_READ_THRESHOLD = 0.6
PLACEHOLDER_MARKERS = ("example", "replace", "with/yours")
UK_TIMEZONE = ZoneInfo("Europe/London")
SCANNER_HEARTBEAT_HOURS = (7, 23)
SCANNER_HEARTBEAT_WINDOW_MINUTES = 20
GOLF_BROWSER_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36"
)

GOLF_NR_SLACK_WEBHOOK_URL = os.getenv("GOLF_NR_SLACK_WEBHOOK_URL", "").strip()
GOLF_NR_SLACK_BOT_TOKEN = os.getenv("GOLF_NR_SLACK_BOT_TOKEN", "").strip()
GOLF_NR_SLACK_CHANNEL = os.getenv("GOLF_NR_SLACK_CHANNEL", "").strip()
LEGACY_SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "").strip()
LEGACY_SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN", "").strip()
LEGACY_SLACK_CHANNEL = os.getenv("SLACK_CHANNEL", "").strip()


SITE_DEFINITIONS: dict[str, dict[str, Any]] = {
    "pgatour": {
        "label": "PGA Tour",
        "allowed_hosts": {"pgatour.com", "www.pgatour.com"},
        "link_pattern": r"/player/",
        "link_selector": 'main a[href*="/player/"]',
        "name_text_mode": "full",
        "boundary_pattern": r"^alternates$",
        "min_field_before_boundary_check": 20,
        "required_confirm_streak": 2,
        "allow_auto_giveup": True,
    },
    "pgachampions": {
        "label": "PGA Tour Champions",
        "allowed_hosts": {"pgatour.com", "www.pgatour.com"},
        "link_pattern": r"/player/",
        "link_selector": 'main a[href*="/player/"]',
        "name_text_mode": "full",
        "boundary_pattern": r"^alternates$",
        "min_field_before_boundary_check": 20,
        "required_confirm_streak": 2,
        "allow_auto_giveup": True,
    },
    "dpworld": {
        "label": "DP World Tour",
        "allowed_hosts": {"europeantour.com", "www.europeantour.com"},
        "link_pattern": r"/players/",
        "link_selector": 'main table a[href*="/players/"]',
        "name_text_mode": "first-line",
        "boundary_pattern": r"cut.?off",
        "min_field_before_boundary_check": 20,
        "required_confirm_streak": 2,
        "allow_auto_giveup": True,
    },
    "lpga": {
        "label": "LPGA",
        "allowed_hosts": {"lpga.com", "www.lpga.com"},
        "link_pattern": r"/athletes/",
        "link_selector": 'a[href*="/athletes/"]',
        "ready_selector": "text=Entered",
        "name_text_mode": "full",
        "boundary_pattern": None,
        "min_field_before_boundary_check": None,
        "membership_ancestor_pattern": r"\b(?:entered|reserve\s*#\d+)\b",
        "reserve_ancestor_pattern": r"\breserve\s*#\d+\b",
        "required_confirm_streak": 4,
        "allow_auto_giveup": False,
        # Rebuild the old all-players baseline silently when this reader lands.
        "reader_revision": "lpga-explicit-reserves-v1",
    },
}

NAME_LOOKS_ABBREVIATED = re.compile(r"^\w\.\s")


class ConfigError(RuntimeError):
    """Raised when the weekly site configuration is missing or invalid."""


class ScannerStop(RuntimeError):
    """Raised by a termination signal so the offline alert can be attempted."""


@dataclass
class DiffResult:
    added: list[dict[str, Any]] = field(default_factory=list)
    removed: list[str] = field(default_factory=list)
    status_lines: list[str] = field(default_factory=list)
    slack_needed: bool = False
    proposed_state: Optional[dict[str, Any]] = None


def log(message: str) -> None:
    print(message, flush=True)


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_config(path: Path = CONFIG_PATH) -> dict[str, Any]:
    if not path.exists():
        raise ConfigError(
            f"Golf field configuration is missing at {path}. Save the weekly URLs on the Golf page in the hub."
        )
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ConfigError(f"Could not read golf field configuration at {path}: {exc}") from exc
    if not isinstance(data, dict) or not isinstance(data.get("sites"), list):
        raise ConfigError(f"Golf field configuration at {path} must contain a 'sites' list.")
    return data


def validate_site_url(tour_id: str, url: str) -> Optional[str]:
    site_def = SITE_DEFINITIONS[tour_id]
    normalized = url.strip()
    if not normalized:
        return "URL is empty"
    if any(marker in normalized.lower() for marker in PLACEHOLDER_MARKERS):
        return "URL is still a placeholder"
    parsed = urlparse(normalized)
    if parsed.scheme != "https" or (parsed.hostname or "").lower() not in site_def["allowed_hosts"]:
        hosts = ", ".join(sorted(site_def["allowed_hosts"]))
        return f"URL must be HTTPS and use {hosts}"
    return None


def configured_sites(config: dict[str, Any]) -> list[tuple[str, dict[str, Any], str, str]]:
    enabled: list[tuple[str, dict[str, Any], str, str]] = []
    errors: list[str] = []
    seen_ids: set[str] = set()

    for site_cfg in config.get("sites", []):
        if not isinstance(site_cfg, dict) or not site_cfg.get("enabled"):
            continue
        tour_id = str(site_cfg.get("id", "")).strip()
        if tour_id not in SITE_DEFINITIONS:
            errors.append(f"unknown enabled site id '{tour_id}'")
            continue
        if tour_id in seen_ids:
            errors.append(f"duplicate enabled site id '{tour_id}'")
            continue
        seen_ids.add(tour_id)
        url = str(site_cfg.get("url", "")).strip()
        url_error = validate_site_url(tour_id, url)
        if url_error:
            errors.append(f"{SITE_DEFINITIONS[tour_id]['label']}: {url_error}")
            continue
        enabled.append(
            (
                tour_id,
                SITE_DEFINITIONS[tour_id],
                url,
                str(site_cfg.get("url_saved_at", "") or ""),
            )
        )

    if errors:
        raise ConfigError("Invalid golf field configuration: " + "; ".join(errors))
    if not enabled:
        raise ConfigError("No golf tours are enabled with valid URLs. Save the weekly URLs on the Golf page in the hub.")
    return enabled


def state_path(tour_id: str) -> Path:
    return STATE_DIR / f"{tour_id}.json"


def load_state(tour_id: str) -> dict[str, Any]:
    path = state_path(tour_id)
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        log(f"WARNING: could not read {site_path_for_log(path)}; starting with a fresh baseline ({exc}).")
        return {}
    return data if isinstance(data, dict) else {}


def save_state(tour_id: str, state: dict[str, Any]) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    path = state_path(tour_id)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")
    temporary.replace(path)


def site_path_for_log(path: Path) -> str:
    try:
        return str(path.relative_to(PROJECT_ROOT))
    except ValueError:
        return str(path)


def format_name(raw: str) -> str:
    collapsed = re.sub(r"\s+", " ", raw).strip()

    def title_word(match: re.Match[str]) -> str:
        word = match.group(0)
        return word[0].upper() + word[1:].lower()

    return re.sub(r"\w\S*", title_word, collapsed)


def is_valid_name(name: str) -> bool:
    if not (1 < len(name) < 40):
        return False
    if not any(unicodedata.category(character).startswith("L") for character in name):
        return False
    if any(character.isdigit() for character in name):
        return False
    return not NAME_LOOKS_ABBREVIATED.match(name)


def _available_xvfb_display() -> str:
    for number in range(90, 190):
        if not Path(f"/tmp/.X11-unix/X{number}").exists():
            return f":{number}"
    return f":{200 + (os.getpid() % 700)}"


@contextmanager
def browser_display() -> Iterator[bool]:
    """Provide a real display on Linux because official sites block headless Chromium."""
    if not sys.platform.startswith("linux"):
        yield True
        return

    existing_display = os.getenv("DISPLAY", "").strip()
    if existing_display:
        log(f"Golf browser using existing display {existing_display}.")
        yield False
        return

    xvfb = shutil.which("Xvfb")
    if not xvfb:
        log(
            "WARNING: Xvfb is not installed; using headless Chromium. "
            "PGA Tour or DP World Tour may return Access Denied. Install the Ubuntu xvfb package."
        )
        yield True
        return

    display = _available_xvfb_display()
    process = subprocess.Popen(
        [xvfb, display, "-screen", "0", "1365x900x24", "-nolisten", "tcp"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    socket_path = Path(f"/tmp/.X11-unix/X{display.lstrip(':')}")
    try:
        for _ in range(50):
            if process.poll() is not None:
                raise RuntimeError("Xvfb stopped before its display became ready")
            if socket_path.exists():
                break
            time.sleep(0.1)
        else:
            raise RuntimeError("Xvfb did not become ready within five seconds")
        os.environ["DISPLAY"] = display
        log(f"Golf browser virtual display ready on {display}.")
        yield False
    finally:
        os.environ.pop("DISPLAY", None)
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=3)


def read_field(page: Page, site_def: dict[str, Any], timeout_s: float = 60.0) -> dict[str, list[str]]:
    """Scroll a JavaScript-rendered page and accumulate field/reserve names."""
    link_pattern = re.compile(site_def["link_pattern"])
    boundary_pattern = (
        re.compile(site_def["boundary_pattern"], re.IGNORECASE)
        if site_def["boundary_pattern"]
        else None
    )
    min_before_boundary = site_def["min_field_before_boundary_check"] or 0
    link_selector = site_def.get("link_selector") or f'a[href*="{site_def["link_pattern"]}"]'
    name_text_mode = site_def.get("name_text_mode", "full")
    membership_ancestor_pattern = site_def.get("membership_ancestor_pattern") or ""
    reserve_ancestor_pattern = site_def.get("reserve_ancestor_pattern") or ""
    seen: set[str] = set()
    field_names: list[str] = []
    alternate_names: list[str] = []
    past_boundary = False

    def scan_current_view() -> None:
        nonlocal past_boundary
        items = page.evaluate(
            """
            ({linkSelector, nameTextMode, membershipAncestorPattern, reserveAncestorPattern}) => {
              const items = [];
              const membershipRegex = membershipAncestorPattern
                ? new RegExp(membershipAncestorPattern, 'i')
                : null;
              const reserveRegex = reserveAncestorPattern
                ? new RegExp(reserveAncestorPattern, 'i')
                : null;
              const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_ELEMENT);
              let element = walker.currentNode;
              while (element) {
                if (element.matches && element.matches(linkSelector)) {
                  const rawText = element.innerText || element.textContent || '';
                  let ancestor = element;
                  let classificationText = '';
                  for (let depth = 0; ancestor && depth < 8; depth += 1) {
                    if (ancestor.tagName === 'BODY' || ancestor.tagName === 'HTML') {
                      break;
                    }
                    const candidateText = ancestor.innerText || '';
                    if (
                      membershipRegex
                      && candidateText.length < 300
                      && membershipRegex.test(candidateText)
                    ) {
                      classificationText = candidateText;
                      break;
                    }
                    ancestor = ancestor.parentElement;
                  }
                  if (!membershipRegex || classificationText) {
                    items.push({
                      type: 'link',
                      href: element.getAttribute('href') || '',
                      text: nameTextMode === 'first-line'
                        ? rawText.split(/\\r?\\n/)[0]
                        : rawText,
                      isReserve: reserveRegex ? reserveRegex.test(classificationText) : false
                    });
                  }
                } else {
                  const directText = Array.from(element.childNodes)
                    .filter(node => node.nodeType === Node.TEXT_NODE)
                    .map(node => node.textContent || '')
                    .join(' ')
                    .trim();
                  if (directText.length > 0 && directText.length < 60) {
                    items.push({type: 'text', text: directText});
                  }
                }
                element = walker.nextNode();
              }
              return items;
            }
            """,
            {
                "linkSelector": link_selector,
                "nameTextMode": name_text_mode,
                "membershipAncestorPattern": membership_ancestor_pattern,
                "reserveAncestorPattern": reserve_ancestor_pattern,
            },
        )

        for item in items:
            if item["type"] == "text":
                if (
                    not past_boundary
                    and boundary_pattern
                    and len(field_names) >= min_before_boundary
                    and boundary_pattern.search(item["text"])
                ):
                    past_boundary = True
                continue
            if not link_pattern.search(item["href"]):
                continue
            name = format_name(item["text"])
            if is_valid_name(name) and name not in seen:
                seen.add(name)
                (alternate_names if past_boundary or item.get("isReserve") else field_names).append(name)

    started = time.monotonic()
    no_growth_streak = 0
    last_seen_count = -1

    while True:
        scan_current_view()
        current_count = len(seen)
        if current_count == last_seen_count:
            no_growth_streak += 1
        else:
            no_growth_streak = 0
            last_seen_count = current_count

        if no_growth_streak >= 15 or time.monotonic() - started > timeout_s:
            break

        page.evaluate(
            """
            () => {
              window.scrollTo(0, document.body.scrollHeight);
              window.dispatchEvent(new WheelEvent('wheel', {deltaY: 800, bubbles: true}));
            }
            """
        )
        page.wait_for_timeout(700)

    return {"field": field_names, "alternates": alternate_names}


def evaluate_reading(
    tour_id: str,
    site_def: dict[str, Any],
    url: str,
    live_field: list[str],
    live_alternates: list[str],
    config_saved_at: str = "",
) -> DiffResult:
    result = DiffResult()
    state = load_state(tour_id)
    previous_field = state.get("confirmed_field")
    previous_alternates = state.get("confirmed_alternates", [])
    previous_url = state.get("baseline_url")
    previous_config_saved_at = str(state.get("config_saved_at", "") or "")
    reader_revision = str(site_def.get("reader_revision", "") or "")
    previous_reader_revision = str(state.get("reader_revision", "") or "")
    reject_streak = int(state.get("reject_streak", 0) or 0)
    miss_streaks = dict(state.get("miss_streaks", {}) or {})
    seen_streaks = dict(state.get("seen_streaks", {}) or {})
    promotion_candidates = set(state.get("promotion_candidates", []) or [])

    if not live_field:
        result.status_lines.append(
            f"{site_def['label']}: found 0 players; the page may not have loaded or its layout may have changed."
        )
        return result

    url_changed = bool(previous_url and previous_url != url)
    configuration_changed = bool(config_saved_at and config_saved_at != previous_config_saved_at)
    if url_changed or configuration_changed:
        result.status_lines.append(
            f"{site_def['label']}: new tournament configuration detected; resetting the baseline quietly."
        )
        previous_field = None
        previous_alternates = []
        miss_streaks = {}
        seen_streaks = {}
        promotion_candidates = set()
        reject_streak = 0
        state["change_history"] = []
        state["tracking_started_at"] = utc_timestamp()
    elif previous_field is not None and reader_revision and reader_revision != previous_reader_revision:
        result.status_lines.append(
            f"{site_def['label']}: field/reserve reader updated; resetting the baseline quietly."
        )
        previous_field = None
        previous_alternates = []
        miss_streaks = {}
        seen_streaks = {}
        promotion_candidates = set()
        reject_streak = 0

    if (
        previous_field
        and len(previous_field) >= 10
        and len(live_field) < len(previous_field) * SUSPICIOUS_SHORT_READ_THRESHOLD
    ):
        allow_giveup = bool(site_def["allow_auto_giveup"])
        if allow_giveup and reject_streak + 1 >= 6:
            result.status_lines.append(
                f"{site_def['label']}: accepted a short list ({len(live_field)} vs {len(previous_field)}) "
                "as a fresh silent baseline after six consecutive short reads."
            )
            previous_field = None
            reject_streak = 0
        else:
            reject_streak += 1
            streak_label = f"{reject_streak}/6" if allow_giveup else f"{reject_streak}; auto-reset disabled"
            result.status_lines.append(
                f"{site_def['label']}: suspiciously short list ({len(live_field)} vs {len(previous_field)}); "
                f"skipping this read ({streak_label})."
            )
            state.update(
                {
                    "reject_streak": reject_streak,
                    "miss_streaks": miss_streaks,
                    "seen_streaks": seen_streaks,
                    "promotion_candidates": sorted(promotion_candidates),
                    "baseline_url": url,
                    "config_saved_at": config_saved_at,
                    "reader_revision": reader_revision,
                }
            )
            save_state(tour_id, state)
            return result
    else:
        reject_streak = 0

    if previous_field is None:
        result.status_lines.append(
            f"{site_def['label']}: stored a silent baseline of {len(live_field)} field players and "
            f"{len(live_alternates)} reserve-list players."
        )
        new_confirmed_field = list(live_field)
        miss_streaks = {}
        seen_streaks = {}
        promotion_candidates = set()
        state.setdefault("tracking_started_at", utc_timestamp())
    else:
        confirmed_set = set(previous_field)
        current_set = set(live_field)
        previous_alternate_set = set(previous_alternates)
        added_raw = [name for name in live_field if name not in confirmed_set]
        missing_raw = [name for name in previous_field if name not in current_set]
        required_streak = int(site_def["required_confirm_streak"])

        for name in missing_raw:
            miss_streaks[name] = int(miss_streaks.get(name, 0)) + 1
        for name in live_field:
            miss_streaks.pop(name, None)
        for name in added_raw:
            seen_streaks[name] = int(seen_streaks.get(name, 0)) + 1
            if name in previous_alternate_set:
                promotion_candidates.add(name)
        for name in list(seen_streaks):
            if name not in added_raw:
                seen_streaks.pop(name, None)
                promotion_candidates.discard(name)

        confirmed_removed = [
            name for name in missing_raw if miss_streaks.get(name, 0) >= required_streak
        ]
        confirmed_added = [
            name for name in added_raw if seen_streaks.get(name, 0) >= required_streak
        ]
        for name in confirmed_removed:
            miss_streaks.pop(name, None)
        for name in confirmed_added:
            seen_streaks.pop(name, None)

        if confirmed_added or confirmed_removed:
            result.added = [
                {"name": name, "promoted": name in promotion_candidates}
                for name in confirmed_added
            ]
            result.removed = confirmed_removed
            result.slack_needed = True
            result.status_lines.append(
                f"{site_def['label']}: FIELD CHANGE; {len(confirmed_added)} added, "
                f"{len(confirmed_removed)} withdrawn."
            )
        elif missing_raw or added_raw:
            result.status_lines.append(
                f"{site_def['label']}: no confirmed changes ({len(missing_raw)} missing, "
                f"{len(added_raw)} newly seen; requires {required_streak} consecutive checks)."
            )
        else:
            result.status_lines.append(
                f"{site_def['label']}: no field changes ({len(live_field)} field, "
                f"{len(live_alternates)} reserve)."
            )
        new_confirmed_field = [
            name for name in previous_field if name not in confirmed_removed
        ] + confirmed_added
        for name in confirmed_added:
            promotion_candidates.discard(name)

    state.update(
        {
            "confirmed_field": new_confirmed_field,
            "confirmed_alternates": list(live_alternates),
            "baseline_url": url,
            "config_saved_at": config_saved_at,
            "reader_revision": reader_revision,
            "reject_streak": reject_streak,
            "miss_streaks": miss_streaks,
            "seen_streaks": seen_streaks,
            "promotion_candidates": sorted(promotion_candidates),
            "last_checked_at": utc_timestamp(),
        }
    )
    result.proposed_state = state
    return result


def append_change_history(result: DiffResult) -> None:
    if result.proposed_state is None or not result.slack_needed:
        return
    timestamp = utc_timestamp()
    history = list(result.proposed_state.get("change_history", []) or [])
    for item in result.added:
        history.append(
            {
                "timestamp": timestamp,
                "change": "Addition",
                "player": item["name"],
                "note": "Promoted from reserve list" if item.get("promoted") else "",
            }
        )
    for player in result.removed:
        history.append(
            {
                "timestamp": timestamp,
                "change": "Withdrawal",
                "player": player,
                "note": "",
            }
        )
    result.proposed_state["change_history"] = history


def process_tour(
    tour_id: str,
    site_def: dict[str, Any],
    url: str,
    config_saved_at: str,
    browser_context: BrowserContext,
) -> DiffResult:
    page = browser_context.new_page()
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=45_000)
        try:
            page.wait_for_selector(
                site_def.get("ready_selector") or site_def["link_selector"],
                state="attached",
                timeout=45_000,
            )
        except Exception:
            # The zero-player safeguard below owns the failure. Keep the page
            # title/final URL available so the log identifies denial pages.
            pass
        page.wait_for_timeout(1_000)
        page_title = page.title()
        final_url = page.url
        reading = read_field(page, site_def)
    finally:
        page.close()
    result = evaluate_reading(
        tour_id,
        site_def,
        url,
        reading["field"],
        reading["alternates"],
        config_saved_at,
    )
    if not reading["field"]:
        result.status_lines.append(
            f"{site_def['label']}: loaded page title {page_title!r}; final URL {final_url}"
        )
    return result


def new_browser_context(browser: Browser) -> BrowserContext:
    context = browser.new_context(
        viewport={"width": 1365, "height": 900},
        user_agent=GOLF_BROWSER_USER_AGENT,
        locale="en-GB",
        timezone_id="Europe/London",
    )
    context.add_init_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
    )
    return context


def slack_configuration(config: dict[str, Any]) -> tuple[str, str, str]:
    webhook = (
        GOLF_NR_SLACK_WEBHOOK_URL
        or str(config.get("slack_webhook_url", "")).strip()
        or LEGACY_SLACK_WEBHOOK_URL
    )
    token = GOLF_NR_SLACK_BOT_TOKEN or LEGACY_SLACK_BOT_TOKEN
    channel = GOLF_NR_SLACK_CHANNEL or LEGACY_SLACK_CHANNEL
    return webhook, token, channel


def slack_config_message(config: dict[str, Any]) -> str:
    webhook, token, channel = slack_configuration(config)
    if webhook:
        return "Golf Slack destination: webhook configured."
    if token and channel:
        return "Golf Slack destination: bot token and channel configured."
    return "Golf Slack destination: NOT CONFIGURED; field changes cannot be delivered."


def slack_message(label: str, added: list[dict[str, Any]], removed: list[str]) -> str:
    text = f"*{label} - Field Update*\n"
    if added:
        lines = [
            f"• {item['name']}" + (" _(promoted from reserve list)_" if item["promoted"] else "")
            for item in added
        ]
        text += "\n➕ *Added*\n" + "\n".join(lines) + "\n"
    if removed:
        text += "\n➖ *Withdrawn*\n" + "\n".join(f"• {name}" for name in removed) + "\n"
    return text


def send_slack_text(config: dict[str, Any], text: str, timeout: float = 10) -> Optional[str]:
    webhook, token, channel = slack_configuration(config)
    try:
        if webhook:
            response = requests.post(webhook, json={"text": text}, timeout=timeout)
            if response.status_code != 200:
                return f"Slack webhook returned status {response.status_code}."
            return None
        if token and channel:
            response = requests.post(
                "https://slack.com/api/chat.postMessage",
                headers={"Authorization": f"Bearer {token}"},
                json={"channel": channel, "text": text},
                timeout=timeout,
            )
            payload = response.json() if response.content else {}
            if response.status_code != 200 or not payload.get("ok"):
                return f"Slack API rejected the message ({payload.get('error') or response.status_code})."
            return None
    except Exception as exc:
        return f"Slack post failed: {exc}"
    return "No Golf Slack webhook or bot token/channel is configured."


def post_to_slack(
    config: dict[str, Any], label: str, added: list[dict[str, Any]], removed: list[str]
) -> Optional[str]:
    return send_slack_text(config, slack_message(label, added, removed))


def scanner_status_message(active: bool, sites: list[tuple[str, dict[str, Any], str, str]]) -> str:
    if active:
        lines = [
            "🟢 *Golf official field scanner active*",
            "Checking every 5 minutes. Scheduled heartbeat: 07:00 and 23:00 UK.",
            "Configured competitions:",
        ]
        lines.extend(f"• *{site_def['label']}:* <{url}|official field page>" for _, site_def, url, _ in sites)
    else:
        lines = ["🔴 *Golf official field scanner offline*", "The continuous official-field checks have stopped."]
        if sites:
            lines.append("Configured competitions at shutdown:")
            lines.extend(f"• *{site_def['label']}:* <{url}|official field page>" for _, site_def, url, _ in sites)
    return "\n".join(lines)


def announce_scanner_status(active: bool, config: dict[str, Any], sites: list[tuple[str, dict[str, Any], str, str]]) -> bool:
    error = send_slack_text(config, scanner_status_message(active, sites), timeout=5)
    if error:
        log(f"ERROR sending Golf scanner {'active' if active else 'offline'} message: {error}")
        return False
    log(f"Golf scanner {'active' if active else 'offline'} message sent to Slack.")
    return True


def configured_status_message() -> tuple[dict[str, Any], list[tuple[str, dict[str, Any], str, str]]] | None:
    try:
        config = load_config()
        return config, configured_sites(config)
    except ConfigError as exc:
        log(f"Scanner active Slack message pending valid weekly URLs: {exc}")
        return None


def scheduled_heartbeat_slot(now_utc: Optional[datetime] = None) -> Optional[str]:
    """Return the current UK heartbeat slot during its retry window."""
    current_utc = now_utc or datetime.now(timezone.utc)
    current_uk = current_utc.astimezone(UK_TIMEZONE)
    for hour in SCANNER_HEARTBEAT_HOURS:
        scheduled = current_uk.replace(hour=hour, minute=0, second=0, microsecond=0)
        elapsed_seconds = (current_uk - scheduled).total_seconds()
        if 0 <= elapsed_seconds < SCANNER_HEARTBEAT_WINDOW_MINUTES * 60:
            return scheduled.isoformat()
    return None


def _raise_scanner_stop(signum, _frame) -> None:
    raise ScannerStop(f"received signal {signum}")


def run_cycle() -> int:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        log("ERROR: Playwright is not installed. Install requirements.txt and Playwright Chromium.")
        return 1

    try:
        config = load_config()
        sites = configured_sites(config)
    except ConfigError as exc:
        log(f"CONFIG ERROR: {exc}")
        return 1

    log(slack_config_message(config))
    exit_code = 0
    with browser_display() as use_headless, sync_playwright() as playwright:
        browser = playwright.chromium.launch(
            headless=use_headless,
            args=["--disable-blink-features=AutomationControlled"],
        )
        context = new_browser_context(browser)
        lpga_browser = None
        lpga_context = None
        try:
            for tour_id, site_def, url, config_saved_at in sites:
                log(f"--- Checking {site_def['label']} ---")
                try:
                    tour_context = context
                    if tour_id == "lpga":
                        if lpga_context is None:
                            lpga_browser = playwright.chromium.launch(
                                headless=True,
                                args=["--disable-blink-features=AutomationControlled"],
                            )
                            lpga_context = new_browser_context(lpga_browser)
                        tour_context = lpga_context
                    result = process_tour(tour_id, site_def, url, config_saved_at, tour_context)
                except Exception as exc:
                    log(f"ERROR checking {site_def['label']}: {exc}")
                    exit_code = 1
                    continue

                for line in result.status_lines:
                    log(line)

                if result.slack_needed:
                    error = post_to_slack(config, site_def["label"], result.added, result.removed)
                    if error:
                        log(f"ERROR posting to Slack for {site_def['label']}: {error}")
                        log("The confirmed change was not committed to state and will be retried next cycle.")
                        exit_code = 1
                        continue
                    log(f"{site_def['label']}: Slack field update sent.")
                    append_change_history(result)

                if result.proposed_state is not None:
                    save_state(tour_id, result.proposed_state)
        finally:
            if lpga_context is not None:
                lpga_context.close()
            if lpga_browser is not None:
                lpga_browser.close()
            context.close()
            browser.close()
    return exit_code


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Monitor official golf field pages for confirmed changes.")
    parser.add_argument("--once", action="store_true", help="Run one check cycle and exit.")
    parser.add_argument(
        "--repeat-minutes",
        type=float,
        default=DEFAULT_REPEAT_MINUTES,
        help="Minutes between cycles in continuous mode (default: 5).",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.repeat_minutes <= 0:
        log("ERROR: --repeat-minutes must be greater than zero.")
        return 2

    log("Starting official Golf field checker.")
    if args.once:
        return run_cycle()

    previous_sigterm = signal.getsignal(signal.SIGTERM)
    signal.signal(signal.SIGTERM, _raise_scanner_stop)
    active_announced = False
    last_heartbeat_slot = ""
    status_config: dict[str, Any] = {}
    status_sites: list[tuple[str, dict[str, Any], str, str]] = []
    try:
        while True:
            if not active_announced:
                status = configured_status_message()
                if status is not None:
                    status_config, status_sites = status
                    active_announced = announce_scanner_status(True, status_config, status_sites)
                    if active_announced:
                        last_heartbeat_slot = scheduled_heartbeat_slot() or last_heartbeat_slot
            try:
                run_cycle()
            except Exception as exc:
                log(f"ERROR: Golf check cycle failed before all tours could be checked: {exc}")
            heartbeat_slot = scheduled_heartbeat_slot()
            if active_announced and heartbeat_slot and heartbeat_slot != last_heartbeat_slot:
                status = configured_status_message()
                if status is not None:
                    heartbeat_config, heartbeat_sites = status
                    if announce_scanner_status(True, heartbeat_config, heartbeat_sites):
                        status_config, status_sites = heartbeat_config, heartbeat_sites
                        last_heartbeat_slot = heartbeat_slot
            delay_seconds = args.repeat_minutes * 60
            log(f"Next Golf field check in {args.repeat_minutes:g} minutes.")
            time.sleep(delay_seconds)
    except (KeyboardInterrupt, ScannerStop) as exc:
        log(f"Golf field checker stopping ({exc}).")
        return 0
    finally:
        if active_announced:
            announce_scanner_status(False, status_config, status_sites)
        signal.signal(signal.SIGTERM, previous_sigterm)


if __name__ == "__main__":
    raise SystemExit(main())
