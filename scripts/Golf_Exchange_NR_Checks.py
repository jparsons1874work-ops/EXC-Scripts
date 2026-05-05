import os
import sys
import time
import re
import csv
import json
import hashlib
import unicodedata
import difflib
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Dict, Tuple, Optional, List, Set

import truststore
truststore.inject_into_ssl()

import requests
import betfairlightweight
from betfairlightweight.filters import market_filter as bf_market_filter

# -*- coding: utf-8 -*-
import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# ==============================
# CONFIG
# ==============================
# NOTE: For safety, prefer setting these as environment variables rather than hardcoding.
BF_USERNAME = os.getenv("BETFAIR_USERNAME") or os.getenv("BF_USERNAME", "")
BF_PASSWORD = os.getenv("BETFAIR_PASSWORD") or os.getenv("BF_PASSWORD", "")
BF_APP_KEY = os.getenv("BETFAIR_APP_KEY") or os.getenv("BF_APP_KEY", "")
BF_CERTS_DIR = (
    os.getenv("BETFAIR_CERTS_DIR")
    or os.getenv("BF_CERTS_DIR")
    or str(Path(os.getenv("BETFAIR_CERT_FILE", "")).parent if os.getenv("BETFAIR_CERT_FILE") else "")
)

# Betfair Golf = eventTypeId "3"
EVENT_TYPE_ID_GOLF = "3"
# Market type codes that represent golf outrights
MARKET_TYPE_CODES = ["OUTRIGHT", "WIN", "WINNER", "TOURNAMENT_WINNER", "OUTRIGHT_WINNER"]

# DataGolf
DG_API_KEY = os.getenv("DG_API_KEY", "")
DG_TOURS = ["pga", "euro", "alt"]  # add "euro", liv = "alt", "lpga", "kft", etc., as needed
DG_BASE = "https://feeds.datagolf.com"

# Naming convention file (CSV/TSV)
# This resolves regardless of where you run the script from.
def find_file_upwards(filename: str, start_dir: Path) -> Path:
    cur = start_dir.resolve()
    while True:
        candidate = cur / filename
        if candidate.exists():
            return candidate
        if cur.parent == cur:
            return start_dir / filename
        cur = cur.parent

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_OVERRIDES_PATH = find_file_upwards("name_overrides.csv", SCRIPT_DIR)
NAME_OVERRIDES_CSV = os.getenv("NAME_OVERRIDES_CSV", str(DEFAULT_OVERRIDES_PATH))

# Alert de-duplication state (persistent)
ALERT_STATE_FILE = os.getenv("ALERT_STATE_FILE", str(SCRIPT_DIR.parent / "runtime" / "output" / "alert_state.json"))

# Matching & filtering
DG_DATE_PADDING_DAYS = 7
EVENT_MATCH_THRESHOLD = float(os.getenv("EVENT_MATCH_THRESHOLD", "0.62"))  # ↑ stricter: 0.65–0.7
ONLY_INCLUDE_EVENTS_ON_DG = True  # only show Betfair events we can match to DG

# Poll interval
POLL_SECONDS = 60

# Alerts (optional)
SLACK_BOT_TOKEN   = os.getenv("SLACK_BOT_TOKEN", "").strip()
SLACK_CHANNEL     = os.getenv("SLACK_CHANNEL", "C09U1JJRWL9").strip()
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "").strip()

EMAIL_SMTP_HOST = os.getenv("EMAIL_SMTP_HOST", "").strip()
EMAIL_SMTP_PORT = int(os.getenv("EMAIL_SMTP_PORT", "587"))
EMAIL_SMTP_USER = os.getenv("EMAIL_SMTP_USER", "").strip()
EMAIL_SMTP_PASS = os.getenv("EMAIL_SMTP_PASS", "").strip()
EMAIL_TO        = os.getenv("EMAIL_TO", "").strip()
EMAIL_FROM      = os.getenv("EMAIL_FROM", EMAIL_SMTP_USER).strip()

# ==============================
# LOG
# ==============================
def log(msg: str):
    now = datetime.now().strftime("%H:%M:%S")
    print(f"[{now}] {msg}")

# ==============================
# ALERT STATE (DEDUP)
# ==============================
def _safe_key(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8", errors="ignore")).hexdigest()

def load_alert_state(path: str) -> Dict[str, str]:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception as e:
        log(f"⚠️ Could not read alert state file '{path}': {e}")
        return {}

def save_alert_state(path: str, state: Dict[str, str]) -> None:
    try:
        Path(path).write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")
    except Exception as e:
        log(f"⚠️ Could not write alert state file '{path}': {e}")

def mismatch_fingerprint(out_players: List[str], in_players: List[str]) -> str:
    payload = {"out": sorted(out_players), "in": sorted(in_players)}
    raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()

# ==============================
# NAME NORMALIZATION + OVERRIDES
# ==============================
SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "v"}
SURNAME_PREFIXES = {"de", "del", "da", "di", "la", "le", "van", "von", "der"}

# Reloaded each pass from NAME_OVERRIDES_CSV.
PLAYER_ALIASES: Dict[str, str] = {}

def _base_normalize_name(s: str) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower().strip()
    s = re.sub(r"\(.*?\)", "", s)
    s = re.sub(r"\[.*?\]", "", s)
    s = re.sub(r"[-–—·\.']", " ", s)
    if "," in s:
        parts = [p.strip() for p in s.split(",")]
        if len(parts) == 2:
            s = f"{parts[1]} {parts[0]}"
        else:
            s = " ".join(parts)
    s = re.sub(r"\s+", " ", s).strip()
    tokens = [t for t in s.split() if not (len(t) == 1 and t.isalpha())]
    if tokens and tokens[-1].strip(".") in SUFFIXES:
        tokens = tokens[:-1]
    return " ".join(tokens)

def load_name_overrides(csv_path: str) -> Dict[str, str]:
    """
    Supports:
      1) CSV with header dg_name,bf_name  (any casing)
      2) TSV with header (e.g. "Betfair<TAB>DataGolf")
      3) Two-column file with no header (comma or tab)
         - If no header, assumes col1=Betfair, col2=DataGolf

    Returns mapping normalized_name -> forced_normalized_name (Betfair-normalized form).
    """
    p = Path(csv_path)
    if not p.exists():
        return {}

    try:
        raw = p.read_text(encoding="utf-8-sig")
    except Exception as e:
        log(f"⚠️ Could not read {csv_path}: {e}")
        return {}

    first_nonempty = next((ln for ln in raw.splitlines() if ln.strip()), "")
    comma_ct = first_nonempty.count(",")
    tab_ct = first_nonempty.count("\t")
    delim = "\t" if tab_ct > comma_ct else ","

    out: Dict[str, str] = {}

    try:
        reader = csv.reader(raw.splitlines(), delimiter=delim)
        rows = [r for r in reader if r and any(c.strip() for c in r)]
        if not rows:
            return {}

        header = [c.strip().lower() for c in rows[0]]
        has_header = False

        if ("dg_name" in header and "bf_name" in header):
            has_header = True
            dg_idx = header.index("dg_name")
            bf_idx = header.index("bf_name")
        elif ("betfair" in header and ("datagolf" in header or "data golf" in header)):
            has_header = True
            bf_idx = header.index("betfair")
            dg_idx = header.index("datagolf") if "datagolf" in header else header.index("data golf")

        start_idx = 1 if has_header else 0

        for r in rows[start_idx:]:
            if len(r) < 2:
                continue

            if has_header:
                if max(dg_idx, bf_idx) >= len(r):
                    continue
                bf_raw = (r[bf_idx] or "").strip()
                dg_raw = (r[dg_idx] or "").strip()
            else:
                bf_raw = (r[0] or "").strip()
                dg_raw = (r[1] or "").strip()

            if not bf_raw or not dg_raw:
                continue

            dg_norm = _base_normalize_name(dg_raw)
            bf_norm = _base_normalize_name(bf_raw)

            out[dg_norm] = bf_norm
            out[bf_norm] = bf_norm

    except Exception as e:
        log(f"⚠️ Could not parse {csv_path}: {e}")
        return {}

    return out

def normalize_name(s: str) -> str:
    norm = _base_normalize_name(s)
    if not norm:
        return ""
    return PLAYER_ALIASES.get(norm, norm)

def canonical_player_key(norm_name: str) -> str:
    if not norm_name:
        return ""
    parts = norm_name.split()
    if len(parts) == 1:
        return parts[0]
    if len(parts) >= 3 and parts[-2] in SURNAME_PREFIXES:
        last = " ".join(parts[-2:])
    else:
        last = parts[-1]
    first_initial = parts[0][0]
    return f"{first_initial} {last}"

# Event-name aliases (not used for player names)
EVENT_ALIASES = {
    "ww technology championship": "world wide technology championship",
    "wwt championship": "world wide technology championship",
}

def norm_event_name(s: str) -> str:
    ns = normalize_name(s)
    return EVENT_ALIASES.get(ns, ns)

# ==============================
# DATETIME HELPERS
# ==============================
def _parse_dt_any(s: str):
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S%z"):
        try:
            dt = datetime.strptime(s, fmt)
            if dt.tzinfo:
                dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
            return dt
        except Exception:
            pass
    return None

# ==============================
# DATAGOLF HTTP (resilient)
# ==============================
def _dg_get_json(url, params, *, tag: str):
    try:
        r = requests.get(url, params=params, timeout=20)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.SSLError as e1:
        log(f"WARNING: DG TLS verify failed on {tag}: {e1}. Retrying once with verify=False (INSECURE).")
        try:
            r = requests.get(url, params=params, timeout=20, verify=False)
            r.raise_for_status()
            return r.json()
        except Exception as e2:
            log(f"❌ DG request failed (even verify=False): {url}  err={e2}")
            return None
    except Exception as e:
        log(f"❌ DG request failed on {tag}: {url}  err={e}")
        return None

def dg_field_updates(tour: str):
    return _dg_get_json(
        f"{DG_BASE}/field-updates",
        {"tour": tour, "file_format": "json", "key": DG_API_KEY},
        tag=f"field-updates[{tour}]",
    )

def dg_get_schedule(tour: str):
    return _dg_get_json(
        f"{DG_BASE}/get-schedule",
        {"tour": tour, "file_format": "json", "key": DG_API_KEY},
        tag=f"get-schedule[{tour}]",
    )

# ==============================
# BUILD A DG EVENT INDEX (name → players)
# ==============================
class DGEvent:
    def __init__(
        self,
        tour: str,
        name: str,
        event_id: Optional[str],
        start: Optional[datetime],
        players: Set[str],
        display: Dict[str, str],
    ):
        self.tour = tour
        self.name = name
        self.event_id = event_id
        self.start = start
        self.players = players
        self.display = display  # key -> DG display name

def _pluck_players(rows) -> Tuple[Set[str], Dict[str, str]]:
    keys: Set[str] = set()
    display: Dict[str, str] = {}
    for r in rows or []:
        if not isinstance(r, dict):
            continue
        nm = r.get("player_name") or r.get("name")
        if not nm:
            continue
        norm = normalize_name(nm)
        key = canonical_player_key(norm)
        if key:
            keys.add(key)
            display.setdefault(key, nm)
    return keys, display

def _extract_players_from_field_updates_payload(js) -> Tuple[Optional[str], Optional[str], Optional[datetime], Set[str], Dict[str, str]]:
    if isinstance(js, dict) and "field" in js and "event_name" in js:
        keys, display = _pluck_players(js.get("field"))
        return (
            js.get("event_name"),
            str(js.get("event_id") or ""),
            _parse_dt_any(js.get("start_date") or js.get("date") or ""),
            keys,
            display,
        )

    if isinstance(js, dict) and isinstance(js.get("events"), list) and js["events"]:
        e = js["events"][0]
        keys, display = _pluck_players(e.get("field"))
        return (
            e.get("event_name") or e.get("tournament_name") or e.get("name"),
            str(e.get("event_id") or ""),
            _parse_dt_any(e.get("start_date") or e.get("date") or ""),
            keys,
            display,
        )
    return (None, None, None, set(), {})

def build_dg_event_index() -> List[DGEvent]:
    out: List[DGEvent] = []

    for tour in DG_TOURS:
        fu = dg_field_updates(tour)
        if isinstance(fu, dict):
            name, eid, start, keys, display = _extract_players_from_field_updates_payload(fu)
            if keys:
                log(f"DG field-updates[{tour}] single-event shape matched (DG='{name}', players={len(keys)}).")
                out.append(DGEvent(tour, name or "", eid or "", start, keys, display))
            else:
                evs = fu.get("events") or []
                if not isinstance(evs, list):
                    log(f"DG field-updates[{tour}] had no 'events' list. keys={list(fu.keys())[:8]}")
                for e in evs:
                    rows = e.get("field") or []
                    keys, display = _pluck_players(rows)
                    name = e.get("event_name") or e.get("tournament_name") or e.get("name") or ""
                    start = _parse_dt_any(e.get("start_date") or e.get("date") or "")
                    out.append(DGEvent(tour, name, str(e.get("event_id") or ""), start, keys, display))
        else:
            if fu is not None:
                log(f"DG field-updates[{tour}] returned non-dict; type={type(fu).__name__}")

        # supplement with schedule
        sched = dg_get_schedule(tour)
        if isinstance(sched, dict) and isinstance(sched.get("events"), list):
            for e in sched["events"]:
                name = e.get("event_name") or e.get("tournament_name") or e.get("name") or ""
                eid  = str(e.get("event_id") or e.get("id") or e.get("dg_event_id") or "")
                start = _parse_dt_any(e.get("start_date") or e.get("start_time") or e.get("date") or "")
                if not any(norm_event_name(name) == norm_event_name(x.name) and x.tour == tour for x in out):
                    out.append(DGEvent(tour, name, eid, start, set(), {}))

    out = [e for e in out if e.name]
    return out

# ==============================
# MATCH A BETFAIR EVENT TO DG
# ==============================
def score_event_match(bf_name: str, bf_start: Optional[datetime], dg: DGEvent) -> float:
    tgt = norm_event_name(bf_name)
    dgname = norm_event_name(dg.name)
    name_sim = difflib.SequenceMatcher(None, tgt, dgname).ratio()
    date_bonus = 0.0
    if bf_start and dg.start:
        delta_days = abs((bf_start - dg.start).days)
        date_bonus = max(0.0, 1.0 - (delta_days / DG_DATE_PADDING_DAYS))
    return name_sim + 0.25 * date_bonus

def best_dg_match(bf_name: str, bf_start: Optional[datetime], index: List[DGEvent]) -> Optional[DGEvent]:
    best, best_score = None, -1.0
    for dg in index:
        s = score_event_match(bf_name, bf_start, dg)
        if s > best_score:
            best, best_score = dg, s
    if best and best_score >= EVENT_MATCH_THRESHOLD:
        log(f"DG match: '{bf_name}' ⇄ '{best.name}' (tour={best.tour}, score={best_score:.2f}, players={len(best.players)})")
        return best
    return None

# ==============================
# BETFAIR
# ==============================
def betfair_login() -> betfairlightweight.APIClient:
    if BF_CERTS_DIR and os.path.isdir(BF_CERTS_DIR):
        trading = betfairlightweight.APIClient(BF_USERNAME, BF_PASSWORD, app_key=BF_APP_KEY, certs=BF_CERTS_DIR)
        trading.login()
    else:
        trading = betfairlightweight.APIClient(BF_USERNAME, BF_PASSWORD, app_key=BF_APP_KEY)
        trading.login_interactive()
    return trading

def betfair_list_outrights(trading):
    mf = bf_market_filter(
        event_type_ids=[EVENT_TYPE_ID_GOLF],
        market_type_codes=MARKET_TYPE_CODES,
        market_start_time={
            "from": (datetime.now(timezone.utc) - timedelta(days=2)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "to":   (datetime.now(timezone.utc) + timedelta(days=14)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        },
    )
    cats = trading.betting.list_market_catalogue(
        filter=mf,
        market_projection=["EVENT", "RUNNER_DESCRIPTION", "MARKET_START_TIME"],
        max_results=200,
        sort="FIRST_TO_START",
    )
    return cats

def betfair_active_selection_ids(trading, market_id: str) -> Set[int]:
    try:
        books = trading.betting.list_market_book(
            market_ids=[market_id],
            price_projection=None,
        )
    except Exception as e:
        log(f"Error fetching market_book for {market_id}: {e}")
        return set()

    if not books:
        return set()

    book = books[0]
    active = {
        r.selection_id
        for r in (book.runners or [])
        if getattr(r, "status", "") == "ACTIVE"
    }
    return active

def betfair_market_runners_set(trading, cat) -> Tuple[Set[str], Dict[str, str]]:
    active_ids = betfair_active_selection_ids(trading, cat.market_id)

    keys: Set[str] = set()
    display: Dict[str, str] = {}
    for r in (cat.runners or []):
        if not r.runner_name:
            continue
        if active_ids and r.selection_id not in active_ids:
            continue
        norm = normalize_name(r.runner_name)
        key = canonical_player_key(norm)
        if key:
            keys.add(key)
            display.setdefault(key, r.runner_name)
    return keys, display

# ==============================
# ALERTS
# ==============================
def send_slack(text: str):
    if SLACK_WEBHOOK_URL:
        try:
            resp = requests.post(SLACK_WEBHOOK_URL, json={"text": text}, timeout=10)
            if resp.status_code != 200:
                log(f"Slack webhook error: status={resp.status_code}, body={resp.text}")
        except Exception as e:
            log(f"Slack webhook send failed: {e}")
        return

    if SLACK_BOT_TOKEN and SLACK_CHANNEL:
        try:
            resp = requests.post(
                "https://slack.com/api/chat.postMessage",
                headers={
                    "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
                    "Content-Type": "application/json; charset=utf-8",
                },
                json={"channel": SLACK_CHANNEL, "text": text},
                timeout=10,
            )
            if resp.status_code != 200:
                log(f"Slack API HTTP error: status={resp.status_code}, body={resp.text}")
            else:
                data = resp.json()
                if not data.get("ok"):
                    log(f"Slack API error: {data.get('error')} (resp={data})")
        except Exception as e:
            log(f"Slack API send failed: {e}")
        return

    log("Slack disabled: no SLACK_WEBHOOK_URL or SLACK_BOT_TOKEN/SLACK_CHANNEL configured.")

def send_email(subject: str, body: str):
    if not (EMAIL_SMTP_HOST and EMAIL_TO and EMAIL_FROM):
        return
    import smtplib
    from email.mime.text import MIMEText
    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = EMAIL_FROM
    msg["To"] = EMAIL_TO
    try:
        with smtplib.SMTP(EMAIL_SMTP_HOST, EMAIL_SMTP_PORT, timeout=15) as s:
            s.starttls()
            if EMAIL_SMTP_USER and EMAIL_SMTP_PASS:
                s.login(EMAIL_SMTP_USER, EMAIL_SMTP_PASS)
            s.send_message(msg)
    except Exception as e:
        log(f"Email send failed: {e}")

def alert_for_event(bf_header: str, out_players: List[str], in_players: List[str]):
    if not out_players and not in_players:
        return

    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    lines = []
    lines.append(bf_header)
    lines.append("-" * len(bf_header))
    lines.append("")

    if out_players:
        lines.append(f"🚨 ❌ [{ts}] OUT (on Betfair, missing on DG):")
        for p in out_players:
            lines.append(f"    - {p}")

    if in_players:
        lines.append(f"🚨 ✅ [{ts}] IN  (on DG, missing on Betfair):")
        for p in in_players:
            lines.append(f"    + {p}")

    text = "```" + "\n".join(lines) + "```"
    send_slack(text)
    send_email(subject=f"[Golf NR Check] {bf_header}", body="\n".join(lines))

# ==============================
# COMPARISON
# ==============================
def compare_and_report(trading, alert_state: Dict[str, str]):
    global PLAYER_ALIASES
    PLAYER_ALIASES = load_name_overrides(NAME_OVERRIDES_CSV)
    if PLAYER_ALIASES:
        log(f"Loaded name overrides: {len(PLAYER_ALIASES)} mappings from '{NAME_OVERRIDES_CSV}'")
    else:
        log(f"No name overrides loaded (file missing/empty?): '{NAME_OVERRIDES_CSV}'")

    dg_index = build_dg_event_index()
    if not dg_index:
        log("⚠️ No DG events visible right now; skipping this pass.")
        return

    cats = betfair_list_outrights(trading)
    if not cats:
        log("No golf outrights found on Betfair.")
        return

    state_dirty = False

    for cat in cats:
        bf_market_id = cat.market_id
        bf_event_name = getattr(cat.event, "name", "") or ""
        bf_market_name = cat.market_name or "Outright"
        bf_start = cat.market_start_time

        if isinstance(bf_start, str):
            for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"):
                try:
                    bf_start = datetime.strptime(bf_start, fmt)
                    break
                except ValueError:
                    pass
        if isinstance(bf_start, datetime) and bf_start.tzinfo:
            bf_start = bf_start.astimezone(timezone.utc).replace(tzinfo=None)

        dg_match = best_dg_match(bf_event_name, bf_start, dg_index)
        if ONLY_INCLUDE_EVENTS_ON_DG and not dg_match:
            continue

        bf_keys, bf_display = betfair_market_runners_set(trading, cat)
        dg_keys = dg_match.players if dg_match else set()
        dg_display = dg_match.display if dg_match else {}

        out_keys = sorted(bf_keys - dg_keys)
        in_keys  = sorted(dg_keys - bf_keys)

        out_players = [bf_display.get(k, k) for k in out_keys]
        in_players  = [dg_display.get(k, k) for k in in_keys]

        header = f"{bf_event_name} :: {bf_market_name} (BF {bf_market_id})"
        if dg_match:
            header += f"  [DG {dg_match.tour}:{dg_match.event_id or 'n/a'}]"

        print("\n" + header)
        print("-" * len(header))

        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if dg_match and not dg_keys:
            print("  Note: DataGolf field empty/unavailable for this event (schedule present but no field yet).")

        if out_players:
            print(f" :alarm: :x: [{ts}] OUT (on Betfair, missing on DG):")
            for p in out_players:
                print(f"    - {p}")
        if in_players:
            print(f" :alarm: :tickmark: [{ts}] IN  (on DG, missing on Betfair):")
            for p in in_players:
                print(f"    + {p}")

        if not out_players and not in_players and bf_keys and dg_keys:
            print(f"  [{ts}] ✅ Lists match - Betfair v DataGolf.")

        # ---- ALERT DEDUP LOGIC (persistent) ----
        state_key = _safe_key(f"{bf_market_id}")
        if out_players or in_players:
            fp = mismatch_fingerprint(out_players, in_players)
            prev_fp = alert_state.get(state_key)
            if prev_fp != fp:
                alert_for_event(header, out_players, in_players)
                alert_state[state_key] = fp
                state_dirty = True
            else:
                log(f"Skipping duplicate alert (no change): {bf_event_name} / {bf_market_name} / {bf_market_id}")
        else:
            # Optional: clear stored fingerprint if market now matches again
            if state_key in alert_state:
                del alert_state[state_key]
                state_dirty = True

    if state_dirty:
        save_alert_state(ALERT_STATE_FILE, alert_state)

# ==============================
# MAIN
# ==============================
def main():
    missing = []
    if not BF_USERNAME or BF_USERNAME == "REPLACE_ME": missing.append("BETFAIR_USERNAME")
    if not BF_PASSWORD or BF_PASSWORD == "REPLACE_ME": missing.append("BETFAIR_PASSWORD")
    if not BF_APP_KEY  or BF_APP_KEY  == "REPLACE_ME": missing.append("BETFAIR_APP_KEY")
    if not DG_API_KEY  or DG_API_KEY  == "REPLACE_ME": missing.append("DG_API_KEY")
    if missing:
        print("⚠️  Please set: " + ", ".join(missing))
        sys.exit(1)

    trading = betfair_login()
    log("Connected to Betfair.")

    alert_state = load_alert_state(ALERT_STATE_FILE)

    try:
        while True:
            compare_and_report(trading, alert_state)
            log(f"Sleeping {POLL_SECONDS}s…")
            time.sleep(POLL_SECONDS)
    except KeyboardInterrupt:
        log("Stopping…")
    finally:
        try:
            save_alert_state(ALERT_STATE_FILE, alert_state)
        except Exception:
            pass
        try:
            trading.logout()
        except Exception:
            pass

if __name__ == "__main__":
    main()
