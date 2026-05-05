#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

"""
Signal Selection Extractor
Simple extraction of selection names and probabilities from Polymarket/Kalshi
No Betfair, no margin calculations, no orders — just the data.
"""

import json
import os
import re
import sys
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

import requests
from dotenv import load_dotenv

load_dotenv()

try:
    import Levenshtein

    HAVE_LEV = True
except Exception:
    import difflib

    HAVE_LEV = False

# ==================== CONFIG ====================

GAMMA_BASE = "https://gamma-api.polymarket.com"
DEFAULT_TIMEOUT = 20


# ==================== HELPERS ====================

def _strip_quotes(s: str) -> str:
    return (s or "").strip().strip("'").strip('"').strip()


def _is_url(s: str) -> bool:
    try:
        u = urlparse(s)
        return bool(u.scheme and u.netloc)
    except Exception:
        return False


def _extract_kalshi_tickers_from_url(url: str) -> Tuple[Optional[str], Optional[str]]:
    try:
        u = urlparse(url)
        parts = [p for p in u.path.split("/") if p]
        if "markets" in parts:
            i = parts.index("markets")
            series_slug = parts[i + 1] if i + 1 < len(parts) else None
            market_slug = parts[-1] if parts else None
            series_ticker = series_slug.upper() if series_slug else None
            market_ticker = market_slug.upper() if market_slug else None
            return series_ticker, market_ticker
    except Exception:
        pass
    return None, None


def normalize_name(s: str) -> str:
    s = s.lower()
    s = s.replace("&", "and")
    s = re.sub(r"[\(\)\[\]\{\}]", " ", s)
    s = re.sub(r"[^a-z0-9\s']", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def safe_json_loads(x: Any, default=None):
    if default is None:
        default = []
    if x is None:
        return default
    if isinstance(x, (list, dict)):
        return x
    if isinstance(x, str):
        try:
            return json.loads(x)
        except Exception:
            return default
    return default


def parse_prob_value(px: Any) -> Optional[float]:
    try:
        v = float(px)
    except Exception:
        return None
    if v <= 0:
        return None
    if 0 < v <= 1.0:
        return v
    if 1.0 < v <= 100.0:
        return v / 100.0
    return None


def prettify_binary_market_label(raw: str) -> str:
    s = raw.strip()
    s = re.sub(r"^will\s+", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\s+win.*$", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\?$", "", s).strip()
    s = s.strip("\"' ")
    return s


# ==================== POLYMARKET ====================

class PolymarketClient:
    def __init__(self):
        self.s = requests.Session()
        self.s.verify = False

    def gamma_get_event_by_slug(self, slug: str) -> Dict[str, Any]:
        url = f"{GAMMA_BASE}/events/slug/{slug}"
        r = self.s.get(url, timeout=DEFAULT_TIMEOUT)
        r.raise_for_status()
        return r.json()


def extract_polymarket_selections(event_slug: str) -> List[Dict[str, Any]]:
    """
    Extract selection names and probabilities from a Polymarket event.
    Returns list of dicts with keys: label, probability
    """
    print(f"\nFetching Polymarket event: {event_slug}")

    poly = PolymarketClient()
    event = poly.gamma_get_event_by_slug(event_slug)
    markets = event.get("markets") or []

    if not markets:
        raise RuntimeError(f"No markets found for event slug: {event_slug}")

    selections = []

    for m in markets:
        outs = safe_json_loads(m.get("outcomes"), default=[])
        # Only process YES/NO markets
        if not (isinstance(outs, list) and len(outs) == 2):
            continue

        a = str(outs[0]).strip().lower()
        b = str(outs[1]).strip().lower()
        if {a, b} != {"yes", "no"}:
            continue

        prices = safe_json_loads(m.get("outcomePrices"), default=[])
        if not (isinstance(prices, list) and len(prices) == 2):
            continue

        yes_prob = parse_prob_value(prices[0])
        if yes_prob is None or not (0.0 < yes_prob < 1.0):
            continue

        raw_label = (m.get("title") or m.get("question") or m.get("slug") or f"market_{m.get('id')}").strip()
        label = prettify_binary_market_label(raw_label)

        selections.append({
            "label": label,
            "probability": float(yes_prob),
            "market_id": m.get("id")
        })

    if not selections:
        raise RuntimeError("Could not extract any YES/NO markets from event")

    # Deduplicate by label (keep higher probability)
    dedup: Dict[str, Dict[str, Any]] = {}
    for s in selections:
        k = normalize_name(s["label"])
        if k not in dedup:
            dedup[k] = s
        else:
            if s["probability"] > dedup[k]["probability"]:
                dedup[k] = s

    return list(dedup.values())


# ==================== KALSHI ====================

def _parse_prob_from_kalshi_market(m: Dict[str, Any]) -> Optional[float]:
    def to_float(x) -> Optional[float]:
        try:
            if x is None:
                return None
            return float(x)
        except Exception:
            return None

    yes_bid = to_float(m.get("yes_bid_dollars"))
    yes_ask = to_float(m.get("yes_ask_dollars"))
    last_price = to_float(m.get("last_price_dollars"))

    if yes_bid is None:
        yes_bid = to_float(m.get("yes_bid"))
    if yes_ask is None:
        yes_ask = to_float(m.get("yes_ask"))
    if last_price is None:
        last_price = to_float(m.get("last_price"))

    def norm_price(p: float) -> Optional[float]:
        if 0.0 < p <= 1.0:
            return p
        if 1.0 < p <= 100.0:
            return p / 100.0
        return None

    if yes_bid is not None and yes_ask is not None and yes_bid >= 0 and yes_ask > 0:
        p = norm_price((yes_bid + yes_ask) / 2.0)
        if p is not None:
            return p

    if last_price is not None and last_price > 0:
        p = norm_price(last_price)
        if p is not None:
            return p

    return None


def _kalshi_market_label(m: Dict[str, Any], series_title: Optional[str]) -> str:
    for k in ["yes_sub_title", "yesSubTitle", "yes_subtitle", "yesSubtitle"]:
        v = m.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()

    for k in ["no_sub_title", "noSubTitle", "no_subtitle", "noSubtitle"]:
        v = m.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()

    candidates = []
    for k in ["subtitle", "sub_title", "subTitle", "name", "short_title", "shortTitle",
              "display_title", "displayTitle", "contract", "contract_name", "contractName"]:
        v = m.get(k)
        if isinstance(v, str) and v.strip():
            candidates.append(v.strip())

    st_norm = normalize_name(series_title or "")
    for v in candidates:
        if not st_norm or normalize_name(v) != st_norm:
            return v

    if candidates:
        return candidates[0]

    t = (m.get("ticker") or m.get("market_ticker") or m.get("marketTicker") or "").strip()
    return t if t else (series_title or "market")


class KalshiClient:
    def __init__(self):
        self.s = requests.Session()
        self.s.verify = False
        self.base_urls = [
            "https://trading-api.kalshi.com/trade-api/v2",
            "https://api.elections.kalshi.com/trade-api/v2",
        ]

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        last_err: Optional[Exception] = None
        for base in self.base_urls:
            if not base:
                continue
            url = base.rstrip("/") + "/" + path.lstrip("/")
            try:
                r = self.s.get(url, params=params, timeout=DEFAULT_TIMEOUT)
                if r.status_code >= 400:
                    raise RuntimeError(f"Kalshi HTTP {r.status_code}: {r.text}")
                return r.json()
            except Exception as e:
                last_err = e
                continue
        raise RuntimeError(f"Kalshi request failed: {last_err}")

    def get_market(self, market_ticker: str) -> Dict[str, Any]:
        return self._get(f"markets/{market_ticker}")

    def list_markets_in_series(self, series_ticker: str) -> List[Dict[str, Any]]:
        all_markets: List[Dict[str, Any]] = []
        cursor = None

        while True:
            params: Dict[str, Any] = {"series_ticker": series_ticker}
            if cursor:
                params["cursor"] = cursor
            j = self._get("markets", params=params)
            chunk = j.get("markets") or j.get("data") or []
            if isinstance(chunk, list):
                all_markets.extend(chunk)
            cursor = j.get("cursor") or j.get("next_cursor")
            if not cursor:
                break

        if all_markets:
            return all_markets

        all_markets = []
        cursor = None
        while True:
            params = {"series_ticker": series_ticker, "status": "open"}
            if cursor:
                params["cursor"] = cursor
            j = self._get("markets", params=params)
            chunk = j.get("markets") or j.get("data") or []
            if isinstance(chunk, list):
                all_markets.extend(chunk)
            cursor = j.get("cursor") or j.get("next_cursor")
            if not cursor:
                break

        return all_markets


def extract_kalshi_selections(identifier: str) -> List[Dict[str, Any]]:
    """
    Extract selection names and probabilities from Kalshi.
    identifier can be: series_ticker, market_ticker, or Kalshi URL
    Returns list of dicts with keys: label, probability
    """
    print(f"\nFetching Kalshi data: {identifier}")

    ident = _strip_quotes(identifier)
    series_ticker: Optional[str] = None
    market_ticker: Optional[str] = None

    if _is_url(ident):
        s, m = _extract_kalshi_tickers_from_url(ident)
        series_ticker, market_ticker = s, m
    else:
        if re.match(r"^[A-Za-z0-9]+-\d+$", ident.strip()):
            market_ticker = ident.upper()
        else:
            series_ticker = ident.upper()

    series_title: Optional[str] = None
    if market_ticker and not series_ticker:
        mj = KalshiClient().get_market(market_ticker)
        mobj = mj.get("market") or mj
        series_ticker = (mobj.get("series_ticker") or mobj.get("seriesTicker") or "").strip() or None
        series_title = (mobj.get("event_title") or mobj.get("eventTitle") or mobj.get("series_title") or mobj.get(
            "seriesTitle") or "").strip() or None
        if not series_ticker:
            series_ticker = market_ticker.split("-")[0].upper()

    if not series_ticker:
        raise RuntimeError("Could not determine Kalshi series_ticker from identifier")

    kalshi = KalshiClient()
    markets = kalshi.list_markets_in_series(series_ticker)

    if not markets:
        raise RuntimeError(f"No Kalshi markets found for: {series_ticker}")

    if not series_title:
        for m in markets:
            for k in ["event_title", "eventTitle", "series_title", "seriesTitle", "question", "prompt"]:
                v = m.get(k)
                if isinstance(v, str) and v.strip():
                    series_title = v.strip()
                    break
            if series_title:
                break

    selections = []
    for m in markets:
        p = _parse_prob_from_kalshi_market(m)
        if p is None or not (0.0 < p < 1.0):
            continue

        label = _kalshi_market_label(m, series_title=series_title)

        selections.append({
            "label": label,
            "probability": float(p),
            "market_ticker": (m.get("ticker") or m.get("market_ticker") or m.get("marketTicker"))
        })

    if not selections:
        raise RuntimeError(f"Could not extract any selections from Kalshi series: {series_ticker}")

    # Deduplicate by label (keep higher probability)
    dedup: Dict[str, Dict[str, Any]] = {}
    for s in selections:
        k = normalize_name(s["label"])
        if k not in dedup:
            dedup[k] = s
        else:
            if s["probability"] > dedup[k]["probability"]:
                dedup[k] = s

    return list(dedup.values())


# ==================== MAIN ====================

def main():
    if len(sys.argv) < 3:
        print("Usage: python Signal_Selection_Extractor.py <signal_source> <identifier>")
        print("  signal_source: 'polymarket' or 'kalshi'")
        print("  identifier: Event slug (polymarket) or series/market ticker/URL (kalshi)")
        sys.exit(1)

    signal_source = sys.argv[1].lower()
    identifier = sys.argv[2]

    if signal_source not in ["polymarket", "kalshi"]:
        print(f"ERROR: signal_source must be 'polymarket' or 'kalshi', got '{signal_source}'")
        sys.exit(1)

    try:
        if signal_source == "polymarket":
            selections = extract_polymarket_selections(identifier)
        else:
            selections = extract_kalshi_selections(identifier)

        print(f"\n--- {signal_source.upper()} Selections ---\n")

        for sel in selections:
            prob_pct = sel["probability"] * 100
            print(f"- {sel['label']:50s} : {sel['probability']:.4f} ({prob_pct:5.2f}%)")



    except Exception as e:
        print(f"\nERROR: {e}\n", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()