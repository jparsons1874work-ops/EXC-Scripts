"""Betfair Exchange API client helpers for Integrity Player Scanner."""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path

import betfairlightweight
from betfairlightweight import filters

from src.retry import retry

logger = logging.getLogger(__name__)


class BetfairClient:
    """Client wrapper for Betfair Exchange API operations."""

    TENNIS_EVENT_TYPE_ID = "2"
    MATCH_ODDS_MARKET_TYPE = "MATCH_ODDS"
    MAX_RESULTS_PER_REQUEST = 1000
    INITIAL_LOOKBACK = timedelta(days=1)
    INITIAL_LOOKAHEAD = timedelta(days=365)
    MIN_WINDOW = timedelta(hours=1)
    MARKET_BOOK_BATCH_SIZE = 40

    def __init__(self, username: str, password: str, app_key: str, certs_path: str):
        """Initialise the client with Betfair credentials and certificate auth settings.

        Parameters:
            username: Betfair account username.
            password: Betfair account password.
            app_key: Betfair application key.
            certs_path: Directory containing Betfair SSL certificate files.
        """
        self.username = username
        self.password = password
        self.app_key = app_key
        self.certs_path = certs_path
        self.client = betfairlightweight.APIClient(
            username=username,
            password=password,
            app_key=app_key,
            certs=certs_path,
        )
        self.api = None

    def login(self) -> bool:
        """Authenticate with Betfair using certificate-based SSO login.

        Returns:
            `True` if login succeeds, otherwise `False`.
        """
        try:
            @retry(max_attempts=3, delay_seconds=2.0, backoff_multiplier=2.0)
            def perform_login():
                return self.client.login()

            perform_login()
            if self.client.session_token:
                self.api = self.client
                logger.info("Betfair login succeeded for user '%s'.", self.username)
                return True

            logger.error("Betfair login failed: no session token returned.")
            return False
        except Exception as exc:  # pragma: no cover - external API behaviour
            logger.exception("Betfair login failed: %s", exc)
            return False

    def logout(self):
        """Log out from the Betfair API and clear the active session."""
        try:
            if self.api is None:
                logger.info("Betfair logout skipped: no active session.")
                return

            self.client.logout()
            logger.info("Betfair logout succeeded.")
        except Exception as exc:  # pragma: no cover - external API behaviour
            logger.exception("Betfair logout failed: %s", exc)
        finally:
            self.api = None

    def keep_alive(self) -> bool:
        """Refresh the current Betfair session.

        Returns:
            `True` if keep-alive succeeds, otherwise `False`.
        """
        try:
            if self.api is None:
                logger.warning("Betfair keep-alive skipped: not logged in.")
                return False

            @retry(max_attempts=3, delay_seconds=2.0, backoff_multiplier=2.0)
            def perform_keep_alive():
                return self.client.keep_alive()

            perform_keep_alive()
            logger.info("Betfair keep-alive succeeded.")
            return True
        except Exception as exc:  # pragma: no cover - external API behaviour
            logger.exception("Betfair keep-alive failed: %s", exc)
            return False

    def get_tennis_match_odds_markets(self) -> list[dict]:
        """Fetch tennis Match Odds markets and return normalised market details.

        Returns:
            A list of dictionaries containing event, market, and runner details.
            Returns an empty list when the request fails.
        """
        try:
            if self.api is None:
                logger.error("Cannot fetch markets: Betfair client is not logged in.")
                return []

            event_filter = filters.market_filter(event_type_ids=[self.TENNIS_EVENT_TYPE_ID])
            events = self._safe_api_call(self.api.betting.list_events, filter=event_filter)
            if events is None:
                logger.warning("Failed to fetch tennis event list after retries.")
            else:
                logger.info("Fetched %d tennis events from Betfair.", len(events))

            start_time = datetime.now(UTC) - self.INITIAL_LOOKBACK
            end_time = datetime.now(UTC) + self.INITIAL_LOOKAHEAD
            catalogues = self._list_market_catalogues_paginated(start_time=start_time, end_time=end_time)
            if not catalogues:
                logger.info("No tennis Match Odds markets returned by Betfair.")
                return []

            market_books = self._get_market_books_map(
                [catalogue.market_id for catalogue in catalogues if getattr(catalogue, "market_id", None)]
            )
            parsed_markets = self._parse_market_catalogue(catalogues)
            for market in parsed_markets:
                market_book = market_books.get(market["market_id"])
                market["in_play"] = bool(getattr(market_book, "inplay", False)) if market_book else False

            logger.info("Prepared %d tennis Match Odds markets.", len(parsed_markets))
            return parsed_markets
        except Exception as exc:  # pragma: no cover - external API behaviour
            logger.exception("Failed to fetch tennis Match Odds markets: %s", exc)
            return []

    def _safe_api_call(self, api_method, *args, **kwargs):
        """Call a Betfair API method with retry logic and swallow final failure.

        Parameters:
            api_method: Bound Betfair API method to call.
            *args: Positional arguments for the API method.
            **kwargs: Keyword arguments for the API method.

        Returns:
            The API result on success, otherwise `None` after all retries fail.
        """
        try:
            decorated_method = retry(
                max_attempts=3,
                delay_seconds=2.0,
                backoff_multiplier=2.0,
            )(api_method)
            return decorated_method(*args, **kwargs)
        except Exception as exc:  # pragma: no cover - external API behaviour
            logger.error("API call failed for %s after retries: %s", getattr(api_method, "__name__", api_method), exc)
            return None

    def _parse_market_catalogue(self, market_catalogue: list) -> list[dict]:
        """Transform Betfair market catalogue objects into serialisable dictionaries.

        Parameters:
            market_catalogue: Raw market catalogue objects returned by Betfair.

        Returns:
            A list of dictionaries containing event, market, and runner metadata.
        """
        parsed_markets: list[dict] = []

        for market in market_catalogue:
            event = getattr(market, "event", None)
            competition = getattr(market, "competition", None)
            runners = []

            for runner in getattr(market, "runners", []) or []:
                runners.append(
                    {
                        "selection_id": getattr(runner, "selection_id", 0),
                        "runner_name": getattr(runner, "runner_name", "Unknown"),
                    }
                )

            parsed_markets.append(
                {
                    "event_id": str(getattr(event, "id", "")),
                    "event_name": getattr(event, "name", "Unknown"),
                    "competition_name": getattr(competition, "name", "Unknown") if competition else "Unknown",
                    "market_id": str(getattr(market, "market_id", "")),
                    "market_start_time": getattr(market, "market_start_time", None),
                    "in_play": False,
                    "runners": runners,
                }
            )

        return parsed_markets

    def _list_market_catalogues_paginated(self, start_time: datetime, end_time: datetime) -> list:
        """Fetch market catalogues across a time range, splitting requests when the page limit is hit.

        Parameters:
            start_time: Inclusive lower bound for market start time.
            end_time: Inclusive upper bound for market start time.

        Returns:
            A deduplicated list of Betfair market catalogue objects.
        """
        seen_market_ids: set[str] = set()
        collected: list = []

        def fetch_window(window_start: datetime, window_end: datetime) -> None:
            market_filter = filters.market_filter(
                event_type_ids=[self.TENNIS_EVENT_TYPE_ID],
                market_type_codes=[self.MATCH_ODDS_MARKET_TYPE],
                market_start_time={
                    "from": window_start.isoformat(),
                    "to": window_end.isoformat(),
                },
            )
            page = self._safe_api_call(
                self.api.betting.list_market_catalogue,
                filter=market_filter,
                market_projection=["EVENT", "COMPETITION", "RUNNER_DESCRIPTION"],
                sort="FIRST_TO_START",
                max_results=self.MAX_RESULTS_PER_REQUEST,
            )
            if page is None:
                logger.error("Failed to fetch market catalogue window %s to %s.", window_start, window_end)
                return

            if len(page) >= self.MAX_RESULTS_PER_REQUEST and (window_end - window_start) > self.MIN_WINDOW:
                midpoint = window_start + (window_end - window_start) / 2
                logger.info(
                    "Market catalogue page limit hit for %s to %s; splitting window.",
                    window_start,
                    window_end,
                )
                fetch_window(window_start, midpoint)
                fetch_window(midpoint, window_end)
                return

            if len(page) >= self.MAX_RESULTS_PER_REQUEST:
                logger.warning(
                    "Market catalogue page limit hit for minimal window %s to %s; results may be truncated.",
                    window_start,
                    window_end,
                )

            for market in page:
                market_id = getattr(market, "market_id", None)
                if not market_id or market_id in seen_market_ids:
                    continue
                seen_market_ids.add(market_id)
                collected.append(market)

        fetch_window(start_time, end_time)
        return collected

    def _get_market_books_map(self, market_ids: list[str]) -> dict[str, object]:
        """Fetch market books in batches keyed by market ID.

        Parameters:
            market_ids: Betfair market IDs to query.

        Returns:
            A dictionary mapping market IDs to market book objects.
        """
        market_books: dict[str, object] = {}

        if not market_ids:
            return market_books

        for index in range(0, len(market_ids), self.MARKET_BOOK_BATCH_SIZE):
            batch = market_ids[index : index + self.MARKET_BOOK_BATCH_SIZE]
            books = self._safe_api_call(self.api.betting.list_market_book, market_ids=batch)
            if books is None:
                logger.error("Failed to fetch market books for batch starting at %d.", index)
                continue

            for book in books:
                market_id = getattr(book, "market_id", None)
                if market_id:
                    market_books[str(market_id)] = book

        return market_books


if __name__ == "__main__":
    # Quick test - requires valid credentials
    with Path("config/credentials.json").open(encoding="utf-8") as file_handle:
        creds = json.load(file_handle)

    client = BetfairClient(
        username=creds["betfair"]["username"],
        password=creds["betfair"]["password"],
        app_key=creds["betfair"]["app_key"],
        certs_path=creds["betfair"]["certs_path"],
    )
    if client.login():
        markets = client.get_tennis_match_odds_markets()
        print(f"Found {len(markets)} tennis Match Odds markets")
        if markets:
            print(f"Sample: {markets[0]}")
        client.logout()
