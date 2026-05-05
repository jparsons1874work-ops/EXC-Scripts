"""Integrity watchlist loading and fuzzy player matching logic."""

from __future__ import annotations

import logging
import re
import unicodedata

import pandas as pd
from rapidfuzz import fuzz

logger = logging.getLogger(__name__)


class IntegrityChecker:
    """Load an integrity watchlist and match player names against it."""

    def __init__(self, excel_path: str):
        """Initialise the checker and load the integrity watchlist.

        Parameters:
            excel_path: Path to the Excel watchlist file.
        """
        self.excel_path = excel_path
        self.watchlist: list[str] = []
        self.watchlist_original: list[str] = []
        self.load_watchlist()

    def load_watchlist(self) -> list[str]:
        """Load and normalise player names from the Excel watchlist.

        Returns:
            A list of normalised watchlist names. Returns an empty list if loading fails.
        """
        loaded_watchlist: list[str] = []
        loaded_watchlist_original: list[str] = []

        try:
            data_frame = pd.read_excel(self.excel_path, engine="openpyxl")
        except FileNotFoundError:
            logger.error("Integrity watchlist file not found: %s", self.excel_path)
            return self.watchlist
        except Exception as exc:
            logger.exception("Failed to read integrity watchlist '%s': %s", self.excel_path, exc)
            return self.watchlist

        player_column = self._find_player_column(data_frame.columns)
        if player_column is None:
            logger.error(
                "Integrity watchlist '%s' does not contain a supported player column.",
                self.excel_path,
            )
            return self.watchlist

        seen_names: set[str] = set()
        for raw_name in data_frame[player_column].dropna().tolist():
            original_name = str(raw_name).strip()
            if not original_name:
                continue

            normalised_name = self._normalise_name(original_name)
            if not normalised_name or normalised_name in seen_names:
                continue

            seen_names.add(normalised_name)
            loaded_watchlist.append(normalised_name)
            loaded_watchlist_original.append(original_name)

        self.watchlist = loaded_watchlist
        self.watchlist_original = loaded_watchlist_original
        logger.info("Loaded %d players from integrity watchlist.", len(self.watchlist))
        return self.watchlist

    def check_player(self, player_name: str) -> dict | None:
        """Check a single player name against the watchlist.

        Parameters:
            player_name: The player name to evaluate.

        Returns:
            A match result dictionary for medium or high confidence matches, otherwise `None`.
        """
        if "/" in str(player_name):
            doubles_matches = self.check_doubles_pair(player_name)
            return doubles_matches[0] if doubles_matches else None

        normalised_input = self._normalise_name(player_name)
        if not normalised_input or not self.watchlist:
            return None

        for index, watchlist_name in enumerate(self.watchlist):
            if watchlist_name == normalised_input:
                return self._build_match_result(player_name, index, 100, "high")

        input_first_name, input_last_name = self._split_name(normalised_input)
        if not input_last_name:
            return None

        reversed_input_name = self._reverse_two_part_name(normalised_input)
        reversed_first_name = ""
        reversed_last_name = ""
        if reversed_input_name:
            reversed_first_name, reversed_last_name = self._split_name(reversed_input_name)

        for index, watchlist_name in enumerate(self.watchlist):
            watchlist_first_name, watchlist_last_name = self._split_name(watchlist_name)
            last_name_similarity = self._get_last_name_similarity(normalised_input, watchlist_name)
            reverse_last_name_similarity = (
                self._get_last_name_similarity(reversed_input_name, watchlist_name) if reversed_input_name else 0
            )
            if last_name_similarity < 70 and reverse_last_name_similarity < 70:
                continue

            if (
                input_last_name == watchlist_last_name
                and self._is_initial_match(input_first_name, watchlist_first_name)
            ):
                return self._build_match_result(player_name, index, 95, "high")
            if (
                reversed_input_name
                and reversed_last_name == watchlist_last_name
                and self._is_initial_match(reversed_first_name, watchlist_first_name)
            ):
                return self._build_match_result(player_name, index, 95, "high")

        for index, watchlist_name in enumerate(self.watchlist):
            watchlist_first_name, watchlist_last_name = self._split_name(watchlist_name)
            if input_last_name != watchlist_last_name:
                if not reversed_input_name or reversed_last_name != watchlist_last_name:
                    continue

            if int(round(fuzz.token_set_ratio(input_first_name, watchlist_first_name))) >= 80:
                return self._build_match_result(player_name, index, 90, "high")
            if (
                reversed_input_name
                and int(round(fuzz.token_set_ratio(reversed_first_name, watchlist_first_name))) >= 80
            ):
                return self._build_match_result(player_name, index, 90, "high")

        for index, watchlist_name in enumerate(self.watchlist):
            watchlist_first_name, _ = self._split_name(watchlist_name)
            last_name_similarity = self._get_last_name_similarity(normalised_input, watchlist_name)
            reverse_last_name_similarity = (
                self._get_last_name_similarity(reversed_input_name, watchlist_name) if reversed_input_name else 0
            )
            if last_name_similarity < 95 and reverse_last_name_similarity < 95:
                continue

            if self._is_initial_match(input_first_name, watchlist_first_name):
                return self._build_match_result(player_name, index, 85, "high")
            if reversed_input_name and self._is_initial_match(reversed_first_name, watchlist_first_name):
                return self._build_match_result(player_name, index, 85, "high")

        for index, watchlist_name in enumerate(self.watchlist):
            last_name_similarity = self._get_last_name_similarity(normalised_input, watchlist_name)
            reverse_last_name_similarity = (
                self._get_last_name_similarity(reversed_input_name, watchlist_name) if reversed_input_name else 0
            )
            full_name_similarity = int(round(fuzz.token_set_ratio(normalised_input, watchlist_name)))
            reverse_full_name_similarity = (
                int(round(fuzz.token_set_ratio(reversed_input_name, watchlist_name))) if reversed_input_name else 0
            )
            if (
                full_name_similarity >= 90 and last_name_similarity >= 85
            ) or (
                reverse_full_name_similarity >= 90 and reverse_last_name_similarity >= 85
            ):
                return self._build_match_result(player_name, index, 80, "medium")

        for index, watchlist_name in enumerate(self.watchlist):
            watchlist_first_name, watchlist_last_name = self._split_name(watchlist_name)
            if input_last_name != watchlist_last_name:
                if not reversed_input_name or reversed_last_name != watchlist_last_name:
                    continue

            if int(round(fuzz.partial_ratio(input_first_name, watchlist_first_name))) >= 75:
                return self._build_match_result(player_name, index, 75, "medium")
            if reversed_input_name and int(round(fuzz.partial_ratio(reversed_first_name, watchlist_first_name))) >= 75:
                return self._build_match_result(player_name, index, 75, "medium")

        return None

    def check_players(self, player_names: list[str]) -> list[dict]:
        """Check multiple player names against the watchlist.

        Parameters:
            player_names: A list of player names to evaluate.

        Returns:
            A list of match result dictionaries, excluding non-matches.
        """
        results: list[dict] = []
        for player_name in player_names:
            if "/" in str(player_name):
                results.extend(self.check_doubles_pair(player_name))
                continue

            match_result = self.check_player(player_name)
            if match_result is not None:
                results.append(match_result)
        return results

    def check_doubles_pair(self, doubles_name: str) -> list[dict]:
        """Check a Betfair doubles pair against the watchlist.

        Parameters:
            doubles_name: Betfair doubles pair name containing `/`.

        Returns:
            A list of doubles match dictionaries, potentially containing matches for one or both players.
        """
        if "/" not in str(doubles_name) or not self.watchlist:
            return []

        results: list[dict] = []
        seen_matches: set[str] = set()

        for raw_part in str(doubles_name).split("/"):
            part = self._normalise_name(raw_part)
            if not part:
                continue

            for watchlist_name in self.watchlist_original:
                match_result = self._check_truncated_surname(part, watchlist_name)
                if match_result is None:
                    continue

                if match_result["watchlist_name"] in seen_matches:
                    continue

                seen_matches.add(match_result["watchlist_name"])
                results.append(match_result)

        return results

    def _normalise_name(self, name: str) -> str:
        """Normalise a player name for matching.

        Parameters:
            name: Raw player name text.

        Returns:
            A cleaned, ASCII-normalised name retaining spaces and periods for initials.
        """
        if name is None:
            return ""

        cleaned_name = str(name).replace("\u00a0", " ").strip().lower()
        cleaned_name = unicodedata.normalize("NFKD", cleaned_name).encode("ASCII", "ignore").decode("ASCII")
        cleaned_name = re.sub(r"[^a-z0-9.\s]", "", cleaned_name)
        cleaned_name = re.sub(r"\s+", " ", cleaned_name).strip()
        return cleaned_name

    def _split_name(self, name: str) -> tuple[str, str]:
        """Split a name into first-name and surname components.

        Parameters:
            name: Player name to split.

        Returns:
            A tuple of `(first_name, last_name)` using everything after the first token as surname.
        """
        normalised_name = self._normalise_name(name)
        if not normalised_name:
            return "", ""

        parts = normalised_name.split()
        if len(parts) == 1:
            return "", parts[0]

        return parts[0].rstrip("."), " ".join(part.rstrip(".") for part in parts[1:])

    def _is_initial_match(self, name1: str, name2: str) -> bool:
        """Check whether two first-name values are exact or initial-compatible.

        Parameters:
            name1: First first-name value.
            name2: Second first-name value.

        Returns:
            `True` if the names are exact matches or one is the initial of the other.
        """
        clean_name1 = self._normalise_name(name1).rstrip(".")
        clean_name2 = self._normalise_name(name2).rstrip(".")

        if clean_name1 == clean_name2:
            return True

        if not clean_name1 or not clean_name2:
            return False

        return (
            len(clean_name1) == 1 and clean_name2.startswith(clean_name1)
        ) or (
            len(clean_name2) == 1 and clean_name1.startswith(clean_name2)
        )

    def _get_last_name_similarity(self, name1: str, name2: str) -> int:
        """Return the similarity score between two surnames.

        Parameters:
            name1: First full name.
            name2: Second full name.

        Returns:
            Token-set similarity score between the extracted surnames.
        """
        _, last_name1 = self._split_name(name1)
        _, last_name2 = self._split_name(name2)
        return int(round(fuzz.token_set_ratio(last_name1, last_name2)))

    def _check_truncated_surname(self, truncated: str, full_watchlist_name: str) -> dict | None:
        """Check whether a truncated doubles surname matches a watchlist surname.

        Parameters:
            truncated: Truncated surname fragment from a doubles market.
            full_watchlist_name: Full player name from the watchlist.

        Returns:
            A doubles match dictionary when the fragment matches strongly enough, otherwise `None`.
        """
        normalised_truncated = self._normalise_name(truncated).rstrip(".")
        if not normalised_truncated:
            return None

        _, full_surname = self._split_name(full_watchlist_name)
        full_surname = self._normalise_name(full_surname).rstrip(".")
        if not full_surname:
            return None

        if len(full_surname) == 0 or len(normalised_truncated) / len(full_surname) < 0.6:
            return None

        if not (
            full_surname.startswith(normalised_truncated)
            or normalised_truncated in full_surname
        ):
            return None

        corresponding_portion = full_surname[: len(normalised_truncated)]
        similarity = int(round(fuzz.token_set_ratio(normalised_truncated, corresponding_portion)))
        if similarity < 90:
            return None

        return {
            "matched": True,
            "input_name": truncated,
            "watchlist_name": full_watchlist_name,
            "confidence_score": 80,
            "confidence_level": "medium",
            "requires_verification": True,
            "match_type": "doubles",
        }

    def _reverse_two_part_name(self, name: str) -> str:
        """Reverse a two-part name to handle inputs entered as surname then first name.

        Parameters:
            name: Full normalised name.

        Returns:
            The reversed two-part name, or an empty string when reversal should not be attempted.
        """
        parts = self._normalise_name(name).split()
        if len(parts) != 2:
            return ""
        return f"{parts[1]} {parts[0]}"

    def _build_match_result(
        self,
        input_name: str,
        watchlist_index: int,
        confidence_score: int,
        confidence_level: str,
    ) -> dict:
        """Build a standard match result payload.

        Parameters:
            input_name: Original input player name.
            watchlist_index: Index of the matched watchlist player.
            confidence_score: Match confidence score.
            confidence_level: Match confidence level.

        Returns:
            A match result dictionary.
        """
        return {
            "matched": True,
            "input_name": input_name,
            "watchlist_name": self.watchlist_original[watchlist_index],
            "confidence_score": confidence_score,
            "confidence_level": confidence_level,
            "requires_verification": confidence_level == "medium",
        }

    def _find_player_column(self, columns) -> str | None:
        """Find the source column that contains player names.

        Parameters:
            columns: DataFrame column labels.

        Returns:
            The matching column name if found, otherwise `None`.
        """
        supported_names = {"player", "name"}
        for column in columns:
            if str(column).strip().lower() in supported_names:
                return column
        return None


if __name__ == "__main__":
    checker = IntegrityChecker("data/integrity_list.xlsx")

    print(f"Watchlist loaded: {len(checker.watchlist)} players")

    should_match = [
        ("Miljan Zekic", "Miljan Zekic"),
        ("M. Zekic", "Miljan Zekic"),
        ("M Zekic", "Miljan Zekic"),
        ("Zekic Miljan", "Miljan Zekic"),
        ("Nikola Milojevic", "Nikola Milojevic"),
        ("Conner Huertas Del Pino", "Conner Huertas Del Pino"),
        ("C. Huertas Del Pino", "Conner Huertas Del Pino"),
        ("A Zhurbin", "A Zhurbin"),
    ]

    should_not_match = [
        ("Anna Bondar", "Should not match Anna Morgina"),
        ("Maxim Mrva", "Should not match Maxime Hamou"),
        ("Victoria Hu", "Should not match Victor Nunez"),
        ("Madison Keys", "Should not match Maan Kesharwani"),
        ("Laurent Lokoli", "Should not match Y Laurent"),
        ("Tatjana Maria", "Should not match Anna Morgina"),
        ("Ivan Gakhov", "Should not match Simon Anthony Ivanov"),
        ("Dan Martin", "Should not match Martin Dimitrov"),
        ("Maxime Chazal", "Should not match Maxime Hamou"),
        ("Tiago Pereira", "Should not match Sergio Redondo Pereira"),
    ]

    doubles_tests = [
        ("Huertas Del/Huertas Del", ["Conner Huertas Del Pino", "Arklon Huertas Del Pino"]),
        ("Zekic/Partner", ["Miljan Zekic"]),
        ("Random/Player", []),
        ("Redondo Per/Someone", ["Sergio Redondo Pereira", "David Redondo Pereira"]),
    ]

    print("\n=== SHOULD MATCH ===")
    for name, expected in should_match:
        result = checker.check_player(name)
        if result:
            print(f"  OK {name} -> {result['watchlist_name']} ({result['confidence_score']}% - {result['confidence_level']})")
        else:
            print(f"  FAIL {name} -> NO MATCH (expected to match)")

    print("\n=== SHOULD NOT MATCH ===")
    for name, note in should_not_match:
        result = checker.check_player(name)
        if result:
            print(f"  FAIL {name} -> {result['watchlist_name']} ({result['confidence_score']}%) - FALSE POSITIVE")
        else:
            print(f"  OK {name} -> No match (correct)")

    print("\n=== DOUBLES MATCHES ===")
    for doubles_name, expected_matches in doubles_tests:
        if "/" in doubles_name:
            results = checker.check_doubles_pair(doubles_name)
            if results:
                print(f"  {doubles_name}:")
                for result in results:
                    print(f"    OK -> {result['watchlist_name']} ({result['confidence_score']}%)")
            else:
                if expected_matches:
                    print(f"  FAIL {doubles_name} -> No match (expected {expected_matches})")
                else:
                    print(f"  OK {doubles_name} -> No match (correct)")
        else:
            print(f"  Skipping non-doubles: {doubles_name}")
