"""Utility helpers for logging, path resolution, formatting, and display cleanup."""

from __future__ import annotations

import logging
import os
import re
from datetime import UTC, datetime


def setup_logging(log_dir: str = "logs", log_level: int = logging.INFO) -> logging.Logger:
    """Configure console and daily file logging for the scanner.

    Parameters:
        log_dir: Directory where log files should be written.
        log_level: Base logger level for the application logger.

    Returns:
        The configured `integrity_scanner` logger.
    """
    resolved_log_dir = resolve_path(log_dir)
    if not os.path.isabs(resolved_log_dir):
        resolved_log_dir = os.path.abspath(resolved_log_dir)

    os.makedirs(resolved_log_dir, exist_ok=True)

    logger = logging.getLogger("integrity_scanner")
    logger.setLevel(log_level)
    logger.propagate = False

    log_file_path = get_log_filepath(log_dir)

    if not logger.handlers:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(
            logging.Formatter(
                fmt="%(asctime)s | %(levelname)-8s | %(message)s",
                datefmt="%H:%M:%S",
            )
        )

        file_handler = logging.FileHandler(log_file_path, mode="a", encoding="utf-8")
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(
            logging.Formatter(
                fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )

        logger.addHandler(console_handler)
        logger.addHandler(file_handler)
    else:
        for handler in logger.handlers:
            if isinstance(handler, logging.FileHandler):
                handler.setLevel(logging.DEBUG)
            else:
                handler.setLevel(logging.INFO)

    logger.info("Logging initialised. Log file: %s", log_file_path)
    return logger


def get_log_filepath(log_dir: str = "logs") -> str:
    """Return the full path to today's scanner log file.

    Parameters:
        log_dir: Directory where log files are stored.

    Returns:
        The absolute path to today's log file.
    """
    resolved_log_dir = resolve_path(log_dir)
    if not os.path.isabs(resolved_log_dir):
        resolved_log_dir = os.path.abspath(resolved_log_dir)

    filename = f"scanner_{datetime.now(UTC).strftime('%Y-%m-%d')}.log"
    return os.path.join(resolved_log_dir, filename)


def resolve_path(relative_path: str) -> str:
    """Resolve a path from project root or when running from the `src` directory.

    Parameters:
        relative_path: Relative path to resolve.

    Returns:
        The first existing resolved path, or the original relative path if not found.
    """
    cwd_candidate = os.path.abspath(relative_path)
    if os.path.exists(cwd_candidate):
        return cwd_candidate

    script_dir = os.path.dirname(os.path.abspath(__file__))
    parent_candidate = os.path.abspath(os.path.join(script_dir, "..", relative_path))
    if os.path.exists(parent_candidate):
        return parent_candidate

    return relative_path


def format_datetime_utc(dt: datetime) -> str:
    """Format a datetime for display in UTC.

    Parameters:
        dt: Datetime value to format.

    Returns:
        A string in `YYYY-MM-DD HH:MM UTC` format, or `Unknown` if the input is missing.
    """
    if dt is None:
        return "Unknown"

    if dt.tzinfo is not None:
        dt = dt.astimezone(UTC)

    return dt.strftime("%Y-%m-%d %H:%M UTC")


def sanitise_for_log(text: str, max_length: int = 500) -> str:
    """Sanitise text for safer logging output.

    Parameters:
        text: Source text to clean.
        max_length: Maximum output length before truncation.

    Returns:
        A cleaned string with control characters removed and length capped.
    """
    if text is None:
        return ""

    cleaned_text = str(text).replace("\r", " ").replace("\n", " ").replace("\t", " ")
    cleaned_text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", "", cleaned_text)
    cleaned_text = re.sub(r"\s+", " ", cleaned_text).strip()

    if len(cleaned_text) > max_length:
        return f"{cleaned_text[: max_length - 3]}..."

    return cleaned_text


def clean_player_name(name: str) -> str:
    """Clean a player name for display purposes.

    Parameters:
        name: Player name text to clean.

    Returns:
        A stripped player name with normalised spaces.
    """
    if name is None:
        return ""

    cleaned_name = str(name).replace("\u00a0", " ").strip()
    cleaned_name = re.sub(r"\s+", " ", cleaned_name)
    return cleaned_name


if __name__ == "__main__":
    # Test logging setup
    logger = setup_logging()

    logger.debug("This is a DEBUG message (only in file)")
    logger.info("This is an INFO message")
    logger.warning("This is a WARNING message")
    logger.error("This is an ERROR message")

    print(f"\nLog file location: {get_log_filepath()}")

    # Test path resolution
    print("\nPath resolution test:")
    print(f"  'config/credentials.json' -> {resolve_path('config/credentials.json')}")
    print(f"  'data/integrity_list.xlsx' -> {resolve_path('data/integrity_list.xlsx')}")

    # Test datetime formatting
    from datetime import datetime, timezone

    print("\nDatetime formatting test:")
    print(f"  Now: {format_datetime_utc(datetime.now(timezone.utc))}")
    print(f"  None: {format_datetime_utc(None)}")

    # Test name cleaning
    print("\nName cleaning test:")
    print(f"  'Stefan Popovic\\u00a0' -> '{clean_player_name('Stefan Popovic\u00a0')}'")
    print(f"  '  Multiple   Spaces  ' -> '{clean_player_name('  Multiple   Spaces  ')}'")
