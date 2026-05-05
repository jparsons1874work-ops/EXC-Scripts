"""Reusable retry decorator for transient failures such as network and API errors.

Usage example:
    @retry(max_attempts=3, delay_seconds=1.0)
    def fetch_data():
        return external_call()
"""

from __future__ import annotations

import functools
import logging
import time

logger = logging.getLogger(__name__)


def retry(
    max_attempts: int = 3,
    delay_seconds: float = 2.0,
    backoff_multiplier: float = 2.0,
    exceptions: tuple = (Exception,),
):
    """Retry a function when it raises selected exceptions.

    Parameters:
        max_attempts: Maximum number of attempts before failing.
        delay_seconds: Initial delay between retry attempts.
        backoff_multiplier: Multiplier applied to the delay after each failure.
        exceptions: Exception types that should trigger a retry.

    Returns:
        A decorator that retries the wrapped function and re-raises the final exception on failure.
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            delay = delay_seconds
            last_exception = None

            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as exc:
                    last_exception = exc
                    if attempt >= max_attempts:
                        break

                    logger.warning(
                        "Attempt %d/%d failed: %s. Retrying in %.1fs...",
                        attempt,
                        max_attempts,
                        exc,
                        delay,
                    )
                    time.sleep(delay)
                    delay *= backoff_multiplier

            logger.error("All %d attempts failed for %s", max_attempts, func.__name__)
            raise last_exception

        return wrapper

    return decorator


if __name__ == "__main__":
    import random

    @retry(max_attempts=3, delay_seconds=0.5)
    def flaky_function():
        if random.random() < 0.7:
            raise ValueError("Random failure!")
        return "Success!"

    print("Testing retry decorator (may take a few attempts)...")
    for i in range(3):
        try:
            result = flaky_function()
            print(f"  Test {i + 1}: {result}")
        except ValueError as error:
            print(f"  Test {i + 1}: Failed after all retries - {error}")
