"""Bounded retry with exponential backoff. Sleep is injectable for tests."""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Callable, TypeVar

T = TypeVar("T")


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 3
    base_delay: float = 0.5
    max_delay: float = 8.0
    sleep: Callable[[float], None] = time.sleep

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1 (it counts the first try)")


def with_retries(fn: Callable[[], T], policy: RetryPolicy, is_transient: Callable[[BaseException], bool],
                 on_retry: Callable[[int, BaseException], None] | None = None) -> tuple[T, int]:
    """Run fn; retry only transient errors. Returns (result, attempts_used)."""
    attempt = 0
    while True:
        attempt += 1
        try:
            return fn(), attempt
        except Exception as exc:  # noqa: BLE001 - classified below
            if attempt >= policy.max_attempts or not is_transient(exc):
                raise
            if on_retry:
                on_retry(attempt, exc)
            policy.sleep(min(policy.max_delay, policy.base_delay * 2 ** (attempt - 1)))
