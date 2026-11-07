"""Deliberately SYNTHETIC fault injection for demos and tests.

Nothing here observes or simulates a real outage: it just makes the COMMIT of chosen files raise a
clearly labelled exception so the retry / rollback / recovery paths can be exercised on purpose.
Never enabled by `run` or `backfill`; only `demo` and the test-suite install it.
"""
from __future__ import annotations

from .errors import TransientError
from .warehouse.base import Checkpoint, SqlWarehouse


class InjectedFault(TransientError):
    """Raised by FaultPlan. Retryable on purpose, like a dropped connection would be."""


class FaultPlan:
    """Make the COMMIT of selected source files fail `n` times each.

    plan = {"orders_batch2.csv": 1}   -> that file's first commit attempt fails, the retry succeeds
    plan = {"orders_batch3.jsonl": 99} -> effectively always fails (retry exhaustion)

    mode="before": the exception is raised instead of committing (nothing reaches the database).
    mode="after":  the real COMMIT runs and THEN the exception is raised, imitating an ambiguous
                   network commit whose acknowledgement was lost.
    """

    def __init__(self, wh: SqlWarehouse, plan: dict[str, int], mode: str = "before"):
        if mode not in ("before", "after"):
            raise ValueError("mode must be 'before' or 'after'")
        self.wh, self.remaining, self.mode = wh, dict(plan), mode
        self.injected: list[str] = []
        self._current: str | None = None
        self._real_commit, self._real_put = wh._commit, wh.put_checkpoint

    def install(self) -> "FaultPlan":
        def put_checkpoint(cp: Checkpoint) -> None:
            self._current = cp.source_name
            self._real_put(cp)

        def commit() -> None:
            name, self._current = self._current, None
            if name is not None and self.remaining.get(name, 0) > 0:
                self.remaining[name] -= 1
                self.injected.append(name)
                if self.mode == "after":
                    self._real_commit()
                raise InjectedFault(f"SYNTHETIC injected commit fault for {name} (mode={self.mode}); not a real outage")
            self._real_commit()

        self.wh.put_checkpoint, self.wh._commit = put_checkpoint, commit  # type: ignore[method-assign]
        return self

    def remove(self) -> None:
        self.wh.__dict__.pop("put_checkpoint", None)
        self.wh.__dict__.pop("_commit", None)
