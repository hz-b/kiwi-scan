# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

"""
Running statistics

This module provides a tiny subset of the interface compatible to ``river.stats``.
Implemented statistics:
  * Mean: running arithmetic mean
  * Var: running variance
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


@dataclass
class Mean:
    """
    Running mean with optional weights.
    """

    n: float = 0.0
    _mean: float = 0.0

    def update(self, x: float, w: float = 1.0) -> "Mean":
        w = float(w)
        if w < 0.0:
            raise ValueError("w must be non-negative")
        if w == 0.0:
            return self

        x = float(x)
        total = self.n + w
        self._mean += (w / total) * (x - self._mean)
        self.n = total
        return self

    def revert(self, x: float, w: float = 1.0) -> "Mean":
        w = float(w)
        if w < 0.0:
            raise ValueError("w must be non-negative")
        if w == 0.0:
            return self
        if w > self.n:
            raise ValueError("Cannot revert more weight than has been observed")

        x = float(x)
        remaining = self.n - w
        if remaining <= 0.0:
            self.n = 0.0
            self._mean = 0.0
            return self

        self._mean = (self.n * self._mean - w * x) / remaining
        self.n = remaining
        return self

    def update_many(self, values: Iterable[float]) -> "Mean":
        for value in values:
            self.update(value)
        return self

    def get(self) -> float:
        return self._mean if self.n > 0.0 else 0.0


class Var:
    """
    Running variance, Compute variance online using the updated mean, 
    avoiding storage of all observations.

    Parameters
    ----------
    ddof:
        Delta degrees of freedom. The returned variance is ``M2 / (n - ddof)``
        when enough weighted samples have been observed, else ``0.0``.
    """

    def __init__(self, ddof: int = 1):
        self.ddof = int(ddof)
        self.mean = Mean()
        self._m2 = 0.0

    @property
    def n(self) -> float:
        return self.mean.n

    def update(self, x: float, w: float = 1.0) -> "Var":
        w = float(w)
        if w < 0.0:
            raise ValueError("w must be non-negative")
        if w == 0.0:
            return self

        x = float(x)
        prev_mean = self.mean.get()
        self.mean.update(x, w)

        delta = x - prev_mean
        delta2 = x - self.mean.get()
        self._m2 += w * delta * delta2

        if self._m2 < 0.0 and abs(self._m2) < 1e-15:
            self._m2 = 0.0

        return self

    def revert(self, x: float, w: float = 1.0) -> "Var":
        w = float(w)
        if w < 0.0:
            raise ValueError("w must be non-negative")
        if w == 0.0:
            return self
        if w > self.mean.n:
            raise ValueError("Cannot revert more weight than has been observed")

        x = float(x)
        if w == self.mean.n:
            self.mean = Mean()
            self._m2 = 0.0
            return self

        old_n = self.mean.n - w
        old_mean = (self.mean.n * self.mean.get() - w * x) / old_n
        self._m2 -= w * (x - old_mean) * (x - self.mean.get())
        self.mean.revert(x, w)

        if self._m2 < 0.0 and abs(self._m2) < 1e-15:
            self._m2 = 0.0

        return self

    def update_many(self, values: Iterable[float]) -> "Var":
        for value in values:
            self.update(value)
        return self

    def get(self) -> float:
        denom = self.mean.n - self.ddof
        if denom <= 0.0:
            return 0.0
        return self._m2 / denom


__all__ = ["Mean", "Var"]
