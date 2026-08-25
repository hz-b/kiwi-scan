# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

from abc import ABC, abstractmethod


class BaseMonitor(ABC):
    @abstractmethod
    def start(self, signal_names, headers=None):
        """Handle a new data point."""
    
    @abstractmethod
    def update(self, vals):
        """Handle a new data point."""

    @abstractmethod
    def loop(self):
        """Handle data in main thread."""

    @abstractmethod
    def close(self):
        """Handle cleanup (e.g., closing the plot window)."""

