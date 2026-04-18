# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

from kiwi_scan.monitor.base import BaseMonitor 

# TODO: relyable nicely formated data output, not to be changed across versions
class PrintMonitor(BaseMonitor):
    def start(self, signal_names):
        print("Signals:", signal_names)
        self.data = []

    def update(self, vals):
        values_only = [v['value'] for v in vals]
        self.data.append(values_only)
        print("Monitor:", values_only)

    def loop(self):
        return

    def close(self):
        # Optionally print all collected values or do something with self.data
        print("Collected data:", self.data)

