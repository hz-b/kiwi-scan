# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

import logging
from kiwi_scan.scan.common import BaseScan
from kiwi_scan.datamodels import ScanConfig
from typing import List, Dict

class ApproachMove(BaseScan):
    """
    A class to perform Anna's DCM move sequence
    """
    def generate_series(self, a, b, N, k):
        """
        Generates a series of N terms starting from 'a' and ending at 'b' using the formula:
        f(n) = a + (b - a) * ((n - 1) / (N - 1)) ** k

        Parameters:
        a (float): Starting value of the range.
        b (float): Ending value of the range.
        N (int): Total number of terms in the series.
        k (float): Parameter that controls the progression.

        Returns:
        list: A list containing the series values.
        """
        series = [a + (b - a) * ((n - 1) / (N - 1)) ** k for n in range(1, N + 1)]
        return series

    def __init__(self, config: ScanConfig, data_dir=None):
        super().__init__(config, data_dir)
        if not self.scan_dimensions:
            raise ValueError("No scan_dimensions in config – nothing to do.")

        # build one positions array per actuator
        self.positions: Dict[str, List[float]] = {}
        for name in self.cfg.actuators:
            # find the matching ScanDimension
            dim = next((d for d in self.scan_dimensions if d.actuator == name), None)
            if dim is None:
                raise ValueError(f"No ScanDimension for actuator '{name}'")
            if dim.steps == 1:
                raise ValueError(f"ScanDimension steps must not be 1")
            # generate that axis’ series:  progression experimentally determined
            self.positions[name] = self.generate_series(dim.start, dim.stop, dim.steps, 0.096)

        if self.debug:
            for name, pos in self.positions.items():
                print(f"[DEBUG] {name}: {len(pos)} points from "
                      f"{self.cfg.scan_dimensions}: {pos[:5]} …")

    def execute(self):
        """
        Execute the move over the pre-defined positions.
        """
        self.scan(self.positions)

