import logging

class RangeExitDetector:
    def __init__(self, start, stop, eps=0.0, out_threshold=3):
        self.start = start
        self.stop = stop
        self.eps = eps
        self.out_threshold = out_threshold

        self.entered = False
        self.out_counter = 0

        # determine scan direction
        self.forward = stop > start

    def _in_range(self, pos, start, stop):
        return start <= pos <= stop if start <= stop else stop <= pos <= start

    def _past_end(self, pos):
        if self.forward:
            return pos > self.stop + self.eps
        else:
            return pos < self.stop - self.eps

    def update(self, pos):
        """
        Returns True if scan should stop.
        """

        # 1. Detect entering the scan range
        if self._in_range(pos, self.start, self.stop):
            self.entered = True
            self.out_counter = 0
            return False

        # 2. Only consider exit AFTER we were inside
        if not self.entered:
            return False

        # 3. Direction-aware exit condition
        if self._past_end(pos):
            self.out_counter += 1
            logging.debug(
                "[RangeExitDetector] EXIT candidate pos=%s (counter=%d/%d)",
                pos, self.out_counter, self.out_threshold
            )
            if self.out_counter >= self.out_threshold:
                return True
        else:
            # still near boundary → reset (handles oscillation)
            self.out_counter = 0

        return False
