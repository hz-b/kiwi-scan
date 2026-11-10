# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

import threading
import unittest
from unittest.mock import Mock, patch

from kiwi_scan.datamodels import ConfigError, ScanTriggers, TriggerAction
from kiwi_scan.epics_wrapper import PutCompletion
from kiwi_scan.scan.trigger_manager import PreparedTriggerAction, TriggerManager


def make_action(trace, name="TRIG", put_timeout=1.0, delay=0.0, outcome="complete"):
    """Prepared action whose put callback behaves as ``outcome`` says."""
    pv = Mock()
    pv.pvname = name

    def put_with_completion(value, *, timeout):
        trace.append(("put_cb", name, value, timeout))
        completion = PutCompletion(name, timeout)
        if outcome == "complete":
            completion.callback()
        elif outcome == "fail":
            completion.fail()
        elif outcome == "unarmed":
            completion.mark_unarmed()
        # "never": the callback does not arrive.
        return completion

    def put(value):
        trace.append(("put", name, value))
        return True

    pv.put_with_completion.side_effect = put_with_completion
    pv.put.side_effect = put
    return PreparedTriggerAction(pv=pv, value=1, delay=delay, put_timeout=put_timeout)


class TestTriggerActionPutTimeout(unittest.TestCase):
    def test_default_is_no_wait(self):
        self.assertEqual(TriggerAction(pv="A", value=1).put_timeout, 0.0)

    def test_numeric_string_is_accepted(self):
        triggers = ScanTriggers.from_dict({"before": [{"pv": "A", "value": 1, "put_timeout": "2.5"}]})
        self.assertEqual(triggers.before[0].put_timeout, 2.5)

    def test_invalid_values_name_the_trigger_action(self):
        for bad in (-1, float("nan"), float("inf"), True, "fast", None):
            with self.subTest(put_timeout=bad), self.assertRaisesRegex(
                ConfigError, r"triggers\.on_point\[0\].*put_timeout"
            ):
                ScanTriggers.from_dict({"on_point": [{"pv": "A", "value": 1, "put_timeout": bad}]})


class TestTriggerManagerPutWait(unittest.TestCase):
    def fire(self, actions, stop_event=None, phase="on_point"):
        manager = TriggerManager(actions_by_phase={phase: actions})
        manager.fire(phase, stop_event=stop_event)

    def test_zero_put_timeout_keeps_plain_put(self):
        trace = []
        action = make_action(trace, put_timeout=0.0)

        self.fire([action])

        self.assertEqual(trace, [("put", "TRIG", 1)])
        self.assertNotIn("put_cb", [entry[0] for entry in trace])

    def test_completion_precedes_delay_and_next_action(self):
        trace = []
        actions = [
            make_action(trace, "ARM", put_timeout=2.0, delay=0.1),
            make_action(trace, "TRIG", put_timeout=0.0),
        ]

        with patch("kiwi_scan.scan.trigger_manager.time.sleep",
                   side_effect=lambda delay: trace.append(("sleep", delay))):
            self.fire(actions)

        self.assertEqual(trace, [
            ("put_cb", "ARM", 1, 2.0),
            ("sleep", 0.1),
            ("put", "TRIG", 1),
        ])

    def test_timeout_warns_once_and_phase_continues(self):
        trace = []
        actions = [
            make_action(trace, "SLOW", put_timeout=0.05, outcome="never"),
            make_action(trace, "NEXT", put_timeout=0.0),
        ]

        with self.assertLogs("kiwi_scan", level="WARNING") as logs:
            self.fire(actions)

        self.assertEqual([t[:2] for t in trace], [("put_cb", "SLOW"), ("put", "NEXT")])
        self.assertEqual(len(logs.records), 1)
        self.assertIn("SLOW", logs.output[0])

    def test_unaccepted_put_callback_warns_and_does_not_wait(self):
        trace = []
        actions = [make_action(trace, "DISC", put_timeout=60.0, outcome="unarmed")]

        with self.assertLogs("kiwi_scan.scan.trigger_manager", level="WARNING") as logs:
            self.fire(actions)

        self.assertIn("DISC", logs.output[0])

    def test_failed_submission_continues_with_next_action(self):
        trace = []
        actions = [
            make_action(trace, "BAD", put_timeout=60.0, outcome="fail"),
            make_action(trace, "NEXT", put_timeout=0.0),
        ]

        self.fire(actions)

        self.assertEqual([t[:2] for t in trace], [("put_cb", "BAD"), ("put", "NEXT")])

    def test_stop_request_ends_wait_but_writes_remaining_actions(self):
        trace = []
        stop_event = threading.Event()
        stop_event.set()
        actions = [
            make_action(trace, "SLOW", put_timeout=60.0, outcome="never"),
            make_action(trace, "RESET", put_timeout=0.0),
        ]

        finished = threading.Event()
        worker = threading.Thread(target=lambda: (self.fire(actions, stop_event), finished.set()))
        worker.start()
        worker.join(2.0)

        self.assertTrue(finished.is_set())
        self.assertEqual([t[:2] for t in trace], [("put_cb", "SLOW"), ("put", "RESET")])

    def test_from_config_passes_put_timeout_to_prepared_action(self):
        triggers = ScanTriggers.from_dict(
            {"before": [{"pv": "A", "value": 1, "put_timeout": 3.0}]}
        )
        with patch("kiwi_scan.scan.trigger_manager.EpicsPV") as pv_class:
            manager = TriggerManager.from_config(triggers)

        pv_class.assert_called_once_with("A", timeout=1.0, queueing_delay=0.0)
        self.assertEqual(manager._actions_by_phase["before"][0].put_timeout, 3.0)


if __name__ == "__main__":
    unittest.main()
