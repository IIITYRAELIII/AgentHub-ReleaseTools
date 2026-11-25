import importlib.util
import json
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch


SCRIPT = Path(__file__).resolve().parents[1] / "python" / "continue_release.py"
spec = importlib.util.spec_from_file_location("continue_release", SCRIPT)
release = importlib.util.module_from_spec(spec)
spec.loader.exec_module(release)


@unittest.skipUnless(sys.platform == "win32", "Windows worker and file lock")
class ContinuationTest(unittest.TestCase):
    def setUp(self):
        scratch = TemporaryDirectory()
        self.addCleanup(scratch.cleanup)
        self.root = Path(scratch.name)
        self.first = self.root / "first"
        self.second = self.root / "second"
        self.runtime = self.root / "runtime"
        self.runtime.mkdir()
        self.connection = self.root / "connection.json"
        release.save(self.connection, {"contextToken": "fixture-capability"})
        release.save(self.runtime / "current.json", {"current": "old"})
        self.first.mkdir()
        self.previous = self.job("first", self.first)
        release.save(self.first / "INPUT.json", self.previous)
        self.report = {"operationId": "first", "status": "failed", "message": "first failed"}
        release.save(self.first / "REPORT.json", self.report)
        self.state = self.first / "CONTINUATION.json"
        release.save(self.state, release.selected_attempt(self.previous))
        self.receipt = {
            "id": "old-item", "clientUserMessageId": "mcp:first:continue", "streamId": "fixture",
            "queued": True, "commandState": "queued",
        }

    def job(self, operation, directory):
        return {
            "operation_id": operation, "directory": str(directory), "continuation_directory": str(self.first),
            "stream": "fixture", "runtime_root": str(self.runtime), "commit": "fixture", "connection_file": str(self.connection),
        }

    def admit(self, desktop):
        installer = SimpleNamespace(native_api=lambda: (None, None, SimpleNamespace(GetCurrentProcess=lambda: 0)),
                                    process_identity=lambda process: {"image": sys.executable})
        with (patch.object(release, "desktop", side_effect=desktop),
              patch.object(release, "git", return_value=b"# installer fixture\n"),
              patch.object(release, "load_installer", return_value=installer)):
            release.prepare_attempt(self.job("second", self.second), self.second, self.root, self.first)

    def test_restart_readback_never_infers_cancellation(self):
        release.save(self.first / "QUEUE-INTENT.json", {"clientUserMessageId": "mcp:first:continue"})
        for snapshot in (None, {"messageReceipts": []}, {"messageReceipts": [
                {**self.receipt, "commandState": "dispatching", "dispatching": True}]}):
            with self.subTest(snapshot=snapshot):
                actions = []

                def desktop(job, action, params, command_id=None):
                    actions.append(action)
                    self.assertEqual(params["messageReceiptIds"], ["mcp:first:continue"])
                    return snapshot

                with self.assertRaisesRegex(RuntimeError, "first:continue; retry was not admitted"):
                    self.admit(desktop)
                self.assertEqual(actions, ["snapshot"])
                self.assertEqual(release.read(self.state), release.selected_attempt(self.previous))
                self.assertFalse(self.second.exists())

        def accepted(job, action, params, command_id=None):
            self.assertEqual(action, "snapshot")
            return {"messageReceipts": [{**self.receipt, "commandState": "started", "providerAccepted": True}]}

        self.admit(accepted)
        outcome = release.read(self.second / "PREDECESSOR.json")["notification"]
        self.assertTrue(outcome["providerAccepted"])
        self.assertFalse(outcome.get("cancelled"))
        with patch.object(release, "desktop") as desktop:
            release.deliver(self.previous, self.first, self.report)
            desktop.assert_not_called()
        self.assertEqual(release.read(self.first / "REPORT.json"), self.report)

    def test_retry_admitted_before_notification_fences_old_worker(self):
        def no_network(*args):
            self.fail("Retiring an attempt that never called Hub needs no network")

        self.admit(no_network)
        self.assertEqual(release.read(self.second / "PREDECESSOR.json")["notification"], {
            "clientUserMessageId": "mcp:first:continue", "notSubmitted": True,
        })
        with patch.object(release, "desktop", side_effect=no_network):
            release.deliver(self.previous, self.first, self.report)
        self.assertFalse((self.first / "QUEUE-INTENT.json").exists())

    def test_readiness_refusal_does_not_turn_into_an_uncertain_send(self):
        def stopped(delay):
            raise InterruptedError

        with (patch.object(release, "ready", return_value=True),
              patch.object(release, "desktop", return_value={"retry": True}) as desktop,
              patch.object(release.time, "sleep", side_effect=stopped)):
            with self.assertRaises(InterruptedError):
                release.deliver(self.previous, self.first, self.report)
            self.assertEqual(desktop.call_args.args[1], "release-continuation")
        self.assertFalse((self.first / "QUEUE-INTENT.json").exists())

    def test_cancellation_ack_lost_uses_the_original(self):
        release.save(self.first / "QUEUE-INTENT.json", {"clientUserMessageId": "mcp:first:continue"})
        replies = iter([
            ("snapshot", {"messageReceipts": [self.receipt]}),
            ("deferred-input", None),
            ("snapshot", {"messageReceipts": [{**self.receipt, "cancelled": True, "commandState": "cancelled"}]}),
        ])

        def desktop(job, action, params, command_id=None):
            expected, response = next(replies)
            self.assertEqual(action, expected)
            if action == "deferred-input":
                self.assertEqual(params, {"action": "cancel", "id": "old-item", "clientUserMessageId": "mcp:first:continue"})
            return response

        self.admit(desktop)
        self.assertIsNone(next(replies, None))
        self.assertTrue(release.read(self.second / "PREDECESSOR.json")["notification"]["cancelled"])

    def test_other_process_cannot_submit_after_retry_selected(self):
        entered = self.root / "entered"
        proceed = self.root / "proceed"
        program = """
import importlib.util, json, sys, time
from pathlib import Path
script, folder, entered, proceed, receipt = sys.argv[1:]
spec = importlib.util.spec_from_file_location("continue_release", script)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
def desktop(job, action, params, command_id):
    assert action == "release-continuation"
    assert params["requestId"] == "first:continue" and command_id == "mcp:first:continue"
    assert params["contextToken"] == "fixture-capability"
    Path(entered).touch()
    for _ in range(500):
        if Path(proceed).exists():
            return {"delivery": json.loads(receipt)}
        time.sleep(.02)
    raise RuntimeError("test gate did not open")
module.desktop = desktop
module.ready = lambda job: True
folder = Path(folder)
module.deliver(module.read(folder / "INPUT.json"), folder, module.read(folder / "REPORT.json"))
"""
        child = subprocess.Popen(
            [sys.executable, "-B", "-c", program, str(SCRIPT), str(self.first),
             str(entered), str(proceed), json.dumps(self.receipt)],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        self.addCleanup(child.communicate, timeout=15)
        self.addCleanup(proceed.touch)
        for _ in range(300):
            if entered.exists() or child.poll() is not None:
                break
            time.sleep(.02)
        self.assertTrue(entered.exists())
        self.assertEqual(release.read(self.first / "QUEUE-INTENT.json"), {"clientUserMessageId": "mcp:first:continue"})
        settled = threading.Event()
        errors = []

        def desktop(job, action, params, command_id=None):
            # Readback must see the original receipt committed by the other process.
            self.assertEqual(release.read(self.first / "QUEUE-RECEIPT.json"), self.receipt)
            if action == "snapshot":
                return {"messageReceipts": [self.receipt]}
            self.assertEqual(action, "deferred-input")
            return {**params, "cancelled": True}

        def retry():
            try:
                self.admit(desktop)
            except Exception as error:
                errors.append(error)
            finally:
                settled.set()

        contender = threading.Thread(target=retry, daemon=True)
        contender.start()
        try:
            self.assertFalse(settled.wait(.3))
            self.assertFalse(self.second.exists())
        finally:
            proceed.touch()
            contender.join(10)
        self.assertTrue(settled.is_set())
        self.assertEqual(errors, [])
        _, stderr = child.communicate(timeout=5)
        self.assertEqual(child.returncode, 0, stderr)
        self.assertEqual(release.read(self.state), release.selected_attempt(self.job("second", self.second)))
        self.assertTrue(release.read(self.second / "PREDECESSOR.json")["notification"]["cancelled"])
