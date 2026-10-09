import asyncio
import io
import json
import signal
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import duco_tls_capture as capture


def arguments(output, **overrides):
    values = dict(output=str(output), host="192.168.3.94", endpoint="info",
                  excel_output=None, no_excel=True, scenario="cold-handshakes",
                  interval=5, concurrency=1, duration=30, timeout=8,
                  repeat=1, resume_after_recovery=False, recovery_duration=90,
                  capture=True, capture_interface="any", capture_max_mb=1, capture_files=2)
    values.update(capture_tail=0, tls_keylog=False)
    values.update(overrides)
    return SimpleNamespace(**values)


class Process:
    def __init__(self, *, ready=True):
        self.returncode = None
        self.stderr = asyncio.StreamReader()
        self.completed = asyncio.get_running_loop().create_future()
        self.signals = []
        if ready:
            self.stderr.feed_data(b"tcpdump: listening on any, link-type LINUX_SLL2\n")

    def exit(self, code):
        self.returncode = code
        self.stderr.feed_eof()
        if not self.completed.done():
            self.completed.set_result(code)

    async def wait(self):
        return await asyncio.shield(self.completed)

    def send_signal(self, value):
        self.signals.append(value)
        self.stderr.feed_data(b"12 packets captured\n0 packets dropped by kernel\n")
        self.exit(0)

    def kill(self):
        self.exit(-9)


class CaptureTests(unittest.IsolatedAsyncioTestCase):
    async def test_ready_capture_is_filtered_bounded_and_stops_cleanly(self):
        with tempfile.TemporaryDirectory() as directory:
            args = arguments(Path(directory) / "test.csv")
            process = Process()
            with patch.object(capture.shutil, "which", return_value="/usr/bin/tcpdump"), \
                 patch.object(capture.asyncio, "create_subprocess_exec", AsyncMock(return_value=process)) as create:
                recorder = capture.PacketCapture(args)
                await recorder.start()
                command = create.call_args.args
                self.assertEqual(command[command.index("-C") + 1], "1")
                self.assertEqual(command[command.index("-W") + 1], "2")
                self.assertEqual(command[-1], "host 192.168.3.94 and tcp and (port 80 or port 443)")
                self.assertNotIn("sudo", command)
                self.assertEqual(create.call_args.kwargs["env"]["LC_ALL"], "C")
                self.assertEqual(recorder.directory.stat().st_mode & 0o777, 0o700)
                (recorder.directory / "traffic.pcap0").write_bytes(b"pcap")
                await recorder.stop()
            metadata = json.loads(recorder.manifest.read_text())
            self.assertEqual(metadata["state"], "stopped")
            self.assertEqual(metadata["max_file_bytes"], 1_000_000)
            self.assertEqual(metadata["files"], [{"path": "traffic.pcap0", "bytes": 4}])
            self.assertEqual(process.signals, [signal.SIGINT])
            self.assertIn("capture_ready_utc", metadata)
            self.assertIn("capture_end_utc", metadata)
            self.assertIn("0 packets dropped by kernel", metadata["tcpdump_messages"])

    async def test_permission_failure_never_starts_requests(self):
        with tempfile.TemporaryDirectory() as directory:
            args = arguments(Path(directory) / "test.csv")
            process = Process(ready=False)
            process.stderr.feed_data(b"tcpdump: You do not have permission to capture\n")
            process.exit(1)
            run_test = AsyncMock()
            with patch.object(capture.shutil, "which", return_value="/usr/bin/tcpdump"), \
                 patch.object(capture.asyncio, "create_subprocess_exec", AsyncMock(return_value=process)), \
                 self.assertRaisesRegex(capture.CaptureError, "permission"):
                await capture.run_with_capture(args, run_test)
            run_test.assert_not_awaited()
            manifest = Path(directory) / "test_capture" / "capture.json"
            self.assertEqual(json.loads(manifest.read_text())["state"], "failed")
            self.assertFalse(Path(args.output).exists())

    async def test_missing_ready_signal_stops_capture_before_requests(self):
        with tempfile.TemporaryDirectory() as directory:
            args = arguments(Path(directory) / "test.csv")
            process = Process(ready=False)
            run_test = AsyncMock()
            with patch.object(capture.shutil, "which", return_value="/usr/bin/tcpdump"), \
                 patch.object(capture.asyncio, "create_subprocess_exec", AsyncMock(return_value=process)), \
                 patch.object(capture.asyncio, "wait", AsyncMock(return_value=(set(), set()))), \
                 self.assertRaisesRegex(capture.CaptureError, "ready"):
                await capture.run_with_capture(args, run_test)
            run_test.assert_not_awaited()
            self.assertEqual(process.signals, [signal.SIGINT])

    async def test_unexpected_capture_exit_cancels_load(self):
        with tempfile.TemporaryDirectory() as directory:
            args = arguments(Path(directory) / "test.csv")
            process = Process()
            stopped = asyncio.Event()

            async def run_test(args, *, capture):
                try:
                    process.exit(1)
                    await asyncio.Event().wait()
                finally:
                    stopped.set()

            with patch.object(capture.shutil, "which", return_value="/usr/bin/tcpdump"), \
                 patch.object(capture.asyncio, "create_subprocess_exec", AsyncMock(return_value=process)), \
                 redirect_stdout(io.StringIO()), self.assertRaisesRegex(capture.CaptureError, "unexpectedly"):
                await capture.run_with_capture(args, run_test)
            self.assertTrue(stopped.is_set())
            metadata = json.loads((Path(directory) / "test_capture" / "capture.json").read_text())
            self.assertEqual(metadata["state"], "failed")

    async def test_test_completion_propagates_result_and_stops_capture(self):
        with tempfile.TemporaryDirectory() as directory:
            args = arguments(Path(directory) / "test.csv")
            process = Process()
            run_test = AsyncMock(return_value=1)
            with patch.object(capture.shutil, "which", return_value="/usr/bin/tcpdump"), \
                 patch.object(capture.asyncio, "create_subprocess_exec", AsyncMock(return_value=process)), \
                 redirect_stdout(io.StringIO()):
                result = await capture.run_with_capture(args, run_test)
            self.assertEqual(result, 1)
            self.assertTrue(Path(args.capture_manifest).exists())
            self.assertEqual(process.signals, [signal.SIGINT])

    async def test_messages_are_bounded_in_memory_and_manifest(self):
        with tempfile.TemporaryDirectory() as directory:
            process = Process()
            for number in range(100):
                process.stderr.feed_data(f"message {number}\n".encode())
            with patch.object(capture.shutil, "which", return_value="/usr/bin/tcpdump"), \
                 patch.object(capture.asyncio, "create_subprocess_exec", AsyncMock(return_value=process)):
                recorder = capture.PacketCapture(arguments(Path(directory) / "test.csv"))
                await recorder.start()
                await recorder.stop()
            self.assertEqual(len(recorder.messages), 20)
            self.assertEqual(len(json.loads(recorder.manifest.read_text())["tcpdump_messages"]), 20)

    async def test_rejects_existing_files_and_invalid_config(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "test.csv"
            existing = Path(directory) / "test_capture"
            existing.mkdir()
            (existing / "traffic.pcap0").write_bytes(b"original")
            with patch.object(capture.shutil, "which", return_value="/usr/bin/tcpdump"):
                recorder = capture.PacketCapture(arguments(output))
                with self.assertRaisesRegex(capture.CaptureError, "already exists"):
                    await recorder.start()
                for overrides in [dict(host="example.invalid"), dict(host="192.168.3.94:443"),
                                  dict(capture_max_mb=0), dict(capture_files=17), dict(capture_interface="-i")]:
                    with self.subTest(overrides=overrides), self.assertRaises(capture.CaptureError):
                        capture.PacketCapture(arguments(output, **overrides))
            self.assertEqual((existing / "traffic.pcap0").read_bytes(), b"original")

    async def test_missing_tool_fails_clearly(self):
        with tempfile.TemporaryDirectory() as directory:
            with patch.object(capture.shutil, "which", return_value=None), self.assertRaisesRegex(capture.CaptureError, "not installed"):
                capture.PacketCapture(arguments(Path(directory) / "test.csv"))

    async def test_cancellation_stops_capture(self):
        with tempfile.TemporaryDirectory() as directory:
            args = arguments(Path(directory) / "test.csv")
            process = Process()
            started = asyncio.Event()

            async def run_test(args, *, capture):
                started.set()
                await asyncio.Event().wait()

            with patch.object(capture.shutil, "which", return_value="/usr/bin/tcpdump"), \
                 patch.object(capture.asyncio, "create_subprocess_exec", AsyncMock(return_value=process)), redirect_stdout(io.StringIO()):
                task = asyncio.create_task(capture.run_with_capture(args, run_test))
                await started.wait()
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
            self.assertEqual(process.signals, [signal.SIGINT])
            self.assertTrue((Path(directory) / "test_capture" / "capture.json").exists())

    async def test_tail_keeps_capture_alive_without_new_requests(self):
        for result in (0, 1):
            with self.subTest(result=result), tempfile.TemporaryDirectory() as directory:
                args = arguments(Path(directory) / "test.csv", capture_tail=15)
                process = Process()
                run_test = AsyncMock(return_value=result)
                tail_observed = []

                async def tail(seconds):
                    tail_observed.append(seconds)
                    self.assertIsNone(process.returncode)
                    self.assertEqual(process.signals, [])
                    metadata = json.loads(Path(args.capture_manifest).read_text())
                    self.assertEqual(metadata["state"], "tail")
                    self.assertIn("test_completed_utc", metadata)

                with patch.object(capture.shutil, "which", return_value="/usr/bin/tcpdump"), \
                     patch.object(capture.asyncio, "create_subprocess_exec", AsyncMock(return_value=process)), \
                     patch.object(capture.asyncio, "sleep", tail), redirect_stdout(io.StringIO()):
                    self.assertEqual(await capture.run_with_capture(args, run_test), result)
                self.assertEqual(tail_observed, [15])
                run_test.assert_awaited_once()
                self.assertEqual(process.signals, [signal.SIGINT])
                metadata = json.loads(Path(args.capture_manifest).read_text())
                self.assertEqual(metadata["capture_tail_s"], 15)
                self.assertIn("tail_end_utc", metadata)

    async def test_capture_failure_during_tail_is_not_silently_ignored(self):
        with tempfile.TemporaryDirectory() as directory:
            args = arguments(Path(directory) / "test.csv", capture_tail=15)
            process = Process()

            async def tail(seconds):
                process.exit(1)
                await asyncio.Event().wait()

            with patch.object(capture.shutil, "which", return_value="/usr/bin/tcpdump"), \
                 patch.object(capture.asyncio, "create_subprocess_exec", AsyncMock(return_value=process)), \
                 patch.object(capture.asyncio, "sleep", tail), redirect_stdout(io.StringIO()), \
                 self.assertRaisesRegex(capture.CaptureError, "unexpectedly"):
                await capture.run_with_capture(args, AsyncMock(return_value=0))
            self.assertEqual(json.loads(Path(args.capture_manifest).read_text())["state"], "failed")

    async def test_interrupted_test_skips_tail(self):
        with tempfile.TemporaryDirectory() as directory:
            args = arguments(Path(directory) / "test.csv", capture_tail=15)
            process = Process()
            with patch.object(capture.shutil, "which", return_value="/usr/bin/tcpdump"), \
                 patch.object(capture.asyncio, "create_subprocess_exec", AsyncMock(return_value=process)), \
                 patch.object(capture.asyncio, "sleep", AsyncMock()) as tail, redirect_stdout(io.StringIO()):
                self.assertEqual(await capture.run_with_capture(args, AsyncMock(return_value=130)), 130)
            tail.assert_not_awaited()
            self.assertEqual(process.signals, [signal.SIGINT])

    async def test_keylog_unsupported_aborts_before_requests_and_stops_capture(self):
        with tempfile.TemporaryDirectory() as directory:
            args = arguments(Path(directory) / "test.csv", tls_keylog=True)
            process = Process()
            run_test = AsyncMock()
            with patch.object(capture.shutil, "which", return_value="/usr/bin/tcpdump"), \
                 patch.object(capture.asyncio, "create_subprocess_exec", AsyncMock(return_value=process)), \
                 patch.object(capture.ssl, "SSLContext", type("UnsupportedContext", (), {})), \
                 redirect_stdout(io.StringIO()), self.assertRaisesRegex(capture.CaptureError, "does not support"):
                await capture.run_with_capture(args, run_test)
            run_test.assert_not_awaited()
            self.assertEqual(process.signals, [signal.SIGINT])
            self.assertEqual(json.loads(Path(args.capture_manifest).read_text())["state"], "failed")

    async def test_invalid_tail_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            for value in (-1, 301, float("nan"), float("inf")):
                with self.subTest(value=value), patch.object(capture.shutil, "which", return_value="/usr/bin/tcpdump"), \
                     self.assertRaisesRegex(capture.CaptureError, "capture-tail"):
                    capture.PacketCapture(arguments(Path(directory) / "test.csv", capture_tail=value))

    async def test_keylogging_context_is_passed_to_requests_and_manifest_contains_only_path(self):
        with tempfile.TemporaryDirectory() as directory:
            args = arguments(Path(directory) / "test.csv", tls_keylog=True)
            process = Process()

            async def run_test(args, *, capture):
                self.assertIsNotNone(args.tls_context.keylog_filename)
                self.assertEqual(Path(args.tls_context.keylog_filename), capture.directory.resolve() / "tls.keys")
                capture.metadata["run_id"] = "local-test"
                capture.write_manifest()
                return 0

            with patch.object(capture.shutil, "which", return_value="/usr/bin/tcpdump"), \
                 patch.object(capture.asyncio, "create_subprocess_exec", AsyncMock(return_value=process)), redirect_stdout(io.StringIO()):
                self.assertEqual(await capture.run_with_capture(args, run_test), 0)
            metadata = json.loads(Path(args.capture_manifest).read_text())
            self.assertEqual(metadata["run_id"], "local-test")
            self.assertEqual(metadata["tls_keylog_state"], "enabled")
            self.assertGreater(metadata["tls_keylog_bytes"], 0)


if __name__ == "__main__":
    unittest.main()