import asyncio
import csv
import importlib.util
import io
import sys
import tempfile
import types
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import AsyncMock, patch


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
spec = importlib.util.spec_from_file_location("duco_tls_stress", ROOT / "duco_tls_stress.py")
stress = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = stress
spec.loader.exec_module(stress)


def arguments(output, **overrides):
    values = dict(
        output=str(output), host="example.invalid", endpoint="info",
        verbose=False, duration=30, concurrency=4, interval=1,
        timeout=8, max_failures=2, repeat=1, repeat_delay=0,
        excel_output=None, excel_interval=3600, no_excel=True,
    )
    values.update(overrides)
    return types.SimpleNamespace(**values)


class Response:
    status = 200

    @property
    def content(self):
        return self

    async def iter_chunked(self, size):
        yield await self.read()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    async def read(self):
        return b"{}"


class Session:
    def get(self, *args, **kwargs):
        return Response()


class LoggingTests(unittest.TestCase):
    def test_append_preserves_rows_and_writes_one_header(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "log.csv"
            first = stress.DucoStressTest(arguments(output))
            asyncio.run(first.request(Session(), "warm_sequential"))
            first.close()
            original = output.read_bytes()
            second = stress.DucoStressTest(arguments(output))
            second.repeat = 2
            asyncio.run(second.request(Session(), "cold_handshakes"))
            second.close()
            self.assertTrue(output.read_bytes().startswith(original))
            with output.open(newline="", encoding="utf-8") as source:
                rows = list(csv.DictReader(source))
            self.assertEqual(len(rows), 2)
            self.assertNotEqual(rows[0]["run_id"], rows[1]["run_id"])
            self.assertEqual(rows[1]["repeat"], "2")
            self.assertEqual(rows[1]["host"], "example.invalid")

    def test_incompatible_header_is_not_overwritten(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "log.csv"
            output.write_text("timestamp,phase\nold,data\n", encoding="utf-8")
            original = output.read_bytes()
            with self.assertRaisesRegex(ValueError, "Incompatible"):
                stress.DucoStressTest(arguments(output))
            self.assertEqual(output.read_bytes(), original)

    def test_incomplete_record_is_not_appended_to(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "log.csv"
            output.write_text(
                ",".join(stress.LOG_COLUMNS) + "\npartial", encoding="utf-8"
            )
            original = output.read_bytes()
            with self.assertRaisesRegex(ValueError, "Incomplete"):
                stress.DucoStressTest(arguments(output))
            self.assertEqual(output.read_bytes(), original)

    def test_body_read_failure_counts_even_after_http_200(self):
        with tempfile.TemporaryDirectory() as directory:
            test = stress.DucoStressTest(arguments(Path(directory) / "log.csv"))
            with patch.object(Response, "read", AsyncMock(side_effect=OSError("lost connection"))), \
                 redirect_stdout(io.StringIO()):
                success = asyncio.run(test.request(Session(), "parallel"))
            test.close()
            self.assertFalse(success)
            self.assertEqual(test.samples["parallel"]["failed"], 1)

    def test_summary_uses_bounded_counters(self):
        with tempfile.TemporaryDirectory() as directory:
            test = stress.DucoStressTest(arguments(Path(directory) / "log.csv"))
            for request_number in range(50):
                asyncio.run(test.request(Session(), "parallel"))
            test.close()
            self.assertEqual(test.samples["parallel"]["ok"], 50)
            self.assertFalse(any(isinstance(value, list) for value in test.samples["parallel"].values()))

    def test_repeats_share_log_and_request_sequence(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "log.csv"

            async def health(test, name):
                await test.request(Session(), "health_" + name)

            with patch.object(stress.DucoStressTest, "health", health), \
                 patch.object(stress.DucoStressTest, "phase_warm", AsyncMock()), \
                 patch.object(stress.DucoStressTest, "phase_cold", AsyncMock()), \
                 patch.object(stress.DucoStressTest, "phase_parallel", AsyncMock()), \
                 redirect_stdout(io.StringIO()):
                asyncio.run(stress.main(arguments(output, repeat=2)))
            with output.open(newline="", encoding="utf-8") as source:
                rows = list(csv.DictReader(source))
            self.assertEqual([row["repeat"] for row in rows], ["1"] * 4 + ["2"] * 4)
            self.assertEqual(len({row["run_id"] for row in rows}), 1)
            self.assertEqual([int(row["request"]) for row in rows], list(range(1, 9)))

    def test_failure_does_not_start_another_repeat(self):
        with tempfile.TemporaryDirectory() as directory:
            health = AsyncMock(side_effect=[RuntimeError("offline"), None])
            warm = AsyncMock()
            with patch.object(stress.DucoStressTest, "health", health), \
                 patch.object(stress.DucoStressTest, "phase_warm", warm), \
                 redirect_stdout(io.StringIO()):
                asyncio.run(stress.main(arguments(Path(directory) / "log.csv", repeat=0)))
            self.assertEqual(health.await_count, 1)
            warm.assert_not_awaited()

    def test_excel_update_is_throttled_and_final_update_is_forced(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "log.csv"
            test = stress.DucoStressTest(arguments(output, no_excel=False))
            with patch.object(stress, "export_report", return_value=1) as export, \
                 redirect_stdout(io.StringIO()):
                test.update_excel()
                test.update_excel()
                self.assertEqual(export.call_count, 1)
                test.update_excel(force=True)
                self.assertEqual(export.call_count, 2)
                export.assert_called_with([output], output.with_suffix(".xlsx"))
            test.close()

    def test_excel_failure_does_not_stop_csv_logging(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "log.csv"
            test = stress.DucoStressTest(arguments(output, no_excel=False))
            with patch.object(stress, "export_report", side_effect=PermissionError("locked")), \
                 redirect_stdout(io.StringIO()):
                test.update_excel(force=True)
            self.assertTrue(asyncio.run(test.request(Session(), "parallel")))
            test.close()
            with output.open(newline="", encoding="utf-8") as source:
                self.assertEqual(len(list(csv.DictReader(source))), 1)

    def test_final_excel_update_also_happens_on_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "log.csv"
            with patch.object(stress.DucoStressTest, "health", AsyncMock(side_effect=RuntimeError("offline"))), \
                 patch.object(stress, "export_report", return_value=0) as export, \
                 redirect_stdout(io.StringIO()):
                asyncio.run(stress.main(arguments(output, no_excel=False, repeat=0)))
            export.assert_called_once_with([output], output.with_suffix(".xlsx"))

    def test_user_cancel_creates_final_report(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "log.csv"
            with patch.object(stress.DucoStressTest, "health", AsyncMock(side_effect=asyncio.CancelledError())), \
                 patch.object(stress, "export_report", return_value=0) as export, \
                 redirect_stdout(io.StringIO()):
                asyncio.run(stress.main(arguments(output, no_excel=False, repeat=0)))
            export.assert_called_once()

    def test_excel_output_cannot_replace_csv(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "log.csv"
            with self.assertRaisesRegex(ValueError, "cannot overwrite"):
                stress.DucoStressTest(arguments(output, no_excel=False, excel_output=str(output)))
            self.assertFalse(output.exists())


if __name__ == "__main__":
    unittest.main()