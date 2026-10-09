import io
import json
import shutil
import ssl
import subprocess
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from aiohttp import web

import duco_tls_capture as capture
import duco_tls_stress as stress
from test_capture import arguments


class TLSKeylogTests(unittest.IsolatedAsyncioTestCase):
    async def test_local_tls_handshakes_write_secrets_for_all_session_patterns(self):
        openssl = shutil.which("openssl")
        if not openssl or not hasattr(ssl.SSLContext, "keylog_filename"):
            self.skipTest("Local TLS verification requires openssl and SSLContext keylogging")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            certificate = root / "certificate.pem"
            private_key = root / "private.pem"
            subprocess.run([
                openssl, "req", "-x509", "-newkey", "rsa:2048", "-nodes",
                "-keyout", str(private_key), "-out", str(certificate),
                "-days", "1", "-subj", "/CN=localhost",
            ], check=True, capture_output=True)
            for version in (ssl.TLSVersion.TLSv1_2, ssl.TLSVersion.TLSv1_3):
                with self.subTest(version=version.name):
                    args = arguments(root / f"{version.name}.csv", tls_keylog=True)
                    with patch.object(capture.shutil, "which", return_value="/usr/bin/tcpdump"):
                        recorder = capture.PacketCapture(args)
                    recorder.directory.mkdir(mode=0o700)
                    args.tls_context = recorder.prepare_keylog()
                    self.assertEqual(args.tls_context.verify_mode, ssl.CERT_NONE)
                    self.assertFalse(args.tls_context.check_hostname)
                    keyfile = recorder.directory / "tls.keys"
                    self.assertEqual(keyfile.stat().st_mode & 0o777, 0o600)
                    server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
                    server_context.load_cert_chain(certificate, private_key)
                    server_context.minimum_version = version
                    server_context.maximum_version = version
                    application = web.Application()

                    async def info(request):
                        return web.json_response({"local": True})

                    application.router.add_get("/info", info)
                    runner = web.AppRunner(application)
                    await runner.setup()
                    site = web.TCPSite(runner, "127.0.0.1", 0, ssl_context=server_context)
                    await site.start()
                    args.host = f"127.0.0.1:{site._server.sockets[0].getsockname()[1]}"
                    args.verbose = False
                    test = stress.DucoStressTest(args)
                    try:
                        async with test.session() as session:
                            self.assertTrue(await test.request(session, "warm_sequential"))
                            self.assertTrue(await test.request(session, "warm_sequential"))
                        for phase in ("cold_handshakes", "recovery_probe"):
                            async with test.session(force_close=True) as session:
                                self.assertTrue(await test.request(session, phase))
                    finally:
                        test.close()
                        await runner.cleanup()
                    records = [line.split() for line in keyfile.read_text().splitlines() if line and not line.startswith("#")]
                    label = "CLIENT_RANDOM" if version == ssl.TLSVersion.TLSv1_2 else "CLIENT_HANDSHAKE_TRAFFIC_SECRET"
                    self.assertEqual(sum(record[0] == label for record in records), 3)
                    self.assertTrue(all(len(record) == 3 for record in records))
                    self.assertTrue(all(len(record[1]) == 64 for record in records))
                    metadata = json.loads(recorder.manifest.read_text())
                    self.assertEqual(metadata["tls_keylog_state"], "enabled")
                    self.assertEqual(Path(metadata["tls_keylog_file"]), keyfile.resolve())
                    self.assertNotIn("CLIENT_RANDOM", recorder.manifest.read_text())

    async def test_existing_keylog_and_symlink_are_never_overwritten(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for symlink in (False, True):
                with self.subTest(symlink=symlink):
                    with patch.object(capture.shutil, "which", return_value="/usr/bin/tcpdump"):
                        recorder = capture.PacketCapture(arguments(root / f"test_{symlink}.csv"))
                    recorder.directory.mkdir(mode=0o700)
                    target = root / f"existing_{symlink}"
                    target.write_text("unchanged", encoding="utf-8")
                    keyfile = recorder.directory / "tls.keys"
                    if symlink:
                        keyfile.symlink_to(target)
                    else:
                        keyfile.write_text("unchanged", encoding="utf-8")
                    with self.assertRaises(FileExistsError):
                        recorder.prepare_keylog()
                    self.assertEqual(target.read_text(), "unchanged")
                    self.assertEqual(keyfile.read_text(), "unchanged")

    async def test_keylogging_is_opt_in_and_requires_capture(self):
        with tempfile.TemporaryDirectory() as directory:
            args = arguments(Path(directory) / "test.csv", capture=False, tls_keylog=True)
            with patch.object(stress, "run_test") as run, redirect_stdout(io.StringIO()):
                self.assertEqual(await stress.main(args), 1)
            run.assert_not_called()
            self.assertFalse(Path(args.output).exists())


if __name__ == "__main__":
    unittest.main()