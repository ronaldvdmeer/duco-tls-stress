"""Optional bounded tcpdump capture; never acquires elevated privileges."""

import asyncio
import ipaddress
import json
import math
import os
import pwd
import shutil
import signal
import ssl
from collections import deque
from datetime import datetime, timezone
from pathlib import Path


class CaptureError(RuntimeError):
    pass


def utc_now():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


class PacketCapture:
    def __init__(self, args):
        self.args = args
        try:
            self.address = str(ipaddress.ip_address(args.host))
        except ValueError as exc:
            raise CaptureError("--capture requires --host to be a literal IP address without a port") from exc
        self.executable = shutil.which("tcpdump")
        if not self.executable:
            raise CaptureError("tcpdump is not installed; install it before using --capture")
        if not 1 <= args.capture_max_mb <= 1024 or not 1 <= args.capture_files <= 16:
            raise CaptureError("Capture limits: 1-1024 MB per file, 1-16 files")
        if not args.capture_interface or args.capture_interface.startswith("-"):
            raise CaptureError("Invalid capture interface")
        self.tail_seconds = getattr(args, "capture_tail", 10.0)
        if not math.isfinite(self.tail_seconds) or not 0 <= self.tail_seconds <= 300:
            raise CaptureError("capture-tail must be finite and between 0 and 300 seconds")
        self.directory = Path(args.output).with_suffix("").with_name(Path(args.output).stem + "_capture")
        self.manifest = self.directory / "capture.json"
        self.process = None
        self.reader = None
        self.ready = asyncio.Event()
        self.messages = deque(maxlen=20)
        self.metadata = {
            "host": self.address, "endpoint": args.endpoint,
            "csv": str(Path(args.output).resolve()),
            "excel": str(Path(args.excel_output).resolve()) if args.excel_output else str(Path(args.output).with_suffix(".xlsx").resolve()),
            "scenario": args.scenario, "interval_s": args.interval,
            "concurrency": args.concurrency, "duration_s": args.duration,
            "timeout_s": args.timeout, "recovery_duration_s": args.recovery_duration,
            "repeat": args.repeat, "resume_after_recovery": args.resume_after_recovery,
            "max_file_bytes": args.capture_max_mb * 1_000_000,
            "max_files": args.capture_files, "state": "starting",
            "rotation": "Oldest files are overwritten; a file may exceed its limit by one packet.",
            "capture_tail_s": self.tail_seconds,
            "tls_keylog_requested": getattr(args, "tls_keylog", False),
        }

    def prepare_keylog(self):
        if not hasattr(ssl.SSLContext, "keylog_filename"):
            raise CaptureError("This Python/OpenSSL client does not support TLS keylogging")
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        context.options |= ssl.OP_NO_COMPRESSION
        context.set_alpn_protocols(["http/1.1"])
        keyfile = self.directory / "tls.keys"
        descriptor = os.open(keyfile, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        os.close(descriptor)
        try:
            context.keylog_filename = str(keyfile.resolve())
        except (AttributeError, NotImplementedError) as exc:
            raise CaptureError("This Python/OpenSSL client cannot enable TLS keylogging") from exc
        self.metadata["tls_keylog_file"] = str(keyfile.resolve())
        self.metadata["tls_keylog_state"] = "enabled"
        self.metadata["tls_keylog_warning"] = "Sensitive: this file can decrypt captured HTTPS; share only intentionally."
        self.write_manifest()
        return context

    def write_manifest(self):
        self.metadata["tcpdump_messages"] = list(self.messages)
        temporary = self.directory / "capture.json.tmp"
        temporary.write_text(json.dumps(self.metadata, indent=2) + "\n", encoding="utf-8")
        temporary.replace(self.manifest)

    async def read_messages(self):
        while line := await self.process.stderr.readline():
            message = line.decode("utf-8", errors="replace").rstrip()
            self.messages.append(message)
            if "listening on " in message:
                self.metadata["capture_ready_utc"] = utc_now()
                self.ready.set()

    async def start(self):
        self.directory.parent.mkdir(parents=True, exist_ok=True)
        try:
            self.directory.mkdir(mode=0o700)
        except FileExistsError as exc:
            raise CaptureError(f"Capture directory already exists: {self.directory}; use a new --output") from exc
        command = [
            self.executable, "-i", self.args.capture_interface,
            "-n", "-s", "0", "-U", "-C", str(self.args.capture_max_mb),
            "-W", str(self.args.capture_files), "-Z", pwd.getpwuid(os.geteuid()).pw_name,
            "-w", str(self.directory / "traffic.pcap"),
            f"host {self.address} and tcp and (port 80 or port 443)",
        ]
        self.metadata["command"] = command
        self.metadata["capture_start_utc"] = utc_now()
        self.write_manifest()
        try:
            self.process = await asyncio.create_subprocess_exec(
                *command, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE,
                env={**os.environ, "LC_ALL": "C"},
            )
            self.reader = asyncio.create_task(self.read_messages())
            readiness = asyncio.create_task(self.ready.wait())
            ended = asyncio.create_task(self.process.wait())
            try:
                await asyncio.wait({readiness, ended}, timeout=5, return_when=asyncio.FIRST_COMPLETED)
                if not self.ready.is_set() or self.process.returncode is not None:
                    raise CaptureError("tcpdump did not become ready: " + " | ".join(self.messages))
            finally:
                for task in (readiness, ended):
                    if not task.done():
                        task.cancel()
                await asyncio.gather(readiness, ended, return_exceptions=True)
            self.metadata["state"] = "running"
            self.write_manifest()
        except BaseException:
            await self.stop(failed=True)
            raise

    async def watch(self):
        await self.process.wait()
        await asyncio.shield(self.reader)
        raise CaptureError("tcpdump stopped unexpectedly: " + " | ".join(self.messages))

    async def stop(self, *, failed=False):
        if self.process is not None and self.process.returncode is None:
            try:
                self.process.send_signal(signal.SIGINT)
            except ProcessLookupError:
                pass
            try:
                await asyncio.wait_for(self.process.wait(), timeout=5)
            except TimeoutError:
                self.process.kill()
                await self.process.wait()
                failed = True
        if self.reader is not None:
            await self.reader
        self.metadata["capture_end_utc"] = utc_now()
        self.metadata["exit_code"] = self.process.returncode if self.process else None
        failed = failed or self.metadata["exit_code"] not in (None, 0)
        self.metadata["state"] = "failed" if failed else "stopped"
        self.metadata["files"] = [
            {"path": path.name, "bytes": path.stat().st_size}
            for path in sorted(self.directory.glob("traffic.pcap*"))
        ]
        if "tls_keylog_file" in self.metadata:
            self.metadata["tls_keylog_bytes"] = Path(self.metadata["tls_keylog_file"]).stat().st_size
        self.write_manifest()


async def run_with_capture(args, run_test):
    capture = PacketCapture(args)
    await capture.start()
    args.capture_manifest = str(capture.manifest.resolve())
    print(f"Packet capture: {capture.directory} (bounded rotating PCAP)", flush=True)
    test_task = None
    tail_task = None
    watch_task = asyncio.create_task(capture.watch())
    failed = False
    try:
        if getattr(args, "tls_keylog", False):
            args.tls_context = capture.prepare_keylog()
            print(f"TLS keylog: {capture.metadata['tls_keylog_file']} (sensitive; permissions 0600)", flush=True)
        test_task = asyncio.create_task(run_test(args, capture=capture))
        finished, pending = await asyncio.wait({test_task, watch_task}, return_when=asyncio.FIRST_COMPLETED)
        if watch_task in finished:
            failed = True
            await watch_task
        result = await test_task
        capture.metadata["test_completed_utc"] = utc_now()
        if result != 130 and capture.tail_seconds:
            capture.metadata["tail_start_utc"] = utc_now()
            capture.metadata["state"] = "tail"
            capture.write_manifest()
            print(f"Capture tail: {capture.tail_seconds:g} seconds; no further requests", flush=True)
            tail_task = asyncio.create_task(asyncio.sleep(capture.tail_seconds))
            finished, pending = await asyncio.wait({tail_task, watch_task}, return_when=asyncio.FIRST_COMPLETED)
            if watch_task in finished:
                failed = True
                await watch_task
            await tail_task
            capture.metadata["tail_end_utc"] = utc_now()
    except BaseException:
        failed = True
        raise
    finally:
        for task in (test_task, tail_task):
            if task is not None:
                if not task.done():
                    task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        watch_task.cancel()
        await asyncio.gather(watch_task, return_exceptions=True)
        await capture.stop(failed=failed)
    return 1 if capture.metadata["state"] == "failed" else result