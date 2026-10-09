#!/usr/bin/env python3
"""Bounded read-only TLS stress test for Duco Connectivity Boards."""

import argparse
import asyncio
import csv
import errno
import math
import ssl
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import aiohttp

from duco_tls_report import export_report
from duco_tls_capture import CaptureError, run_with_capture


LOG_COLUMNS = [
    "timestamp", "phase", "protocol", "request",
    "http_status", "latency_ms", "error", "run_id", "repeat",
    "host", "endpoint",
    "record_type", "scenario", "exception_type", "error_stage",
    "dns_ms", "connect_ms", "queue_ms", "http_ms", "body_ms",
    "new_connections", "established_connections", "reused_connections",
    "active_requests", "batch_id", "recovery_id", "recovery_event",
    "recovery_elapsed_s", "recovery_success_streak", "recovery_failure_streak",
    "first_failure_utc", "first_success_utc", "confirmed_utc", "recovery_outcome",
    "batch_size",
    "request_start_utc", "interval_s", "capture_manifest",
]
DEFAULT_OUTPUT = Path(__file__).resolve().parent / "logs" / "duco_tls.csv"


@dataclass
class RequestTrace:
    stage: str = "http_request"
    starts: dict = field(default_factory=dict)
    durations: dict = field(default_factory=dict)
    new_connections: int = 0
    established_connections: int = 0
    reused_connections: int = 0

    def begin(self, stage):
        self.stage = stage
        self.starts[stage] = time.monotonic()

    def end(self, stage):
        if stage in self.starts:
            self.durations[stage] = self.durations.get(stage, 0) + (time.monotonic() - self.starts.pop(stage)) * 1000

    def finish(self):
        for stage in list(self.starts):
            self.end(stage)


def trace_config():
    trace = aiohttp.TraceConfig(trace_config_ctx_factory=lambda **kwargs: kwargs["trace_request_ctx"])

    async def dns_start(session, state, params):
        state.begin("dns")

    async def dns_end(session, state, params):
        state.end("dns")
        state.stage = "tcp_tls_connect"

    async def connect_start(session, state, params):
        if state.new_connections or state.reused_connections:
            state.end("http_request")
        else:
            state.starts.pop("http_request", None)
        state.new_connections += 1
        state.begin("tcp_tls_connect")

    async def connect_end(session, state, params):
        state.end("tcp_tls_connect")
        state.established_connections += 1
        state.begin("http_request")

    async def reuse(session, state, params):
        state.reused_connections += 1
        state.begin("http_request")

    async def queue_start(session, state, params):
        state.starts.pop("http_request", None)
        state.begin("queue")

    async def queue_end(session, state, params):
        state.end("queue")
        state.stage = "http_request"

    trace.on_dns_resolvehost_start.append(dns_start)
    trace.on_dns_resolvehost_end.append(dns_end)
    trace.on_connection_create_start.append(connect_start)
    trace.on_connection_create_end.append(connect_end)
    trace.on_connection_reuseconn.append(reuse)
    trace.on_connection_queued_start.append(queue_start)
    trace.on_connection_queued_end.append(queue_end)
    return trace


def error_stage(exc, state):
    if state.stage == "response_body":
        return "response_body"
    if isinstance(exc, ssl.SSLError) or isinstance(exc, (
        aiohttp.ClientSSLError, aiohttp.ClientConnectorCertificateError,
    )):
        return "tls_handshake"
    if isinstance(exc, aiohttp.ClientConnectorDNSError):
        return "dns"
    if isinstance(exc, aiohttp.ClientConnectorError):
        if isinstance(exc.os_error, ConnectionRefusedError) or exc.os_error.errno in {
            errno.ECONNREFUSED, errno.ENETUNREACH, errno.EHOSTUNREACH, errno.EADDRNOTAVAIL,
        }:
            return "tcp_connect"
        return state.stage
    return state.stage


def timestamp():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


class DucoStressTest:
    def __init__(self, args):
        self.args = args
        self.requests = 0
        self.failure_streak = 0
        self.samples = {}
        self.run_id = uuid4().hex
        self.repeat = 1
        self.scenario = "health"
        self.active_requests = 0
        self.batch_id = 0
        self.first_failure = None
        self.recovery_id = ""
        self.last_response_utc = ""
        self.last_request_completed_utc = ""
        self.logfile = Path(args.output)
        self.excel_file = Path(args.excel_output) if args.excel_output else self.logfile.with_suffix(".xlsx")
        self.next_excel = 0.0
        if not args.no_excel:
            if self.excel_file.resolve() == self.logfile.resolve():
                raise ValueError("Excel output cannot overwrite the CSV log")
            if self.excel_file.suffix.lower() != ".xlsx":
                raise ValueError("Excel output must have the .xlsx extension")
        self.logfile.parent.mkdir(parents=True, exist_ok=True)

        has_data = self.logfile.exists() and self.logfile.stat().st_size > 0
        if has_data:
            with self.logfile.open("r", newline="", encoding="utf-8") as existing:
                if next(csv.reader(existing), None) != LOG_COLUMNS:
                    raise ValueError(
                        f"Incompatible CSV header in {self.logfile}; "
                        "use a new --output path. Existing data was not changed."
                    )
            with self.logfile.open("rb") as existing:
                existing.seek(-1, 2)
                # Appending after an unfinished record would corrupt the next CSV row.
                if existing.read(1) != b"\n":
                    raise ValueError(
                        f"Incomplete last CSV record in {self.logfile}; "
                        "use a new --output path. Existing data was not changed."
                    )

        self.log = self.logfile.open(
            "a", newline="", encoding="utf-8", buffering=1
        )
        self.csv = csv.writer(self.log)
        if not has_data:
            self.csv.writerow(LOG_COLUMNS)

    def session(self, *, force_close=False, concurrency=1, timeout=None):
        connector = aiohttp.TCPConnector(
            ssl=getattr(self.args, "tls_context", False),  # Diagnostic only; device CA not trusted locally.
            force_close=force_close,
            limit=concurrency,
            limit_per_host=concurrency,
            use_dns_cache=True,
        )
        client = aiohttp.ClientSession(
            connector=connector,
            timeout=aiohttp.ClientTimeout(
                total=self.args.timeout if timeout is None else timeout,
                ceil_threshold=float("inf"),
            ),
            # Environment proxies could make us test their TLS instead of the device's.
            trust_env=False,
            headers={"User-Agent": "Duco-TLS-StressTest/1.0"},
            trace_configs=[trace_config()],
        )
        # aiohttp silently retries idempotent requests; that would hide the first disconnect.
        client._retry_connection = False
        return client

    async def request(self, session, phase, protocol="https"):
        self.requests += 1
        request_id = self.requests
        url = f"{protocol}://{self.args.host}/{self.args.endpoint}"
        request_start = timestamp()
        start = time.monotonic()
        status = 0
        error = ""
        exception_type = ""
        failed_stage = ""
        cancelled = False
        state = RequestTrace()
        state.begin("http_request")
        self.active_requests += 1
        active_requests = self.active_requests

        try:
            async with session.get(url, allow_redirects=False, trace_request_ctx=state) as response:
                state.end("http_request")
                status = response.status
                # Include body transfer in latency and allow the connection to be reused.
                state.begin("response_body")
                async for chunk in response.content.iter_chunked(16_384):
                    pass
                state.end("response_body")
                if status != 200:
                    error = f"Unexpected HTTP {status}"
                    failed_stage = "http_status"
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
            exception_type = type(exc).__name__
            failed_stage = error_stage(exc, state)
        except asyncio.CancelledError:
            error = "Cancelled after failure or interruption"
            exception_type = "CancelledError"
            failed_stage = state.stage
            cancelled = True
        finally:
            state.finish()
            self.active_requests -= 1

        latency = (time.monotonic() - start) * 1000
        success = status == 200 and not error
        completed = timestamp()
        self.last_request_completed_utc = completed
        if status and not cancelled:
            self.last_response_utc = completed
        if protocol == "https" and not success and not cancelled and self.first_failure is None:
            self.recovery_id = uuid4().hex
            self.first_failure = {
                "timestamp": completed, "monotonic": time.monotonic(),
                "scenario": self.scenario,
            }

        self.csv.writerow([
            completed, phase, protocol, request_id,
            status, round(latency, 2), error, self.run_id, self.repeat,
            self.args.host, self.args.endpoint,
            "cancelled" if cancelled else "request", self.scenario, exception_type, failed_stage,
            *[round(state.durations[stage], 2) if stage in state.durations else ""
              for stage in ("dns", "tcp_tls_connect", "queue", "http_request", "response_body")],
            state.new_connections, state.established_connections, state.reused_connections,
            active_requests, self.batch_id,
            self.recovery_id, *[""] * 8,
            self.args.concurrency if phase == "parallel" else 1,
            request_start, 10 if phase == "recovery_probe" else self.args.interval,
            getattr(self.args, "capture_manifest", ""),
        ])

        sample = self.samples.setdefault(phase, {
            "ok": 0, "failed": 0, "total_ms": 0.0, "max_ms": 0.0,
        })
        if success:
            sample["ok"] += 1
            sample["total_ms"] += latency
            sample["max_ms"] = max(sample["max_ms"], latency)
        else:
            sample["failed"] += 1

        if self.args.verbose or not success:
            print(
                f"[{timestamp()}] {phase:20} "
                f"{protocol.upper():5} "
                f"HTTP={status:3} "
                f"{latency:8.1f} ms "
                f"{error}",
                flush=True,
            )

        if cancelled:
            raise asyncio.CancelledError()
        return success

    async def health(self, name):
        print(f"\n=== HEALTH CHECK: {name} ===", flush=True)

        # A pooled TLS connection could mask failures to establish a new connection.
        async with self.session(force_close=True) as session:
            https_ok = await self.request(
                session, f"health_{name}", "https"
            )

        async with self.session(force_close=True) as session:
            http_ok = await self.request(
                session, f"health_{name}", "http"
            )

        print(
            f"HTTPS={'OK' if https_ok else 'FAILED'} | "
            f"HTTP={'OK' if http_ok else 'FAILED'}",
            flush=True,
        )

        if not https_ok:
            if http_ok:
                print(
                    "WARNING: HTTP works but HTTPS request fails; inspect error_stage.",
                    flush=True,
                )
            raise RuntimeError("HTTPS health check failed")

    def check_failures(self, results):
        if all(results):
            self.failure_streak = 0
        else:
            self.failure_streak += 1
            raise RuntimeError("HTTPS failed; stopping load immediately")

    async def phase_warm(self, seconds):
        print("\n=== PHASE 1: WARM SEQUENTIAL ===", flush=True)
        deadline = time.monotonic() + seconds

        async with self.session(concurrency=1) as session:
            while time.monotonic() < deadline:
                ok = await self.request(session, "warm_sequential")
                self.check_failures([ok])
                await asyncio.sleep(self.args.interval)

    async def phase_cold(self, seconds):
        print("\n=== PHASE 2: COLD TLS HANDSHAKES ===", flush=True)
        deadline = time.monotonic() + seconds

        while time.monotonic() < deadline:
            async with self.session(force_close=True) as session:
                ok = await self.request(session, "cold_handshakes")
            self.check_failures([ok])
            await asyncio.sleep(self.args.interval)

    async def phase_parallel(self, seconds):
        print("\n=== PHASE 3: PARALLEL REQUESTS ===", flush=True)
        deadline = time.monotonic() + seconds

        async with self.session(
            concurrency=self.args.concurrency
        ) as session:
            while time.monotonic() < deadline:
                self.check_failures(await self.parallel_batch(session))
                await asyncio.sleep(self.args.interval)

    async def parallel_batch(self, session):
        self.batch_id += 1
        tasks = [asyncio.create_task(self.request(session, "parallel"))
                 for _ in range(self.args.concurrency)]
        try:
            for finished in asyncio.as_completed(tasks):
                if not await finished:
                    raise RuntimeError("Parallel HTTPS failed; cancelling remaining requests")
            return [True]
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    def recovery_event(self, event, first_success, confirmed, successes, failures, outcome="", elapsed=None):
        failure = self.first_failure
        values = {
            "timestamp": timestamp(), "phase": "recovery", "protocol": "https",
            "run_id": self.run_id, "repeat": self.repeat,
            "host": self.args.host, "endpoint": self.args.endpoint,
            "record_type": "recovery", "scenario": failure["scenario"],
            "recovery_id": self.recovery_id, "recovery_event": event,
            "recovery_elapsed_s": round(time.monotonic() - failure["monotonic"] if elapsed is None else elapsed, 3),
            "recovery_success_streak": successes, "recovery_failure_streak": failures,
            "first_failure_utc": failure["timestamp"], "first_success_utc": first_success,
            "confirmed_utc": confirmed, "recovery_outcome": outcome,
            "capture_manifest": getattr(self.args, "capture_manifest", ""),
        }
        self.csv.writerow([values.get(column, "") for column in LOG_COLUMNS])
        self.log.flush()

    async def recovery(self):
        self.recovery_id = self.recovery_id or uuid4().hex
        successes = 0
        failures = 1
        first_success = ""
        confirmed = ""
        confirmed_elapsed = None
        deadline = self.first_failure["monotonic"] + getattr(self.args, "recovery_duration", 600)
        next_probe = self.first_failure["monotonic"] + 10
        self.recovery_event("started", first_success, confirmed, successes, failures)
        print("\n=== RECOVERY: load stopped; probing every 10 seconds ===", flush=True)
        try:
            while time.monotonic() < deadline:
                if next_probe >= deadline:
                    await asyncio.sleep(max(0, deadline - time.monotonic()))
                    break
                await asyncio.sleep(max(0, next_probe - time.monotonic()))
                probe_start = time.monotonic()
                if probe_start >= deadline:
                    break
                async with self.session(force_close=True, timeout=min(self.args.timeout, deadline - probe_start)) as session:
                    https_ok = await self.request(session, "recovery_probe", "https")
                https_completed = timestamp()
                https_elapsed = time.monotonic() - self.first_failure["monotonic"]
                if https_ok:
                    successes += 1
                    failures = 0
                    if not first_success:
                        first_success = https_completed
                        self.recovery_event("first_success", first_success, confirmed, successes, failures)
                    if successes >= 3:
                        confirmed = https_completed
                        confirmed_elapsed = https_elapsed
                else:
                    successes = 0
                    failures += 1
                remaining = deadline - time.monotonic()
                if remaining > 0:
                    async with self.session(force_close=True, timeout=min(self.args.timeout, remaining)) as session:
                        await self.request(session, "recovery_probe", "http")
                self.recovery_event("probe", first_success, confirmed, successes, failures)
                if confirmed:
                    self.recovery_event("recovered", first_success, confirmed, successes, failures, "temporary", confirmed_elapsed)
                    print("HTTPS recovered: three consecutive successful fresh connections", flush=True)
                    return True
                next_probe = probe_start + 10
            self.recovery_event("timeout", first_success, confirmed, successes, failures, "not_recovered")
            print("HTTPS not confirmed recovered within observation window", flush=True)
            return False
        except (asyncio.CancelledError, KeyboardInterrupt):
            self.recovery_event("interrupted", first_success, confirmed, successes, failures, "interrupted")
            raise
        finally:
            self.recovery_id = ""

    def summary(self):
        print("\n" + "=" * 65)
        print("TEST SUMMARY")
        print("=" * 65)

        for phase, data in self.samples.items():
            successes = data["ok"]
            failures = data["failed"]

            if successes:
                print(
                    f"{phase:23} "
                    f"OK={successes:5} "
                    f"FAIL={failures:4} "
                    f"AVG={data['total_ms'] / successes:8.1f}ms "
                    f"MAX={data['max_ms']:8.1f}ms"
                )
            else:
                print(
                    f"{phase:23} OK=0 FAIL={failures}"
                )

        print(f"\nTotal requests: {self.requests}")
        print(f"CSV log       : {self.logfile}")

    def update_excel(self, *, force=False):
        if self.args.no_excel or (not force and time.monotonic() < self.next_excel):
            return
        self.log.flush()
        try:
            requests = export_report([self.logfile], self.excel_file)
            print(f"Excel overview: {self.excel_file} ({requests} requests)", flush=True)
        except Exception as exc:
            print(
                f"WARNING: Excel update failed: {type(exc).__name__}: {exc}. "
                "CSV logging is unaffected; the previous Excel is retained.",
                flush=True,
            )
        self.next_excel = time.monotonic() + self.args.excel_interval

    def close(self):
        self.log.close()


async def run_test(args, *, capture=None):
    test = DucoStressTest(args)

    print("=" * 65)
    print("DUCO TLS STRESS TEST")
    print("=" * 65)
    print(f"Host          : {args.host}")
    print(f"Endpoint      : /{args.endpoint}")
    print(f"Duration      : {args.duration} seconds")
    print(f"Concurrency   : {args.concurrency}")
    print(f"Interval      : {args.interval} seconds")
    print(f"HTTPS stop    : first failure (legacy upper bound: {args.max_failures})")
    print(f"Scenario      : {getattr(args, 'scenario', 'all')}")
    print(f"Recovery      : {getattr(args, 'recovery_duration', 600)} seconds; 10-second probes")
    print(f"Resume        : {getattr(args, 'resume_after_recovery', False)}")
    print(f"Repeats       : {args.repeat or 'until stopped'}")
    print(f"Repeat delay  : {args.repeat_delay} seconds")
    print(f"Run ID        : {test.run_id}")
    print(f"CSV log       : {test.logfile}")
    print(f"Excel overview: {'disabled' if args.no_excel else test.excel_file}")
    print("Requests      : GET only")
    print("Certificate   : verification disabled (test only)")
    print("=" * 65)

    scenarios = {
        "warm-sequential": ("warm_sequential", test.phase_warm, "warm"),
        "cold-handshakes": ("cold_handshakes", test.phase_cold, "cold"),
        "parallel": ("parallel", test.phase_parallel, "parallel"),
    }
    selected = getattr(args, "scenario", "all")
    selected = list(scenarios) if selected == "all" else [selected]
    exit_code = 0
    try:
        if capture is not None:
            capture.metadata["run_id"] = test.run_id
            capture.write_manifest()
        while args.repeat == 0 or test.repeat <= args.repeat:
            print(f"\n=== REPEAT {test.repeat} ===", flush=True)
            test.first_failure = None
            test.failure_streak = 0
            test.scenario = scenarios[selected[0]][0]
            try:
                await test.health("before")
                for scenario in selected:
                    test.scenario, phase, name = scenarios[scenario]
                    await phase(args.duration / len(selected))
                    await test.health("after_" + name)
            except RuntimeError as exc:
                print(f"\nLOAD STOPPED: {exc}", flush=True)
                if test.first_failure is None:
                    raise
                recovered = await test.recovery()
                exit_code = 0 if recovered else 1
                if not recovered or not getattr(args, "resume_after_recovery", False):
                    break

            if args.repeat != 0 and test.repeat >= args.repeat:
                break
            test.update_excel()
            await asyncio.sleep(args.repeat_delay)
            test.repeat += 1

    except RuntimeError as exc:
        print(f"\nTEST STOPPED: {exc}", flush=True)
        exit_code = 1

    except (KeyboardInterrupt, asyncio.CancelledError):
        print("\nTEST STOPPED: interrupted by user", flush=True)
        exit_code = 130

    finally:
        try:
            if capture is not None:
                capture.metadata["last_response_utc"] = test.last_response_utc
                capture.metadata["last_request_completed_utc"] = test.last_request_completed_utc
                capture.metadata["requests_finished_utc"] = timestamp()
                capture.write_manifest()
            test.summary()
            test.update_excel(force=True)
        finally:
            test.close()
    return exit_code


async def main(args):
    try:
        if getattr(args, "tls_keylog", False) and not getattr(args, "capture", False):
            raise CaptureError("--tls-keylog requires --capture")
        if getattr(args, "capture", False):
            return await run_with_capture(args, run_test)
        return await run_test(args)
    except (CaptureError, OSError) as exc:
        print(f"TEST STOPPED: {exc}", flush=True)
        return 1
    except asyncio.CancelledError:
        print("TEST STOPPED: capture interrupted by user", flush=True)
        return 130


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Duco TLS read-only reliability test"
    )

    parser.add_argument("--host", default="192.168.3.94")
    parser.add_argument(
        "--endpoint",
        choices=["info", "nodes", "config"],
        default="info",
    )
    parser.add_argument("--duration", type=int, default=300)
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument("--timeout", type=float, default=8.0)
    parser.add_argument("--max-failures", type=int, default=2,
                        help="Legacy upper bound (1-5); HTTPS now stops on the first failure")
    parser.add_argument(
        "--scenario", choices=["all", "warm-sequential", "cold-handshakes", "parallel"],
        default="all", help="Run only the selected scenario, or all three",
    )
    parser.add_argument(
        "--recovery-duration", type=float, default=600,
        help="Maximum recovery observation seconds from the first HTTPS failure",
    )
    parser.add_argument(
        "--resume-after-recovery", action="store_true",
        help="Allow the next repeat after confirmed recovery; never restart the failed cycle",
    )
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--capture", action="store_true", help="Record device TCP traffic using tcpdump (requires existing capture permissions)")
    parser.add_argument("--capture-interface", default="any", help="tcpdump interface (default: any)")
    parser.add_argument("--capture-max-mb", type=int, default=32, help="Decimal MB per rotating capture file (1-1024)")
    parser.add_argument("--capture-files", type=int, default=4, help="Number of rotating capture files (1-16)")
    parser.add_argument("--capture-tail", type=float, default=10, help="Capture-only seconds after test completion (0-300, default: 10)")
    parser.add_argument("--tls-keylog", action="store_true", help="Save sensitive TLS session secrets in the capture directory (requires --capture)")
    parser.add_argument(
        "--repeat", type=int, default=1,
        help="Number of cycles; 0 repeats until stopped or a failure occurs",
    )
    parser.add_argument("--repeat-delay", type=float, default=30.0)
    parser.add_argument(
        "--excel-output",
        help="Excel overview path; default is the CSV path with .xlsx extension",
    )
    parser.add_argument(
        "--excel-interval", type=float, default=3600.0,
        help="Minimum seconds between Excel updates, checked between repeats",
    )
    parser.add_argument("--no-excel", action="store_true", help="Only write the CSV log")

    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT),
        help="Continuous CSV log; compatible existing logs are appended to",
    )

    args = parser.parse_args()

    if not 30 <= args.duration <= 1800:
        parser.error("Duration must be 30–1800 seconds")

    if not 1 <= args.concurrency <= 4:
        parser.error("Concurrency must be 1–4")

    if args.interval < 1:
        parser.error("Minimum interval is 1 second")

    if not 1 <= args.max_failures <= 5:
        parser.error("max-failures must be 1–5")

    if args.repeat < 0:
        parser.error("repeat must be 0 or greater")

    if args.repeat_delay < 0:
        parser.error("repeat-delay must be 0 or greater")

    if args.excel_interval <= 0:
        parser.error("excel-interval must be greater than 0")

    for name in ("interval", "timeout", "repeat_delay", "excel_interval", "recovery_duration"):
        if not math.isfinite(getattr(args, name)):
            parser.error(f"{name.replace('_', '-')} must be finite")
    if args.timeout <= 0 or args.recovery_duration <= 0:
        parser.error("timeout and recovery-duration must be greater than 0")
    if not 1 <= args.capture_max_mb <= 1024 or not 1 <= args.capture_files <= 16:
        parser.error("capture-max-mb must be 1-1024 and capture-files must be 1-16")
    if not math.isfinite(args.capture_tail) or not 0 <= args.capture_tail <= 300:
        parser.error("capture-tail must be finite and between 0 and 300 seconds")
    if args.tls_keylog and not args.capture:
        parser.error("--tls-keylog requires --capture")

    raise SystemExit(asyncio.run(main(args)))
