# Duco TLS Stress Test

[![Python 3.10+](https://img.shields.io/badge/Python-3.10%2B-3776AB?logo=python&logoColor=white)](#installation)
[![Platform: Linux](https://img.shields.io/badge/Platform-Linux-555555?logo=linux&logoColor=white)](#installation)
[![Requests: GET only](https://img.shields.io/badge/Requests-GET%20only-238636)](#safety-limits)
[![Reports: CSV + XLSX](https://img.shields.io/badge/Reports-CSV%20%2B%20XLSX-00796B)](#csv-and-excel)

A bounded, read-only reliability test for Duco Connectivity Boards.
Compare connection patterns, record HTTPS failures, and observe whether the
board recovers on its own. Results are available as a CSV source log and a
formatted Excel report, with optional packet capture and TLS session secrets.

> [!WARNING]
> Only test devices you are authorized to access. Certificate verification
> is disabled for diagnostics. Even GET requests place load on the device;
> do not run multiple stress tests at the same time.

## Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [Comparing Scenarios](#comparing-scenarios)
- [Recovery Monitoring](#recovery-monitoring)
- [CSV and Excel](#csv-and-excel)
- [Packet Capture](#packet-capture)
- [TLS Session Secrets](#tls-session-secrets)
- [Interpreting Diagnostics](#interpreting-diagnostics)
- [Long-Running Tests](#long-running-tests)
- [Options](#options)
- [Tests](#tests)
- [Sharing Results](#sharing-results)

## Installation

The commands below are for Linux with Bash. Run them from the project directory
after cloning or downloading the repository.

| Requirement | Purpose |
| --- | --- |
| Python 3.10 or newer, with `venv` and `pip` | Stress test and Excel export |
| `aiohttp` and `openpyxl` | Installed through the requirements file |
| `tcpdump` with capture permissions | Only for `--capture` |
| Python/OpenSSL with `SSLContext.keylog_filename` | Only for `--tls-keylog` |
| Wireshark | Optional packet analysis and decryption |

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
```

Python dependencies and supported versions are listed in
[requirements.txt](requirements.txt). An existing process still uses the old
code: stop it cleanly with Ctrl+C before starting an updated version.

## Quick Start

### Full Diagnostic Test

Set `--host` to your board's IP address. This example runs all three scenarios
once and automatically creates a formatted Excel report:

```bash
.venv/bin/python duco_tls_stress.py \
  --host 192.168.3.94 \
  --endpoint info \
  --scenario all \
  --duration 300 \
  --concurrency 4 \
  --interval 1 \
  --repeat 1 \
  --recovery-duration 600 \
  --output "logs/duco_tls_diagnostic_$(date +%Y%m%d_%H%M%S).csv" \
  --verbose
```

The 300-second load duration is divided between the three scenarios. At the
first HTTPS failure, load stops and recovery is observed for up to 600 seconds.
The stress test **does not automatically resume** after recovery. Health
checks, recovery monitoring, and export time are additional to the load duration.

The example uses a timestamped filename for each run. An `.xlsx` report with
the same base name is created alongside the CSV source log.

### Safety Limits

- GET requests only; no POST/PATCH requests and no automatic device reboots.
- At most four concurrent requests and at least a one-second pause between batches.
- Stop immediately at the first HTTPS failure, including HTTP 200 with a body-read error.
- Cancel outstanding parallel requests; requests already sent cannot be undone.
- Configurable load duration of 30 to 1800 seconds per cycle.

## Comparing Scenarios

| `--scenario` | Connection Pattern |
| --- | --- |
| `warm-sequential` | Sequential requests with at most one pooled connection; reuse where possible |
| `cold-handshakes` | Sequential requests, each using a fresh connection |
| `parallel` | Batches of up to the configured `--concurrency` |
| `all` | All three scenarios in sequence; the default |

Run a scenario independently to isolate connection behavior:

```bash
.venv/bin/python duco_tls_stress.py --scenario warm-sequential --output logs/warm_diagnostics.csv
.venv/bin/python duco_tls_stress.py --scenario cold-handshakes --output logs/cold_diagnostics.csv
.venv/bin/python duco_tls_stress.py --scenario parallel --concurrency 4 \
  --duration 30 --recovery-duration 600 --output logs/parallel_diagnostics.csv
```

With `all`, `--duration` is divided equally; a single scenario receives the
entire duration. Fresh HTTPS and HTTP health checks are performed before and
after each scenario that runs. Cold requests remain sequential even if
`--concurrency` is set above 1.

### New Connection Frequency

Disconnects can also occur with sequential cold requests; concurrency is not
a necessary condition. Compare cold tests with pauses of 1, 5, and 10 seconds,
using the same firmware, endpoint, and initial conditions. Use separate output
paths and do not run the tests simultaneously.

```bash
.venv/bin/python duco_tls_stress.py \
  --host 192.168.3.94 --endpoint info \
  --scenario cold-handshakes --duration 300 --concurrency 1 \
  --interval 5 --repeat 1 --recovery-duration 600 \
  --output "logs/duco_cold_i5_$(date +%Y%m%d_%H%M%S).csv" --verbose
```

For a 10-second pause, change `--interval 5` to `--interval 10` and
`duco_cold_i5_` to `duco_cold_i10_`. The interval is a pause **after** the
response: a two-second request with interval 5 produces roughly seven seconds
between request starts. Recovery probes have their own schedule.

A short, error-free run does not prove the problem has disappeared. Also
compare failures per new connection: longer pauses produce fewer connection
attempts within the same test duration.

## Recovery Monitoring

Load stops after the first HTTPS failure. The recovery monitor performs a
fresh HTTPS check followed by a fresh HTTP check every ten seconds. The first
probe starts ten seconds after the observed failure. If a probe pair takes
longer, the next pair starts only after it finishes; probes never overlap.

Recovery requires **three consecutive complete HTTPS responses with HTTP 200**.
An HTTPS failure resets the success streak to zero. HTTP remains a comparison
measurement and does not need to succeed for HTTPS recovery to be confirmed.

The monitor records the first failure, each probe, failure and success streaks,
the first HTTPS success, and confirmed recovery time. Recovery duration runs
from the first failure to the third HTTPS success, not to the end of the HTTP
check. These are observation times: the actual transition may occur between probes.

| Recovery Outcome | Meaning |
| --- | --- |
| `temporary` | Recovery confirmed by three consecutive HTTPS successes |
| `not_recovered` | No confirmed recovery within the observation window |
| `interrupted` | Recovery monitoring interrupted by the user |

`--recovery-duration` limits observation from the first failure (600 seconds
by default). Request timeouts are capped to the remaining time; the final
HTTP check may therefore be skipped. A lack of confirmed recovery does not
mean a reboot is necessary.

### Resuming and Exit Codes

By default, the test stops after recovery, even with `--repeat 0`. Only
`--resume-after-recovery` permits a **new cycle** after `--repeat-delay`.
The failed cycle is not resumed. `--repeat` remains the upper limit:
with `--repeat 1`, this option does not start an additional cycle.

| Exit Code | Meaning |
| --- | --- |
| `0` | Completed or confirmed recovered; not necessarily error-free |
| `1` | No confirmed recovery, or a test/capture error |
| `130` | Interrupted by the user |
| `2` | Invalid CLI arguments |

Always review failures and recovery outcomes in Excel, not just the exit code.
`--max-failures` remains a compatibility option (1-5); stopping at the first
HTTPS failure always takes precedence over this legacy batch limit.

## CSV and Excel

CSV is the source log, written as measurements complete. Excel is a formatted
`.xlsx` workbook with filters, frozen headers, alternating row colors, readable
column widths, numeric formats, and highlighted recovery outcomes.

| File | Purpose |
| --- | --- |
| `logs/duco_tls.csv` | Default source log with requests and recovery events |
| `logs/duco_tls.xlsx` | Default Excel overview of that log |
| Custom `--output` | CSV path; Excel uses the same base name by default |

All timestamps are UTC. Each start receives a unique `run_id`; `repeat` starts
at 1 and request numbers continue throughout the run. Relative output paths
are resolved from the current working directory; default paths are relative
to the script directory.

Use only one writer per CSV. Compatible logs are appended to, not overwritten.
A different header or an incomplete final line is rejected without changing
previous data. Use a new output path for an old schema; the exporter still
supports legacy 7- and 11-column logs.

### Worksheets

Worksheet names and report labels currently remain in Dutch. The names below
match the generated workbook; their contents are described in English.

| Worksheet | Contents |
| --- | --- |
| Overzicht | Counts, failure rate, response times, and measurement period |
| Scenarios | Requests, connections, reuse, and phase timings by scenario, protocol, batch size, interval, and measurement type |
| Foutdiagnose | Failure groups by exception, stage, connection type, and active requests |
| Fouten | Failed and cancelled requests, diagnostics, and source file |
| Herstel | Recovery events, streaks, first success, and confirmation by recovery ID |
| Scenarioherstel | Recovery series, outcomes, and average/minimum/maximum recovery duration |
| Uurtrend | Combined hourly statistics with latency and failure charts |
| Uren | Statistics by hour, host, endpoint, phase, and protocol |
| Dagen | The same breakdown by day |
| Herhalingen | Statistics by run, repeat, host, and endpoint |

### Reading Measurements

- HTTP 200 with a body-read error counts as a failure.
- Average and maximum latency include failures; `Gem. OK ms` includes only successful requests.
- Cancelled requests count as failures, not as proven device faults; their counts are reported separately.
- Recovery events do not count as requests; recovery probes do, separated from load requests by measurement type.
- `Scenarioherstel` counts only recovery probes, not the original failure or cancelled peers.
- Missing diagnostics mean unknown or unmeasured, not zero; coverage columns identify partial measurements.
- `Uurtrend` combines phases and protocols; use `Uren` or `Scenarios` to compare similar measurements.

### Automatic and Manual Export

Excel is updated after the first completed repeat, periodically between
repeats, and at shutdown; not after every request. `--excel-interval` defaults
to 3600 seconds. Initial and final exports are exempt from this interval.
For example, use `--excel-interval 1` for an export after each repeat.
`--no-excel` disables automatic export only, not CSV logging.

The exporter writes a temporary file and replaces the previous report only
after a successful export. A locked file or export error leaves the previous
report intact while CSV logging continues. Close Excel and export again if needed:

```bash
.venv/bin/python duco_tls_report.py
.venv/bin/python duco_tls_report.py logs/warm_diagnostics.csv --output logs/warm_diagnostics.xlsx
```

Combine legacy logs or all measurements without changing the source files:

```bash
.venv/bin/python duco_tls_report.py 'logs/duco_tls_*.csv' --output logs/historical.xlsx
.venv/bin/python duco_tls_report.py 'logs/*.csv' --output logs/all_results.xlsx
```

The exporter reads snapshots and does not wait for new records. An incomplete
final physical line is skipped; invalid data leaves the previous report intact.
Legacy logs without run metadata receive a `legacy:` run ID; missing host and
endpoint values are shown as `onbekend` (unknown). Each source file is read once,
but overlapping measurements across files are not deduplicated. Automatic
export uses only the source log for the current test.

## Packet Capture

With `--capture`, tcpdump records TCP traffic between this client and the board
on ports 80 and 443, including recovery monitoring. Capture requires Linux,
tcpdump, capture permissions, and a literal IP address without a port in `--host`.
Other client traffic to the same board on those ports may also be recorded.

### Cold Test with Capture and Secrets

Run this command from the project directory. It combines a cold test with a
five-second pause, packet capture, opt-in keylogging, and a capture tail:

```bash
sudo "$(pwd)/.venv/bin/python" duco_tls_stress.py \
  --host 192.168.3.94 \
  --endpoint info \
  --scenario cold-handshakes \
  --duration 300 \
  --concurrency 1 \
  --interval 5 \
  --repeat 1 \
  --recovery-duration 600 \
  --capture \
  --capture-interface any \
  --capture-max-mb 32 \
  --capture-files 4 \
  --tls-keylog \
  --capture-tail 10 \
  --output "logs/duco_cold_i5_$(date +%Y%m%d_%H%M%S).csv" \
  --verbose
```

> [!CAUTION]
> This example writes TLS session secrets. PCAP plus secrets can make captured
> HTTPS content decryptable. Omit `--tls-keylog` for packet capture only.

The script never invokes sudo itself. Enter any password directly in the
terminal. If capture permissions are already configured, omit `sudo`. Only
run trusted code as root; output may then be owned by root. If necessary,
adjust ownership only for the files belonging to this run. Without capture,
omit all `--capture` options, `--tls-keylog`, and `sudo`.

If capture cannot become ready, no test requests are sent. An unexpected
capture exit stops the test; it never silently continues without capture.

### Files and Ring Buffer

A directory with the log's base name followed by `_capture` is created
alongside CSV and Excel:

| File | Contents |
| --- | --- |
| `traffic.pcap*` | Binary, rotating packet capture |
| `capture.json` | Run ID, settings, timestamps, tcpdump status, files, and recent messages |
| `tls.keys` | Only with `--tls-keylog`: sensitive TLS session secrets |

The capture directory has mode `0700`. HTTP health checks are unencrypted and
may contain device information; even PCAP without secrets is not necessarily
appropriate for public distribution.

The default ring buffer holds at most four files of roughly 32 million bytes
each; a file may exceed its limit by at most one packet. Configurable limits:
1-1024 MB per file and 1-16 files. Rotation overwrites the oldest packets.
Allocate enough space for the observation window and archive relevant runs
promptly after completion. Also check messages about packet drops.

### Capture Tail and Correlation

Capture remains active for ten seconds by default after the test finishes,
including closed client sessions and the final Excel export. `--capture-tail`
accepts 0-300 seconds; this extra time is in addition to any export time after
the last response. No new requests are sent during the tail. FIN/RST, TLS
closure messages, and late packets can still be recorded.

Tcpdump remains monitored during the tail. Ctrl+C skips or interrupts the tail
and shuts capture down cleanly. A tail is not guaranteed after a fatal error.

CSV contains `request_start_utc` and completion time `timestamp`; Excel shows
request start times for failures and capture metadata in `Scenarios`. The
manifest includes `last_response_utc`, `last_request_completed_utc`,
`requests_finished_utc`, `test_completed_utc`, `tail_start_utc`, and `tail_end_utc`.
The last response may have been partially read before a body error; cancelled
requests are not counted as responses for this timestamp.

## TLS Session Secrets

Keylogging is disabled by default. `--tls-keylog` requires `--capture` and
`SSLContext.keylog_filename`. Missing support stops the run before requests
are sent. The SSLContext retains the diagnostic setting without certificate
verification and is used for all HTTPS sessions, including health checks
and recovery probes.

The `tls.keys` file uses NSS/SSLKEYLOGFILE format and is created exclusively
with mode `0600`. Existing files and symlinks are not overwritten. Secret
values never appear in CSV, Excel, JSON, or console output; metadata includes
only the path, status, and size.

> [!CAUTION]
> Treat session secrets as confidential material. Share PCAP and secrets only
> deliberately with authorized investigators, never in a public repository.
> Unix permissions do not automatically protect copies on shares or in archives.

These secrets are session material, not the server's private key or a device
password. The file grows with new TLS connections and does not rotate with
the PCAP ring buffer, so retained packets do not lose their matching secrets.
Connection reuse does not produce new secrets on every request; a failed
handshake may yield little or no key material. Use bounded runs and remove
old files according to your retention policy.

### Decryption in Wireshark

1. Open the PCAP and go to **Preferences > Protocols > TLS**.
2. Set **(Pre)-Master-Secret log filename** to the matching `tls.keys` file.
3. Enable TLS-over-TCP reassembly and have Wireshark reprocess the capture.
4. Isolate a connection with `tcp.stream eq N` and inspect its handshake, HTTP, and closure.

TLS 1.2 and TLS 1.3 can be decrypted with matching secrets and sufficient
captured packets. Missing or overwritten packets can prevent reconstruction.
Failure to decrypt is not, by itself, evidence of an invalid handshake.

## Interpreting Diagnostics

### Client Traces

Where available, `aiohttp.TraceConfig` records DNS, connection, queue, HTTP,
and body timings, new connection attempts, established connections, reuse,
batch ID, batch size, and active requests at request start.

| Measurement | Meaning and Limitation |
| --- | --- |
| `dns_ms` | DNS resolution; may be absent with IP addresses or caching |
| `connect_ms` | Combined DNS + TCP + optional TLS; not a separate TLS handshake duration |
| `queue_ms` | Waiting for available connector capacity |
| `http_ms` | HTTP request phase until response headers arrive |
| `body_ms` | Reading the response body |
| `active_requests` | Active requests at start; not a complete overlap or concurrent-handshake measurement |

DNS is part of connection setup and must not be added to `connect_ms` again.
Reused connections may have no connection timing. A blank value means unknown
or unmeasured, not zero. HTTPS handshakes are inferred from connection attempts;
traces do not prove which internal TLS step took place.

| Error Stage | Interpretation |
| --- | --- |
| `dns` | DNS exception or last observed resolution phase |
| `tcp_connect` | Recognizable TCP error, such as connection refused |
| `tls_handshake` | Explicit SSL exception |
| `tcp_tls_connect` | Connection setup; TCP versus TLS cannot be reliably distinguished |
| `http_request` | Failure after connection setup, during the HTTP request phase |
| `http_status` | Unexpected HTTP status |
| `response_body` | Failure while reading the response body |
| `queue` | Failure while waiting for connector capacity |

A generic connector reset or connection timeout does not prove whether TCP
or TLS failed. `ServerDisconnectedError` after connection setup therefore
does not prove a failed handshake either. Without specific evidence, the
last observed trace phase remains the classification.

aiohttp's internal GET retry is disabled so it cannot mask the first
disconnect. This requires a private library setting. Supported versions are
limited to aiohttp 3.12-3.14; rerun integration tests before updating beyond
that range.

### Comparing Packets and Traces

- **TCP setup:** compare SYN, SYN/ACK, and ACK; check retransmissions and which peer sends FIN/RST.
- **TLS exchange:** compare ClientHello, ServerHello, and, with decryption, Finished against client timings.
- **Request and response:** compare decrypted HTTP with request start/end times and HTTP/body timings.
- **Closure:** check TLS `close_notify`, TCP FIN/RST, and late packets during the capture tail.

Without session secrets, HTTPS content and parts of the TLS 1.3 handshake
remain hidden. Network latency is still part of the measurement, even with
decryption. PCAP provides additional evidence, not automatic separate TCP/TLS
columns in the report. Label uncertainty explicitly.

## Long-Running Tests

Repeat cycles until Ctrl+C or the first HTTPS failure:

```bash
.venv/bin/python duco_tls_stress.py --repeat 0
```

For a limit of, for example, two weeks, use an existing tmux/screen session
and GNU `timeout`, so a disconnected SSH session does not stop the run:

```bash
timeout --signal=INT 14d .venv/bin/python duco_tls_stress.py --repeat 0
```

Load still stops at the first failure and does not automatically resume after
recovery. `timeout` returns exit code 124 when its limit is reached.
SIGINT/Ctrl+C closes CSV and performs a final Excel export; forced termination,
SIGTERM, or power loss do not guarantee a final export. CSV can be processed
again with the standalone exporter.

### Memory and Disk Space

The stress test keeps fixed phase counters and the current recovery state;
response bodies are read in 16 KiB chunks. The exporter streams CSV and uses
a temporary SQLite database with fixed caches (2 MiB main and 2 MiB temp)
for growing aggregates. Excel is written in write-only mode; temporary
database and worksheet files are cleaned up after export.

Not every raw successful request is copied to Excel. The same report is
replaced rather than creating a new report per probe. Worksheets split
automatically at 1,048,576 rows; metadata grows with those splits.

CSV, failure details, aggregate groups, and optional TLS secrets still grow
on disk. Exporting a longer log takes more time and temporary space. PCAP
rotation can overwrite earlier evidence. Use logs per measurement period,
archive relevant runs, and choose a retention policy: keeping every
measurement indefinitely without disk growth is not possible.

## Options

```bash
.venv/bin/python duco_tls_stress.py --help
.venv/bin/python duco_tls_report.py --help
```

| Option | Default | Meaning or Limit |
| --- | --- | --- |
| `--host` | `192.168.3.94` | Target board; a literal IP address when using capture |
| `--endpoint` | `info` | `info`, `nodes`, or `config` |
| `--scenario` | `all` | `all`, `warm-sequential`, `cold-handshakes`, or `parallel` |
| `--duration` | `300` | Load duration per cycle, 30-1800 seconds |
| `--concurrency` | `4` | 1-4 concurrent requests in the parallel scenario |
| `--interval` | `1` | Pause after each request/batch, at least 1 second |
| `--timeout` | `8` | Total request timeout, positive and finite |
| `--repeat` | `1` | Number of cycles; `0` repeats until stopped or a failure occurs |
| `--repeat-delay` | `30` | Pause between cycles, at least 0 seconds |
| `--recovery-duration` | `600` | Maximum recovery observation time, positive and finite |
| `--resume-after-recovery` | Off | Allow a subsequent cycle after confirmed recovery |
| `--max-failures` | `2` | Legacy batch limit of 1-5; first-failure stop takes precedence |
| `--output` | `logs/duco_tls.csv` in the script directory | CSV source log |
| `--excel-output` | CSV path with `.xlsx` extension | Custom Excel path; must not replace CSV |
| `--excel-interval` | `3600` | Minimum time between periodic exports, positive and finite |
| `--no-excel` | Off | Disable automatic Excel export |
| `--verbose` | Off | Also print successful requests to the console |
| `--capture` | Off | Record TCP traffic using tcpdump |
| `--capture-interface` | `any` | Capture interface |
| `--capture-max-mb` | `32` | Decimal MB per capture file, 1-1024 |
| `--capture-files` | `4` | Rotating capture files, 1-16 |
| `--capture-tail` | `10` | Extra capture time after test completion, 0-300 seconds |
| `--tls-keylog` | Off | Record sensitive TLS session secrets; requires capture |

## Tests

```bash
.venv/bin/python -m unittest discover -s tests -v
```

The suite uses temporary files, mocked responses/processes, and local HTTP/TLS
servers. It sends no requests to the Duco device, and capture tests do not
require root privileges. The local TLS keylog test requires the `openssl`
CLI; it is skipped if the required support is unavailable.

Tests cover logging, stop behavior, scenario selection, recovery, tracing,
capture shutdown and tail handling, TLS 1.2/1.3 keylogging, Excel formatting,
legacy logs, bounded aggregation, and cleanup on errors. Local tests do not
prove specific board behavior; separate live measurements are required for that.

## Sharing Results

For a reproducible report to DUCO, keep CSV, Excel, capture metadata, and
relevant PCAP together. Also record board model, firmware, test scenario,
interval, new connection count, first failure, recovery outcome, and any
external load. Compare client traces with packets before attributing a
failure to TCP, TLS, or HTTP.

> [!WARNING]
> Do not publish raw measurement logs, captures, or TLS secrets without
> reviewing their contents. They may contain device information and network
> details. Share secrets only with authorized investigators over an
> appropriately secured channel.

Do not commit generated measurement files or the virtual environment to a
public repository. For GitHub issues, prefer a reviewed summary with relevant,
anonymized measurements.