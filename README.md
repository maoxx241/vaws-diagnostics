# vaws-diagnostics

Structured logs, phase timings, sanitized support bundles, automatic GitHub
issues and an independent Grok diagnosis worker for VAWS components. The Python
package has **no runtime dependencies**. GitHub publishing uses an existing `gh`
login; Grok diagnosis uses an optional, separately installed Grok Build CLI.

Logging does not make network requests, install packages, grant task ownership,
retry a workload or wait for a model. Each component retains its execution and
resource state. Importing this package does not configure Python's root logger.

## Record an operation

```python
from vaws_diagnostics import configure, wrap_context

log = configure("vaws-coordinator")
with log.operation("prepare") as operation:
    with operation.phase("lock.wait"):
        acquire_lock()
    with operation.phase("build"):
        build()
    operation.event("INFO", "reuse.selected", cache_hit=True)
diagnostics = operation.summary()
```

Exceptions are recorded and re-raised unchanged. Returned failures can use
`operation.fail("transport", retryable=False, submission_state="uncertain")`.
The summary contains the real start/end, monotonic duration, bounded phase
summary, operation/trace IDs, local record reference and diagnostic write status.
Use `wrap_context(callable)` when submitting work to a thread pool;
`current_context()` / `bind_context()` carry diagnostic association through
internal RPC metadata. These identifiers are never task or resource authority.

`DEBUG`, `INFO` (default), `WARNING` (`WARN` accepted), `ERROR`, and `CRITICAL`
follow Python logging levels. `VAWS_LOG_LEVEL` sets the level.
`VAWS_DIAGNOSTICS_ROOT` overrides the platform user state directory:

- Windows: `%LOCALAPPDATA%/vaws/diagnostics`
- Linux/macOS: `$XDG_STATE_HOME/vaws/diagnostics`, defaulting to
  `~/.local/state/vaws/diagnostics`

Each process writes `events/<component>/<pid>-<nonce>.jsonl`, rotating at 1 MiB
with three backups. Records are limited to 16 KiB. UTC timestamps and a random
process clock identifier accompany monotonic times; never subtract monotonic
times from different clock identifiers or add parallel phases into elapsed time.
File errors produce one bounded stderr warning and a `logging_failed` flag; they
cannot mask the business result or prevent cleanup. stderr is never MCP stdout.

## Inspect or attach diagnostics

```sh
vaws-diagnostics bundle --root /path/to/diagnostics --operation-id OPERATION_ID --output support.json
```

This command is offline. It reads existing logger segments only, without live
probes or repair. The default bundle is bounded to 1 MiB / 1,000 events, with
explicit clipping, omission and unreadable-input indicators. Files, directories,
commands, prompts and arbitrary raw output are not uploaded. The export first
selects known structured fields, then applies the shared redaction rules and a
final scan. A redaction failure prevents publishing; it does not suppress the
original local incident. The bundle includes a content hash.

Automatic issues embed a smaller bounded JSON evidence window in the issue body,
so diagnostics remain readable without a transient attachment server. Stack
locations are module/function/line records, with no source text or frame locals.
Package version/revision and exception chains help maintainers locate failures.
Private free-form output stays excluded, even if a regex scanner finds no secret.

## Enable automatic issues

Enabling the independent worker is the installation's explicit choice to publish
sanitized failures to the selected repository. Merely importing or installing the
library does not upload anything. No per-tool approval or Agent report is needed.
Explicit caller input errors and user cancellation remain in local diagnostics;
they do not automatically create VAWS bug reports. Unknown failures are retained
for diagnosis rather than guessed to be caller mistakes.

```sh
gh auth login
vaws-diagnostics worker --root /path/to/diagnostics --state /path/to/reporter-state
```

The default destination is
`vllm-ascend-workspace/vllm-ascend-workspace`; `--repository owner/repo` overrides
it. Repeat `--root` to watch multiple explicit diagnostic roots. `--once` runs one
cycle for Task Scheduler, systemd timers or other service managers; otherwise the
worker repeats every 60 seconds. `--interval` changes this (minimum 5 seconds).
`--since` accepts a fixed ISO timestamp with a timezone when an installation
should observe future incidents without backfilling historical logs. Keep the
same timestamp across restarts; it does not delete older local evidence.
Run it as the user whose tools produce these logs, with that user's GitHub login.
Do not put tokens in command arguments, repository files or diagnostic bundles.

```sh
vaws-diagnostics status --state /path/to/reporter-state
```

The outbox keeps immutable sanitized evidence, occurrence counts, publication
state and last error. It deduplicates by component, version, operation, failing
phase and error fingerprint. Byte cursors survive worker restarts; deduplication
also handles rereading rotated segments. The queue is capped at 1,000 incidents,
with a maximum of 10 issue submissions per hour. Full queues retain existing
incidents and expose backpressure. Published records and occurrence identifiers
expire after 30 days. Worker retention removes settled log segments beyond seven
days, 128 MiB or 512 files, preserving files modified in the last five minutes;
`limited` reports when recent writers prevent meeting the retention target.

GitHub errors back off. A POST timeout or a crash after starting a POST is
**uncertain**, not proof that creation failed. The next attempt searches direct
REST issue listings for a stable marker before posting. If no match is found,
uncertain entries remain visible for reconciliation and are not blindly resent.
This is deliberately not an exactly-once claim. Reconciliation is bounded and
will report an exhausted listing window instead of declaring a false absence.

## Add the Grok diagnostic bot

Create a dedicated profile and log in to it using the installed Grok CLI:

```sh
vaws-diagnostics grok-profile --home /path/to/grok-bot-home
GROK_HOME=/path/to/grok-bot-home grok login
vaws-diagnostics worker --root /path/to/diagnostics --state /path/to/reporter-state \
  --grok grok --grok-home /path/to/grok-bot-home --grok-work /path/to/empty-bot-work
```

On PowerShell, set `$env:GROK_HOME` before `grok login`. A Windows installation
can also run the worker in WSL and pass the Windows `gh.exe` path through `--gh`.
The existing personal Grok configuration is never overwritten. The dedicated
configuration disables tools, memory, compatibility discovery and subagents;
the adapter inspects active hooks/plugins/MCP/project instructions before each
request. Newly materialized built-in skills are disabled in this owned profile.
The Grok Build CLI integration has been exercised with version 1.0.25.

The bot reads only the structured diagnostic evidence in marked VAWS issues.
Issue prose, links and comments do not become execution instructions. Grok has
no enabled business tools and returns observations, hypotheses, missing evidence
and suggested checks. Its response is bounded and leak-scanned before posting.
A separate queue caps model requests at ten per hour, caches results and reconciles comment markers after lost
responses, avoiding duplicate comments and unnecessary model reruns. The bot does
not change services, close issues or merge fixes. A model hypothesis is not a
confirmed root cause. GitHub/model availability never delays the original tool.

This is a locally supervised Grok Build diagnostic worker, not a provisioned
Grok cloud Bot or a newly created GitHub account. Comments use the configured
GitHub credential and identify the diagnostic worker explicitly.

## Development

```sh
uv venv
uv pip install -e '.[test]'
uv run --no-project python -m pytest -q
uv build
```

CI tests Python 3.11 and 3.13 on Linux, Windows and macOS, and installs a built
wheel without dependencies. Tests cover write failures, context propagation,
concurrent rotation, partial and malicious logs, outbox races and capacity,
GitHub rate limiting, lost POST replies and bot isolation. Synthetic public
issue/comment acceptance is separate from transport mocks.

The record model follows [Python logging](https://docs.python.org/3/howto/logging.html)
and the [OpenTelemetry logs data model](https://opentelemetry.io/docs/specs/otel/logs/data-model/),
without requiring an OpenTelemetry collector or SDK.
