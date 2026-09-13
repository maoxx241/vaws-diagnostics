"""Support bundle and independent reporter/bot worker commands."""
from __future__ import annotations

import argparse
import json
import signal
import threading
from pathlib import Path

from .outbox import Outbox
from .reporter import DEFAULT_REPOSITORY, GitHub, ingest, publish_one


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(prog="vaws-diagnostics", description="Local diagnostics and independently enabled automatic issue reporting")
    sub = result.add_subparsers(dest="command", required=True)
    bundle = sub.add_parser("bundle", help="export a bounded sanitized support bundle, without uploading")
    bundle.add_argument("--root", required=True)
    bundle.add_argument("--operation-id")
    bundle.add_argument("--output")
    status = sub.add_parser("status", help="inspect the local publishing queue without network access")
    status.add_argument("--state", required=True)
    profile = sub.add_parser("grok-profile", help="create a dedicated tool-disabled Grok profile; existing personal config is never replaced")
    profile.add_argument("--home", required=True)
    worker = sub.add_parser("worker", help="enable local failure reporting and optional Grok diagnosis")
    worker.add_argument("--root", action="append", required=True, help="explicit diagnostic root; repeat for multiple components/workspaces")
    worker.add_argument("--state", required=True)
    worker.add_argument("--repository", default=DEFAULT_REPOSITORY)
    worker.add_argument("--gh", default="gh")
    worker.add_argument("--grok", help="optional Grok executable; uses a dedicated existing authenticated profile")
    worker.add_argument("--grok-home")
    worker.add_argument("--grok-work")
    worker.add_argument("--once", action="store_true")
    worker.add_argument("--interval", type=float, default=60)
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    if args.command == "bundle":
        from .bundle import collect_bundle
        print(json.dumps(collect_bundle(args.root, operation_id=args.operation_id, output=args.output), ensure_ascii=True))
        return 0
    if args.command == "grok-profile":
        from .bot import prepare_profile
        home = prepare_profile(args.home)
        print(json.dumps({"home": str(home), "authentication": "Run grok login with GROK_HOME set to this directory."}))
        return 0
    state = Path(args.state).resolve()
    if args.command == "status":
        result = {}
        for name in ("reporter", "bot"):
            path = state / f"{name}.sqlite3"
            result[name] = Outbox(path).rows() if path.exists() else []
        print(json.dumps(result, ensure_ascii=True))
        return 0
    if args.interval < 5:
        parser().error("worker interval must be at least 5 seconds")
    if args.grok and not (args.grok_home and args.grok_work):
        parser().error("--grok requires --grok-home and --grok-work")
    from . import configure, __version__
    recorder = configure("vaws-diagnostics", root=state / "diagnostics", version=__version__)
    queue, github = Outbox(state / "reporter.sqlite3"), GitHub(args.repository, executable=args.gh)
    stop = threading.Event()
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, lambda *_: stop.set())
    grok = bot_queue = None
    if args.grok:
        from .bot import Grok
        grok, bot_queue = Grok(args.grok, home=args.grok_home, work=args.grok_work), Outbox(state / "bot.sqlite3")
    while not stop.is_set():
        result = {"ingestion": [], "reporter": None, "bot": None}
        try:
            with recorder.operation("worker.cycle") as op:
                for root in args.root:
                    with op.phase("ingest"):
                        result["ingestion"].append(ingest(root, queue))
                    from .maintenance import prune
                    with op.phase("retention"):
                        op.event("INFO", "retention.complete", **prune(root))
                with op.phase("report"):
                    result["reporter"] = publish_one(queue, github)
                    op.event('WARNING' if result['reporter']['status'] in {'retry', 'uncertain', 'blocked', 'rate_limited'} else 'INFO',
                             'reporter.result', status=result['reporter']['status'], error_code=result['reporter'].get('error'))
                if grok and bot_queue:
                    from .bot import diagnose_one, enqueue_issues
                    with op.phase("diagnose"):
                        enqueue_issues(github, bot_queue)
                        result["bot"] = diagnose_one(bot_queue, github, grok)
                        op.event('WARNING' if result['bot']['status'] in {'retry', 'uncertain', 'blocked', 'rate_limited'} else 'INFO',
                                 'bot.result', status=result['bot']['status'], error_code=result['bot'].get('error'))
            print(json.dumps(result, ensure_ascii=True), flush=True)
        except Exception as exc:
            # Operation context already records the local exception. Do not copy
            # credentials or raw subprocess errors into scheduler console logs.
            print(json.dumps({"status": "degraded", "error_type": type(exc).__name__}), flush=True)
            if args.once:
                return 1
        if args.once:
            return 0
        stop.wait(args.interval)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
