import argparse
import json
import time
from pathlib import Path

from dotenv import load_dotenv

from .connectors import collect, commit_state
from .engine import Pipeline
from .snapshot import export_snapshot
from .synthetic import generate


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(description="Arena football + VALORANT data lab")
    parser.add_argument("--workspace", type=Path, default=Path("data/demo"))
    sub = parser.add_subparsers(dest="command", required=True)
    demo = sub.add_parser("demo")
    demo.add_argument("--matches", type=int, default=600)
    demo.add_argument("--seed", type=int, default=42)
    demo.add_argument("--showcase", action="store_true", help="Three original competitions per sport")
    replay = sub.add_parser("replay")
    replay.add_argument("file", type=Path)
    backfill = sub.add_parser("backfill")
    backfill.add_argument("file", type=Path)
    backfill.add_argument("--from", dest="begin", required=True)
    backfill.add_argument("--to", dest="end", required=True)
    backfill.add_argument("--max-events", type=int, default=100_000)
    benchmark = sub.add_parser("benchmark", help="Actual Spark file-layout experiment; requires Linux Spark")
    benchmark.add_argument("--snapshot", type=Path, required=True)
    sub.add_parser("export")
    for command in ("collect", "poll"):
        api = sub.add_parser(command)
        api.add_argument("--provider", choices=["football-data", "pandascore"], required=True)
        api.add_argument("--competition", default="PL")
        api.add_argument("--pages", type=int, default=1)
        if command == "poll":
            api.add_argument("--cycles", type=int, default=3)
            api.add_argument("--interval", type=int, default=300)
    args = parser.parse_args()
    if args.command == "benchmark":
        from .incremental import benchmark as run_benchmark
        run_benchmark(args.snapshot, args.workspace)
        return
    pipeline = Pipeline(args.workspace)
    if args.command == "demo":
        config_path = args.workspace / "demo-config.json"
        config = dict(matches=args.matches, seed=args.seed)
        if args.showcase:
            config["showcase"] = True
        if config_path.exists() and json.loads(config_path.read_text(encoding="utf-8")) != config:
            parser.error("This demo workspace has another seed/match configuration; use a fresh --workspace")
        if not config_path.exists() and pipeline.events():
            candidate = generate(args.matches, args.seed, showcase=args.showcase)
            from .operations import validated_new_events
            try:
                validated_new_events(pipeline.events(), [r for r in candidate if r.get("event_id", "").startswith("synthetic:")])
            except ValueError:
                parser.error("Existing observations conflict with this demo configuration; use a fresh --workspace")
        result = pipeline.ingest(generate(args.matches, args.seed, showcase=args.showcase))
        config_path.write_text(json.dumps(config), encoding="utf-8")
        snapshot = export_snapshot(pipeline, args.workspace / "snapshots")
        print(json.dumps(dict(**result, snapshot_dir=str(snapshot.resolve())), indent=2))
    elif args.command in ("replay", "backfill"):
        records = [json.loads(line) for line in args.file.read_text(encoding="utf-8").splitlines() if line.strip()]
        if args.command == "backfill":
            from .operations import bounded_window
            records = bounded_window(records, args.begin, args.end, args.max_events)
        print(json.dumps(pipeline.ingest(records), indent=2))
    elif args.command == "export":
        print(export_snapshot(pipeline, args.workspace / "snapshots").resolve())
    else:
        cycles = args.cycles if args.command == "poll" else 1
        if not 1 <= cycles <= 100 or getattr(args, "interval", 300) < 30:
            parser.error("Use 1..100 cycles and an interval of at least 30 seconds")
        for cycle in range(cycles):
            state_path = args.workspace / f"{args.provider}-cursor.json"
            events, meta = collect(args.provider, state_path, args.competition, args.pages)
            result = pipeline.ingest(events)
            commit_state(state_path, meta["state"])
            print(json.dumps(dict(**result, source_records=meta["returned"], unsupported=meta["unsupported"])))
            if events:
                print(export_snapshot(pipeline, args.workspace / "snapshots").resolve())
            if cycle + 1 < cycles:
                time.sleep(args.interval)


if __name__ == "__main__":
    main()
