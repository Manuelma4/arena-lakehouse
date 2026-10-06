"""Publish a complete immutable snapshot through an atomic directory rename."""

import hashlib
import json
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from . import __version__
from .engine import history, latest, team_stats
from .prediction import train_and_predict
from .schema import canonical, digest

DOCUMENTS = {
    "overview": ("Arena Lakehouse", "Arena Lakehouse integrates football and professional VALORANT match data. "
        "The committed demo uses fictional teams and synthetic results. Real data comes from football-data.org "
        "and PandaScore with free personal tokens. Bronze preserves raw observations; Silver selects canonical "
        "source revisions; Gold holds match and team summaries. The initial reference engine uses SQLite and Arrow. "
        "Spark/Delta is a separately validated execution backend."),
    "metrics": ("Metric definitions", "Win rate is wins divided by completed matches. Football matches can draw; "
        "draws count as completed matches but not wins. VALORANT series cannot draw. Football score totals are goals; "
        "VALORANT score totals are maps, not rounds or kills. Scheduled, cancelled and postponed matches do not "
        "contribute to completed-match team summaries. Scores are not comparable across sports."),
    "predictions": ("Prediction methodology", "The scaled_logistic_v1 baseline uses pre-match Elo difference, "
        "last-five form difference and capped history counts. Outcome labels never enter features. Only results "
        "completed before kickoff AND observed before the cutoff may be used. Football has team A win, draw and "
        "team B win probabilities; VALORANT has team A and team B series-win probabilities. Chronological "
        "60/20/20 train/validation/test partitions keep equal kickoff timestamps together. The frequency baseline "
        "uses training outcomes only. Synthetic evaluation demonstrates the pipeline, not real predictive skill. "
        "Probabilities are not calibrated. Missing eligible history means prediction unavailable."),
    "streaming": ("Streaming and replay", "Free providers are polled REST sources. Polling changes are emitted "
        "as append-only match observations. This is not provider push or a free play-by-play stream. Spark "
        "Structured Streaming processes files with AvailableNow and persistent checkpoints. Late revisions "
        "require recomputing affected match and team summaries. Replaying an identical event is idempotent. "
        "Conflicting source revisions are quarantined rather than choosing an arbitrary payload."),
    "quality": ("Quality and incident handling", "Finished matches require nonnegative scores and completion "
        "before observation. Teams must differ. VALORANT results must agree with best-of and have one winner. "
        "Malformed payloads go to quarantine. Source revision identities and times must be consistent. "
        "After a failed ingest transaction, no partial canonical observations are committed. A successful replay "
        "recovers the state. Snapshot checksums detect modifications; snapshot versions prevent mixed datasets."),
    "providers": ("Provider capabilities", "football-data.org free access offers basic fixtures/results and "
        "requires X-Auth-Token. PandaScore uses Bearer tokens; free VALORANT match schedules are supported. "
        "Detailed historical player statistics and round events are not mandatory and may require paid plans. "
        "A fetched historical result was observed at collection time, not magically at its historic kickoff. "
        "Secrets stay in environment variables. Provider data and team logos are not MIT fixtures."),
    "privacy": ("Privacy and access boundaries", "This project ingests team-level match metadata, not private "
        "player profiles. Tokens never enter snapshots or prompts. SQL queries are predefined and parameterized. "
        "The copilot cannot run arbitrary SQL or modify the source lakehouse. Exported data is immutable and "
        "provider-scoped IDs avoid team collisions. Account authentication and multi-user access controls remain "
        "future deployment work; the local demo binds to loopback."),
    "limitations": ("Known limitations", "This is an early research portfolio milestone, not a validated "
        "production prediction service. Match-level results cannot explain round economy, agent picks, injuries "
        "or map vetoes. No guaranteed winner or exact-score forecast is available. A relevant citation does not "
        "prove a causal explanation. Distinguish observed evidence, model output and unanswered questions. "
        "Real-data quality, model calibration and full agent evaluation require subsequent milestones."),
}

# Empty results still have a contract: a newly connected provider can have only
# upcoming fixtures. Arrow cannot infer a usable schema from an empty list.
TEAM_STATS_SCHEMA = pa.schema([
    ("sport", pa.string()), ("team_id", pa.string()), ("team_name", pa.string()),
    ("matches", pa.int64()), ("wins", pa.int64()), ("draws", pa.int64()), ("losses", pa.int64()),
    ("score_for", pa.int64()), ("score_against", pa.int64()),
    ("last_match_at", pa.string()), ("win_rate", pa.float64()),
])


def write_json(path: Path, value):
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False, allow_nan=False) + "\n", encoding="utf-8")


def export_snapshot(pipeline, output: Path, with_predictions=True) -> Path:
    quality = pipeline.quality()
    if quality["summary"]["last_run_status"] != "succeeded":
        raise ValueError("Publish only after a successful ingest")
    events = pipeline.events()
    if not events:
        raise ValueError("Cannot export an empty dataset")
    providers = sorted({e["provider"] for e in events})
    if "synthetic" in providers and len(providers) != 1:
        raise ValueError("Synthetic and API data require separate workspaces")
    predictions, model_report = train_and_predict(events) if with_predictions else ([], {"status": "not_run"})
    matches, stats = latest(events), team_stats(latest(events))
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=".staging-", dir=output))
    (stage / "gold").mkdir()
    (stage / "docs").mkdir()
    (stage / "bronze").mkdir()
    pq.write_table(pa.Table.from_pylist(matches), stage / "gold/matches.parquet")
    pq.write_table(pa.Table.from_pylist(stats, schema=TEAM_STATS_SCHEMA), stage / "gold/team_stats.parquet")
    pq.write_table(pa.Table.from_pylist(history(events)), stage / "gold/match_history.parquet")
    (stage / "bronze/events.jsonl").write_text("\n".join(canonical(e) for e in events) + "\n", encoding="utf-8")
    tables = []
    for name, description in (("matches", "Current canonical match revisions"),
                              ("team_stats", "Completed-match team aggregates"),
                              ("match_history", "Observation-time match revision history")):
        arrow_schema = pq.read_schema(stage / f"gold/{name}.parquet")
        tables.append(dict(name=name, layer="gold", description=description,
            columns=[dict(name=f.name, type=str(f.type), description=f.name.replace("_", " ")) for f in arrow_schema],
            primary_key=["sport", "team_id"] if name == "team_stats" else ["match_id", "revision"] if name == "match_history" else ["match_id"]))
    write_json(stage / "catalog.json", tables)
    write_json(stage / "metrics.json", [
        dict(name="win_rate", description="Wins / finished matches", unit="ratio", table="team_stats", formula="wins / matches"),
        dict(name="score_for", description="Goals for football; maps won for VALORANT", unit="sport-specific", table="team_stats", formula="SUM(score_for)"),
    ])
    write_json(stage / "lineage.json", [
        dict(source="provider_api_or_replay", target="bronze_events", transformation="validated observations"),
        dict(source="bronze_events", target="matches", transformation="highest accepted source revision"),
        dict(source="matches", target="team_stats", transformation="finished matches grouped by sport and team"),
        dict(source="bronze_events", target="predictions", transformation="point-in-time features and baseline model"),
    ])
    write_json(stage / "quality.json", quality)
    write_json(stage / "incidents.json", [dict(incident_id=digest(c)[:16], **{
        k: c[k] for k in ("rule", "table", "severity")}, description=f"{c['failed']} quarantined observations",
        status="open", event_count=c["failed"], remediation="Inspect quarantined source records, correct and replay")
        for c in quality["checks"]])
    write_json(stage / "predictions.json", predictions)
    write_json(stage / "model_report.json", model_report)
    for slug, (title, body) in DOCUMENTS.items():
        (stage / f"docs/{slug}.md").write_text(f"# {title}\n\n{body}\n", encoding="utf-8")
    files = {p.relative_to(stage).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
             for p in sorted(stage.rglob("*")) if p.is_file()}
    manifest = dict(schema_version="2.0", dataset_version=digest({"events": events})[:16],
                    producer_version=__version__, generated_at=datetime.now(timezone.utc).isoformat(),
                    as_of=max(e["observed_at"] for e in events),
                    data_kind="synthetic" if providers == ["synthetic"] else "api", providers=providers, files=files)
    write_json(stage / "manifest.json", manifest)
    destination = output / f"snapshot-{manifest['dataset_version']}-{uuid.uuid4().hex[:8]}"
    stage.replace(destination)
    pointer = output / ".LATEST.tmp"
    pointer.write_text(destination.name, encoding="utf-8")
    pointer.replace(output / "LATEST")
    return destination
