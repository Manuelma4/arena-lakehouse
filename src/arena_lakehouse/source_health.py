"""Read-only source-check SLOs over the local polling ledger; no provider calls."""

import argparse
import json
import math
import sqlite3
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .provider_policy import ERROR_CODES, PROVIDERS


def safe(path):
    if ".." in path.parts or any(parent.is_symlink() for parent in (path, *path.parents)):
        raise ValueError("Source health paths must not contain traversal or links")


def utc(value):
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Source health needs aware clocks")
    return value.astimezone(timezone.utc)


def source_health(workspace, provider, *, now=None, window_seconds=86400,
                  max_age_seconds=7200, success_target=0.99):
    if provider not in PROVIDERS:
        raise ValueError("Unknown provider")
    if (type(window_seconds) is not int or not 60 <= window_seconds <= 31 * 86400
            or type(max_age_seconds) is not int or not 30 <= max_age_seconds <= 31 * 86400
            or type(success_target) not in (int, float) or not math.isfinite(success_target)
            or not 0 < success_target <= 1):
        raise ValueError("Invalid source health policy")
    now = utc(now or datetime.now(timezone.utc))
    root = Path(workspace) / provider
    database = root / "live.db"
    safe(database)
    result = dict(schema_version="arena-source-health-1.0", provider=provider,
        evaluated_at=now.isoformat(), window_seconds=window_seconds,
        max_success_age_seconds=max_age_seconds, success_target=success_target,
        scope="Local provider check attempts; not match coverage or score latency",
        status="unavailable", completed_checks=0, successful_checks=0,
        success_ratio=None, last_success_at=None, success_age_seconds=None,
        observed_window_seconds=0, alerts=[], error_counts={}, publication_selected=False)
    if not database.is_file():
        result["alerts"] = ["no_local_ledger"]
        return result
    try:
        with closing(sqlite3.connect(database.resolve().as_uri() + "?mode=ro", uri=True, timeout=5)) as db:
            db.row_factory = sqlite3.Row
            # Bound the retained sample; never instantiate a worker or create a DB.
            rows = list(db.execute("SELECT started_at,finished_at,status,error_code FROM polls "
                                   "ORDER BY id DESC LIMIT 100001"))
        if len(rows) > 100000:
            raise ValueError("Source health ledger exceeds the bounded sample")
        cutoff = now - timedelta(seconds=window_seconds)
        completed, successes, running = [], [], []
        for row in rows:
            started = utc(row["started_at"])
            finished = utc(row["finished_at"]) if row["finished_at"] else None
            if started > now or finished is not None and (finished > now or finished < started):
                raise ValueError("Source health ledger has inconsistent clocks")
            if row["status"] == "succeeded" and finished is not None:
                successes.append(finished)
            if finished is not None and cutoff <= finished <= now:
                completed.append(row)
            if row["status"] == "polling" and finished is None:
                running.append(started)
        successful = sum(row["status"] == "succeeded" for row in completed)
        ratio = successful / len(completed) if completed else None
        last_success = max(successes) if successes else None
        age = (now - last_success).total_seconds() if last_success else None
        alerts = []
        if age is None:
            alerts.append("no_successful_collection")
        elif age > max_age_seconds:
            alerts.append("source_check_overdue")
        if ratio is not None and ratio < success_target:
            alerts.append("check_success_below_target")
        if any((now - started).total_seconds() > max_age_seconds for started in running):
            alerts.append("poll_not_finished")
        errors = {}
        for row in completed:
            if row["status"] != "succeeded":
                code = row["error_code"] if row["error_code"] in ERROR_CODES else "worker_error"
                errors[code] = errors.get(code, 0) + 1
        # Presence is operational telemetry, not checksum acceptance of the pair.
        selector = root / "snapshots" / "CURRENT_PUBLICATION.json"
        safe(selector)
        selected = selector.is_file()
        if selected and selector.stat().st_size > 64 * 1024:
            raise ValueError("Local selector exceeds the bounded contract")
        observed = min(window_seconds, max(0, (now - min(utc(r["started_at"]) for r in rows)).total_seconds())) if rows else 0
        result.update(status="attention" if alerts else "healthy" if completed else "insufficient_history",
            completed_checks=len(completed), successful_checks=successful, success_ratio=ratio,
            last_success_at=last_success.isoformat() if last_success else None, success_age_seconds=age,
            observed_window_seconds=observed, alerts=alerts, error_counts=errors,
            publication_selected=selected)
        return result
    except (sqlite3.Error, OSError, ValueError, TypeError, KeyError):
        result.update(status="unavailable", alerts=["invalid_local_ledger"])
        return result


def main():
    parser = argparse.ArgumentParser(description="Read-only local source health; no collection or alerts sent")
    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--provider", choices=tuple(PROVIDERS), required=True)
    parser.add_argument("--window-seconds", type=int, default=86400)
    parser.add_argument("--max-age-seconds", type=int, default=7200)
    parser.add_argument("--success-target", type=float, default=0.99)
    args = parser.parse_args()
    result = source_health(args.workspace, args.provider, window_seconds=args.window_seconds,
                           max_age_seconds=args.max_age_seconds, success_target=args.success_target)
    print(json.dumps(result, indent=2, allow_nan=False))
    return 0 if result["status"] == "healthy" else 1


if __name__ == "__main__":
    raise SystemExit(main())
