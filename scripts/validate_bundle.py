"""Validate bundle shapes against `databricks bundle schema`, without auth.

This does not resolve workspace substitutions, permissions or runtime support.
Run authenticated `databricks bundle validate` before any optional deployment.
"""

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import jsonschema
import yaml


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--schema", type=Path, required=True)
    parser.add_argument("--cli-version", default="unrecorded")
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    bundle = yaml.safe_load((root / "databricks.yml").read_text(encoding="utf-8"))
    for pattern in bundle.get("include", []):
        for path in root.glob(pattern):
            content = yaml.safe_load(path.read_text(encoding="utf-8"))
            for kind, resources in content.get("resources", {}).items():
                bundle.setdefault("resources", {}).setdefault(kind, {}).update(resources)
    official = json.loads(args.schema.read_text(encoding="utf-8-sig"))
    jsonschema.validators.validator_for(official)(official).validate(bundle)
    for job in bundle["resources"]["jobs"].values():
        if "schedule" in job or "trigger" in job:
            raise AssertionError("Optional portfolio jobs must not run automatically")
    report = dict(status="schema_verified_locally", validated_at=datetime.now(timezone.utc).isoformat(),
        cli_version=args.cli_version, schema_sha256=hashlib.sha256(args.schema.read_bytes()).hexdigest(),
        yaml_files=["databricks.yml", "resources/arena.yml"], managed_runtime_status="pending_workspace_verification",
        authentication_attempted=False, deployed=False, schedule_or_trigger_defined=False,
        limitations="Offline official JSON Schema validation only; workspace substitutions, access, availability and behavior remain unverified")
    if args.report:
        args.report.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
