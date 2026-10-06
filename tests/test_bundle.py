import ast
from pathlib import Path

import yaml


def test_optional_bundle_is_manual_and_has_recoverable_dependencies():
    root = Path(__file__).resolve().parents[1]
    bundle = yaml.safe_load((root / "databricks.yml").read_text(encoding="utf-8"))
    resources = yaml.safe_load((root / "resources/arena.yml").read_text(encoding="utf-8"))["resources"]
    job = resources["jobs"]["arena_verified_refresh"]
    pipeline = resources["pipelines"]["arena_pipeline"]
    assert "schedule" not in job and "trigger" not in job
    assert job["max_concurrent_runs"] == 1
    assert [task["task_key"] for task in job["tasks"]] == ["update_layers", "verify_gold"]
    assert job["tasks"][1]["depends_on"] == [{"task_key": "update_layers"}]
    assert all(task["max_retries"] <= 2 for task in job["tasks"])
    assert pipeline["continuous"] is False
    assert pipeline["catalog"] == "${var.catalog}"
    assert "source_volume_path" in bundle["variables"]
    for file in ("databricks/pipeline.py", "databricks/verify_gold.py"):
        ast.parse((root / file).read_text(encoding="utf-8"))
