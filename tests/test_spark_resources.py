import builtins

import pytest

from arena_lakehouse.spark_backend import session


@pytest.mark.parametrize("name,value", [
    ("ARENA_SPARK_THREADS", "0"), ("ARENA_SPARK_THREADS", "3"),
    ("ARENA_SPARK_THREADS", "1;invalid"), ("ARENA_SPARK_THREADS", "１"),
    ("ARENA_SPARK_DRIVER_MEMORY_MB", "511"), ("ARENA_SPARK_DRIVER_MEMORY_MB", "2049"),
    ("ARENA_SPARK_DRIVER_MEMORY_MB", "unbounded"),
])
def test_bad_resource_budget_rejected_before_import_or_start(monkeypatch, name, value):
    monkeypatch.setenv(name, value)
    original = builtins.__import__
    def guarded(module, *args, **kwargs):
        if module.startswith("pyspark"):
            pytest.fail("Invalid policy attempted to start Spark")
        return original(module, *args, **kwargs)
    monkeypatch.setattr(builtins, "__import__", guarded)
    with pytest.raises(ValueError, match="resource budget"):
        session()
