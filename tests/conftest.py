"""Default pytest execution is offline, before application test modules are imported."""

from collections import Counter
import json
from pathlib import Path

import pytest

from offline_guard import OfflineBoundary


SESSION_EVENT_START = len(OfflineBoundary.blocked_operations)
OFFLINE_BOUNDARY = OfflineBoundary()
OFFLINE_BOUNDARY.install()
BOUNDARY_TEST_FILE = Path(__file__).with_name("test_offline_test_boundary.py").resolve()


def pytest_addoption(parser):
    parser.addoption("--offline-boundary-report", metavar="PATH", default=None,
                     help="Write operation counts to an explicitly test-owned JSON report path")


def pytest_configure(config):
    config._arena_offline_rows = []
    config._arena_offline_collection_end = SESSION_EVENT_START
    config._arena_offline_runtime_indices = set()
    config._arena_offline_expected_indices = set()


def pytest_collection_finish(session):
    session.config._arena_offline_collection_end = len(OfflineBoundary.blocked_operations)
    events = OfflineBoundary.blocked_operations[SESSION_EVENT_START:]
    if events:
        raise pytest.UsageError("Offline pytest boundary detected blocked operations during collection")


@pytest.hookimpl(tryfirst=True)
def pytest_runtest_setup(item):
    item._arena_offline_start = len(OfflineBoundary.blocked_operations)


@pytest.hookimpl(wrapper=True, tryfirst=True)
def pytest_runtest_teardown(item, nextitem):
    try:
        yield
    finally:
        start = getattr(item, "_arena_offline_start", len(OfflineBoundary.blocked_operations))
        end = len(OfflineBoundary.blocked_operations)
        events = OfflineBoundary.blocked_operations[start:end]
        counts = dict(sorted(Counter(events).items()))
        expected = Path(item.path).resolve() == BOUNDARY_TEST_FILE
        item.config._arena_offline_runtime_indices.update(range(start, end))
        if expected:
            item.config._arena_offline_expected_indices.update(range(start, end))
        item.user_properties.extend([
            ("offline_boundary_expected_test", expected),
            ("offline_boundary_blocked_operations", len(events)),
            ("offline_boundary_operation_counts", json.dumps(counts, sort_keys=True)),
        ])
        item.config._arena_offline_rows.append(dict(
            file=item.location[0], test=getattr(item, "originalname", None) or item.name.split("[")[0],
            expected_boundary_test=expected, blocked_operations=len(events), operation_counts=counts,
        ))
        if events and not expected:
            pytest.fail("Offline pytest boundary observed blocked operations, including caught exceptions: "
                        + ", ".join(counts), pytrace=False)


@pytest.hookimpl(wrapper=True, tryfirst=True)
def pytest_sessionfinish(session, exitstatus):
    # Finish after other session hooks so late, caught rejections cannot vanish
    # from either the exit status or the measured full-session totals.
    yield
    config = session.config
    operations = OfflineBoundary.blocked_operations
    all_indices = set(range(SESSION_EVENT_START, len(operations)))
    collection_indices = set(range(SESSION_EVENT_START, config._arena_offline_collection_end))
    runtime_indices = config._arena_offline_runtime_indices & all_indices
    expected_indices = config._arena_offline_expected_indices & all_indices
    unattributed_indices = all_indices - collection_indices - runtime_indices
    unexpected = len(all_indices - expected_indices)
    if unexpected and session.exitstatus == pytest.ExitCode.OK:
        session.exitstatus = pytest.ExitCode.TESTS_FAILED
    target = session.config.getoption("--offline-boundary-report")
    if target is not None:
        rows = session.config._arena_offline_rows
        report = dict(
            schema_version="arena-pytest-offline-boundary-1.1", exit_status=int(session.exitstatus),
            tests=rows, total_blocked_operations=len(all_indices),
            collection_blocked_operations=len(collection_indices),
            collection_operation_counts=dict(sorted(Counter(operations[i] for i in collection_indices).items())),
            runtime_blocked_operations=len(runtime_indices),
            intentional_boundary_test_operations=len(expected_indices),
            unattributed_blocked_operations=len(unattributed_indices),
            unattributed_operation_counts=dict(sorted(Counter(operations[i] for i in unattributed_indices).items())),
            unexpected_blocked_operations=unexpected,
        )
        path = Path(target)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def pytest_unconfigure(config):
    OFFLINE_BOUNDARY.close()
