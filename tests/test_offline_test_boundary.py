"""The pytest boundary fails before real transports while allowing owned in-process fixtures."""

import asyncio
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys

import dotenv
import httpx
import pytest

from offline_guard import OfflineBoundary, OfflineBoundaryViolation


@pytest.mark.parametrize("method", ["connect", "connect_ex"])
@pytest.mark.parametrize("host", ["127.0.0.1", "192.0.2.1"])
def test_socket_connections_fail_before_native_transport(method, host):
    with socket.socket() as candidate, pytest.raises(OfflineBoundaryViolation, match="Offline pytest boundary"):
        getattr(candidate, method)((host, 11434))


def test_connection_helper_and_dns_are_blocked():
    with pytest.raises(OfflineBoundaryViolation, match="socket.create_connection"):
        socket.create_connection(("127.0.0.1", 11434))
    with pytest.raises(OfflineBoundaryViolation, match="socket.getaddrinfo"):
        socket.getaddrinfo("provider.invalid", 443)
    with pytest.raises(OfflineBoundaryViolation, match="socket.getnameinfo"):
        socket.getnameinfo(("192.0.2.1", 80), 0)


def test_native_connection_double_is_not_called(monkeypatch):
    # Install a nested boundary over a native-method spy; rejection precedes the spy.
    called = []
    monkeypatch.setattr(socket.socket, "connect", lambda *args: called.append(True))
    guard = OfflineBoundary()
    guard.install()
    try:
        with socket.socket() as candidate, pytest.raises(OfflineBoundaryViolation):
            candidate.connect(("127.0.0.1", 11434))
        assert called == []
    finally:
        guard.close()


def test_socketpair_remains_functional_without_loopback_exception():
    first, second = socket.socketpair()
    try:
        first.sendall(b"owned fixture")
        assert second.recv(32) == b"owned fixture"
    finally:
        first.close()
        second.close()
    with socket.socket() as candidate, pytest.raises(OfflineBoundaryViolation):
        candidate.connect(("127.0.0.1", 11434))


def test_sync_real_http_is_rejected_before_httpcore(monkeypatch):
    import httpcore

    monkeypatch.setattr(httpcore.ConnectionPool, "handle_request",
                        lambda *args: pytest.fail("Native HTTP pool must not be reached"))
    with httpx.Client() as client, pytest.raises(OfflineBoundaryViolation, match="httpx.HTTPTransport"):
        client.get("http://127.0.0.1:11434/api/tags")


def test_async_real_http_is_rejected_before_httpcore(monkeypatch):
    import httpcore

    async def native_pool(*args, **kwargs):
        pytest.fail("Native asynchronous HTTP pool must not be reached")

    monkeypatch.setattr(httpcore.AsyncConnectionPool, "handle_async_request", native_pool)

    async def run():
        async with httpx.AsyncClient() as client:
            with pytest.raises(OfflineBoundaryViolation, match="httpx.AsyncHTTPTransport"):
                await client.get("http://127.0.0.1:11434/api/tags")

    asyncio.run(run())


def test_owned_mock_transports_are_available_sync_and_async():
    calls = []

    def respond(request):
        calls.append(request.url.path)
        return httpx.Response(200, json={"source": "test double"})

    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        assert client.get("https://fixture.invalid/sync").json() == {"source": "test double"}

    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            assert (await client.get("https://fixture.invalid/async")).json() == {"source": "test double"}

    asyncio.run(run())
    assert calls == ["/sync", "/async"]


def test_bare_async_asgi_transport_needs_no_network():
    # The producer does not depend on a web framework. An owned ASGI fixture
    # proves that denying real HTTP transports does not deny in-process I/O.
    async def app(scope, receive, send):
        assert scope["type"] == "http" and scope["path"] == "/fixture"
        await send({"type": "http.response.start", "status": 200,
                    "headers": [(b"content-type", b"application/json")]})
        await send({"type": "http.response.body", "body": b'{"fixture":true}'})

    async def run():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://fixture") as client:
            assert (await client.get("/fixture")).json() == {"fixture": True}

    asyncio.run(run())


def test_dotenv_does_not_read_files_even_if_disabled_flag_removed(tmp_path, monkeypatch):
    path = tmp_path / ".env"
    path.write_text("ARENA_PRIVATE_TEST_SENTINEL=must-not-load\n", encoding="utf-8")
    monkeypatch.delenv("PYTHON_DOTENV_DISABLED")
    assert dotenv.load_dotenv(path, override=True) is False
    assert "ARENA_PRIVATE_TEST_SENTINEL" not in os.environ


def test_inherited_configuration_is_removed_and_subprocess_receives_only_test_defaults(monkeypatch):
    names = ["ARENA_SNAPSHOT", "ARENA_STATE", "MLFLOW_TRACKING_URI", "OLLAMA_URL", "LANGSMITH_API_KEY"]
    original = {name: "private-sentinel-value" for name in names}
    for name, value in original.items():
        monkeypatch.setenv(name, value)
    previous_connect = socket.socket.connect
    previous_dotenv = dotenv.load_dotenv
    guard = OfflineBoundary()
    guard.install()
    try:
        child = subprocess.run(
            [sys.executable, "-c", "import os,json; print(json.dumps({k:v for k,v in os.environ.items() "
             "if k.startswith(('ARENA_', 'MLFLOW_', 'OLLAMA_', 'LANGSMITH_'))}))"],
            capture_output=True, text=True, check=True, timeout=15,
        )
        values = json.loads(child.stdout)
        assert set(values) == {"ARENA_STATE", "ARENA_LIVE_WORKSPACE"}
        assert all(value != "private-sentinel-value" for value in values.values())
        assert Path(values["ARENA_STATE"]).parent == Path(guard.temporary.name)
        os.environ["ARENA_TEST_NEW_BOUNDARY_SETTING"] = "owned-test-value"
    finally:
        guard.close()
    assert {name: os.environ[name] for name in names} == original
    assert socket.socket.connect is previous_connect
    assert dotenv.load_dotenv is previous_dotenv
    assert "ARENA_TEST_NEW_BOUNDARY_SETTING" not in os.environ


def test_caught_boundary_violation_still_fails_teardown_and_is_reported(tmp_path):
    # Exercise the actual pytest hooks in a child process, using only generated
    # fixture code. The deliberately swallowed exception never reaches a socket.
    tests = tmp_path / "tests"
    tests.mkdir()
    source = Path(__file__).parent
    for name in ("conftest.py", "offline_guard.py"):
        shutil.copyfile(source / name, tests / name)
    (tests / "test_caught_boundary.py").write_text(
        "import socket\n"
        "def test_caught_violation():\n"
        "    try:\n"
        "        socket.create_connection(('127.0.0.1', 11434))\n"
        "    except AssertionError:\n"
        "        pass\n", encoding="utf-8",
    )
    report = tmp_path / "boundary-report.json"
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "tests", "--offline-boundary-report", str(report)],
        cwd=tmp_path, capture_output=True, text=True, timeout=30,
        env={**os.environ, "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1"},
    )
    assert result.returncode == 1
    assert "including caught exceptions" in result.stdout
    measured = json.loads(report.read_text(encoding="utf-8"))
    assert measured["unexpected_blocked_operations"] == measured["total_blocked_operations"] == 1
    assert measured["tests"][0]["operation_counts"] == {"socket.create_connection": 1}
    assert measured["tests"][0]["expected_boundary_test"] is False


@pytest.mark.parametrize("phase", ["collection", "session_finish"])
def test_caught_out_of_test_violation_is_counted_and_fails_session(tmp_path, phase):
    tests = tmp_path / "tests"
    tests.mkdir()
    source = Path(__file__).parent
    for name in ("conftest.py", "offline_guard.py"):
        shutil.copyfile(source / name, tests / name)
    caught = ("try:\n"
              "    socket.create_connection(('127.0.0.1', 11434))\n"
              "except AssertionError:\n"
              "    pass\n")
    test_code = "def test_owned_fixture():\n    assert True\n"
    if phase == "collection":
        test_code = "import socket\n" + caught + test_code
    else:
        # Another plugin's late session hook runs after the tests. The outer
        # boundary session wrapper must still account for its caught rejection.
        (tests / "conftest.py").write_text(
            (tests / "conftest.py").read_text(encoding="utf-8")
            + '\npytest_plugins = ["fixture_late_hook"]\n', encoding="utf-8",
        )
        (tmp_path / "fixture_late_hook.py").write_text(
            "import socket\ndef pytest_sessionfinish(session, exitstatus):\n"
            + "".join("    " + line + "\n" for line in caught.splitlines()), encoding="utf-8",
        )
    (tests / "test_owned_fixture.py").write_text(test_code, encoding="utf-8")
    report = tmp_path / "boundary-report.json"
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "tests", "--offline-boundary-report", str(report)],
        cwd=tmp_path, capture_output=True, text=True, timeout=30,
        env={**os.environ, "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1"},
    )
    assert result.returncode != 0, result.stdout
    measured = json.loads(report.read_text(encoding="utf-8"))
    assert measured["exit_status"] == result.returncode
    assert measured["unexpected_blocked_operations"] == measured["total_blocked_operations"] == 1
    assert measured["runtime_blocked_operations"] == measured["intentional_boundary_test_operations"] == 0
    expected_bucket = "collection" if phase == "collection" else "unattributed"
    assert measured[expected_bucket + "_blocked_operations"] == 1
    assert measured[expected_bucket + "_operation_counts"] == {"socket.create_connection": 1}
