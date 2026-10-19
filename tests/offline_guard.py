"""Process-local pytest boundary; production and evaluation commands do not import this module."""

from __future__ import annotations

import os
from pathlib import Path
import socket
import sys
import tempfile
import threading
from unittest.mock import patch


class OfflineBoundaryViolation(AssertionError):
    """A missing fixture double attempted a real service or socket operation."""


class OfflineBoundary:
    # Shared across nested guards so a deliberately caught exception is still
    # visible to the pytest teardown check. Contains operation names only.
    blocked_operations = []
    PREFIXES = ("ARENA_", "MLFLOW_", "OLLAMA_", "LANGCHAIN_", "LANGSMITH_")
    CREDENTIALS = {"PANDASCORE_TOKEN", "FOOTBALL_DATA_TOKEN", "FOOTBALL_DATA_API_KEY"}
    NETWORK_EVENTS = {
        "socket.connect", "socket.bind", "socket.getaddrinfo", "socket.gethostbyname",
        "socket.gethostbyaddr", "socket.getnameinfo", "socket.sendto", "socket.sendmsg",
    }

    def __init__(self):
        self.active = False
        self.local = threading.local()
        self.patches = []
        self.environment = {}
        self.temporary = None
        self.blocked_events = []

    def reject(self, operation):
        # Never include request URLs, addresses, environment values or bodies.
        self.blocked_events.append(operation)
        self.blocked_operations.append(operation)
        raise OfflineBoundaryViolation(
            f"Offline pytest boundary blocked {operation}; provide an explicit fixture double or mock transport"
        )

    def audit(self, event, _arguments):
        if self.active and event in self.NETWORK_EVENTS and not getattr(self.local, "socketpair_depth", 0):
            self.reject(event)

    def _patch(self, owner, attribute, replacement):
        replacement_patch = patch.object(owner, attribute, replacement)
        replacement_patch.start()
        self.patches.append(replacement_patch)

    def install(self):
        if self.active:
            return
        names = {key for key in os.environ if key.upper().startswith(self.PREFIXES)
                 or key.upper() in self.CREDENTIALS}
        names.add("PYTHON_DOTENV_DISABLED")
        self.environment = {key: os.environ.get(key) for key in names}
        for key in names:
            os.environ.pop(key, None)
        self.temporary = tempfile.TemporaryDirectory(prefix="arena-offline-pytest-")
        root = Path(self.temporary.name)
        self.set_environment("ARENA_STATE", str(root / "state"))
        self.set_environment("ARENA_LIVE_WORKSPACE", str(root / "live"))
        self.set_environment("PYTHON_DOTENV_DISABLED", "1")
        self.active = True
        # CPython audit hooks cannot be removed. This hook becomes inert on close().
        sys.addaudithook(self.audit)

        original_socketpair = socket.socketpair

        def internal_socketpair(*args, **kwargs):
            depth = getattr(self.local, "socketpair_depth", 0)
            self.local.socketpair_depth = depth + 1
            try:
                return original_socketpair(*args, **kwargs)
            finally:
                self.local.socketpair_depth = depth

        self._patch(socket, "socketpair", internal_socketpair)
        for name in ("connect", "connect_ex", "sendto"):
            original = getattr(socket.socket, name)

            def guarded_socket(candidate, *args, _original=original, _name=name, **kwargs):
                if getattr(self.local, "socketpair_depth", 0):
                    return _original(candidate, *args, **kwargs)
                self.reject("socket." + _name)

            self._patch(socket.socket, name, guarded_socket)

        def no_connection(*args, **kwargs):
            self.reject("socket.create_connection")

        self._patch(socket, "create_connection", no_connection)

        import dotenv
        import dotenv.main
        import httpx

        def no_dotenv(*args, **kwargs):
            return False

        def no_http(*args, **kwargs):
            self.reject("httpx.HTTPTransport")

        async def no_async_http(*args, **kwargs):
            self.reject("httpx.AsyncHTTPTransport")

        self._patch(dotenv, "load_dotenv", no_dotenv)
        self._patch(dotenv.main, "load_dotenv", no_dotenv)
        self._patch(httpx.HTTPTransport, "handle_request", no_http)
        self._patch(httpx.AsyncHTTPTransport, "handle_async_request", no_async_http)

    def set_environment(self, name, value):
        if name not in self.environment:
            self.environment[name] = os.environ.get(name)
        os.environ[name] = value

    def close(self):
        self.active = False
        for replacement_patch in reversed(self.patches):
            replacement_patch.stop()
        self.patches.clear()
        # A pytest.main() caller must not inherit new test-introduced source or
        # tracing configuration after this boundary restores its original state.
        for key in list(os.environ):
            if key.upper().startswith(self.PREFIXES) or key.upper() in self.CREDENTIALS:
                os.environ.pop(key, None)
        for key, original in self.environment.items():
            if original is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = original
        self.environment.clear()
        if self.temporary is not None:
            self.temporary.cleanup()
            self.temporary = None
