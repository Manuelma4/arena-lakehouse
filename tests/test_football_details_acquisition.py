"""Original TEST ONLY HTTP bodies; never make a provider request."""

import copy
import hashlib
import http.client
import io
import json
import socket
import zipfile
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from arena_lakehouse import football_details_acquisition as acquisition
from arena_lakehouse.football_details import MATCH_MEMBER, build_package
from test_football_details import fixture_observations

START = datetime(2026, 10, 8, 11, tzinfo=timezone.utc)
SECRET = "TEST_ONLY_SIGNED_SECRET"


class Clock:
    def __init__(self):
        self.elapsed = 0.0

    def monotonic(self):
        return self.elapsed

    def utc_now(self):
        return START + timedelta(seconds=self.elapsed)


class Response:
    def __init__(self, body=b"", *, status=200, headers=None, clock=None, failure=None, delay=0.01):
        self.body = body
        self.status = status
        self.headers = headers if headers is not None else {"Content-Length": str(len(body))}
        self.clock = clock
        self.delay = delay
        self.failure = failure
        self.position = 0
        self.closed = False
        self.timeouts = []

    def read_chunk(self, maximum, timeout):
        self.timeouts.append(timeout)
        self.clock.elapsed += self.delay
        if self.failure:
            raise self.failure
        chunk = self.body[self.position:self.position + maximum]
        self.position += len(chunk)
        return chunk

    def close(self):
        self.closed = True


class Transport:
    def __init__(self, responses, clock, *, opening_delay=0.01):
        self.responses = responses
        self.clock = clock
        self.opening_delay = opening_delay
        self.calls = []

    def open(self, url, timeout):
        self.calls.append((url, timeout))
        self.clock.elapsed += self.opening_delay
        if len(self.calls) > len(self.responses):
            raise AssertionError("Unexpected TEST ONLY request")
        expected, response = self.responses[len(self.calls) - 1]
        assert url == expected
        if isinstance(response, Exception):
            raise response
        return response


def encode(value):
    return (json.dumps(value, ensure_ascii=False, sort_keys=True) + "\n").encode()


def metadata(source):
    return dict(id=source.article_id, version=source.article_version, title=source.title,
                doi=f"10.6084/m9.figshare.{source.article_id}.v{source.article_version}",
                is_public=True, is_confidential=False, is_embargoed=False, is_metadata_record=False,
                download_disabled=False, has_linked_file=False, published_date=source.published_at,
                authors=[dict(full_name="Luca Pappalardo"), dict(full_name="Emanuele Massucco")],
                license=dict(value=1, name=acquisition.LICENSE, url=acquisition.LICENSE_URL),
                description="Original TEST ONLY metadata; no original downloaded payload",
                files=[dict(id=source.file_id, name=source.filename, size=source.bytes, is_link_only=False,
                            download_url=source.download_url, supplied_md5=source.md5, computed_md5=source.md5)])


@pytest.fixture
def capture(monkeypatch):
    # Test-only original football records, with byte pins changed only in this fixture.
    matches, players, teams = fixture_observations()
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        info = zipfile.ZipInfo(MATCH_MEMBER, date_time=(1980, 1, 1, 0, 0, 0))
        archive.writestr(info, encode(matches))
    bodies = [buffer.getvalue(), encode(players), encode(teams)]
    sources = tuple(replace(source, bytes=len(body), md5=hashlib.md5(body).hexdigest(),
                            sha256=hashlib.sha256(body).hexdigest())
                    for source, body in zip(acquisition.SOURCES, bodies, strict=True))
    monkeypatch.setattr(acquisition, "SOURCES", sources)
    clock = Clock()
    responses = []
    for source, body in zip(sources, bodies, strict=True):
        storage = f"https://{acquisition.DOWNLOAD_HOST}{source.storage_path}?X-Amz-Signature={SECRET}"
        responses.extend([
            (source.metadata_url, Response(encode(metadata(source)), clock=clock)),
            (source.download_url, Response(status=302, headers={"Location": storage}, clock=clock)),
            (storage, Response(body, clock=clock)),
        ])
    return sources, bodies, clock, Transport(responses, clock)


def run(tmp_path, capture, **kwargs):
    _, _, clock, transport = capture
    output = tmp_path / "capture"
    result = acquisition.acquire(output, transport=transport, monotonic=clock.monotonic,
                                 utc_now=clock.utc_now, **kwargs)
    return result, output


def failed(tmp_path, capture, code=None, **kwargs):
    with pytest.raises(acquisition.AcquisitionError) as error:
        run(tmp_path, capture, **kwargs)
    if code:
        assert str(error.value) == code
    output = tmp_path / "capture"
    assert not (output / "source-manifest.json").exists()
    ledger = json.loads((output / "acquisition.json").read_bytes())
    assert ledger["status"] == "failed"
    assert SECRET not in json.dumps(ledger)
    assert not (output / ".acquisition.lock").exists()
    assert not list(output.glob(".part-*"))
    return output, ledger


def test_production_pins_match_retained_official_originals():
    assert [(s.article_id, s.article_version, s.file_id, s.bytes) for s in acquisition.SOURCES] == [
        (7770422, 1, 14464622, 645097), (7765196, 3, 15073721, 1737347), (7765310, 3, 15073697, 27404)]
    assert acquisition.SOURCES[0].sha256 == "c8f92bb7533e5c127e043cee764c991b5c25b4f5e70a65be931baae0b1765ce9"
    assert acquisition.SOURCES[1].sha256 == "877a111cb1005b73df5645e9338bd74fb4b496bace2fbc545a72abb3b73efa2e"
    assert acquisition.SOURCES[2].sha256 == "9f7a4a3b3d92c0be33f40613ad6e6eb4316c3b9771ec74c61a22c9b8ece23a4d"


def test_acquires_versioned_metadata_originals_and_adapter_compatible_manifest(tmp_path, capture):
    result, output = run(tmp_path, capture)
    sources, bodies, _, transport = capture
    assert result["status"] == "acquired" and result["requests"] == 9
    descriptor = json.loads((output / "source-manifest.json").read_bytes())
    ledger = json.loads((output / "acquisition.json").read_bytes())
    assert result["bytes_read"] == sum(len(response.body) for _, response in transport.responses)
    assert all(response.closed for _, response in transport.responses)
    assert all(0 < timeout <= acquisition.MAX_IO_SECONDS for _, timeout in transport.calls)
    assert SECRET not in (output / "acquisition.json").read_text()
    assert SECRET not in (output / "source-manifest.json").read_text()
    for source, body, row in zip(sources, bodies, descriptor["sources"], strict=True):
        assert (output / source.filename).read_bytes() == body
        assert row["metadata_observed_at"].startswith("2026-10-08T11:")
        assert row["published_at"].startswith("2019-")
        assert row["url"] == source.download_url
        assert row["metadata_sha256"] == hashlib.sha256((output / source.metadata_filename).read_bytes()).hexdigest()
    assert ledger["requests"][1]["body_complete"] is False
    package = build_package(output / "matches.zip", output / "players.json", output / "teams.json",
                            output / "source-manifest.json", tmp_path / "package")
    assert package["record_counts"]["matches"] == 1
    assert package["capabilities"]["pre_match_availability_evidence"] is False


def test_direct_official_downloads_are_six_requests(tmp_path, capture):
    sources, _, _, transport = capture
    transport.responses = [(url, response) for i, (url, response) in enumerate(transport.responses) if i % 3 != 1]
    transport.responses = [(sources[i // 2].download_url if i % 2 else url, response)
                           for i, (url, response) in enumerate(transport.responses)]
    result, output = run(tmp_path, capture)
    assert result["requests"] == 6
    assert json.loads((output / "acquisition.json").read_bytes())["status"] == "acquired"


def test_complete_capture_replays_without_network_or_byte_clock_changes(tmp_path, capture):
    _, output = run(tmp_path, capture)
    before = {path.name: (path.read_bytes(), path.stat().st_mtime_ns) for path in output.iterdir()}
    _, _, clock, transport = capture
    count = len(transport.calls)
    result = acquisition.acquire(output, transport=transport, monotonic=clock.monotonic, utc_now=clock.utc_now)
    assert result["status"] == "already_verified" and result["requests"] == 0
    assert len(transport.calls) == count
    assert before == {path.name: (path.read_bytes(), path.stat().st_mtime_ns) for path in output.iterdir()}


@pytest.mark.parametrize("mutation", [
    lambda d: d.update(id=1), lambda d: d.update(version=2), lambda d: d.update(version=True),
    lambda d: d.update(title="Changed"), lambda d: d.update(doi="10.6084/m9.figshare.7770422.v2"),
    lambda d: d.update(is_public=False), lambda d: d.update(is_confidential=True),
    lambda d: d.update(is_embargoed=True), lambda d: d.update(download_disabled=True),
    lambda d: d.update(has_linked_file=True), lambda d: d.update(is_metadata_record=True),
    lambda d: d.update(published_date="2019-01-01T00:00:00Z"),
    lambda d: d["license"].update(name="CC BY-NC 4.0"), lambda d: d["license"].update(url="https://example.test"),
    lambda d: d["license"].update(value=True), lambda d: d.update(authors=[]),
    lambda d: d["authors"][0].update(full_name=[]), lambda d: d.update(files=[]),
    lambda d: d["files"].append(copy.deepcopy(d["files"][0])),
    lambda d: d["files"][0].update(id=1), lambda d: d["files"][0].update(name="other.zip"),
    lambda d: d["files"][0].update(size=1), lambda d: d["files"][0].update(is_link_only=True),
    lambda d: d["files"][0].update(download_url=f"https://example.test/?token={SECRET}"),
    lambda d: d["files"][0].update(supplied_md5="a" * 32), lambda d: d["files"][0].update(computed_md5="b" * 32),
])
def test_metadata_drift_blocks_files_before_download(tmp_path, capture, mutation):
    source, _, clock, transport = capture
    value = metadata(source[0])
    mutation(value)
    transport.responses[0] = (source[0].metadata_url, Response(encode(value), clock=clock))
    output, ledger = failed(tmp_path, capture)
    assert ledger["request_count"] == 1
    assert not (output / "matches.zip").exists()


@pytest.mark.parametrize("body", [b"{\"id\":1,\"id\":2}", b"{\"id\":NaN}", b"[]", b"\xff", b"not JSON"])
def test_malformed_metadata_has_no_published_descriptor(tmp_path, capture, body):
    sources, _, clock, transport = capture
    transport.responses[0] = (sources[0].metadata_url, Response(body, clock=clock))
    failed(tmp_path, capture)


@pytest.mark.parametrize("location", [
    "http://s3-eu-west-1.amazonaws.com/pfigshare-u-files/14464622/matches.zip",
    "https://s3-eu-west-1.amazonaws.com.evil.test/pfigshare-u-files/14464622/matches.zip",
    "https://evil.test/matches.zip", "https://127.0.0.1/matches.zip",
    "https://user@s3-eu-west-1.amazonaws.com/pfigshare-u-files/14464622/matches.zip",
    "https://s3-eu-west-1.amazonaws.com:443/pfigshare-u-files/14464622/matches.zip",
    "https://s3-eu-west-1.amazonaws.com/pfigshare-u-files/OTHER/matches.zip",
    "https://s3-eu-west-1.amazonaws.com/pfigshare-u-files/14464622/../matches.zip",
    "https://s3-eu-west-1.amazonaws.com/pfigshare-u-files/14464622/matches.zip#fragment",
    "https://s3-eu-west-1.amazonaws.com/pfigshare-u-files/14464622/matches.zip\r\n",
    "https://s3-eu-west-1.amazonaws.com/pfigshare-u-files/14464622/%6datches.zip",
    "https://s3-eu-west-1.amazonaws.com:invalid/pfigshare-u-files/14464622/matches.zip",
    "/relative", None, "x" * 8193,
])
def test_redirect_requires_exact_independently_established_host_and_path(tmp_path, capture, location):
    _, _, _, transport = capture
    transport.responses[1][1].headers["Location"] = location
    _, ledger = failed(tmp_path, capture)
    assert ledger["request_count"] == 2


@pytest.mark.parametrize("index,status", [(0, 302), (1, 301), (1, 303), (1, 307), (1, 308), (2, 302),
                                         (0, 401), (2, 403), (0, 429), (0, 503)])
def test_no_metadata_redirect_second_redirect_challenge_or_retry(tmp_path, capture, index, status):
    _, _, _, transport = capture
    transport.responses[index][1].status = status
    transport.responses[index][1].headers["Location"] = f"https://evil.test/?token={SECRET}"
    _, ledger = failed(tmp_path, capture)
    assert ledger["request_count"] == index + 1
    assert len(transport.calls) == index + 1


@pytest.mark.parametrize("mutation,code", [
    (lambda r: setattr(r, "body", r.body[:-1]), "response_length_mismatch"),
    (lambda r: setattr(r, "body", bytes([r.body[0] ^ 1]) + r.body[1:]), "original_digest_mismatch"),
    (lambda r: r.headers.update({"Content-Length": "999999999"}), "response_size_bound"),
    (lambda r: r.headers.update({"Content-Length": "+1"}), "response_size_bound"),
    (lambda r: r.headers.update({"Content-Length": "-1"}), "response_size_bound"),
    (lambda r: r.headers.update({"Content-Encoding": "gzip"}), "encoded_body_not_allowed"),
])
def test_corrupt_truncated_oversized_or_encoded_originals_are_not_published(tmp_path, capture, mutation, code):
    _, _, _, transport = capture
    mutation(transport.responses[2][1])
    failed(tmp_path, capture, code)


def test_unknown_length_stream_is_bounded_instead_of_buffered_unbounded(tmp_path, capture):
    _, _, _, transport = capture
    response = transport.responses[0][1]
    response.headers = {}
    response.body = b"x" * (acquisition.MAX_METADATA_BYTES + 10_000)
    _, ledger = failed(tmp_path, capture, "response_size_bound")
    assert response.position == acquisition.MAX_METADATA_BYTES + 1
    assert ledger["bytes_read"] == acquisition.MAX_METADATA_BYTES + 1


def test_aggregate_body_and_request_budgets_are_enforced(tmp_path, capture, monkeypatch):
    monkeypatch.setattr(acquisition, "MAX_TOTAL_BYTES", 10)
    _, ledger = failed(tmp_path, capture, "total_response_size_bound")
    assert ledger["bytes_read"] == 11


def test_wire_request_budget_prevents_opening_an_extra_connection(tmp_path, capture, monkeypatch):
    monkeypatch.setattr(acquisition, "MAX_REQUESTS", 2)
    _, ledger = failed(tmp_path, capture, "request_budget_exceeded")
    assert ledger["request_count"] == 2 and len(capture[3].calls) == 2


@pytest.mark.parametrize("where", ["open", "read"])
def test_checked_deadline_prevents_manifest_and_does_not_renew_budget(tmp_path, capture, where):
    transport = capture[3]
    if where == "open":
        transport.opening_delay = 0.2
    else:
        transport.responses[0][1].delay = 0.2
    _, ledger = failed(tmp_path, capture, "acquisition_deadline_exceeded", deadline_seconds=0.1)
    assert ledger["request_count"] == 1
    assert transport.calls[0][1] == 0.1


@pytest.mark.parametrize("failure", [OSError(f"remote {SECRET}"), TimeoutError(f"signed {SECRET}"),
                                     acquisition.AcquisitionError(f"https://secret.test/?token={SECRET}")])
def test_transport_exception_text_is_never_retained(tmp_path, capture, failure):
    sources, _, _, transport = capture
    transport.responses[0] = (sources[0].metadata_url, failure)
    failed(tmp_path, capture)


def test_stream_failure_closes_response_and_does_not_publish(tmp_path, capture):
    transport = capture[3]
    transport.responses[2][1].failure = OSError(f"signed URL {SECRET}")
    failed(tmp_path, capture, "transport_failure")
    assert transport.responses[2][1].closed


@pytest.mark.parametrize("seconds", [0, -1, 181, float("inf"), float("nan"), True, "120"])
def test_deadline_input_rejected_without_creating_output(tmp_path, capture, seconds):
    with pytest.raises(acquisition.AcquisitionError, match="invalid_deadline_seconds"):
        run(tmp_path, capture, deadline_seconds=seconds)
    assert not (tmp_path / "capture").exists()
    assert not capture[3].calls


@pytest.mark.parametrize("failed_filename", ["matches.zip", "article-7765196-v3.json", "source-manifest.json"])
def test_partial_publication_retains_sanitized_failure_ledger_without_descriptor(tmp_path, capture, monkeypatch,
                                                                               failed_filename):
    original = acquisition._atomic_write

    def partial(path, body, **kwargs):
        if path.name == failed_filename:
            # A failed rename/write, not a truncated completed input manifest.
            raise OSError(f"TEST ONLY local path {SECRET}")
        return original(path, body, **kwargs)

    monkeypatch.setattr(acquisition, "_atomic_write", partial)
    output, ledger = failed(tmp_path, capture, "local_write_failure")
    assert ledger["request_count"] == 9
    before = {p.name: p.read_bytes() for p in output.iterdir()}
    with pytest.raises(acquisition.AcquisitionError, match="existing_capture_incomplete_or_unrelated"):
        acquisition.acquire(output, transport=capture[3])
    assert before == {p.name: p.read_bytes() for p in output.iterdir()}


def test_unrelated_directory_and_existing_lock_are_preserved_without_requests(tmp_path, capture):
    output = tmp_path / "capture"
    output.mkdir()
    (output / "keep.txt").write_bytes(b"TEST ONLY retained file")
    with pytest.raises(acquisition.AcquisitionError, match="existing_capture_incomplete_or_unrelated"):
        acquisition.acquire(output, transport=capture[3])
    assert list(p.name for p in output.iterdir()) == ["keep.txt"]
    (output / ".acquisition.lock").write_bytes(b"TEST ONLY other writer")
    with pytest.raises(acquisition.AcquisitionError, match="acquisition_locked"):
        acquisition.acquire(output, transport=capture[3])
    assert (output / ".acquisition.lock").read_bytes() == b"TEST ONLY other writer"
    assert not capture[3].calls


@pytest.mark.parametrize("filename", ["matches.zip", "players.json", "teams.json", "article-7770422-v1.json",
                                      "source-manifest.json", "acquisition.json"])
def test_replay_rechecks_every_original_metadata_and_manifest_and_refuses_drift(tmp_path, capture, filename):
    _, output = run(tmp_path, capture)
    path = output / filename
    body = path.read_bytes()
    path.write_bytes(bytes([body[0] ^ 1]) + body[1:])
    before = {p.name: p.read_bytes() for p in output.iterdir()}
    request_count = len(capture[3].calls)
    with pytest.raises(acquisition.AcquisitionError):
        acquisition.acquire(output, transport=capture[3])
    assert len(capture[3].calls) == request_count
    assert before == {p.name: p.read_bytes() for p in output.iterdir()}


def test_replay_rejects_edited_capture_clock_even_with_recomputed_descriptor_hash(tmp_path, capture):
    _, output = run(tmp_path, capture)
    descriptor = json.loads((output / "source-manifest.json").read_bytes())
    descriptor["sources"][0]["observed_at"] = "2026-10-09T11:00:00Z"
    body = acquisition._json(descriptor)
    (output / "source-manifest.json").write_bytes(body)
    ledger = json.loads((output / "acquisition.json").read_bytes())
    ledger["source_manifest_sha256"] = hashlib.sha256(body).hexdigest()
    (output / "acquisition.json").write_bytes(acquisition._json(ledger))
    with pytest.raises(acquisition.AcquisitionError, match="existing_capture_invalid"):
        acquisition.acquire(output, transport=capture[3])


def test_public_transport_request_has_no_proxy_cookies_auth_or_automatic_redirects(monkeypatch):
    recorded = {}

    class Opener:
        def open(self, request, timeout):
            recorded["request"] = request
            recorded["timeout"] = timeout
            return type("TESTResponse", (), {"status": 200, "headers": {}})()

    def opener(*handlers):
        recorded["handlers"] = handlers
        return Opener()

    monkeypatch.setattr(acquisition, "build_opener", opener)
    response = acquisition.PublicTransport().open(acquisition.SOURCES[0].metadata_url, 2.0)
    assert response.status == 200
    assert recorded["handlers"][0].proxies == {}
    assert recorded["handlers"][1].redirect_request(None, None, 302, None, None, None) is None
    headers = {key.lower(): value for key, value in recorded["request"].header_items()}
    assert not set(headers) & {"authorization", "cookie", "proxy-authorization"}
    assert headers["accept-encoding"] == "identity"
    assert recorded["timeout"] == 2.0


def test_real_stdlib_http_response_reads_final_chunk_and_eof_with_timeout_bound():
    # TEST ONLY in-process socket pair, not an internet connection.
    client, server = socket.socketpair()
    try:
        server.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nabc")
        raw = http.client.HTTPResponse(client)
        raw.begin()
        response = acquisition._Response(raw)
        assert response.read_chunk(10, 0.25) == b"abc"
        assert response.read_chunk(10, 0.25) == b""
        response.close()
    finally:
        client.close()
        server.close()


def test_cli_prints_only_summary_and_sanitized_error(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(acquisition, "acquire", lambda *args, **kwargs: {"status": "already_verified", "requests": 0})
    acquisition.main(["--output", str(tmp_path / "capture")])
    assert json.loads(capsys.readouterr().out) == {"status": "already_verified", "requests": 0}
    monkeypatch.setattr(acquisition, "acquire", lambda *args, **kwargs: (_ for _ in ()).throw(OSError(SECRET)))
    with pytest.raises(SystemExit) as error:
        acquisition.main(["--output", str(tmp_path / "capture")])
    assert error.value.code == 1 and SECRET not in capsys.readouterr().err
