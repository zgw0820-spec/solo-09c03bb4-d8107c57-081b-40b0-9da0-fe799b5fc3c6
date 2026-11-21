"""End-to-end HTTP tests against the FastAPI app (in-process ASGI client)."""
from __future__ import annotations

import importlib
import os

import pytest

# Configure before the app module is imported.
os.environ["DB_PATH"] = ":memory-test:"  # replaced per-test via fixture env below
os.environ["STALE_AFTER_SECONDS"] = "10"
os.environ["REQUIRE_REMOTE_WRITE_HEADER"] = "1"

from fastapi.testclient import TestClient  # noqa: E402

from app import config  # noqa: E402
from app.main import create_app  # noqa: E402
from tests.conftest import BASE_TS, RW_HEADERS, body, series  # noqa: E402


@pytest.fixture()
def client(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "DB_PATH", str(tmp_path / "http.db"))
    monkeypatch.setattr(config, "STALE_AFTER_SECONDS", 10.0)
    app = create_app()
    with TestClient(app) as c:
        yield c


def post(client, cluster, replica, points, now_offset=0, **kw):
    # HTTP layer uses wall clock for staleness; test points carry absolute ms.
    return client.post(
        "/api/v1/write",
        content=body([series(cluster, replica, points, **kw)]),
        headers=RW_HEADERS,
    )


def test_healthz(client):
    assert client.get("/healthz").json() == {"status": "ok"}


def test_remote_write_accepted_and_persisted(client):
    resp = post(client, "demo", "A", [(0, 1.0), (1000, 2.0)])
    assert resp.status_code == 204
    assert resp.headers["x-ingest-accepted"] == "1"
    assert resp.headers["x-ingest-role"] == "initial"

    status = client.get("/api/status").json()
    assert status["clusters"][0]["active_replica"] == "A"
    rows = client.get("/api/series").json()["series"]
    assert len(rows) == 1
    sid = rows[0]["id"]
    samples = client.get(f"/api/series/{sid}/samples").json()["samples"]
    assert [s["value"] for s in samples] == [1.0, 2.0]


def test_standby_batch_returns_204_but_marks_rejected(client):
    post(client, "demo", "A", [(0, 1.0)])
    resp = post(client, "demo", "B", [(1000, 9.0)])
    assert resp.status_code == 204
    assert resp.headers["x-ingest-accepted"] == "0"
    assert resp.headers["x-ingest-role"] == "standby"


def test_missing_protocol_headers_rejected(client):
    payload = body([series("demo", "A", [(0, 1.0)])])
    resp = client.post("/api/v1/write", content=payload)
    assert resp.status_code == 400
    resp = client.post(
        "/api/v1/write", content=payload,
        headers={"X-Prometheus-Remote-Write-Version": "0.1"},
    )
    assert resp.status_code == 400


def test_bad_payload_rejected(client):
    resp = client.post(
        "/api/v1/write", content=b"junk", headers=RW_HEADERS
    )
    assert resp.status_code == 400


def test_takeover_over_real_wall_clock(client):
    # Initial active A.
    assert post(client, "demo", "A", [(0, 1.0)]).status_code == 204
    # B immediately -> standby.
    assert post(client, "demo", "B", [(1000, 2.0)]).headers["x-ingest-role"] == "standby"

    import time
    time.sleep(10.2)  # exceed STALE_AFTER_SECONDS without any new point from A

    resp = post(client, "demo", "B", [(2000, 3.0)])
    assert resp.status_code == 204
    assert resp.headers["x-ingest-role"] == "takeover"
    status = client.get("/api/status").json()["clusters"][0]
    assert status["active_replica"] == "B"
    assert status["stale"] is False
    events = client.get("/api/events").json()["events"]
    assert events[0]["event_type"] == "TAKEOVER"


def test_old_retransmit_over_http_does_not_grow_series(client):
    post(client, "demo", "A", [(0, 1.0), (1000, 2.0)])
    resp = post(client, "demo", "A", [(0, 1.0), (1000, 2.0)])
    assert resp.headers["x-ingest-stored"] == "0"
    assert resp.headers["x-ingest-stale"] == "2"
    rows = client.get("/api/series").json()["series"]
    assert rows[0]["sample_count"] == 2


def test_empty_write_request_is_noop(client):
    resp = client.post("/api/v1/write", content=body([]), headers=RW_HEADERS)
    assert resp.status_code == 204
    assert resp.headers["x-ingest-role"] == "noop"
    assert client.get("/api/series").json()["series"] == []


def test_index_page_served(client):
    resp = client.get("/")
    assert resp.status_code == 200
    assert "Remote Write" in resp.text


def test_replica_endpoints_graceful_without_docker(client, monkeypatch):
    from app import dockerctl

    def boom():
        raise dockerctl.DockerUnavailable("no socket")

    monkeypatch.setattr(dockerctl, "_client", boom)
    assert client.get("/api/replicas").status_code == 503
    assert client.post("/api/replicas/A/pause").status_code == 503
    assert client.post("/api/replicas/ZZ/pause").status_code == 404
