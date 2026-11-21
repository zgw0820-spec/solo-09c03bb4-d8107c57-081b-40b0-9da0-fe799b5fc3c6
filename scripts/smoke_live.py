"""Live smoke test against a running uvicorn receiver (run with -I)."""
import site
import sys
import time
import urllib.request

# -I ignores PYTHON* env vars, so make the user-provided deps explicit.
site.addsitedir("/home/node/.local/lib/python3.11/site-packages")
sys.path.insert(0, "/workspace/receiver")
sys.path.insert(0, "/workspace/tests")

from conftest import RW_HEADERS, body, series  # noqa: E402

import os
BASE = os.environ.get("SMOKE_BASE", "http://127.0.0.1:8077")


def post(points, replica, cluster="demo"):
    req = urllib.request.Request(
        BASE + "/api/v1/write",
        data=body([series(cluster, replica, points)]),
        headers=RW_HEADERS,
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=5) as resp:
        return resp.status, {k.lower(): v for k, v in resp.headers.items()}


def get(path):
    with urllib.request.urlopen(BASE + path, timeout=5) as resp:
        return resp.read().decode()


print("health:", get("/healthz"))
t0 = int(time.time() * 1000)

s, h = post([(0, 1.0), (1000, 2.0)], "A")
print("A initial:", s, h.get("x-ingest-role"), h.get("x-ingest-stored"))

s, h = post([(2000, 9.0)], "B")
print("B standby:", s, h.get("x-ingest-role"), h.get("x-ingest-accepted"))

# same old batch again -> retransmit, no new storage, timer unchanged
s, h = post([(0, 1.0), (1000, 2.0)], "A")
print("A retransmit:", s, "stored=", h.get("x-ingest-stored"), "stale=", h.get("x-ingest-stale"))

print("waiting 10.5s for stale window ...")
time.sleep(10.5)

s, h = post([(2000, 3.0)], "B")
print("B takeover:", s, h.get("x-ingest-role"), h.get("x-ingest-stored"))

status = get("/api/status")
print("status:", status)
series_rows = get("/api/series")
print("series:", series_rows)
events = get("/api/events")
print("events:", events)

# sample count of the single series
import json
sid = json.loads(series_rows)["series"][0]["id"]
samples = json.loads(get(f"/api/series/{sid}/samples"))["samples"]
print("stored values:", [p["value"] for p in samples])
assert [p["value"] for p in samples] == [1.0, 2.0, 3.0], "tail-only storage broken"
assert json.loads(status)["clusters"][0]["active_replica"] == "B"
print("SMOKE OK")
