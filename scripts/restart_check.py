"""Restart-persistence check: state must survive an immediate restart.

Flow: A becomes active -> kill receiver -> restart on the same DB file within
the stale window -> B is still rejected as standby -> state remains A.
"""
import json
import os
import site
import sys
import urllib.request

site.addsitedir("/home/node/.local/lib/python3.11/site-packages")
sys.path.insert(0, "/workspace/receiver")
sys.path.insert(0, "/workspace/tests")

from conftest import RW_HEADERS, body, series  # noqa: E402

PORT = os.environ["PORT"]
BASE = f"http://127.0.0.1:{PORT}"


def post(replica, points):
    req = urllib.request.Request(
        BASE + "/api/v1/write",
        data=body([series("demo", replica, points)]),
        headers=RW_HEADERS,
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=5) as r:
        return r.headers["x-ingest-role"], r.headers["x-ingest-accepted"]


def active():
    st = json.load(urllib.request.urlopen(BASE + "/api/status"))
    return st["clusters"][0]["active_replica"] if st["clusters"] else None


print("initial:", post("A", [(0, 1.0)]), "active =", active())
print("standby B:", post("B", [(1000, 9.0)]))
print("active still:", active())
print("READY_FOR_RESTART")
