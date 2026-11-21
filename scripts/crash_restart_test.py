"""Self-contained crash/restart test with the stale window held open.

Runs uvicorn as subprocesses against one SQLite file:
  1. A becomes active, B rejected as standby.
  2. Receiver is killed hard (SIGKILL, WAL left on disk).
  3. Receiver restarts on the same DB within a long stale window.
  4. B is still rejected as standby, active replica stays A.
"""
import json
import os
import signal
import site
import subprocess
import sys
import time
import urllib.request

site.addsitedir("/home/node/.local/lib/python3.11/site-packages")
sys.path.insert(0, "/workspace/receiver")
sys.path.insert(0, "/workspace/tests")

from conftest import RW_HEADERS, body, series  # noqa: E402

PORT = "8090"
BASE = f"http://127.0.0.1:{PORT}"
DB_DIR = "/tmp/smoke4"
os.makedirs(DB_DIR, exist_ok=True)
DB = f"{DB_DIR}/receiver.db"
if os.path.exists(DB):
    for suffix in ("", "-wal", "-shm"):
        try:
            os.remove(DB + suffix)
        except FileNotFoundError:
            pass

ENV = dict(os.environ, DB_PATH=DB, STALE_AFTER_SECONDS="120",
           PYTHONPATH="/workspace/receiver")


def start_receiver():
    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "app.main:app",
         "--host", "127.0.0.1", "--port", PORT, "--log-level", "warning"],
        cwd="/workspace/receiver", env=ENV,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
    )
    for _ in range(50):
        try:
            urllib.request.urlopen(BASE + "/healthz", timeout=1)
            return proc
        except Exception:
            time.sleep(0.2)
    proc.kill()
    raise RuntimeError("receiver did not start")


def post(replica, points):
    req = urllib.request.Request(
        BASE + "/api/v1/write",
        data=body([series("demo", replica, points)]),
        headers=RW_HEADERS, method="POST",
    )
    with urllib.request.urlopen(req, timeout=5) as r:
        return r.headers["x-ingest-role"], r.headers["x-ingest-accepted"]


def active():
    st = json.load(urllib.request.urlopen(BASE + "/api/status"))
    return st["clusters"][0]["active_replica"] if st["clusters"] else None


# 1. first lifetime
p1 = start_receiver()
print("prime  -> A:", post("A", [(0, 1.0)]))
print("prime  -> B:", post("B", [(1000, 9.0)]), "active:", active())
assert active() == "A"

# 2. hard kill, no graceful shutdown
p1.send_signal(signal.SIGKILL)
p1.wait(timeout=10)
print("receiver SIGKILLed; WAL files:",
      sorted(f for f in os.listdir(DB_DIR) if f.startswith("receiver.db")))

# 3. restart immediately against the same database file
p2 = start_receiver()
restart_age = time.time()  # well inside the 120s stale window
print("restarted within stale window")

# 4. persisted state + standby still enforced
got_active = active()
role, accepted = post("B", [(2000, 9.0)])
print("after restart -> active:", got_active, "| B write:", role, accepted)
rows = json.load(urllib.request.urlopen(BASE + "/api/series"))["series"]
print("series:", len(rows), "samples:", rows[0]["sample_count"])

p2.send_signal(signal.SIGTERM)
p2.wait(timeout=10)

assert got_active == "A", "active replica must survive hard restart"
assert role == "standby" and accepted == "0"
assert rows[0]["sample_count"] == 1, "standby point must not be stored"
assert "replica" not in rows[0]["labels_json"]
print("CRASH-RESTART PERSISTENCE OK")
