"""Runtime configuration, all knobs are environment variables."""
from __future__ import annotations

import os


def _env(name: str, default: str) -> str:
    value = os.environ.get(name)
    return value if value not in (None, "") else default


def _env_float(name: str, default: float) -> float:
    raw = _env(name, str(default))
    try:
        return float(raw)
    except ValueError:
        return default


# SQLite database file (a directory is created on startup).
DB_PATH = _env("DB_PATH", "/data/receiver.db")

# A replica that has not stored a new point for this many seconds is
# considered stale and may be replaced by another replica carrying new points.
STALE_AFTER_SECONDS = _env_float("STALE_AFTER_SECONDS", 10.0)

# Require Prometheus remote-write 1.0 protocol markers on requests.
REQUIRE_REMOTE_WRITE_HEADER = _env("REQUIRE_REMOTE_WRITE_HEADER", "1") == "1"

# Docker control socket used by the demo pause/resume buttons.
DOCKER_BASE_URL = _env("DOCKER_BASE_URL", "unix:///var/run/docker.sock")

# Fixed demo replica -> docker container names ("name=container" pairs).
REPLICA_CONTAINERS: dict[str, str] = {}
for pair in _env(
    "REPLICA_CONTAINERS",
    "A=rw-demo-prom-a,B=rw-demo-prom-b",
).split(","):
    pair = pair.strip()
    if "=" in pair:
        name, container = pair.split("=", 1)
        REPLICA_CONTAINERS[name.strip()] = container.strip()
