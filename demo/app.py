"""Local demo scrape target.

Exposes Prometheus text-format counters that keep increasing while the
service is "running". The pause/resume buttons on the receiver UI control the
Prometheus containers themselves; this target also offers its own
pause/resume endpoints so the demo can be exercised without docker.
"""
from __future__ import annotations

import threading
import time

from fastapi import FastAPI
from fastapi.responses import PlainTextResponse

app = FastAPI(title="Demo Metrics Target")

_state_lock = threading.Lock()
_state = {
    "running": True,
    "requests_total": 0.0,
    "uptime_seconds_total": 0.0,
}
_started = time.time()


def _tick() -> None:
    while True:
        time.sleep(1)
        with _state_lock:
            if _state["running"]:
                _state["requests_total"] += 1
                _state["uptime_seconds_total"] = time.time() - _started


@app.on_event("startup")
async def _start_background() -> None:
    thread = threading.Thread(target=_tick, daemon=True, name="metrics-tick")
    thread.start()


@app.get("/metrics", response_class=PlainTextResponse)
async def metrics() -> str:
    with _state_lock:
        running = _state["running"]
        requests_total = _state["requests_total"]
        uptime = _state["uptime_seconds_total"]
    lines = [
        "# HELP demo_requests_total Simulated request counter.",
        "# TYPE demo_requests_total counter",
        f'demo_requests_total{{job="demo"}} {requests_total}',
        "# HELP demo_uptime_seconds_total Target uptime in seconds.",
        "# TYPE demo_uptime_seconds_total counter",
        f'demo_uptime_seconds_total{{job="demo"}} {uptime:.3f}',
        "# HELP demo_target_running 1 when the local demo target is running.",
        "# TYPE demo_target_running gauge",
        f'demo_target_running{{job="demo"}} {1 if running else 0}',
    ]
    return "\n".join(lines) + "\n"


@app.post("/pause")
async def pause() -> dict:
    with _state_lock:
        _state["running"] = False
    return {"running": False}


@app.post("/resume")
async def resume() -> dict:
    with _state_lock:
        _state["running"] = True
    return {"running": True}


@app.get("/healthz")
async def healthz() -> dict:
    with _state_lock:
        return {"running": _state["running"]}
