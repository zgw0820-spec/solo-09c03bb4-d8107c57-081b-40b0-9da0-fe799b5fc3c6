"""FastAPI entrypoint: remote-write ingestion plus the demo web UI."""
from __future__ import annotations

import time
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from . import config, dockerctl
from .db import Database, IngestError
from .remote_write import (
    PROTOBUF,
    REMOTE_WRITE_VERSION,
    SNAPPY,
    RemoteWriteError,
    decode_write_request,
)

STATIC_DIR = Path(__file__).parent / "static"


def now_ms() -> int:
    return int(time.time() * 1000)


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db = Database(
        path=config.DB_PATH,
        stale_after_seconds=config.STALE_AFTER_SECONDS,
    )
    yield


def create_app() -> FastAPI:
    app = FastAPI(title="Remote-Write Failover Receiver", lifespan=lifespan)

    @app.post("/api/v1/write")
    async def remote_write(request: Request) -> Response:
        if config.REQUIRE_REMOTE_WRITE_HEADER:
            if request.headers.get("x-prometheus-remote-write-version") != (
                REMOTE_WRITE_VERSION
            ):
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "missing or invalid X-Prometheus-Remote-Write-Version, "
                        f"expected {REMOTE_WRITE_VERSION}"
                    ),
                )
            content_type = request.headers.get("content-type", "")
            if PROTOBUF not in content_type or SNAPPY not in content_type:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "Content-Type must be 'application/x-protobuf' with the "
                        "'snappy' protocol parameter"
                    ),
                )

        raw = await request.body()
        try:
            series_list = decode_write_request(raw)
        except RemoteWriteError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        # An empty write request is valid on the wire; nothing to apply.
        if not series_list:
            return Response(status_code=204, headers={
                "X-Ingest-Accepted": "1",
                "X-Ingest-Role": "noop",
                "X-Ingest-Stored": "0",
                "X-Ingest-Stale": "0",
            })

        db: Database = app.state.db
        try:
            outcome = db.ingest(series_list, now_ms())
        except IngestError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        # Remote-write always answers 204 once the request was processed;
        # rejected standby/retransmit batches are a normal state-machine result.
        return Response(status_code=204, headers={
            "X-Ingest-Accepted": "1" if outcome.accepted else "0",
            "X-Ingest-Role": outcome.role,
            "X-Ingest-Stored": str(outcome.samples_stored),
            "X-Ingest-Stale": str(outcome.samples_stale),
        })

    @app.get("/healthz")
    async def healthz() -> dict:
        return {"status": "ok"}

    @app.get("/api/status")
    async def get_status() -> dict:
        db: Database = app.state.db
        clusters = db.status(now_ms())
        return {
            "now_ms": now_ms(),
            "stale_after_ms": db.stale_after_ms,
            "clusters": [cluster.__dict__ for cluster in clusters],
        }

    @app.get("/api/series")
    async def get_series() -> dict:
        db: Database = app.state.db
        return {"series": db.list_series()}

    @app.get("/api/series/{series_id}/samples")
    async def get_series_samples(series_id: int, limit: int = 240) -> dict:
        db: Database = app.state.db
        return {
            "series_id": series_id,
            "limit": limit,
            "samples": db.get_samples(series_id, limit),
        }

    @app.get("/api/events")
    async def get_events(limit: int = 30) -> dict:
        db: Database = app.state.db
        return {"events": db.list_events(limit)}

    @app.get("/api/replicas")
    async def get_replicas() -> JSONResponse:
        try:
            return JSONResponse({"replicas": dockerctl.list_replica_containers()})
        except dockerctl.DockerUnavailable as exc:
            return JSONResponse(
                status_code=503,
                content={"detail": f"docker control unavailable: {exc}"},
            )

    @app.post("/api/replicas/{replica}/{action}")
    async def control_replica(replica: str, action: str) -> JSONResponse:
        try:
            return JSONResponse(dockerctl.control(replica, action))
        except KeyError:
            raise HTTPException(
                status_code=404,
                detail=f"unknown replica '{replica}', known: "
                f"{sorted(config.REPLICA_CONTAINERS)}",
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        except dockerctl.DockerUnavailable as exc:
            return JSONResponse(
                status_code=503,
                content={"detail": f"docker control unavailable: {exc}"},
            )

    @app.get("/")
    async def index() -> FileResponse:
        return FileResponse(STATIC_DIR / "index.html")

    app.mount(
        "/static",
        StaticFiles(directory=str(STATIC_DIR)),
        name="static",
    )
    return app


app = create_app()
