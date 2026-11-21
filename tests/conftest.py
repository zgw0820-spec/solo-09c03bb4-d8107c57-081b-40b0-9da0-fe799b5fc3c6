"""Test helpers: build remote-write 1.0 requests the way Prometheus does."""
from __future__ import annotations

import sys
from pathlib import Path

# Make the receiver package importable without installing it.
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "receiver"))

from app.remote_write import Label, Sample, TimeSeries, encode_write_request  # noqa: E402

BASE_TS = 2_000_000_000_000  # fixed unix-ms base so tests are deterministic


def labels(cluster: str, replica: str, name: str = "demo_requests_total",
           extra: dict | None = None) -> tuple[Label, ...]:
    mapping = {
        "__name__": name,
        "cluster": cluster,
        "replica": replica,
        "job": "demo",
    }
    if extra:
        mapping.update(extra)
    return tuple(Label(name=k, value=v) for k, v in sorted(mapping.items()))


def series(cluster: str, replica: str, points: list[tuple[int, float]],
           name: str = "demo_requests_total",
           extra: dict | None = None) -> TimeSeries:
    return TimeSeries(
        labels=labels(cluster, replica, name=name, extra=extra),
        samples=tuple(Sample(timestamp_ms=BASE_TS + ts, value=v) for ts, v in points),
    )


def body(series_list: list[TimeSeries]) -> bytes:
    return encode_write_request(series_list)


RW_HEADERS = {
    "Content-Type": "application/x-protobuf; proto=prometheus.WriteRequest; encoding=snappy",
    "X-Prometheus-Remote-Write-Version": "1.0",
    "Content-Encoding": "snappy",
}
