"""Failover state-machine tests against a real (temporary) SQLite database."""
from __future__ import annotations

import pytest

from app.db import IngestError, Database
from tests.conftest import BASE_TS, series


@pytest.fixture()
def db(tmp_path):
    return Database(str(tmp_path / "test.db"), stale_after_seconds=10.0)


def ingest(db, cluster, replica, points, now, **kw):
    return db.ingest([series(cluster, replica, points, **kw)], now)


def test_initial_replica_is_selected_with_new_points(db):
    out = ingest(db, "demo", "A", [(0, 1.0), (1000, 2.0)], now=BASE_TS + 1000)
    assert out.accepted
    assert out.role == "initial"
    status = db.status(BASE_TS + 1000)[0]
    assert status.active_replica == "A"
    assert status.stale is False


def test_replica_label_stripped_shared_series(db):
    ingest(db, "demo", "A", [(0, 1.0)], now=BASE_TS)
    ingest(db, "demo", "B", [(1000, 2.0)], now=BASE_TS + 500)  # rejected, B standby
    rows = db.list_series()
    assert len(rows) == 1  # replicas map to the same series
    assert "replica" not in rows[0]["labels_json"]
    assert rows[0]["sample_count"] == 1


def test_standby_replica_rejected_while_active_fresh(db):
    ingest(db, "demo", "A", [(0, 1.0)], now=BASE_TS)
    out = ingest(db, "demo", "B", [(1000, 9.0)], now=BASE_TS + 5_000)
    assert not out.accepted
    assert out.role == "standby"
    # no B samples landed
    samples = db.get_samples(db.list_series()[0]["id"])
    assert [s["value"] for s in samples] == [1.0]


def test_takeover_only_after_stale_window_and_only_with_new_points(db):
    ingest(db, "demo", "A", [(0, 1.0), (1000, 2.0)], now=BASE_TS + 1000)
    # last successful new-point write happened at BASE+1000
    # 9.9s after that write: not stale yet, B still rejected
    out = ingest(db, "demo", "B", [(2000, 3.0)], now=BASE_TS + 1000 + 9_900)
    assert out.role == "standby"
    # 10.0s after that write: stale, but B only offers points behind tail -> denied
    old = ingest(db, "demo", "B", [(500, 99.0)], now=BASE_TS + 1000 + 10_000)
    assert not old.accepted
    assert "takeover denied" in old.reason
    assert db.status(BASE_TS + 1000 + 10_000)[0].active_replica == "A"
    # stale + genuinely new point -> B takes over atomically
    ok = ingest(db, "demo", "B", [(2000, 3.0)], now=BASE_TS + 1000 + 10_000)
    assert ok.accepted and ok.switched
    assert ok.old_replica == "A" and ok.new_replica == "B"
    status = db.status(BASE_TS + 1000 + 10_000)[0]
    assert status.active_replica == "B"
    samples = db.get_samples(db.list_series()[0]["id"])
    assert [s["value"] for s in samples] == [1.0, 2.0, 3.0]


def test_old_retransmit_does_not_store_or_refresh_timer(db):
    ingest(db, "demo", "A", [(0, 1.0), (1000, 2.0)], now=BASE_TS + 1000)
    # exactly the same batch again
    again = ingest(db, "demo", "A", [(0, 1.0), (1000, 2.0)], now=BASE_TS + 9_000)
    assert again.accepted
    assert again.samples_stored == 0
    assert again.samples_stale == 2
    status = db.status(BASE_TS + 9_000)[0]
    assert status.last_write_ms == BASE_TS + 1000  # timer not renewed
    # tail unchanged
    samples = db.get_samples(db.list_series()[0]["id"])
    assert [s["timestamp_ms"] for s in samples] == [BASE_TS, BASE_TS + 1000]
    # so 10s after the last real write A is stale and B can take over
    out = ingest(db, "demo", "B", [(2000, 3.0)], now=BASE_TS + 1000 + 10_000)
    assert out.switched


def test_mixed_replica_request_rejected(db):
    with pytest.raises(IngestError):
        db.ingest(
            [series("demo", "A", [(0, 1.0)]), series("demo", "B", [(0, 1.0)])],
            now_ms=BASE_TS,
        )


def test_missing_cluster_or_replica_label_rejected(db):
    from app.remote_write import Label, TimeSeries, Sample

    bad = TimeSeries(
        labels=(Label("__name__", "m"), Label("cluster", "demo")),
        samples=(Sample(timestamp_ms=BASE_TS, value=1.0),),
    )
    with pytest.raises(IngestError):
        db.ingest([bad], now_ms=BASE_TS)


def test_switch_and_inserts_are_atomic_on_failure(db, monkeypatch):
    ingest(db, "demo", "A", [(0, 1.0), (1000, 2.0)], now=BASE_TS + 1000)
    original = db._series_identity

    def boom(labels):  # fail mid-transaction during B's takeover attempt
        if labels.get("replica") == "B":
            raise RuntimeError("simulated disk error")
        return original(labels)

    monkeypatch.setattr(db, "_series_identity", boom)
    with pytest.raises(RuntimeError):
        ingest(db, "demo", "B", [(2000, 3.0)], now=BASE_TS + 1000 + 10_000)

    # Re-open the database: committed state must still be the last good write.
    db2 = Database(db.path, stale_after_seconds=10.0)
    status = db2.status(BASE_TS + 10_000)[0]
    assert status.active_replica == "A"
    samples = db2.get_samples(db2.list_series()[0]["id"])
    assert [s["value"] for s in samples] == [1.0, 2.0]
    # and the failed attempt left no partial series/sample rows
    assert len(db2.list_series()) == 1


def test_state_survives_restart_and_rejects_standby_with_old_clock(db):
    ingest(db, "demo", "A", [(0, 1.0)], now=BASE_TS)
    reopened = Database(db.path, stale_after_seconds=10.0)
    status = reopened.status(BASE_TS + 1000)[0]
    assert status.active_replica == "A"
    # after restart the fresh/standby rule is still enforced from persisted state
    out = reopened.ingest(
        [series("demo", "B", [(1000, 5.0)])], now_ms=BASE_TS + 2000
    )
    assert out.role == "standby" and not out.accepted


def test_multiple_clusters_are_independent(db):
    ingest(db, "c1", "A", [(0, 1.0)], now=BASE_TS)
    ingest(db, "c2", "B", [(0, 1.0)], now=BASE_TS)
    statuses = {s.cluster: s.active_replica for s in db.status(BASE_TS)}
    assert statuses == {"c1": "A", "c2": "B"}


def test_takeover_event_recorded(db):
    ingest(db, "demo", "A", [(0, 1.0)], now=BASE_TS)
    ingest(db, "demo", "B", [(1000, 2.0)], now=BASE_TS + 10_000)
    events = db.list_events()
    kinds = [e["event_type"] for e in events]
    assert kinds == ["TAKEOVER", "SELECTED"]
    assert events[0]["old_replica"] == "A"
    assert events[0]["new_replica"] == "B"


def test_duplicate_timestamp_within_batch_deduped(db):
    ts = series("demo", "A", [(1000, 2.0), (1000, 2.0), (2000, 3.0)])
    out = db.ingest([ts], now_ms=BASE_TS)
    assert out.accepted
    assert out.samples_stored == 2


def test_concurrent_writers_never_corrupt_state(db):
    import threading

    errors: list[Exception] = []

    def writer(replica: str, base_offset: int):
        try:
            for i in range(25):
                # Spread timestamps so most batches carry at least one new point.
                ts = base_offset + i * 500
                db.ingest(
                    [series("demo", replica, [(ts, float(ts))])],
                    now_ms=BASE_TS + i * 1000,
                )
        except Exception as exc:  # pragma: no cover - failure signal
            errors.append(exc)

    t1 = threading.Thread(target=writer, args=("A", 0))
    t2 = threading.Thread(target=writer, args=("B", 10_000))
    t1.start(); t2.start(); t1.join(); t2.join()
    assert not errors

    # Exactly one active replica and every stored point is strictly increasing.
    statuses = db.status(BASE_TS + 25_000)
    assert len(statuses) == 1
    sid = db.list_series()[0]["id"]
    timestamps = [s["timestamp_ms"] for s in db.get_samples(sid, limit=10_000)]
    assert timestamps == sorted(set(timestamps))
    # no replica label leaked into the stored identity
    assert all("replica" not in row["labels_json"] for row in db.list_series())
