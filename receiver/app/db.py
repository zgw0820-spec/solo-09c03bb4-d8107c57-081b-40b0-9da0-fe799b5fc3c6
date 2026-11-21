"""SQLite-backed storage and the replica failover state machine.

Design summary
--------------
* One row per time series. The replica label is stripped before a series is
  identified, so the two Prometheus replicas feed the *same* series.
* Per series only samples newer than the current tail point are stored;
  retransmitted old batches are counted but never inserted and never refresh
  the activity timer.
* ``cluster_state`` remembers the currently feeding replica. A replica is
  stale when it has not stored a new point for ``stale_after`` seconds.
  Another replica that actually carries new points may then take over.
* Switching the active replica and inserting samples happen in a single
  ``BEGIN IMMEDIATE`` transaction, so they are atomic and the committed
  cluster_state row always describes the last successful write -- even after
  a crash or restart (WAL enabled).
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import threading
from dataclasses import dataclass, field

from .remote_write import TimeSeries

CLUSTER_LABEL = "cluster"
REPLICA_LABEL = "replica"


class IngestError(ValueError):
    """A request that cannot be ingested (bad labels, mixed groups ...)."""


@dataclass
class IngestOutcome:
    accepted: bool
    cluster: str
    replica: str
    role: str  # initial | active | takeover | standby | rejected
    series_seen: int = 0
    series_created: int = 0
    samples_received: int = 0
    samples_stored: int = 0
    samples_stale: int = 0
    reason: str = ""
    old_replica: str | None = None
    new_replica: str | None = None
    switched: bool = False


@dataclass
class ClusterStatus:
    cluster: str
    active_replica: str | None
    switched_ms: int | None
    last_seen_ms: int | None
    last_write_ms: int | None
    stale: bool
    replicas: list[dict] = field(default_factory=list)


class Database:
    def __init__(self, path: str, stale_after_seconds: float):
        self.path = path
        self.stale_after_ms = int(stale_after_seconds * 1000)
        self._init_lock = threading.Lock()
        directory = os.path.dirname(os.path.abspath(path))
        os.makedirs(directory, exist_ok=True)
        self.init_db()

    # ------------------------------------------------------------------ #
    # connection / schema
    # ------------------------------------------------------------------ #
    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=10)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=5000")
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def init_db(self) -> None:
        with self._init_lock, self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS series (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    fingerprint TEXT NOT NULL UNIQUE,
                    cluster     TEXT NOT NULL,
                    name        TEXT NOT NULL,
                    labels_json TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS samples (
                    series_id    INTEGER NOT NULL REFERENCES series(id),
                    timestamp_ms INTEGER NOT NULL,
                    value        REAL NOT NULL,
                    PRIMARY KEY (series_id, timestamp_ms)
                ) WITHOUT ROWID;
                CREATE INDEX IF NOT EXISTS idx_samples_series_ts
                    ON samples(series_id, timestamp_ms);

                CREATE TABLE IF NOT EXISTS cluster_state (
                    cluster        TEXT PRIMARY KEY,
                    active_replica TEXT NOT NULL,
                    last_write_ms  INTEGER,
                    updated_ms     INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS replica_liveness (
                    cluster       TEXT NOT NULL,
                    replica       TEXT NOT NULL,
                    last_seen_ms  INTEGER,
                    last_write_ms INTEGER,
                    PRIMARY KEY (cluster, replica)
                ) WITHOUT ROWID;

                CREATE TABLE IF NOT EXISTS events (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts_ms       INTEGER NOT NULL,
                    cluster     TEXT NOT NULL,
                    event_type  TEXT NOT NULL,
                    old_replica TEXT,
                    new_replica TEXT,
                    reason      TEXT
                );
                """
            )

    # ------------------------------------------------------------------ #
    # ingestion
    # ------------------------------------------------------------------ #
    def ingest(
        self, series_list: list[TimeSeries], now_ms: int
    ) -> IngestOutcome:
        groups = self._group_by_replica(series_list)
        if len(groups) > 1:
            raise IngestError(
                "one remote-write request must carry a single "
                "(cluster, replica) pair"
            )
        (cluster, replica), group_series = next(iter(groups.items()))
        return self._ingest_group(cluster, replica, group_series, now_ms)

    @staticmethod
    def _group_by_replica(
        series_list: list[TimeSeries],
    ) -> dict[tuple[str, str], list[TimeSeries]]:
        groups: dict[tuple[str, str], list[TimeSeries]] = {}
        for series in series_list:
            labels = series.label_map()
            cluster = labels.get(CLUSTER_LABEL)
            replica = labels.get(REPLICA_LABEL)
            if not cluster or not replica:
                raise IngestError(
                    "every series must carry non-empty 'cluster' and "
                    "'replica' labels"
                )
            groups.setdefault((cluster, replica), []).append(series)
        return groups

    @staticmethod
    def _series_identity(labels: dict[str, str]) -> tuple[str, str, str]:
        """Return (fingerprint, metric name, canonical labels) without replica."""
        identity = {k: v for k, v in labels.items() if k != REPLICA_LABEL}
        if not identity:
            raise IngestError("series has no labels left after stripping replica")
        canonical = json.dumps(
            identity, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        )
        fingerprint = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        return fingerprint, identity.get("__name__", ""), canonical

    def _ingest_group(
        self,
        cluster: str,
        replica: str,
        series_list: list[TimeSeries],
        now_ms: int,
    ) -> IngestOutcome:
        outcome = IngestOutcome(
            accepted=False, cluster=cluster, replica=replica, role="rejected"
        )
        conn = self._connect()
        try:
            # Serialise writers: switch + inserts must be one atomic step.
            conn.execute("BEGIN IMMEDIATE")

            conn.execute(
                """
                INSERT INTO replica_liveness(cluster, replica, last_seen_ms, last_write_ms)
                VALUES (?, ?, ?, NULL)
                ON CONFLICT(cluster, replica) DO UPDATE SET last_seen_ms=excluded.last_seen_ms
                """,
                (cluster, replica, now_ms),
            )
            state = conn.execute(
                "SELECT active_replica, last_write_ms, updated_ms "
                "FROM cluster_state WHERE cluster=?",
                (cluster,),
            ).fetchone()

            active = state["active_replica"] if state else None
            if active is None:
                role = "initial"
            elif active == replica:
                role = "active"
            else:
                active_liveness = conn.execute(
                    "SELECT last_write_ms FROM replica_liveness "
                    "WHERE cluster=? AND replica=?",
                    (cluster, active),
                ).fetchone()
                active_last_write = (
                    active_liveness["last_write_ms"] if active_liveness else None
                )
                stale = (
                    active_last_write is None
                    or now_ms - active_last_write >= self.stale_after_ms
                )
                # Note the difference is measured against the *current* clock,
                # so retransmitted batches cannot keep a dead replica fresh.
                role = "takeover" if stale else "standby"
            outcome.role = role
            outcome.old_replica = active
            outcome.series_seen = len(series_list)
            outcome.samples_received = sum(
                len(series.samples) for series in series_list
            )

            if role == "standby":
                # Active replica is still fresh: nothing from this one is stored.
                conn.commit()
                outcome.reason = (
                    f"active replica '{active}' is fresh; standby data rejected"
                )
                return outcome

            stored_total = 0
            stale_total = 0
            created_total = 0
            for series in series_list:
                labels = series.label_map()
                fingerprint, name, canonical = self._series_identity(labels)
                existed = conn.execute(
                    "SELECT 1 FROM series WHERE fingerprint=?", (fingerprint,)
                ).fetchone()
                if existed is None:
                    conn.execute(
                        "INSERT INTO series(fingerprint, cluster, name, labels_json) "
                        "VALUES (?, ?, ?, ?)",
                        (fingerprint, cluster, name, canonical),
                    )
                    created_total += 1
                row = conn.execute(
                    "SELECT id FROM series WHERE fingerprint=?", (fingerprint,)
                ).fetchone()
                if row is None:  # pragma: no cover - defensive
                    raise IngestError("failed to resolve series id")
                series_id = row["id"]

                tail = conn.execute(
                    "SELECT MAX(timestamp_ms) AS tail FROM samples WHERE series_id=?",
                    (series_id,),
                ).fetchone()["tail"]

                fresh_samples: list[tuple[int, float]] = []
                seen_ts: set[int] = set()
                for sample in series.samples:
                    ts = sample.timestamp_ms
                    if ts in seen_ts:
                        continue
                    seen_ts.add(ts)
                    if tail is not None and ts <= tail:
                        stale_total += 1
                        continue
                    fresh_samples.append((ts, sample.value))

                for ts, value in fresh_samples:
                    # OR IGNORE keeps retransmits idempotent even on races.
                    conn.execute(
                        "INSERT OR IGNORE INTO samples(series_id, timestamp_ms, value) "
                        "VALUES (?, ?, ?)",
                        (series_id, ts, value),
                    )
                    stored_total += conn.execute("SELECT changes()").fetchone()[0]

            outcome.samples_stale = stale_total
            outcome.samples_stored = stored_total
            outcome.series_created = created_total
            outcome.accepted = True

            new_points = outcome.samples_stored > 0
            if role == "takeover" and not new_points:
                # A stale active replica may only be replaced by *new* points.
                conn.rollback()
                outcome.accepted = False
                outcome.role = "rejected"
                outcome.samples_stored = 0
                outcome.series_created = 0
                outcome.reason = (
                    "active replica is stale but this batch contains no point "
                    "newer than the stored tail; takeover denied"
                )
                return outcome

            if new_points:
                conn.execute(
                    "UPDATE replica_liveness SET last_write_ms=? "
                    "WHERE cluster=? AND replica=?",
                    (now_ms, cluster, replica),
                )

            if role in ("initial", "takeover") and new_points:
                conn.execute(
                    """
                    INSERT INTO cluster_state(cluster, active_replica, last_write_ms, updated_ms)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(cluster) DO UPDATE SET
                        active_replica=excluded.active_replica,
                        last_write_ms=excluded.last_write_ms,
                        updated_ms=excluded.updated_ms
                    """,
                    (cluster, replica, now_ms, now_ms),
                )
                event_type = "SELECTED" if role == "initial" else "TAKEOVER"
                conn.execute(
                    "INSERT INTO events(ts_ms, cluster, event_type, old_replica, "
                    "new_replica, reason) VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        now_ms,
                        cluster,
                        event_type,
                        active,
                        replica,
                        outcome.reason or None,
                    ),
                )
                outcome.switched = role == "takeover"
                outcome.new_replica = replica
            elif new_points:
                conn.execute(
                    "UPDATE cluster_state SET last_write_ms=? WHERE cluster=?",
                    (now_ms, cluster),
                )

            if not new_points:
                outcome.reason = (
                    "all samples are at or behind the stored tail (retransmit)"
                )
            conn.commit()
            return outcome
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    # ------------------------------------------------------------------ #
    # read models for the web UI
    # ------------------------------------------------------------------ #
    def status(self, now_ms: int) -> list[ClusterStatus]:
        with self._connect() as conn:
            liveness_rows = conn.execute(
                "SELECT cluster, replica, last_seen_ms, last_write_ms "
                "FROM replica_liveness ORDER BY cluster, replica"
            ).fetchall()
            state_rows = conn.execute(
                "SELECT cluster, active_replica, last_write_ms, updated_ms "
                "FROM cluster_state"
            ).fetchall()

        replicas: dict[str, list[dict]] = {}
        for row in liveness_rows:
            replicas.setdefault(row["cluster"], []).append(dict(row))

        result: list[ClusterStatus] = []
        for row in state_rows:
            cluster = row["cluster"]
            last_write = row["last_write_ms"]
            stale = (
                last_write is None
                or now_ms - last_write >= self.stale_after_ms
            )
            active_last_seen = None
            for item in replicas.get(cluster, []):
                if item["replica"] == row["active_replica"]:
                    active_last_seen = item["last_seen_ms"]
            result.append(
                ClusterStatus(
                    cluster=cluster,
                    active_replica=row["active_replica"],
                    switched_ms=row["updated_ms"],
                    last_seen_ms=active_last_seen,
                    last_write_ms=last_write,
                    stale=stale,
                    replicas=replicas.get(cluster, []),
                )
            )
        return result

    def list_series(self) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT s.id, s.cluster, s.name, s.labels_json,
                       (SELECT COUNT(*) FROM samples m WHERE m.series_id=s.id) AS sample_count,
                       (SELECT timestamp_ms FROM samples m
                         WHERE m.series_id=s.id ORDER BY timestamp_ms DESC LIMIT 1) AS last_ts,
                       (SELECT value FROM samples m
                         WHERE m.series_id=s.id ORDER BY timestamp_ms DESC LIMIT 1) AS last_value
                FROM series s ORDER BY s.cluster, s.name, s.id
                """
            ).fetchall()
        return [dict(row) for row in rows]

    def get_samples(self, series_id: int, limit: int = 240) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT timestamp_ms, value FROM samples "
                "WHERE series_id=? ORDER BY timestamp_ms DESC LIMIT ?",
                (series_id, limit),
            ).fetchall()
        return [dict(row) for row in reversed(rows)]

    def list_events(self, limit: int = 30) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT ts_ms, cluster, event_type, old_replica, new_replica, reason "
                "FROM events ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [dict(row) for row in rows]
