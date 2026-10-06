import json
import sqlite3
from datetime import datetime, timezone

from .domain import ConflictError, NotFoundError


def utcnow():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class SQLiteRepository:
    def __init__(self, path):
        self.path = str(path)
        self._initialize()

    def _connect(self):
        connection = sqlite3.connect(self.path, timeout=30)
        connection.row_factory = sqlite3.Row
        return connection

    def _initialize(self):
        with self._connect() as connection:
            connection.executescript("""
                CREATE TABLE IF NOT EXISTS entities (
                    id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL,
                    status TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    data TEXT NOT NULL,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_entities_kind_status
                    ON entities(kind, status);
                CREATE TABLE IF NOT EXISTS audit_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    entity_id TEXT NOT NULL,
                    actor_id TEXT NOT NULL,
                    actor_role TEXT NOT NULL,
                    action TEXT NOT NULL,
                    from_status TEXT,
                    to_status TEXT NOT NULL,
                    detail TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_audit_entity
                    ON audit_log(entity_id, id);
                CREATE TABLE IF NOT EXISTS idempotency (
                    actor_id TEXT NOT NULL,
                    idem_key TEXT NOT NULL,
                    entity_id TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY(actor_id, idem_key)
                );
                CREATE TABLE IF NOT EXISTS subscriptions (
                    id TEXT PRIMARY KEY,
                    station_id TEXT NOT NULL,
                    source_id TEXT NOT NULL,
                    max_magnitude REAL NOT NULL,
                    active INTEGER NOT NULL DEFAULT 1,
                    version INTEGER NOT NULL,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE UNIQUE INDEX IF NOT EXISTS idx_subscription_station_source
                    ON subscriptions(station_id, source_id);
                CREATE TABLE IF NOT EXISTS broadcast_batches (
                    id TEXT PRIMARY KEY,
                    candidate_id TEXT,
                    reason TEXT NOT NULL,
                    created_by TEXT,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_batch_candidate
                    ON broadcast_batches(candidate_id);
                CREATE TABLE IF NOT EXISTS deliveries (
                    id TEXT PRIMARY KEY,
                    candidate_id TEXT NOT NULL,
                    station_id TEXT NOT NULL,
                    subscription_id TEXT NOT NULL,
                    batch_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    receipt_action TEXT,
                    receipt_by TEXT,
                    receipt_at TEXT,
                    last_error TEXT,
                    version INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE UNIQUE INDEX IF NOT EXISTS idx_delivery_unique_active
                    ON deliveries(candidate_id, station_id) WHERE status != 'voided';
                CREATE INDEX IF NOT EXISTS idx_delivery_candidate
                    ON deliveries(candidate_id);
                CREATE INDEX IF NOT EXISTS idx_delivery_station
                    ON deliveries(station_id);
                CREATE INDEX IF NOT EXISTS idx_delivery_batch
                    ON deliveries(batch_id);
                CREATE INDEX IF NOT EXISTS idx_delivery_status
                    ON deliveries(status);
            """)

    @staticmethod
    def _entity_from_row(row):
        return {
            "id": row["id"],
            "kind": row["kind"],
            "status": row["status"],
            "version": int(row["version"]),
            "data": json.loads(row["data"]),
            "created_by": row["created_by"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def create_entity(self, entity_id, kind, status, data, actor_id):
        now = utcnow()
        payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO entities(id, kind, status, version, data, created_by, created_at, updated_at) "
                "VALUES (?, ?, ?, 1, ?, ?, ?, ?)",
                (entity_id, kind, status, payload, actor_id, now, now),
            )
        return self.get_entity(entity_id)

    def get_entity(self, entity_id):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM entities WHERE id = ?", (entity_id,)
            ).fetchone()
        return self._entity_from_row(row) if row else None

    def list_entities(self, kind=None, status=None):
        clauses = []
        params = []
        if kind:
            clauses.append("kind = ?")
            params.append(kind)
        if status:
            clauses.append("status = ?")
            params.append(status)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM entities" + where + " ORDER BY created_at, id", params
            ).fetchall()
        return [self._entity_from_row(row) for row in rows]

    def find_entities(self, kind, field, value):
        return [
            entity
            for entity in self.list_entities(kind=kind)
            if (entity["id"] == value if field == "id" else entity["data"].get(field) == value)
        ]

    def update_entity(self, entity_id, expected_version, status, data):
        now = utcnow()
        payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT version FROM entities WHERE id = ?", (entity_id,)
            ).fetchone()
            if not row:
                raise NotFoundError("entity not found: " + entity_id)
            current_version = int(row["version"])
            if expected_version is not None and current_version != int(expected_version):
                raise ConflictError(
                    "version conflict: expected %s, found %s"
                    % (expected_version, current_version)
                )
            connection.execute(
                "UPDATE entities SET status = ?, version = version + 1, data = ?, updated_at = ? "
                "WHERE id = ? AND version = ?",
                (status, payload, now, entity_id, current_version),
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
        return self.get_entity(entity_id)

    def append_audit(self, entity_id, actor_id, actor_role, action, from_status, to_status, detail):
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO audit_log(entity_id, actor_id, actor_role, action, from_status, to_status, detail, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    entity_id,
                    actor_id,
                    actor_role,
                    action,
                    from_status,
                    to_status,
                    json.dumps(detail, ensure_ascii=False, sort_keys=True),
                    utcnow(),
                ),
            )

    def list_audit(self, entity_id=None):
        with self._connect() as connection:
            if entity_id:
                rows = connection.execute(
                    "SELECT * FROM audit_log WHERE entity_id = ? ORDER BY id", (entity_id,)
                ).fetchall()
            else:
                rows = connection.execute("SELECT * FROM audit_log ORDER BY id").fetchall()
        return [
            {
                "id": row["id"],
                "entity_id": row["entity_id"],
                "actor_id": row["actor_id"],
                "actor_role": row["actor_role"],
                "action": row["action"],
                "from_status": row["from_status"],
                "to_status": row["to_status"],
                "detail": json.loads(row["detail"]),
                "created_at": row["created_at"],
            }
            for row in rows
        ]

    def get_idempotency(self, actor_id, idem_key):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT entity_id FROM idempotency WHERE actor_id = ? AND idem_key = ?",
                (actor_id, idem_key),
            ).fetchone()
        return row["entity_id"] if row else None

    def save_idempotency(self, actor_id, idem_key, entity_id):
        with self._connect() as connection:
            connection.execute(
                "INSERT OR REPLACE INTO idempotency(actor_id, idem_key, entity_id, created_at) "
                "VALUES (?, ?, ?, ?)",
                (actor_id, idem_key, entity_id, utcnow()),
            )

    # ------------------------------------------------------------------
    # Subscriptions
    # ------------------------------------------------------------------
    @staticmethod
    def _subscription_from_row(row):
        return {
            "id": row["id"],
            "station_id": row["station_id"],
            "source_id": row["source_id"],
            "max_magnitude": float(row["max_magnitude"]),
            "active": bool(row["active"]),
            "version": int(row["version"]),
            "created_by": row["created_by"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def create_subscription(self, subscription_id, station_id, source_id, max_magnitude, actor_id):
        now = utcnow()
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO subscriptions(id, station_id, source_id, max_magnitude, active, version, "
                "created_by, created_at, updated_at) VALUES (?, ?, ?, ?, 1, 1, ?, ?, ?)",
                (subscription_id, station_id, source_id, float(max_magnitude), actor_id, now, now),
            )
        return self.get_subscription(subscription_id)

    def get_subscription(self, subscription_id):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM subscriptions WHERE id = ?", (subscription_id,)
            ).fetchone()
        return self._subscription_from_row(row) if row else None

    def find_subscription(self, station_id, source_id):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM subscriptions WHERE station_id = ? AND source_id = ?",
                (station_id, source_id),
            ).fetchone()
        return self._subscription_from_row(row) if row else None

    def list_subscriptions(self, active=None, source_id=None, station_id=None):
        clauses = []
        params = []
        if active is not None:
            clauses.append("active = ?")
            params.append(1 if active else 0)
        if source_id:
            clauses.append("source_id = ?")
            params.append(source_id)
        if station_id:
            clauses.append("station_id = ?")
            params.append(station_id)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM subscriptions" + where + " ORDER BY created_at, id", params
            ).fetchall()
        return [self._subscription_from_row(row) for row in rows]

    def update_subscription(self, subscription_id, expected_version, **fields):
        allowed = {"max_magnitude", "active"}
        sets = ["version = version + 1", "updated_at = ?"]
        params = [utcnow()]
        for key, value in fields.items():
            if key not in allowed:
                continue
            sets.append(key + " = ?")
            params.append(value)
        params.append(subscription_id)
        params.append(expected_version)
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT version FROM subscriptions WHERE id = ?", (subscription_id,)
            ).fetchone()
            if not row:
                connection.rollback()
                raise NotFoundError("subscription not found: " + subscription_id)
            current = int(row["version"])
            if current != int(expected_version):
                connection.rollback()
                raise ConflictError(
                    "version conflict: expected %s, found %s" % (expected_version, current)
                )
            connection.execute(
                "UPDATE subscriptions SET " + ", ".join(sets) + " WHERE id = ? AND version = ?",
                params,
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
        return self.get_subscription(subscription_id)

    # ------------------------------------------------------------------
    # Broadcast batches
    # ------------------------------------------------------------------
    @staticmethod
    def _batch_from_row(row):
        return {
            "id": row["id"],
            "candidate_id": row["candidate_id"],
            "reason": row["reason"],
            "created_by": row["created_by"],
            "created_at": row["created_at"],
        }

    def create_batch(self, batch_id, candidate_id, reason, created_by=None):
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO broadcast_batches(id, candidate_id, reason, created_by, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (batch_id, candidate_id, reason, created_by, utcnow()),
            )
        return self.get_batch(batch_id)

    def get_batch(self, batch_id):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM broadcast_batches WHERE id = ?", (batch_id,)
            ).fetchone()
        return self._batch_from_row(row) if row else None

    def list_batches(self, candidate_id=None):
        if candidate_id:
            with self._connect() as connection:
                rows = connection.execute(
                    "SELECT * FROM broadcast_batches WHERE candidate_id = ? ORDER BY id",
                    (candidate_id,),
                ).fetchall()
        else:
            with self._connect() as connection:
                rows = connection.execute(
                    "SELECT * FROM broadcast_batches ORDER BY id"
                ).fetchall()
        return [self._batch_from_row(row) for row in rows]

    # ------------------------------------------------------------------
    # Deliveries
    # ------------------------------------------------------------------
    @staticmethod
    def _delivery_from_row(row):
        return {
            "id": row["id"],
            "candidate_id": row["candidate_id"],
            "station_id": row["station_id"],
            "subscription_id": row["subscription_id"],
            "batch_id": row["batch_id"],
            "status": row["status"],
            "attempts": int(row["attempts"]),
            "receipt_action": row["receipt_action"],
            "receipt_by": row["receipt_by"],
            "receipt_at": row["receipt_at"],
            "last_error": row["last_error"],
            "version": int(row["version"]),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def create_delivery(self, delivery_id, candidate_id, station_id, subscription_id, batch_id):
        now = utcnow()
        try:
            with self._connect() as connection:
                connection.execute(
                    "INSERT INTO deliveries(id, candidate_id, station_id, subscription_id, batch_id, "
                    "status, attempts, version, created_at, updated_at) VALUES (?, ?, ?, ?, ?, "
                    "'pending', 0, 1, ?, ?)",
                    (delivery_id, candidate_id, station_id, subscription_id, batch_id, now, now),
                )
        except sqlite3.IntegrityError:
            raise ConflictError(
                "delivery already exists for candidate %s station %s" % (candidate_id, station_id)
            )
        return self.get_delivery(delivery_id)

    def get_delivery(self, delivery_id):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM deliveries WHERE id = ?", (delivery_id,)
            ).fetchone()
        return self._delivery_from_row(row) if row else None

    def find_active_delivery(self, candidate_id, station_id):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM deliveries WHERE candidate_id = ? AND station_id = ? "
                "AND status != 'voided' LIMIT 1",
                (candidate_id, station_id),
            ).fetchone()
        return self._delivery_from_row(row) if row else None

    def list_deliveries(self, candidate_id=None, station_id=None, status=None, batch_id=None,
                        active_only=False):
        clauses = []
        params = []
        if candidate_id:
            clauses.append("candidate_id = ?")
            params.append(candidate_id)
        if station_id:
            clauses.append("station_id = ?")
            params.append(station_id)
        if status:
            clauses.append("status = ?")
            params.append(status)
        if batch_id:
            clauses.append("batch_id = ?")
            params.append(batch_id)
        if active_only:
            clauses.append("status != 'voided'")
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM deliveries" + where + " ORDER BY created_at, id", params
            ).fetchall()
        return [self._delivery_from_row(row) for row in rows]

    def _transition_delivery(self, delivery_id, expected_version, sets, params):
        params = list(params) + [delivery_id, expected_version]
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT version FROM deliveries WHERE id = ?", (delivery_id,)
            ).fetchone()
            if not row:
                connection.rollback()
                raise NotFoundError("delivery not found: " + delivery_id)
            current = int(row["version"])
            if current != int(expected_version):
                connection.rollback()
                raise ConflictError(
                    "version conflict: expected %s, found %s" % (expected_version, current)
                )
            connection.execute(
                "UPDATE deliveries SET " + ", ".join(sets) + " WHERE id = ? AND version = ?",
                params,
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
        return self.get_delivery(delivery_id)

    def transition_delivery_status(self, delivery_id, expected_version, status, **fields):
        sets = ["status = ?", "version = version + 1", "updated_at = ?"]
        params = [status, utcnow()]
        for key, value in fields.items():
            if key in ("receipt_action", "receipt_by", "receipt_at", "last_error"):
                sets.append(key + " = ?")
                params.append(value)
        return self._transition_delivery(delivery_id, expected_version, sets, params)

    def retry_delivery(self, delivery_id, expected_version):
        sets = ["status = 'pending'", "attempts = attempts + 1",
                "version = version + 1", "updated_at = ?"]
        return self._transition_delivery(delivery_id, expected_version, sets, [utcnow()])

    def void_deliveries(self, candidate_id, subscription_id=None):
        clauses = ["candidate_id = ?", "status IN ('pending', 'sent', 'failed')"]
        where_params = [candidate_id]
        if subscription_id:
            clauses.append("subscription_id = ?")
            where_params.append(subscription_id)
        params = [utcnow()] + where_params
        with self._connect() as connection:
            cursor = connection.execute(
                "UPDATE deliveries SET status = 'voided', version = version + 1, updated_at = ? "
                "WHERE " + " AND ".join(clauses),
                params,
            )
            return cursor.rowcount

    def list_pending_deliveries(self):
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM deliveries WHERE status IN ('pending', 'failed') "
                "ORDER BY created_at, id"
            ).fetchall()
        return [self._delivery_from_row(row) for row in rows]

    def ping(self):
        with self._connect() as connection:
            connection.execute("SELECT 1").fetchone()
        return True
