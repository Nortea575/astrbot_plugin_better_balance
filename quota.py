"""Persistent, daily quota accounting for Better Balance.

Only completed LLM turns consume quota. Reservations are persisted separately
so concurrent turns cannot claim the same remaining slot.
"""

from __future__ import annotations

import copy
import json
import re
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Callable, Iterator, Mapping

from .configuration import default_config as _canonical_defaults
from .configuration import normalize_config
from .settings import NativeSettings


_BEIJING = timezone(timedelta(hours=8), name="Asia/Shanghai")
_RESET_RE = re.compile(r"^(?:[01]\d|2[0-3]):[0-5]\d$")


@dataclass(frozen=True)
class Scope:
    platform_id: str
    kind: str
    target_id: str


@dataclass(frozen=True)
class Reservation:
    token: str
    scope: Scope
    period: str


def _default_config() -> dict:
    return _canonical_defaults()


def _normalize_config(payload: object, base: Mapping[str, object]) -> dict:
    return normalize_config(payload, base)


class Store:
    """A SQLite-backed quota store.

    ``root`` is normally a data directory. A path ending in ``.db``,
    ``.sqlite`` or ``.sqlite3`` is treated as the database file itself.
    ``clock`` is an optional zero-argument callable returning a datetime;
    naive datetimes are interpreted as Beijing local time.
    """

    def __init__(
        self,
        root: Path,
        clock: Callable[[], datetime] | None = None,
        config: Mapping | None = None,
    ) -> None:
        root_path = Path(root)
        if root_path.exists() and root_path.is_file():
            database = root_path
            database.parent.mkdir(parents=True, exist_ok=True)
        elif root_path.suffix.lower() in {".db", ".sqlite", ".sqlite3"}:
            root_path.parent.mkdir(parents=True, exist_ok=True)
            database = root_path
        else:
            root_path.mkdir(parents=True, exist_ok=True)
            database = root_path / "quota.sqlite3"

        self._clock = clock or (lambda: datetime.now(_BEIJING))
        self._lock = threading.RLock()
        self._closed = False
        self._session_id = uuid.uuid4().hex
        self._native_settings = NativeSettings(config) if config is not None else None
        self._conn = sqlite3.connect(
            str(database), timeout=30, isolation_level=None, check_same_thread=False
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA busy_timeout = 30000")
        self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS usage (
                period TEXT NOT NULL,
                platform_id TEXT NOT NULL,
                kind TEXT NOT NULL,
                target_id TEXT NOT NULL,
                used INTEGER NOT NULL CHECK (used >= 0),
                PRIMARY KEY (period, platform_id, kind, target_id)
            );
            CREATE TABLE IF NOT EXISTS reservations (
                token TEXT PRIMARY KEY,
                request_id TEXT NOT NULL,
                platform_id TEXT NOT NULL,
                kind TEXT NOT NULL,
                target_id TEXT NOT NULL,
                period TEXT NOT NULL,
                status TEXT NOT NULL CHECK (status IN ('pending', 'committed', 'released')),
                owner_session TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE (platform_id, kind, target_id, request_id)
            );
            CREATE INDEX IF NOT EXISTS reservations_scope_period_status
                ON reservations (period, platform_id, kind, target_id, status);
            """
        )

        with self._transaction() as conn:
            config_row = conn.execute(
                "SELECT value FROM meta WHERE key = 'config'"
            ).fetchone()
            if self._native_settings is not None:
                if self._meta(conn, "period_reset_time") is None:
                    prior_reset_time = None
                    if config_row is not None:
                        try:
                            prior_reset_time = _normalize_config(
                                json.loads(config_row["value"]), _default_config()
                            )["reset_time"]
                        except (TypeError, ValueError, json.JSONDecodeError):
                            pass
                    if prior_reset_time is None:
                        prior_reset_at = self._meta(conn, "reset_at")
                        if prior_reset_at is not None:
                            try:
                                prior_reset_time = datetime.fromisoformat(
                                    prior_reset_at
                                ).strftime("%H:%M")
                            except ValueError:
                                pass
                    if prior_reset_time is not None:
                        self._set_meta(conn, "period_reset_time", prior_reset_time)
                native_raw = self._native_settings.load()
                native_config = _normalize_config(native_raw, _default_config())
                migrated = self._meta(conn, "native_migrated") == "1"
                if not migrated:
                    legacy_config = None
                    if config_row is not None:
                        try:
                            legacy_config = _normalize_config(
                                json.loads(config_row["value"]), _default_config()
                            )
                        except (TypeError, ValueError, json.JSONDecodeError):
                            legacy_config = None
                    if native_config == _default_config() and legacy_config is not None:
                        native_config = legacy_config
                    self._native_settings.save(native_config)
                    self._set_meta(conn, "native_migrated", "1")
                elif (
                    native_raw != native_config
                    or self._native_settings.needs_migration
                ):
                    self._native_settings.save(native_config)
                conn.execute("DELETE FROM meta WHERE key = 'config'")
                config = native_config
            elif config_row is None:
                config = _default_config()
                self._set_meta(conn, "config", json.dumps(config, ensure_ascii=False))
            else:
                persisted = json.loads(config_row["value"])
                config = _normalize_config(persisted, _default_config())
                if persisted != config:
                    self._set_meta(
                        conn, "config", json.dumps(config, ensure_ascii=False)
                    )
            # AstrBot restarts the plugin process with execv, preserving its PID.
            # A new Store marks all previous unfinished tickets as abandoned.
            conn.execute(
                "UPDATE reservations SET status = 'released' WHERE status = 'pending'"
            )
            self._ensure_period(conn, config, self._now_local())

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            self._ensure_open()
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                yield self._conn
                self._conn.execute("COMMIT")
            except BaseException:
                if self._conn.in_transaction:
                    self._conn.execute("ROLLBACK")
                raise

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("Store 已关闭")

    def _now_local(self) -> datetime:
        value = self._clock()
        if not isinstance(value, datetime):
            raise TypeError("clock 必须返回 datetime")
        if value.tzinfo is None:
            return value.replace(tzinfo=_BEIJING)
        return value.astimezone(_BEIJING)

    @staticmethod
    def _set_meta(conn: sqlite3.Connection, key: str, value: str) -> None:
        conn.execute(
            "INSERT INTO meta(key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )

    @staticmethod
    def _meta(conn: sqlite3.Connection, key: str) -> str | None:
        row = conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
        return None if row is None else row["value"]

    @staticmethod
    def _as_boundary(value: datetime) -> str:
        return value.isoformat(timespec="minutes")

    @staticmethod
    def _as_reset_at(value: datetime) -> str:
        return value.isoformat(timespec="seconds")

    @staticmethod
    def _boundary_for(day: date, reset_time: str) -> datetime:
        hour, minute = (int(part) for part in reset_time.split(":"))
        return datetime.combine(day, time(hour, minute), tzinfo=_BEIJING)

    def _period_bounds(self, now: datetime, reset_time: str) -> tuple[str, str]:
        today_boundary = self._boundary_for(now.date(), reset_time)
        if today_boundary <= now:
            start = today_boundary
            end = self._boundary_for(now.date() + timedelta(days=1), reset_time)
        else:
            start = self._boundary_for(now.date() - timedelta(days=1), reset_time)
            end = today_boundary
        return self._as_boundary(start), self._as_reset_at(end)

    def _ensure_period(
        self, conn: sqlite3.Connection, config: Mapping[str, object], now: datetime
    ) -> tuple[str, str]:
        period = self._meta(conn, "period")
        reset_at = self._meta(conn, "reset_at")
        if period is None or reset_at is None:
            period, reset_at = self._period_bounds(now, str(config["reset_time"]))
            self._set_meta(conn, "period", period)
            self._set_meta(conn, "reset_at", reset_at)
            self._set_meta(conn, "period_reset_time", str(config["reset_time"]))
            return period, reset_at

        stored_reset_time = self._meta(conn, "period_reset_time")
        previous_reset_time = stored_reset_time
        if previous_reset_time is None or not _RESET_RE.fullmatch(previous_reset_time):
            previous_reset_time = datetime.fromisoformat(reset_at).strftime("%H:%M")
        reset_datetime = datetime.fromisoformat(reset_at)
        period_expired = now >= reset_datetime
        schedule_changed = str(config["reset_time"]) != previous_reset_time
        if period_expired:
            period, reset_at = self._period_bounds(now, previous_reset_time)
        if schedule_changed:
            _period_from_new_schedule, reset_at = self._period_bounds(
                now, str(config["reset_time"])
            )
        if period_expired or schedule_changed or stored_reset_time != previous_reset_time:
            self._set_meta(conn, "period", period)
            self._set_meta(conn, "reset_at", reset_at)
            self._set_meta(conn, "period_reset_time", str(config["reset_time"]))
        return period, reset_at

    @staticmethod
    def _validate_scope(scope: Scope) -> Scope:
        if not isinstance(scope, Scope):
            raise ValueError("scope 必须是 Scope")
        for name, value in (
            ("platform_id", scope.platform_id),
            ("target_id", scope.target_id),
        ):
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"scope.{name} 不能为空")
        if not isinstance(scope.kind, str) or scope.kind not in {"private", "group"}:
            raise ValueError("scope.kind 必须是 private 或 group")
        return scope

    @staticmethod
    def _total_for(config: Mapping[str, object], scope: Scope) -> int:
        if scope.kind == "private":
            for group in config["user_groups"]:
                if scope.target_id in group["user_ids"]:
                    return int(group["daily_limit"])
            return int(config["default_user_limit"])
        for rule in config["group_rules"]:
            if scope.target_id == rule["group_id"]:
                return int(rule["daily_limit"])
        return int(config["default_group_limit"])

    def _get_current_config(self, conn: sqlite3.Connection | None = None) -> dict:
        if self._native_settings is not None:
            return _normalize_config(self._native_settings.load(), _default_config())
        if conn is not None:
            row = conn.execute(
                "SELECT value FROM meta WHERE key = 'config'"
            ).fetchone()
            if row is None:
                return _default_config()
            return copy.deepcopy(json.loads(row["value"]))
        with self._lock:
            self._ensure_open()
            row = self._conn.execute(
                "SELECT value FROM meta WHERE key = 'config'"
            ).fetchone()
            if row is None:
                return _default_config()
            return copy.deepcopy(json.loads(row["value"]))

    @staticmethod
    def _read_used(
        conn: sqlite3.Connection, scope: Scope, period: str
    ) -> int:
        row = conn.execute(
            "SELECT used FROM usage WHERE period = ? AND platform_id = ? "
            "AND kind = ? AND target_id = ?",
            (period, scope.platform_id, scope.kind, scope.target_id),
        ).fetchone()
        return 0 if row is None else int(row["used"])

    def _snapshot_in_transaction(
        self,
        conn: sqlite3.Connection,
        scope: Scope,
        config: Mapping[str, object],
        period: str,
        reset_at: str,
    ) -> dict:
        used = self._read_used(conn, scope, period)
        reserved = int(
            conn.execute(
                "SELECT COUNT(*) AS n FROM reservations WHERE status = 'pending' "
                "AND period = ? AND platform_id = ? AND kind = ? AND target_id = ?",
                (period, scope.platform_id, scope.kind, scope.target_id),
            ).fetchone()["n"]
        )
        total = self._total_for(config, scope)
        mode = "blacklist" if total == -1 else "unlimited" if total == 0 else "limited"
        return {
            "platform_id": scope.platform_id,
            "kind": scope.kind,
            "target_id": scope.target_id,
            "used": used,
            "total": total,
            "mode": mode,
            "reserved": reserved,
            "remaining": (
                0
                if mode == "blacklist"
                else None
                if mode == "unlimited"
                else max(0, total - used - reserved)
            ),
            "reset_at": reset_at,
            "period": period,
        }

    def get_config(self) -> dict:
        if self._native_settings is not None:
            with self._lock:
                self._ensure_open()
                return _normalize_config(self._native_settings.load(), _default_config())
        with self._lock:
            self._ensure_open()
            row = self._conn.execute(
                "SELECT value FROM meta WHERE key = 'config'"
            ).fetchone()
            return copy.deepcopy(json.loads(row["value"]))

    def save_config(self, payload: object) -> dict:
        with self._transaction() as conn:
            current = self._get_current_config(conn)
            normalized = _normalize_config(payload, current)
            now = self._now_local()
            period, _old_reset_at = self._ensure_period(conn, current, now)
            if self._native_settings is not None:
                self._native_settings.save(normalized)
                conn.execute("DELETE FROM meta WHERE key = 'config'")
            else:
                self._set_meta(
                    conn,
                    "config",
                    json.dumps(normalized, ensure_ascii=False, separators=(",", ":")),
                )
            if normalized["reset_time"] != current["reset_time"]:
                _period_from_new_schedule, reset_at = self._period_bounds(
                    now, normalized["reset_time"]
                )
                # Keep the existing period and its usage until the next boundary
                # under the newly selected reset time.
                self._set_meta(conn, "period", period)
                self._set_meta(conn, "reset_at", reset_at)
                self._set_meta(
                    conn, "period_reset_time", normalized["reset_time"]
                )
            return copy.deepcopy(normalized)

    def snapshot(self, scope: Scope) -> dict:
        scope = self._validate_scope(scope)
        with self._transaction() as conn:
            config = self._get_current_config(conn)
            period, reset_at = self._ensure_period(conn, config, self._now_local())
            return self._snapshot_in_transaction(
                conn, scope, config, period, reset_at
            )

    def reserve(self, scope: Scope, request_id: str) -> Reservation | None:
        scope = self._validate_scope(scope)
        if not isinstance(request_id, str) or not request_id.strip():
            raise ValueError("request_id 不能为空")
        with self._transaction() as conn:
            existing = conn.execute(
                "SELECT token, period, status FROM reservations WHERE platform_id = ? "
                "AND kind = ? AND target_id = ? AND request_id = ?",
                (scope.platform_id, scope.kind, scope.target_id, request_id),
            ).fetchone()
            if existing is not None:
                if existing["status"] == "released":
                    return None
                return Reservation(existing["token"], scope, existing["period"])

            config = self._get_current_config(conn)
            period, _reset_at = self._ensure_period(conn, config, self._now_local())
            total = self._total_for(config, scope)
            used = self._read_used(conn, scope, period)
            reserved = int(
                conn.execute(
                    "SELECT COUNT(*) AS n FROM reservations WHERE status = 'pending' "
                    "AND period = ? AND platform_id = ? AND kind = ? AND target_id = ?",
                    (period, scope.platform_id, scope.kind, scope.target_id),
                ).fetchone()["n"]
            )
            if total == -1 or (total > 0 and used + reserved >= total):
                return None

            token = uuid.uuid4().hex
            now_text = self._as_reset_at(self._now_local())
            conn.execute(
                "INSERT INTO reservations(token, request_id, platform_id, kind, target_id, "
                "period, status, owner_session, created_at) VALUES (?, ?, ?, ?, ?, ?, "
                "'pending', ?, ?)",
                (
                    token,
                    request_id,
                    scope.platform_id,
                    scope.kind,
                    scope.target_id,
                    period,
                    self._session_id,
                    now_text,
                ),
            )
            return Reservation(token, scope, period)

    def commit(self, token: str) -> bool:
        if not isinstance(token, str) or not token:
            return False
        with self._transaction() as conn:
            self._ensure_period(
                conn, self._get_current_config(conn), self._now_local()
            )
            row = conn.execute(
                "SELECT period, platform_id, kind, target_id, status "
                "FROM reservations WHERE token = ?",
                (token,),
            ).fetchone()
            if row is None or row["status"] == "released":
                return False
            if row["status"] == "committed":
                return True
            conn.execute(
                "INSERT INTO usage(period, platform_id, kind, target_id, used) "
                "VALUES (?, ?, ?, ?, 1) ON CONFLICT(period, platform_id, kind, target_id) "
                "DO UPDATE SET used = used + 1",
                (row["period"], row["platform_id"], row["kind"], row["target_id"]),
            )
            conn.execute(
                "UPDATE reservations SET status = 'committed' WHERE token = ?",
                (token,),
            )
            return True

    def release(self, token: str) -> bool:
        if not isinstance(token, str) or not token:
            return False
        with self._transaction() as conn:
            row = conn.execute(
                "SELECT status FROM reservations WHERE token = ?", (token,)
            ).fetchone()
            if row is None or row["status"] == "committed":
                return False
            if row["status"] == "pending":
                conn.execute(
                    "UPDATE reservations SET status = 'released' WHERE token = ?",
                    (token,),
                )
            return True

    def list_usage(self) -> list[dict]:
        with self._transaction() as conn:
            config = self._get_current_config(conn)
            period, reset_at = self._ensure_period(conn, config, self._now_local())
            scopes = conn.execute(
                "SELECT platform_id, kind, target_id FROM usage WHERE period = ? "
                "UNION SELECT platform_id, kind, target_id FROM reservations "
                "WHERE period = ? AND status = 'pending' "
                "ORDER BY platform_id, kind, target_id",
                (period, period),
            ).fetchall()
            return [
                self._snapshot_in_transaction(
                    conn,
                    Scope(row["platform_id"], row["kind"], row["target_id"]),
                    config,
                    period,
                    reset_at,
                )
                for row in scopes
            ]

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            with self._transaction():
                self._conn.execute(
                    "UPDATE reservations SET status = 'released' "
                    "WHERE status = 'pending' AND owner_session = ?",
                    (self._session_id,),
                )
            self._conn.close()
            self._closed = True

    def __enter__(self) -> "Store":
        self._ensure_open()
        return self

    def __exit__(self, _exc_type, _exc, _traceback) -> None:
        self.close()
