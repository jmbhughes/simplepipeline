from __future__ import annotations

import logging
import multiprocessing
import sqlite3
import time as time_module
import uuid
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Callable, Iterable, Sequence

from .models import (
    Product,
    ScheduledRule,
    latest_version_products,
    normalize_timestamp,
    parse_timestamp,
    semantic_version_key,
    timestamp_text,
    validate_product_code,
)


LOGGER = logging.getLogger(__name__)


class Pipeline:
    """Persist products and scheduled product attempts in a SQLite database."""

    def __init__(self, database: str | Path) -> None:
        self.database = str(database)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        return connection

    def _initialize(self) -> None:
        with self._connect() as db:
            db.executescript(
                """
                CREATE TABLE IF NOT EXISTS products (
                    id INTEGER PRIMARY KEY,
                    product_code TEXT NOT NULL CHECK(length(product_code) BETWEEN 1 AND 16),
                    timestamp TEXT NOT NULL,
                    path TEXT NOT NULL,
                    version TEXT NOT NULL DEFAULT '0.0.0',
                    created_at TEXT NOT NULL,
                    UNIQUE(product_code, timestamp, version)
                );
                CREATE INDEX IF NOT EXISTS products_lookup
                    ON products(product_code, timestamp);
                CREATE TABLE IF NOT EXISTS candidates (
                    id INTEGER PRIMARY KEY,
                    rule_name TEXT NOT NULL,
                    output_product_code TEXT NOT NULL CHECK(length(output_product_code) BETWEEN 1 AND 16),
                    timestamp TEXT NOT NULL,
                    status TEXT NOT NULL CHECK(status IN ('pending', 'complete', 'failed')),
                    output_product_id INTEGER REFERENCES products(id),
                    error TEXT,
                    claim_token TEXT,
                    claimed_at TEXT,
                    UNIQUE(rule_name, timestamp)
                );
                CREATE TABLE IF NOT EXISTS candidate_inputs (
                    candidate_id INTEGER NOT NULL REFERENCES candidates(id) ON DELETE CASCADE,
                    product_id INTEGER NOT NULL REFERENCES products(id),
                    ordinal INTEGER NOT NULL,
                    PRIMARY KEY(candidate_id, ordinal),
                    UNIQUE(candidate_id, product_id)
                );
                """
            )
            product_columns = {
                row["name"] for row in db.execute("PRAGMA table_info(products)")
            }
            if "version" not in product_columns:
                db.execute("PRAGMA foreign_keys = OFF")
                db.execute("BEGIN")
                db.execute(
                    "CREATE TABLE products_new ("
                    "id INTEGER PRIMARY KEY, "
                    "product_code TEXT NOT NULL CHECK(length(product_code) BETWEEN 1 AND 16), "
                    "timestamp TEXT NOT NULL, path TEXT NOT NULL, "
                    "version TEXT NOT NULL DEFAULT '0.0.0', created_at TEXT NOT NULL, "
                    "UNIQUE(product_code, timestamp, version))"
                )
                db.execute(
                    "INSERT INTO products_new(id, product_code, timestamp, path, version, created_at) "
                    "SELECT id, product_code, timestamp, path, '0.0.0', created_at FROM products"
                )
                db.execute("DROP TABLE products")
                db.execute("ALTER TABLE products_new RENAME TO products")
                db.execute(
                    "CREATE INDEX products_lookup ON products(product_code, timestamp)"
                )
                db.commit()
                db.execute("PRAGMA foreign_keys = ON")
            candidate_columns = {
                row["name"] for row in db.execute("PRAGMA table_info(candidates)")
            }
            if "claim_token" not in candidate_columns:
                db.execute("ALTER TABLE candidates ADD COLUMN claim_token TEXT")
            if "claimed_at" not in candidate_columns:
                db.execute("ALTER TABLE candidates ADD COLUMN claimed_at TEXT")

    def add_product(self, product: Product) -> Product:
        created_at = timestamp_text(datetime.now(timezone.utc))
        with self._connect() as db:
            existing = db.execute(
                "SELECT id, path FROM products "
                "WHERE product_code = ? AND timestamp = ? AND version = ?",
                (
                    product.product_code,
                    timestamp_text(product.timestamp),
                    product.version,
                ),
            ).fetchone()
            if existing:
                if existing["path"] != product.path:
                    raise ValueError(
                        f"product {product.product_code} at {timestamp_text(product.timestamp)} already exists at {existing['path']}"
                    )
                return Product(
                    product.product_code,
                    product.timestamp,
                    product.path,
                    existing["id"],
                    product.version,
                )
            cursor = db.execute(
                "INSERT INTO products(product_code, timestamp, path, version, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (
                    product.product_code,
                    timestamp_text(product.timestamp),
                    product.path,
                    product.version,
                    created_at,
                ),
            )
            product_id = int(cursor.lastrowid)
        return Product(
            product.product_code,
            product.timestamp,
            product.path,
            product_id,
            product.version,
        )

    def products(self, product_code: str | None = None) -> list[Product]:
        if product_code is not None:
            validate_product_code(product_code)
        with self._connect() as db:
            if product_code is None:
                rows = db.execute("SELECT * FROM products ORDER BY timestamp, product_code, id").fetchall()
            else:
                rows = db.execute(
                    "SELECT * FROM products WHERE product_code = ? ORDER BY timestamp, id",
                    (product_code,),
                ).fetchall()
        return [
            Product(
                row["product_code"],
                parse_timestamp(row["timestamp"]),
                row["path"],
                row["id"],
                row["version"],
            )
            for row in rows
        ]

    def run_candidate(
        self,
        rule: ScheduledRule,
        output_timestamp: datetime,
        processor: Callable[[list[Product], datetime], str],
        *,
        output_version: str = "0.0.0",
        claim_timeout: timedelta = timedelta(days=1),
    ) -> str:
        """Attempt a scheduled output once; pending candidates may be retried.

        The processor receives the selected immutable input set and output time,
        and must return the output file path. Existing completed outputs are never
        recomputed when additional inputs arrive. A candidate claimed by another
        worker is left alone until its claim expires.
        """
        if claim_timeout <= timedelta(0):
            raise ValueError("claim_timeout must be positive")
        semantic_version_key(output_version)
        output_timestamp = normalize_timestamp(output_timestamp)
        timestamp = timestamp_text(output_timestamp)
        claim_token = uuid.uuid4().hex
        claim_time = datetime.now(timezone.utc)
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT * FROM candidates WHERE rule_name = ? AND timestamp = ?",
                (rule.name, timestamp),
            ).fetchone()
            if row and row["status"] == "complete":
                return "complete"
            if row and row["claim_token"] and row["claimed_at"]:
                claim_age = claim_time - parse_timestamp(row["claimed_at"])
                if claim_age < claim_timeout:
                    return "processing"

            if row is None:
                cursor = db.execute(
                    "INSERT INTO candidates(rule_name, output_product_code, timestamp, status) VALUES (?, ?, ?, 'pending')",
                    (rule.name, rule.output_product_code, timestamp),
                )
                candidate_id = int(cursor.lastrowid)
            else:
                candidate_id = int(row["id"])

            input_codes = getattr(
                rule, "input_product_codes", (rule.input_product_code,)
            )
            placeholders = ", ".join("?" for _ in input_codes)
            input_rows = db.execute(
                f"SELECT * FROM products WHERE product_code IN ({placeholders}) "
                "ORDER BY timestamp, path, id",
                input_codes,
            ).fetchall()
            available = [
                Product(
                    r["product_code"],
                    parse_timestamp(r["timestamp"]),
                    r["path"],
                    r["id"],
                    r["version"],
                )
                for r in input_rows
            ]
            selected = rule.select_inputs(available, output_timestamp)
            if not selected:
                db.execute(
                    "UPDATE candidates SET status = 'pending', error = NULL, claim_token = NULL, claimed_at = NULL WHERE id = ?",
                    (candidate_id,),
                )
                return "pending"

            db.execute(
                "UPDATE candidates SET status = 'pending', error = NULL, claim_token = ?, claimed_at = ? WHERE id = ?",
                (claim_token, timestamp_text(claim_time), candidate_id),
            )

        try:
            output_path = processor(selected, output_timestamp)
            if not output_path:
                raise ValueError("processor must return a non-empty output path")
        except Exception as error:
            with self._connect() as db:
                db.execute(
                    "UPDATE candidates SET status = 'failed', error = ?, claim_token = NULL, claimed_at = NULL WHERE id = ? AND claim_token = ?",
                    (str(error), candidate_id, claim_token),
                )
            raise

        try:
            with self._connect() as db:
                db.execute("BEGIN IMMEDIATE")
                claim = db.execute(
                    "SELECT claim_token FROM candidates WHERE id = ?", (candidate_id,)
                ).fetchone()
                if claim is None or claim["claim_token"] != claim_token:
                    raise RuntimeError("candidate processing claim expired before completion")
                existing = db.execute(
                    "SELECT id FROM products WHERE product_code = ? AND timestamp = ? AND version = ?",
                    (rule.output_product_code, timestamp, output_version),
                ).fetchone()
                if existing:
                    output_id = int(existing["id"])
                else:
                    cursor = db.execute(
                        "INSERT INTO products(product_code, timestamp, path, version, created_at) "
                        "VALUES (?, ?, ?, ?, ?)",
                        (
                            rule.output_product_code,
                            timestamp,
                            output_path,
                            output_version,
                            timestamp_text(datetime.now(timezone.utc)),
                        ),
                    )
                    output_id = int(cursor.lastrowid)
                db.executemany(
                    "INSERT INTO candidate_inputs(candidate_id, product_id, ordinal) VALUES (?, ?, ?)",
                    [(candidate_id, product.product_id, i) for i, product in enumerate(selected)],
                )
                db.execute(
                    "UPDATE candidates SET status = 'complete', output_product_id = ?, error = NULL, claim_token = NULL, claimed_at = NULL WHERE id = ?",
                    (output_id, candidate_id),
                )
                return "complete"
        except Exception as error:
            with self._connect() as db:
                db.execute(
                    "UPDATE candidates SET status = 'failed', error = ?, claim_token = NULL, claimed_at = NULL WHERE id = ? AND claim_token = ?",
                    (str(error), candidate_id, claim_token),
                )
            raise

    def candidate_inputs(self, rule_name: str, output_timestamp: datetime) -> list[Product]:
        with self._connect() as db:
            row = db.execute(
                "SELECT id FROM candidates WHERE rule_name = ? AND timestamp = ?",
                (rule_name, timestamp_text(output_timestamp)),
            ).fetchone()
            if row is None:
                return []
            inputs = db.execute(
                "SELECT p.* FROM candidate_inputs ci JOIN products p ON p.id = ci.product_id "
                "WHERE ci.candidate_id = ? ORDER BY ci.ordinal",
                (row["id"],),
            ).fetchall()
        return [
            Product(
                r["product_code"],
                parse_timestamp(r["timestamp"]),
                r["path"],
                r["id"],
                r["version"],
            )
            for r in inputs
        ]

    def candidate_status(self, rule_name: str, output_timestamp: datetime) -> str | None:
        with self._connect() as db:
            row = db.execute(
                "SELECT status, claim_token FROM candidates WHERE rule_name = ? AND timestamp = ?",
                (rule_name, timestamp_text(output_timestamp)),
            ).fetchone()
        if row is None:
            return None
        return "processing" if row["claim_token"] else row["status"]

    def incomplete_candidates(self, rule_name: str) -> list[datetime]:
        with self._connect() as db:
            rows = db.execute(
                "SELECT timestamp FROM candidates "
                "WHERE rule_name = ? AND status != 'complete' ORDER BY timestamp",
                (rule_name,),
            ).fetchall()
        return [parse_timestamp(row["timestamp"]) for row in rows]

    def product_timestamps(
        self, product_code: str, *, excluding_rule: str | None = None
    ) -> list[datetime]:
        validate_product_code(product_code)
        with self._connect() as db:
            rows = db.execute(
                "SELECT * FROM products WHERE product_code = ? ORDER BY timestamp, id",
                (product_code,),
            ).fetchall()
            completed = set()
            if excluding_rule is not None:
                completed = {
                    row["timestamp"]
                    for row in db.execute(
                        "SELECT timestamp FROM candidates "
                        "WHERE rule_name = ? AND status = 'complete'",
                        (excluding_rule,),
                    )
                }
        products = latest_version_products(
            [
                Product(
                    row["product_code"],
                    parse_timestamp(row["timestamp"]),
                    row["path"],
                    row["id"],
                    row["version"],
                )
                for row in rows
            ]
        )
        return [
            product.timestamp
            for product in products
            if timestamp_text(product.timestamp) not in completed
        ]

    def scheduler_cursor(self, name: str) -> datetime | None:
        with self._connect() as db:
            db.execute(
                "CREATE TABLE IF NOT EXISTS scheduler_cursors "
                "(name TEXT PRIMARY KEY, timestamp TEXT NOT NULL)"
            )
            row = db.execute(
                "SELECT timestamp FROM scheduler_cursors WHERE name = ?", (name,)
            ).fetchone()
        return parse_timestamp(row["timestamp"]) if row else None

    def set_scheduler_cursor(self, name: str, timestamp: datetime) -> None:
        with self._connect() as db:
            db.execute(
                "CREATE TABLE IF NOT EXISTS scheduler_cursors "
                "(name TEXT PRIMARY KEY, timestamp TEXT NOT NULL)"
            )
            db.execute(
                "INSERT INTO scheduler_cursors(name, timestamp) VALUES (?, ?) "
                "ON CONFLICT(name) DO UPDATE SET timestamp = excluded.timestamp "
                "WHERE excluded.timestamp > scheduler_cursors.timestamp",
                (name, timestamp_text(timestamp)),
            )


class DailyWorker:
    """Continuously run daily UTC candidates in a separate process."""

    def __init__(
        self,
        database: str | Path,
        rules: Sequence[ScheduledRule],
        processor: Callable[[list[Product], datetime], str],
        *,
        start_date: date,
        poll_interval: float = 60,
        claim_timeout: timedelta = timedelta(days=1),
        output_version: str = "0.0.0",
    ) -> None:
        if not rules:
            raise ValueError("at least one rule is required")
        if poll_interval <= 0:
            raise ValueError("poll_interval must be positive")
        if claim_timeout <= timedelta(0):
            raise ValueError("claim_timeout must be positive")
        semantic_version_key(output_version)
        names = [rule.name for rule in rules]
        if len(set(names)) != len(names):
            raise ValueError("rule names must be unique")
        self.database = str(database)
        self.rules = tuple(rules)
        self.processor = processor
        self.start_date = start_date
        self.poll_interval = poll_interval
        self.claim_timeout = claim_timeout
        self.output_version = output_version
        self.pipeline = Pipeline(self.database)
        self._next_date = start_date
        self._scheduled: set[tuple[int, date]] = set()
        self._process: multiprocessing.Process | None = None
        self._stop_event = None

    def run_once(self, now: datetime | None = None) -> dict[tuple[str, datetime], str]:
        """Schedule all elapsed daily candidates and retry incomplete jobs."""
        current = normalize_timestamp(now or datetime.now(timezone.utc))
        while self._next_date <= current.date():
            self._scheduled.update(
                (rule_index, self._next_date)
                for rule_index in range(len(self.rules))
            )
            self._next_date += timedelta(days=1)

        results: dict[tuple[str, datetime], str] = {}
        for rule_index, candidate_date in sorted(self._scheduled):
            rule = self.rules[rule_index]
            candidate_time = datetime.combine(candidate_date, time.min, tzinfo=timezone.utc)
            key = (rule.name, candidate_time)
            try:
                status = self.pipeline.run_candidate(
                    rule,
                    candidate_time,
                    self.processor,
                    output_version=self.output_version,
                    claim_timeout=self.claim_timeout,
                )
            except Exception:
                LOGGER.exception(
                    "Candidate %s at %s failed; it will be retried",
                    rule.name,
                    candidate_time.isoformat(),
                )
                status = "failed"
            results[key] = status
            if status == "complete":
                self._scheduled.discard((rule_index, candidate_date))
        return results

    def run_forever(self, stop_event=None) -> None:
        """Poll on a fixed interval until the optional stop event is set."""
        while stop_event is None or not stop_event.is_set():
            self.run_once()
            if stop_event is None:
                time_module.sleep(self.poll_interval)
            elif stop_event.wait(self.poll_interval):
                return

    def start(self) -> None:
        """Start this worker in a spawned child process."""
        if self._process is not None and self._process.is_alive():
            raise RuntimeError("worker is already running")
        context = multiprocessing.get_context("spawn")
        self._stop_event = context.Event()
        self._process = context.Process(
            target=_run_worker_process,
            args=(
                self.database,
                self.rules,
                self.processor,
                self.start_date,
                self.poll_interval,
                self.claim_timeout,
                self.output_version,
                self._stop_event,
            ),
            name="simplepipeline-worker",
        )
        self._process.start()

    def stop(self, timeout: float | None = None) -> bool:
        """Request a graceful stop; return whether the process has stopped."""
        if self._process is None or self._stop_event is None:
            return True
        self._stop_event.set()
        self._process.join(timeout)
        return not self._process.is_alive()

    def join(self, timeout: float | None = None) -> int | None:
        """Wait for the worker process and return its exit code, if available."""
        if self._process is None:
            raise RuntimeError("worker has not been started")
        self._process.join(timeout)
        return self._process.exitcode

    @property
    def is_alive(self) -> bool:
        return self._process is not None and self._process.is_alive()


def _run_worker_process(
    database: str,
    rules: Sequence[ScheduledRule],
    processor: Callable[[list[Product], datetime], str],
    start_date: date,
    poll_interval: float,
    claim_timeout: timedelta,
    output_version: str,
    stop_event,
) -> None:
    DailyWorker(
        database,
        rules,
        processor,
        start_date=start_date,
        poll_interval=poll_interval,
        claim_timeout=claim_timeout,
        output_version=output_version,
    ).run_forever(stop_event)


def daily_utc_candidates(start: date, end: date) -> Iterable[datetime]:
    """Yield midnight UTC timestamps for dates in [start, end)."""
    current = start
    while current < end:
        yield datetime.combine(current, time.min, tzinfo=timezone.utc)
        current += timedelta(days=1)
