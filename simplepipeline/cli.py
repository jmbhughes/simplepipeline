from __future__ import annotations

import argparse
import csv
import logging
import sqlite3
import sys
from pathlib import Path

from .config import ConfigWorker
from .dashboard import DashboardServer


LOGGER = logging.getLogger(__name__)


def export_candidate_timings(database: str | Path, output: str = "-") -> None:
    """Export candidate timing columns as CSV."""
    with sqlite3.connect(str(database), timeout=30) as connection:
        connection.row_factory = sqlite3.Row
        columns = {row["name"] for row in connection.execute("PRAGMA table_info(candidates)")}
        if not {"started_at", "finished_at", "duration_seconds"}.issubset(columns):
            raise ValueError(
                "The database is missing candidate timing columns. Run the pipeline "
                "at least once with the current code to initialize them."
            )
        rows = connection.execute(
            "SELECT rule_name, output_product_code, timestamp, status, started_at, "
            "finished_at, duration_seconds, error FROM candidates "
            "ORDER BY started_at, id"
        ).fetchall()

    fieldnames = [
        "rule_name",
        "output_product_code",
        "timestamp",
        "status",
        "started_at",
        "finished_at",
        "duration_seconds",
        "error",
    ]

    if output == "-":
        writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row[key] for key in fieldnames})
        return

    destination = Path(output)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row[key] for key in fieldnames})


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv and argv[0] in {"export-timings", "export-candidate-timings"}:
        export_parser = argparse.ArgumentParser(
            description="Export candidate run timing metrics as CSV"
        )
        export_parser.add_argument("database", help="path to the SQLite database")
        export_parser.add_argument(
            "-o",
            "--output",
            default="-",
            help="CSV output path; use '-' to print to stdout",
        )
        args = export_parser.parse_args(argv[1:])
        try:
            export_candidate_timings(args.database, args.output)
        except ValueError as error:
            export_parser.error(str(error))
        return 0

    parser = argparse.ArgumentParser(description="Run a configured pipeline worker")
    parser.add_argument("config", help="path to the pipeline TOML configuration")
    parser.add_argument(
        "--resource-usage",
        dest="resource_usage",
        action="store_true",
        default=None,
        help="log CPU and memory usage for each processed flow",
    )
    parser.add_argument(
        "--resource-usage-csv",
        dest="resource_usage_csv",
        help="write per-flow CPU and memory usage to this CSV file",
    )
    parser.add_argument(
        "--resource-usage-interval",
        dest="resource_usage_interval_seconds",
        type=float,
        help="minimum seconds between resource-usage log entries; defaults to every processed flow",
    )
    parser.add_argument(
        "--dashboard",
        action="store_true",
        help="serve the live status dashboard in a browser",
    )
    parser.add_argument(
        "--dashboard-host",
        default="127.0.0.1",
        help="dashboard address (default: localhost; use 0.0.0.0 to expose it)",
    )
    parser.add_argument(
        "--dashboard-port",
        type=int,
        default=8765,
        help="dashboard port (default: 8765)",
    )
    parser.add_argument("--verbose", action="store_true", help="show debug-level logs")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    if (
        args.resource_usage is not None
        or args.resource_usage_csv is not None
        or args.resource_usage_interval_seconds is not None
    ):
        worker = ConfigWorker(
            args.config,
            resource_usage=(
                True
                if args.resource_usage_csv is not None
                or args.resource_usage_interval_seconds is not None
                else args.resource_usage
            ),
            resource_usage_csv=args.resource_usage_csv,
            resource_usage_interval_seconds=args.resource_usage_interval_seconds,
        )
    else:
        worker = ConfigWorker(args.config)
    dashboard = (
        DashboardServer(worker.database, args.dashboard_host, args.dashboard_port)
        if args.dashboard
        else None
    )
    worker.start()
    if dashboard is not None:
        try:
            dashboard.start()
        except Exception:
            worker.stop()
            worker.join()
            raise
    LOGGER.info("Pipeline worker running. Press Ctrl+C to stop.")
    try:
        exit_code = worker.join()
    except KeyboardInterrupt:
        worker.stop()
        exit_code = worker.join()
        LOGGER.info("Pipeline worker stopped.")
    finally:
        if dashboard is not None:
            dashboard.close()
    return 0 if exit_code in (0, None) else 1
