from __future__ import annotations

import argparse
import logging

from .config import ConfigWorker
from .dashboard import DashboardServer


LOGGER = logging.getLogger(__name__)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a configured pipeline worker")
    parser.add_argument("config", help="path to the pipeline TOML configuration")
    parser.add_argument(
        "--resource-usage",
        "--log-resource-usage",
        dest="resource_usage",
        action="store_true",
        default=None,
        help="log CPU and memory usage for each processed flow",
    )
    parser.add_argument(
        "--resource-usage-csv",
        "--log-resource-usage-csv",
        dest="resource_usage_csv",
        help="write per-flow CPU and memory usage to this CSV file",
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
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    if args.resource_usage is not None or args.resource_usage_csv is not None:
        worker = ConfigWorker(
            args.config,
            resource_usage=(
                True
                if args.resource_usage_csv is not None
                else args.resource_usage
            ),
            resource_usage_csv=args.resource_usage_csv,
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
