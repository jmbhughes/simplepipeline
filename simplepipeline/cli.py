from __future__ import annotations

import argparse

from .config import ConfigWorker


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a configured pipeline worker")
    parser.add_argument("config", help="path to the pipeline TOML configuration")
    args = parser.parse_args()

    worker = ConfigWorker(args.config)
    worker.start()
    print(f"Pipeline worker started from {args.config}. Press Ctrl+C to stop.")
    try:
        exit_code = worker.join()
    except KeyboardInterrupt:
        worker.stop()
        exit_code = worker.join()
        print("Pipeline worker stopped.")
    return 0 if exit_code in (0, None) else 1
