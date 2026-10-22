from datetime import datetime, timezone
from pathlib import Path

from simplepipeline import ConfigWorker, Pipeline, Product


DATABASE = Path("pipeline.sqlite3")
CONFIG = Path(__file__).with_name("pipeline.toml")


def make_l2(*args: str | datetime) -> str:
    output_timestamp = args[-1]
    inputs = args[:-1]
    if not isinstance(output_timestamp, datetime):
        raise TypeError("the final processor argument must be the output timestamp")
    output = f"data/l2-{output_timestamp:%Y%m%dT%H%M%SZ}.fits"
    print(f"Creating {output} from {len(inputs)} inputs")
    # Call the scientific processing code here.
    return output


def main() -> None:
    pipeline = Pipeline(DATABASE)
    timestamp = datetime.now(timezone.utc).replace(microsecond=0)
    pipeline.add_product(Product("L1", timestamp, "data/example-l1.fits"))

    worker = ConfigWorker(CONFIG)
    worker.start()
    print(f"Configured pipeline worker started with {CONFIG}. Press Ctrl+C to stop.")
    try:
        exit_code = worker.join()
        if exit_code != 0:
            raise RuntimeError(f"pipeline worker exited with status {exit_code}")
    except KeyboardInterrupt:
        worker.stop()
        worker.join()
        print("Configured pipeline worker stopped.")


if __name__ == "__main__":
    main()
