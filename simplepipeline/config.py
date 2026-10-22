from __future__ import annotations

import importlib
import logging
import multiprocessing
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Iterable, cast

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib

from .engine import Pipeline
from .models import (
    Product,
    ScheduledRule,
    normalize_timestamp,
    semantic_version_key,
)
from .rules import (
    CompositeRule,
    EvenlyDistributedWindowRule,
    LatestBeforeRule,
    RandomDistributedWindowRule,
    SameTimestampRule,
)


LOGGER = logging.getLogger(__name__)
Processor = Callable[..., object]
ProductDiscovery = Callable[[list[Product], datetime], Iterable[Product]]


@dataclass(frozen=True)
class _ConfiguredFunction:
    rule: ScheduledRule
    processor: Processor
    cadence_type: str
    trigger_product: str | None = None
    interval: timedelta | None = None
    start_at: datetime | None = None
    sensor_directory: Path | None = None
    sensor_extension: str | None = None
    product_factory: Callable[[Path], Product] | None = None
    product_discovery: ProductDiscovery | None = None


def _parse_timestamp(value: str, setting: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"{setting} must be an ISO-8601 timestamp with a timezone")
    try:
        return normalize_timestamp(datetime.fromisoformat(value.replace("Z", "+00:00")))
    except (TypeError, ValueError) as error:
        raise ValueError(f"{setting} must be an ISO-8601 timestamp with a timezone") from error


def _load_processor(reference: str, config_directory: Path) -> Processor:
    try:
        module_name, function_name = reference.split(":", maxsplit=1)
    except ValueError as error:
        raise ValueError(
            f"processor must use the 'module:function' format: {reference!r}"
        ) from error
    if not module_name or not function_name:
        raise ValueError(f"invalid processor reference: {reference!r}")
    directory = str(config_directory)
    if directory not in sys.path:
        sys.path.insert(0, directory)
    module = importlib.import_module(module_name)
    processor = getattr(module, function_name, None)
    if not callable(processor):
        raise ValueError(f"processor {reference!r} does not identify a callable")
    return processor


def _load_product_discovery(
    reference: str, config_directory: Path
) -> ProductDiscovery:
    try:
        module_name, function_name = reference.split(":", maxsplit=1)
    except ValueError as error:
        raise ValueError(
            "discover_products must use the 'module:function' format: "
            f"{reference!r}"
        ) from error
    if not module_name or not function_name:
        raise ValueError(f"invalid product discovery reference: {reference!r}")
    directory = str(config_directory)
    if directory not in sys.path:
        sys.path.insert(0, directory)
    module = importlib.import_module(module_name)
    discovery = getattr(module, function_name, None)
    if not callable(discovery):
        raise ValueError(
            f"product discovery {reference!r} does not identify a callable"
        )
    return cast(ProductDiscovery, discovery)


def _load_product_factory(
    reference: str, config_directory: Path
) -> Callable[[Path], Product]:
    try:
        module_name, function_name = reference.split(":", maxsplit=1)
    except ValueError as error:
        raise ValueError(
            "product_factory must use the 'module:function' format: "
            f"{reference!r}"
        ) from error
    if not module_name or not function_name:
        raise ValueError(f"invalid product factory reference: {reference!r}")
    directory = str(config_directory)
    if directory not in sys.path:
        sys.path.insert(0, directory)
    module = importlib.import_module(module_name)
    factory = getattr(module, function_name, None)
    if not callable(factory):
        raise ValueError(
            f"product factory {reference!r} does not identify a callable"
        )

    def create_product(path: Path) -> Product:
        product = factory(path)
        if not isinstance(product, Product):
            raise TypeError(
                f"product factory {reference!r} must return a Product"
            )
        return product

    return create_product


def _make_single_rule(
    name: str,
    rule_settings: dict,
    rule_type: str,
    output_code: str,
) -> ScheduledRule:
    input_codes = rule_settings.get("inputs")
    if (
        not isinstance(input_codes, list)
        or not input_codes
        or not all(isinstance(code, str) for code in input_codes)
    ):
        raise ValueError(f"function {name!r} rule must declare a non-empty inputs array")
    if rule_type == "same_timestamp":
        return SameTimestampRule(name, input_codes, output_code)
    if rule_type == "latest_before":
        if len(input_codes) != 1:
            raise ValueError(
                f"function {name!r}: latest_before requires exactly one input"
            )
        return LatestBeforeRule(name, input_codes[0], output_code)
    if rule_type in ("evenly_distributed_window", "random_distributed_window"):
        if len(input_codes) != 1:
            raise ValueError(
                f"function {name!r}: {rule_type} requires exactly one input"
            )
        try:
            window = timedelta(seconds=float(rule_settings["window_seconds"]))
            count = int(rule_settings["count"])
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError(
                f"function {name!r} requires window_seconds and count"
            ) from error
        rule_class = (
            EvenlyDistributedWindowRule
            if rule_type == "evenly_distributed_window"
            else RandomDistributedWindowRule
        )
        return rule_class(name, input_codes[0], output_code, window=window, count=count)
    raise ValueError(
        f"function {name!r} has unsupported rule {rule_type!r}; "
        "use 'same_timestamp', 'latest_before', 'evenly_distributed_window', "
        "or 'random_distributed_window'"
    )


def _make_rule(
    name: str, settings: dict, *, dynamic_output: bool = False
) -> ScheduledRule:
    output_code = settings.get("output")
    if output_code is None and dynamic_output:
        output_code = "dynamic"
    if not isinstance(output_code, str):
        raise ValueError(f"function {name!r} must declare an output product code")
    if "rules" not in settings:
        return _make_single_rule(name, settings, settings.get("rule"), output_code)

    rule_settings = settings["rules"]
    if not isinstance(rule_settings, list) or not rule_settings:
        raise ValueError(f"function {name!r} must declare a non-empty rules array")
    rules = []
    for index, item in enumerate(rule_settings):
        if not isinstance(item, dict):
            raise ValueError(f"function {name!r} rule {index + 1} must be a table")
        rule_type = item.get("type", "rule")
        rules.append(
            _make_single_rule(
                f"{name}[{index + 1}:{rule_type}]",
                item,
                rule_type,
                output_code,
            )
        )
    return CompositeRule(name, rules)


class ConfigWorker:
    """Run configured pipeline functions from a TOML file."""

    def __init__(self, config_path: str | Path) -> None:
        self.config_path = Path(config_path).expanduser().resolve()
        with self.config_path.open("rb") as config_file:
            settings = tomllib.load(config_file)

        worker_settings = settings.get("worker", {})
        if not isinstance(worker_settings, dict):
            raise ValueError("[worker] must be a TOML table")
        self.output_version = worker_settings.get("output_version", "0.0.0")
        if not isinstance(self.output_version, str):
            raise ValueError("worker.output_version must be a string")
        semantic_version_key(self.output_version)
        database_setting = worker_settings.get("database", "pipeline.sqlite3")
        if not isinstance(database_setting, str):
            raise ValueError("worker.database must be a path string")
        database_path = Path(database_setting).expanduser()
        if not database_path.is_absolute():
            database_path = self.config_path.parent / database_path
        self.database = str(database_path.resolve())
        self.poll_interval = float(worker_settings.get("poll_interval_seconds", 60))
        self.claim_timeout = timedelta(
            seconds=float(worker_settings.get("claim_timeout_seconds", 86400))
        )
        self.max_attempts = worker_settings.get("max_attempts", 3)
        self.max_workers = worker_settings.get("max_workers", 1)
        if (
            isinstance(self.max_attempts, bool)
            or not isinstance(self.max_attempts, int)
            or self.max_attempts <= 0
        ):
            raise ValueError("worker.max_attempts must be a positive integer")
        if (
            isinstance(self.max_workers, bool)
            or not isinstance(self.max_workers, int)
            or self.max_workers <= 0
        ):
            raise ValueError("worker.max_workers must be a positive integer")
        if self.poll_interval <= 0:
            raise ValueError("worker.poll_interval_seconds must be positive")
        if self.claim_timeout <= timedelta(0):
            raise ValueError("worker.claim_timeout_seconds must be positive")

        functions = settings.get("functions")
        if not isinstance(functions, dict) or not functions:
            raise ValueError("configuration must define at least one [functions.<name>] table")
        self.functions: list[_ConfiguredFunction] = []
        self._sensor_observations: dict[tuple[str, str], tuple[int, int]] = {}
        self._sensor_registered_paths: set[tuple[str, str]] = set()
        for name, function_settings in functions.items():
            if not isinstance(function_settings, dict):
                raise ValueError(f"function {name!r} must be a TOML table")
            discovery_reference = function_settings.get("discover_products")
            if discovery_reference is not None and not isinstance(
                discovery_reference, str
            ):
                raise ValueError(
                    f"function {name!r} discover_products must be a "
                    "'module:function' reference"
                )
            product_discovery = (
                _load_product_discovery(
                    discovery_reference, self.config_path.parent
                )
                if discovery_reference is not None
                else None
            )
            rule = _make_rule(
                name,
                function_settings,
                dynamic_output=product_discovery is not None,
            )
            processor_reference = function_settings.get("processor")
            if not isinstance(processor_reference, str):
                raise ValueError(f"function {name!r} must declare processor = 'module:function'")
            processor = _load_processor(processor_reference, self.config_path.parent)
            if "output_version" in function_settings:
                raise ValueError(
                    f"function {name!r} output_version is no longer supported; "
                    "set [worker].output_version instead"
                )
            cadence = function_settings.get("cadence")
            if not isinstance(cadence, dict):
                raise ValueError(f"function {name!r} must declare a [functions.{name}.cadence] table")
            cadence_type = cadence.get("type")
            if cadence_type == "on_input":
                trigger_product = cadence.get("product", rule.input_product_codes[0])
                if trigger_product not in rule.input_product_codes:
                    raise ValueError(
                        f"function {name!r} trigger product must be listed in inputs"
                    )
                self.functions.append(
                    _ConfiguredFunction(
                        rule,
                        processor,
                        cadence_type,
                        trigger_product=trigger_product,
                        product_discovery=product_discovery,
                    )
                )
            elif cadence_type == "interval":
                try:
                    interval = timedelta(seconds=float(cadence["every_seconds"]))
                    start_at = _parse_timestamp(cadence["start_at"], "cadence.start_at")
                except (KeyError, TypeError, ValueError) as error:
                    raise ValueError(
                        f"function {name!r} interval cadence requires positive "
                        "every_seconds and timezone-aware start_at"
                    ) from error
                if interval <= timedelta(0):
                    raise ValueError(
                        f"function {name!r} cadence.every_seconds must be positive"
                    )
                self.functions.append(
                    _ConfiguredFunction(
                        rule,
                        processor,
                        cadence_type,
                        interval=interval,
                        start_at=start_at,
                        product_discovery=product_discovery,
                    )
                )
            elif cadence_type == "sensor":
                directory_setting = cadence.get("directory")
                factory_reference = cadence.get("product_factory")
                extension = cadence.get("extension")
                if not isinstance(directory_setting, str) or not directory_setting:
                    raise ValueError(
                        f"function {name!r} sensor cadence requires a directory"
                    )
                if not isinstance(factory_reference, str):
                    raise ValueError(
                        f"function {name!r} sensor cadence requires "
                        "product_factory = 'module:function'"
                    )
                if extension is not None and (
                    not isinstance(extension, str)
                    or len(extension) < 2
                    or not extension.startswith(".")
                    or extension.strip() != extension
                    or "/" in extension
                    or "\\" in extension
                ):
                    raise ValueError(
                        f"function {name!r} sensor extension must be a filename "
                        "extension beginning with '.'"
                    )
                directory = Path(directory_setting).expanduser()
                if not directory.is_absolute():
                    directory = self.config_path.parent / directory
                trigger_product = cadence.get("product", rule.input_product_codes[0])
                if trigger_product not in rule.input_product_codes:
                    raise ValueError(
                        f"function {name!r} sensor trigger product must be listed in inputs"
                    )
                self.functions.append(
                    _ConfiguredFunction(
                        rule,
                        processor,
                        cadence_type,
                        trigger_product=trigger_product,
                        sensor_directory=directory.resolve(),
                        sensor_extension=extension,
                        product_factory=_load_product_factory(
                            factory_reference, self.config_path.parent
                        ),
                        product_discovery=product_discovery,
                    )
                )
            else:
                raise ValueError(
                    f"function {name!r} cadence.type must be 'interval', "
                    "'on_input', or 'sensor'"
                )

        self.pipeline = Pipeline(self.database)
        self._processes: list[multiprocessing.Process] = []
        self._stop_event = None
        LOGGER.info(
            "Loaded %d pipeline function(s) from %s using database %s",
            len(self.functions),
            self.config_path,
            self.database,
        )

    def run_once(self, now: datetime | None = None) -> dict[tuple[str, datetime], str]:
        """Run due candidates and retry unfinished candidates."""
        current = normalize_timestamp(now or datetime.now(timezone.utc))
        results: dict[tuple[str, datetime], str] = {}
        for function in self.functions:
            rule = function.rule
            candidate_times = set(
                self.pipeline.incomplete_candidates(
                    rule.name, max_attempts=self.max_attempts
                )
            )
            if function.cadence_type == "sensor":
                assert function.sensor_directory is not None
                assert function.product_factory is not None
                if not function.sensor_directory.is_dir():
                    raise NotADirectoryError(
                        f"sensor directory does not exist: {function.sensor_directory}"
                    )
                registered_paths = {
                    Path(product.path).expanduser().resolve()
                    for product in self.pipeline.products()
                }
                for path in sorted(function.sensor_directory.iterdir()):
                    if not path.is_file():
                        continue
                    if (
                        function.sensor_extension is not None
                        and not path.name.lower().endswith(
                            function.sensor_extension.lower()
                        )
                    ):
                        continue
                    key = (rule.name, str(path))
                    if key in self._sensor_registered_paths:
                        continue
                    if path.resolve() in registered_paths:
                        self._sensor_registered_paths.add(key)
                        continue
                    stat = path.stat()
                    signature = (stat.st_size, stat.st_mtime_ns)
                    if self._sensor_observations.get(key) != signature:
                        self._sensor_observations[key] = signature
                        LOGGER.debug(
                            "Sensor observed file; waiting for it to stabilize: %s",
                            path,
                        )
                        continue
                    product = function.product_factory(path)
                    registered = self.pipeline.register_sensor_product(path, product)
                    self._sensor_registered_paths.add(key)
                    if registered is None:
                        LOGGER.debug(
                            "Sensor file was registered by another worker: %s", path
                        )
                        continue
                    LOGGER.info(
                        "Registered sensor file %s as %s at %s",
                        path,
                        registered.product_code,
                        registered.timestamp.isoformat(),
                    )

            if function.cadence_type in ("on_input", "sensor"):
                assert function.trigger_product is not None
                candidate_times.update(
                    self.pipeline.product_timestamps(
                        function.trigger_product,
                        excluding_rule=rule.name,
                        max_attempts=self.max_attempts,
                    )
                )
            else:
                assert function.interval is not None
                assert function.start_at is not None
                saved_cursor = self.pipeline.scheduler_cursor(rule.name)
                cursor = saved_cursor or function.start_at
                while cursor <= current:
                    candidate_times.add(cursor)
                    cursor += function.interval
                if cursor != saved_cursor:
                    self.pipeline.set_scheduler_cursor(rule.name, cursor)

            for candidate_time in sorted(candidate_times):
                try:
                    LOGGER.debug(
                        "Processing candidate %s at %s",
                        rule.name,
                        candidate_time.isoformat(),
                    )
                    status = self.pipeline.run_candidate(
                        rule,
                        candidate_time,
                        lambda inputs, output_time: function.processor(
                            *(product.path for product in inputs),
                            output_time,
                        ),
                        output_version=self.output_version,
                        product_discovery=function.product_discovery,
                        claim_timeout=self.claim_timeout,
                        max_attempts=self.max_attempts,
                    )
                except Exception:
                    attempts = self.pipeline.candidate_attempts(
                        rule.name, candidate_time
                    )
                    retry_message = (
                        "maximum attempts reached"
                        if attempts >= self.max_attempts
                        else "it will be retried"
                    )
                    LOGGER.exception(
                        "Candidate %s at %s failed on attempt %s/%s; %s",
                        rule.name,
                        candidate_time.isoformat(),
                        attempts,
                        self.max_attempts,
                        retry_message,
                    )
                    status = "failed"
                if status == "complete":
                    LOGGER.info(
                        "Completed candidate %s at %s",
                        rule.name,
                        candidate_time.isoformat(),
                    )
                elif status == "pending":
                    LOGGER.debug(
                        "Candidate %s at %s is waiting for inputs",
                        rule.name,
                        candidate_time.isoformat(),
                    )
                results[(rule.name, candidate_time)] = status
        return results

    def run_forever(self, stop_event=None) -> None:
        """Poll the configuration's schedules until stopped."""
        LOGGER.info("Worker polling every %s seconds", self.poll_interval)
        while stop_event is None or not stop_event.is_set():
            self.run_once()
            if stop_event is None:
                time.sleep(self.poll_interval)
            elif stop_event.wait(self.poll_interval):
                return

    def start(self) -> None:
        """Start the configured worker processes."""
        if any(process.is_alive() for process in self._processes):
            raise RuntimeError("worker is already running")
        context = multiprocessing.get_context("spawn")
        self._stop_event = context.Event()
        self._processes = []
        try:
            for worker_number in range(1, self.max_workers + 1):
                process = context.Process(
                    target=_run_config_process,
                    args=(str(self.config_path), self._stop_event),
                    name=f"simplepipeline-config-worker-{worker_number}",
                )
                process.start()
                self._processes.append(process)
                LOGGER.info(
                    "Started worker process %s/%s (pid %s)",
                    worker_number,
                    self.max_workers,
                    process.pid,
                )
        except Exception:
            self._stop_event.set()
            for process in self._processes:
                process.join()
            raise

    def stop(self, timeout: float | None = None) -> bool:
        """Request a graceful stop and return whether all processes stopped."""
        if not self._processes or self._stop_event is None:
            return True
        self._stop_event.set()
        deadline = None if timeout is None else time.monotonic() + timeout
        for process in self._processes:
            remaining = (
                None if deadline is None else max(0, deadline - time.monotonic())
            )
            process.join(remaining)
            if not process.is_alive():
                LOGGER.info("Worker process stopped (exit code %s)", process.exitcode)
        return not any(process.is_alive() for process in self._processes)

    def join(self, timeout: float | None = None) -> int | None:
        """Wait for all worker processes and return their combined exit status."""
        if not self._processes:
            raise RuntimeError("worker has not been started")
        deadline = None if timeout is None else time.monotonic() + timeout
        for process in self._processes:
            remaining = (
                None if deadline is None else max(0, deadline - time.monotonic())
            )
            process.join(remaining)
        if any(process.is_alive() for process in self._processes):
            return None
        return next(
            (process.exitcode for process in self._processes if process.exitcode),
            0,
        )


def _run_config_process(config_path: str, stop_event) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        ConfigWorker(config_path).run_forever(stop_event)
    except KeyboardInterrupt:
        return
