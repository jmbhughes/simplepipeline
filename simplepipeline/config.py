from __future__ import annotations

import importlib
import logging
import multiprocessing
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable

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
    RandomDistributedWindowRule,
    SameTimestampRule,
)


LOGGER = logging.getLogger(__name__)
Processor = Callable[[list[Product], datetime], str]


@dataclass(frozen=True)
class _ConfiguredFunction:
    rule: ScheduledRule
    processor: Processor
    cadence_type: str
    output_version: str = "0.0.0"
    trigger_product: str | None = None
    interval: timedelta | None = None
    start_at: datetime | None = None
    sensor_directory: Path | None = None
    sensor_extension: str | None = None
    product_factory: Callable[[Path], Product] | None = None


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
        "use 'same_timestamp', 'evenly_distributed_window', "
        "or 'random_distributed_window'"
    )


def _make_rule(name: str, settings: dict) -> ScheduledRule:
    output_code = settings.get("output")
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
        rules.append(
            _make_single_rule(name, item, item.get("type"), output_code)
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
        if (
            isinstance(self.max_attempts, bool)
            or not isinstance(self.max_attempts, int)
            or self.max_attempts <= 0
        ):
            raise ValueError("worker.max_attempts must be a positive integer")
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
            rule = _make_rule(name, function_settings)
            processor_reference = function_settings.get("processor")
            if not isinstance(processor_reference, str):
                raise ValueError(f"function {name!r} must declare processor = 'module:function'")
            processor = _load_processor(processor_reference, self.config_path.parent)
            output_version = function_settings.get("output_version", "0.0.0")
            if not isinstance(output_version, str):
                raise ValueError(f"function {name!r} output_version must be a string")
            semantic_version_key(output_version)
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
                        output_version=output_version,
                        trigger_product=trigger_product,
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
                        output_version=output_version,
                        interval=interval,
                        start_at=start_at,
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
                        output_version=output_version,
                        trigger_product=trigger_product,
                        sensor_directory=directory.resolve(),
                        sensor_extension=extension,
                        product_factory=_load_product_factory(
                            factory_reference, self.config_path.parent
                        ),
                    )
                )
            else:
                raise ValueError(
                    f"function {name!r} cadence.type must be 'interval', "
                    "'on_input', or 'sensor'"
                )

        self.pipeline = Pipeline(self.database)
        self._process: multiprocessing.Process | None = None
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
                    registered = self.pipeline.add_product(product)
                    self._sensor_registered_paths.add(key)
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
                        function.processor,
                        output_version=function.output_version,
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
        """Start the configured worker in a separate process."""
        if self._process is not None and self._process.is_alive():
            raise RuntimeError("worker is already running")
        context = multiprocessing.get_context("spawn")
        self._stop_event = context.Event()
        self._process = context.Process(
            target=_run_config_process,
            args=(str(self.config_path), self._stop_event),
            name="simplepipeline-config-worker",
        )
        self._process.start()
        LOGGER.info("Started worker process (pid %s)", self._process.pid)

    def stop(self, timeout: float | None = None) -> bool:
        """Request a graceful stop and return whether the process stopped."""
        if self._process is None or self._stop_event is None:
            return True
        self._stop_event.set()
        self._process.join(timeout)
        if not self._process.is_alive():
            LOGGER.info("Worker process stopped (exit code %s)", self._process.exitcode)
        return not self._process.is_alive()

    def join(self, timeout: float | None = None) -> int | None:
        """Wait for the worker and return its exit code when available."""
        if self._process is None:
            raise RuntimeError("worker has not been started")
        self._process.join(timeout)
        return self._process.exitcode


def _run_config_process(config_path: str, stop_event) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        ConfigWorker(config_path).run_forever(stop_event)
    except KeyboardInterrupt:
        return
