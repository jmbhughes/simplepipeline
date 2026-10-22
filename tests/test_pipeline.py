from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import json
import runpy
import sqlite3
import sys
from threading import Barrier, Event, Thread
import time
import types
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import urlopen

import pytest

import simplepipeline.cli as cli_module
import simplepipeline.config as config_module
import simplepipeline.rules as rules_module
from simplepipeline import (
    ConfigWorker,
    DailyWorker,
    EvenlyDistributedWindowRule,
    LatestBeforeRule,
    Pipeline,
    Product,
    RandomDistributedWindowRule,
)
from simplepipeline.config import _run_config_process
from simplepipeline.dashboard import DashboardServer, read_dashboard_status
from simplepipeline.engine import daily_utc_candidates
from simplepipeline.models import (
    Rule,
    latest_version_products,
    normalize_timestamp,
    parse_timestamp,
    semantic_version_key,
    timestamp_text,
    validate_product_code,
)
from simplepipeline.rules import SameTimestampRule


UTC = timezone.utc
END = datetime(2026, 10, 16, tzinfo=UTC)


def _worker_processor(inputs, output_time):
    return "/products/output.fits"


def _configured_worker_processor(*args):
    return "/products/output.fits"


def _write_config(
    path,
    *,
    cadence,
    processor="test_processors:process",
    output_version="0.0.0",
    max_workers=None,
):
    max_workers_setting = (
        "" if max_workers is None else f"max_workers = {max_workers}\n"
    )
    path.write_text(
        f'''[worker]
database = "worker.sqlite3"
poll_interval_seconds = 1
        output_version = "{output_version}"
        {max_workers_setting}

[functions.combine]
inputs = ["L1", "L1B"]
output = "L2"
rule = "same_timestamp"
processor = "{processor}"

[functions.combine.cadence]
{cadence}
'''
    )


def test_product_codes_and_timestamps_are_validated():
    with pytest.raises(ValueError):
        Product("A" * 17, END, "/tmp/input")
    with pytest.raises(ValueError):
        Product("L0", datetime(2026, 10, 16), "/tmp/input")
    with pytest.raises(ValueError):
        Product("L0", END, "/tmp/input", version="01.0.0")


def test_semantic_versions_follow_semver_precedence():
    versions = [
        "1.0.0-alpha",
        "1.0.0-alpha.1",
        "1.0.0-alpha.beta",
        "1.0.0-beta",
        "1.0.0",
        "1.0.1",
        "1.1.0",
        "2.0.0",
    ]
    assert sorted(versions, key=semantic_version_key) == versions


def test_weekly_rule_waits_until_exact_count_and_persists_selected_inputs(tmp_path):
    pipeline = Pipeline(tmp_path / "pipeline.sqlite3")
    rule = EvenlyDistributedWindowRule(
        "weekly", "L1", "L2", window=timedelta(days=7), count=100
    )
    called = []

    def processor(inputs, output_time):
        called.append((inputs, output_time))
        return "/products/output.fits"

    for i in range(99):
        pipeline.add_product(Product("L1", END - timedelta(days=7) + timedelta(minutes=i * 10), f"/inputs/{i}"))
    assert pipeline.run_candidate(rule, END, processor) == "pending"
    assert called == []
    assert pipeline.candidate_status("weekly", END) == "pending"

    # One extra eligible file allows processing. The most evenly distributed
    # choice is fixed in provenance after successful completion.
    pipeline.add_product(Product("L1", END - timedelta(minutes=1), "/inputs/extra"))
    assert pipeline.run_candidate(rule, END, processor) == "complete"
    assert len(called) == 1
    selected_before = pipeline.candidate_inputs("weekly", END)
    assert len(selected_before) == 100

    pipeline.add_product(Product("L1", END - timedelta(minutes=2), "/inputs/late"))
    assert pipeline.run_candidate(rule, END, processor) == "complete"
    assert len(called) == 1
    assert pipeline.candidate_inputs("weekly", END) == selected_before


def test_week_window_is_start_inclusive_end_exclusive(tmp_path):
    pipeline = Pipeline(tmp_path / "pipeline.sqlite3")
    rule = EvenlyDistributedWindowRule(
        "weekly", "L1", "L2", window=timedelta(days=7), count=2
    )
    pipeline.add_product(Product("L1", END - timedelta(days=7), "/inputs/start"))
    pipeline.add_product(Product("L1", END, "/inputs/end"))
    assert pipeline.run_candidate(rule, END, lambda inputs, _: "/output") == "pending"


def test_random_distributed_window_samples_eligible_latest_version_products(
    monkeypatch,
):
    rule = RandomDistributedWindowRule(
        "random-window", "L1", "L2", window=timedelta(days=7), count=2
    )
    candidates = [
        Product("L1", END - timedelta(days=7), "/inputs/start", version="2.0.0"),
        Product("L1", END - timedelta(days=3), "/inputs/middle-old", version="1.0.0"),
        Product("L1", END - timedelta(days=3), "/inputs/middle-new", version="2.0.0"),
        Product("L1", END, "/inputs/end"),
        Product("L1B", END - timedelta(days=1), "/inputs/other-code"),
    ]
    sampled = []

    def sample(population, count):
        sampled.append((population, count))
        return [population[-1], population[0]]

    monkeypatch.setattr(rules_module.random, "sample", sample)
    selected = rule.select_inputs(candidates, END)

    assert sampled[0][1] == 2
    assert [product.path for product in sampled[0][0]] == [
        "/inputs/start",
        "/inputs/middle-new",
    ]
    assert [product.path for product in selected] == [
        "/inputs/start",
        "/inputs/middle-new",
    ]
    assert rule.select_inputs(candidates[:1], END) == []


def test_pipeline_stops_retrying_after_max_attempts(tmp_path):
    pipeline = Pipeline(tmp_path / "pipeline.sqlite3")
    rule = SameTimestampRule("retry-limit", "L1", "L2")
    pipeline.add_product(Product("L1", END, "/inputs/one"))
    calls = []

    def failing_processor(inputs, output_time):
        calls.append(output_time)
        raise RuntimeError("processing failed")

    for attempt in range(3):
        with pytest.raises(RuntimeError, match="processing failed"):
            pipeline.run_candidate(rule, END, failing_processor, max_attempts=3)
        assert pipeline.candidate_attempts("retry-limit", END) == attempt + 1

    assert pipeline.run_candidate(rule, END, failing_processor, max_attempts=3) == "failed"
    assert len(calls) == 3
    assert pipeline.incomplete_candidates("retry-limit", max_attempts=3) == []
    assert pipeline.candidate_status("retry-limit", END) == "failed"
    with sqlite3.connect(pipeline.database) as connection:
        attempts, error = connection.execute(
            "SELECT attempts, error FROM candidates WHERE rule_name = ?",
            ("retry-limit",),
        ).fetchone()
    assert attempts == 3
    assert error == "processing failed"


def test_candidate_selection_uses_only_most_recent_input_version(tmp_path):
    pipeline = Pipeline(tmp_path / "pipeline.sqlite3")
    rule = EvenlyDistributedWindowRule(
        "weekly", "L1", "L2", window=timedelta(days=7), count=1
    )
    pipeline.add_product(
        Product("L1", END - timedelta(minutes=1), "/inputs/old", version="1.9.0")
    )
    pipeline.add_product(
        Product("L1", END - timedelta(days=8), "/inputs/new-outside-window", version="2.0.0")
    )
    assert pipeline.run_candidate(rule, END, lambda inputs, _: "/output") == "pending"

    selected = []
    pipeline.add_product(
        Product("L1", END - timedelta(minutes=2), "/inputs/new", version="2.0.0")
    )
    assert pipeline.run_candidate(
        rule,
        END,
        lambda inputs, _: selected.extend(inputs) or "/output",
        output_version="3.1.0",
    ) == "complete"
    assert [product.version for product in selected] == ["2.0.0"]
    assert pipeline.products("L2")[0].version == "3.1.0"


def test_same_timestamp_rule_uses_latest_version_for_each_input_code():
    rule = SameTimestampRule("combine", ["L1", "L1B"], "L2")
    candidates = [
        Product("L1", END, "/inputs/l1-old", version="1.0.0"),
        Product("L1", END - timedelta(seconds=1), "/inputs/l1-new", version="2.0.0"),
        Product("L1B", END, "/inputs/l1b", version="1.0.0"),
    ]

    assert rule.select_inputs(candidates, END) == []
    candidates.append(Product("L1", END, "/inputs/l1-new-at-end", version="2.0.0"))
    assert [item.path for item in rule.select_inputs(candidates, END)] == [
        "/inputs/l1-new-at-end",
        "/inputs/l1b",
    ]


def test_latest_before_rule_selects_highest_version_before_output_time():
    rule = LatestBeforeRule("latest-reference", "REF", "L2")
    highest_version = Product(
        "REF", END - timedelta(days=1), "/inputs/newer-v9", version="9.0.0"
    )
    candidates = [
        Product("REF", END - timedelta(days=2), "/inputs/old-v9", version="9.0.0"),
        Product(
            "REF",
            END - timedelta(hours=1),
            "/inputs/newer-v8",
            version="8.0.0",
        ),
        highest_version,
        Product("REF", END, "/inputs/exact"),
        Product("REF", END + timedelta(hours=1), "/inputs/future"),
        Product("OTHER", END - timedelta(minutes=1), "/inputs/other"),
    ]

    assert rule.select_inputs(candidates, END) == [highest_version]
    assert rule.select_inputs([Product("REF", END, "/inputs/exact")], END) == []


def test_existing_database_migrates_versions_without_losing_provenance(tmp_path):
    database = tmp_path / "legacy.sqlite3"
    with sqlite3.connect(database) as db:
        db.executescript(
            """
            PRAGMA foreign_keys = ON;
            CREATE TABLE products (
                id INTEGER PRIMARY KEY,
                product_code TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                path TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(product_code, timestamp)
            );
            CREATE TABLE candidates (
                id INTEGER PRIMARY KEY,
                rule_name TEXT NOT NULL,
                output_product_code TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                status TEXT NOT NULL,
                output_product_id INTEGER REFERENCES products(id),
                error TEXT,
                UNIQUE(rule_name, timestamp)
            );
            CREATE TABLE candidate_inputs (
                candidate_id INTEGER NOT NULL REFERENCES candidates(id),
                product_id INTEGER NOT NULL REFERENCES products(id),
                ordinal INTEGER NOT NULL,
                PRIMARY KEY(candidate_id, ordinal),
                UNIQUE(candidate_id, product_id)
            );
            INSERT INTO products VALUES (
                1, 'L1', '2026-10-16T00:00:00.000000Z', '/inputs/legacy',
                '2026-10-16T00:00:00.000000Z'
            );
            INSERT INTO candidates VALUES (
                1, 'legacy', 'L1', '2026-10-16T00:00:00.000000Z',
                'complete', 1, NULL
            );
            INSERT INTO candidate_inputs VALUES (1, 1, 0);
            """
        )

    pipeline = Pipeline(database)
    assert pipeline.candidate_inputs("legacy", END)[0].version == "0.0.0"
    assert pipeline.candidate_outputs("legacy", END)[0].path == "/inputs/legacy"
    pipeline.add_product(
        Product("L1", END, "/inputs/v2", version="2.0.0")
    )
    with sqlite3.connect(database) as db:
        assert db.execute("PRAGMA foreign_key_check").fetchall() == []
    assert len(pipeline.products("L1")) == 2


def test_daily_schedule_yields_midnight_utc():
    times = list(daily_utc_candidates(END.date(), END.date() + timedelta(days=2)))
    assert times == [END, END + timedelta(days=1)]


def test_daily_worker_retries_pending_candidates_until_inputs_are_available(tmp_path):
    pipeline = Pipeline(tmp_path / "pipeline.sqlite3")
    rule = EvenlyDistributedWindowRule(
        "daily", "L1", "L2", window=timedelta(days=7), count=1
    )
    worker = DailyWorker(
        tmp_path / "pipeline.sqlite3",
        [rule],
        lambda inputs, output_time: "/products/output.fits",
        start_date=END.date(),
    )

    assert worker.run_once(END) == {("daily", END): "pending"}
    pipeline.add_product(Product("L1", END - timedelta(minutes=1), "/inputs/input"))
    assert worker.run_once(END) == {("daily", END): "complete"}
    assert worker.run_once(END) == {}
    assert pipeline.candidate_status("daily", END) == "complete"


def test_daily_worker_validates_options_and_unique_rule_names(tmp_path):
    rule = SameTimestampRule("one", "L1", "L2")
    for kwargs, message in [
        ({"rules": []}, "at least one rule"),
        ({"poll_interval": 0}, "poll_interval"),
        ({"claim_timeout": timedelta(0)}, "claim_timeout"),
        ({"max_attempts": 0}, "max_attempts"),
        ({"output_version": "bad"}, "semantic version"),
        (
            {"rules": [rule, SameTimestampRule("one", "L1B", "L2")]},
            "unique",
        ),
    ]:
        rules = kwargs.pop("rules", [rule])
        with pytest.raises(ValueError, match=message):
            DailyWorker(
                tmp_path / "invalid.sqlite3",
                rules,
                _worker_processor,
                start_date=END.date(),
                **kwargs,
            )


def test_processing_claim_does_not_block_ingestion_or_duplicate_work(tmp_path):
    pipeline = Pipeline(tmp_path / "pipeline.sqlite3")
    rule = EvenlyDistributedWindowRule(
        "daily", "L1", "L2", window=timedelta(days=7), count=1
    )
    pipeline.add_product(Product("L1", END - timedelta(minutes=1), "/inputs/input"))
    processor_started = Event()
    finish_processor = Event()
    calls = []

    def processor(inputs, output_time):
        calls.append(output_time)
        processor_started.set()
        assert finish_processor.wait(timeout=5)
        return "/products/output.fits"

    result = []
    thread = Thread(
        target=lambda: result.append(pipeline.run_candidate(rule, END, processor))
    )
    thread.start()
    assert processor_started.wait(timeout=5)

    assert pipeline.candidate_status("daily", END) == "processing"
    pipeline.add_product(Product("L1", END - timedelta(minutes=2), "/inputs/late"))
    assert pipeline.run_candidate(rule, END, processor) == "processing"
    assert calls == [END]

    finish_processor.set()
    thread.join(timeout=5)
    assert not thread.is_alive()
    assert result == ["complete"]


def test_daily_worker_starts_and_stops_in_a_separate_process(tmp_path):
    database = tmp_path / "pipeline.sqlite3"
    pipeline = Pipeline(database)
    today = datetime.now(UTC).date()
    candidate_time = datetime.combine(today, datetime.min.time(), tzinfo=UTC)
    pipeline.add_product(Product("L1", candidate_time - timedelta(minutes=1), "/inputs/input"))
    rule = EvenlyDistributedWindowRule(
        "daily", "L1", "L2", window=timedelta(days=7), count=1
    )
    worker = DailyWorker(
        database,
        [rule],
        _worker_processor,
        start_date=today,
        poll_interval=0.01,
        output_version="1.4.2",
    )

    worker.start()
    deadline = time.monotonic() + 5
    while (
        pipeline.candidate_status("daily", candidate_time) != "complete"
        and time.monotonic() < deadline
    ):
        time.sleep(0.01)
    assert pipeline.candidate_status("daily", candidate_time) == "complete"
    assert pipeline.products("L2")[0].version == "1.4.2"
    assert worker.stop(timeout=5)
    assert worker.join(timeout=0) == 0


def test_config_worker_triggers_when_registered_inputs_share_timestamp(
    tmp_path, monkeypatch, caplog
):
    caplog.set_level("INFO")
    calls = []
    processor_module = types.ModuleType("test_processors")
    processor_module.process = lambda *args: (
        calls.append(args) or "/products/output.fits"
    )
    monkeypatch.setitem(sys.modules, "test_processors", processor_module)
    config_path = tmp_path / "pipeline.toml"
    _write_config(config_path, cadence='type = "on_input"\nproduct = "L1"')
    worker = ConfigWorker(config_path)
    pipeline = Pipeline(tmp_path / "worker.sqlite3")

    assert worker.run_once(END) == {}
    pipeline.add_product(Product("L1", END, "/inputs/one"))
    assert worker.run_once(END) == {("combine", END): "pending"}
    pipeline.add_product(Product("L1", END, "/inputs/one-v2", version="2.0.0"))
    assert worker.run_once(END) == {("combine", END): "pending"}
    pipeline.add_product(Product("L1B", END, "/inputs/two"))
    assert worker.run_once(END) == {("combine", END): "complete"}
    assert len(calls) == 1
    assert calls == [
        ("/inputs/one-v2", "/inputs/two", END)
    ]
    assert "Completed candidate combine at" in caplog.text


def test_config_worker_runs_and_persists_fixed_interval_schedule(tmp_path, monkeypatch):
    calls = []
    processor_module = types.ModuleType("test_processors")
    processor_module.process = lambda *args: (
        calls.append(args[-1]) or f"/products/{args[-1]:%S}.fits"
    )
    monkeypatch.setitem(sys.modules, "test_processors", processor_module)
    config_path = tmp_path / "pipeline.toml"
    start_at = END - timedelta(seconds=20)
    _write_config(
        config_path,
        cadence=(
            'type = "interval"\n'
            "every_seconds = 10\n"
            f'start_at = "{start_at.isoformat()}"'
        ),
        output_version="3.2.1",
    )
    worker = ConfigWorker(config_path)
    pipeline = Pipeline(tmp_path / "worker.sqlite3")
    for seconds in (20, 10, 0):
        pipeline.add_product(
            Product(
                "L1",
                END - timedelta(seconds=seconds),
                f"/inputs/{seconds}",
            )
        )
    pipeline.add_product(Product("L1B", start_at, "/inputs/extra"))
    pipeline.add_product(Product("L1B", END - timedelta(seconds=10), "/inputs/extra-10"))
    pipeline.add_product(Product("L1B", END, "/inputs/extra-0"))

    results = worker.run_once(END)
    assert list(results.values()) == ["complete", "complete", "complete"]
    assert calls == [start_at, END - timedelta(seconds=10), END]
    assert all(product.version == "3.2.1" for product in pipeline.products("L2"))
    assert ConfigWorker(config_path).run_once(END) == {}


def test_config_worker_reads_max_attempts_from_toml(tmp_path, monkeypatch):
    calls = []
    processor_module = types.ModuleType("retry_processors")

    def process(*args):
        calls.append(args[-1])
        raise RuntimeError("temporary processor failure")

    processor_module.process = process
    monkeypatch.setitem(sys.modules, "retry_processors", processor_module)
    config_path = tmp_path / "pipeline.toml"
    _write_config(
        config_path,
        cadence='type = "on_input"\nproduct = "L1"',
        processor="retry_processors:process",
    )
    config_path.write_text(
        config_path.read_text().replace(
            'poll_interval_seconds = 1',
            'poll_interval_seconds = 1\nmax_attempts = 2',
        )
    )
    worker = ConfigWorker(config_path)
    pipeline = Pipeline(tmp_path / "worker.sqlite3")
    pipeline.add_product(Product("L1", END, "/inputs/one"))
    pipeline.add_product(Product("L1B", END, "/inputs/two"))

    for _ in range(2):
        assert worker.run_once(END) == {("combine", END): "failed"}
    assert worker.run_once(END) == {}
    assert len(calls) == 2
    assert pipeline.candidate_attempts("combine", END) == 2
    assert ConfigWorker(config_path).run_once(END) == {}
    assert len(calls) == 2


def test_config_worker_combines_multiple_rules(tmp_path, monkeypatch):
    calls = []
    processor_module = types.ModuleType("test_processors")
    processor_module.process = lambda *args: (
        calls.append(args) or "/products/output.fits"
    )
    monkeypatch.setitem(sys.modules, "test_processors", processor_module)
    config_path = tmp_path / "pipeline.toml"
    config_path.write_text(
        '''[worker]
database = "worker.sqlite3"

[functions.combine]
output = "L2"
processor = "test_processors:process"

[[functions.combine.rules]]
type = "same_timestamp"
inputs = ["L1"]

[[functions.combine.rules]]
type = "evenly_distributed_window"
inputs = ["L1B"]
window_seconds = 10
count = 2

[functions.combine.cadence]
type = "on_input"
product = "L1"
'''
    )
    worker = ConfigWorker(config_path)
    pipeline = Pipeline(tmp_path / "worker.sqlite3")
    pipeline.add_product(Product("L1", END, "/inputs/l1"))

    assert worker.run_once(END) == {("combine", END): "pending"}
    pipeline.add_product(Product("L1B", END - timedelta(seconds=9), "/inputs/l1b-early"))
    pipeline.add_product(Product("L1B", END - timedelta(seconds=1), "/inputs/l1b-late"))

    assert worker.run_once(END) == {("combine", END): "complete"}
    assert calls == [(
        "/inputs/l1",
        "/inputs/l1b-early",
        "/inputs/l1b-late",
        END,
    )]
    assert [
        product.path for product in pipeline.candidate_inputs("combine", END)
    ] == [
        "/inputs/l1",
        "/inputs/l1b-early",
        "/inputs/l1b-late",
    ]


def test_config_worker_uses_latest_before_rule(tmp_path, monkeypatch):
    calls = []
    processor_module = types.ModuleType("latest_before_processors")
    processor_module.process = lambda *args: (
        calls.extend(args[:-1]) or "/products/output.fits"
    )
    monkeypatch.setitem(sys.modules, "latest_before_processors", processor_module)
    config_path = tmp_path / "pipeline.toml"
    config_path.write_text(
        f'''[worker]
database = "worker.sqlite3"

[functions.combine]
output = "L2"
processor = "latest_before_processors:process"

[[functions.combine.rules]]
type = "same_timestamp"
inputs = ["RAW"]

[[functions.combine.rules]]
type = "latest_before"
inputs = ["REF"]

[functions.combine.cadence]
type = "interval"
every_seconds = 86400
start_at = "{END.isoformat()}"
'''
    )
    worker = ConfigWorker(config_path)
    pipeline = Pipeline(tmp_path / "worker.sqlite3")
    pipeline.add_product(Product("RAW", END, "/inputs/raw"))
    pipeline.add_product(
        Product("REF", END - timedelta(days=2), "/inputs/ref-old", version="9.0.0")
    )
    pipeline.add_product(
        Product("REF", END - timedelta(days=1), "/inputs/ref-latest", version="1.0.0")
    )
    pipeline.add_product(Product("REF", END, "/inputs/ref-at-output"))

    assert worker.run_once(END) == {("combine", END): "complete"}
    assert calls == [
        "/inputs/raw",
        "/inputs/ref-old",
    ]


def test_config_worker_discovers_products_without_declaring_output(
    tmp_path, monkeypatch
):
    calls = []
    processor_module = types.ModuleType("discovery_processors")

    def process(*args):
        calls.append(("process", *args))

    def discover(inputs, output_time):
        calls.append(("discover", output_time))
        return [
            Product("CAL", output_time, "/outputs/calibration.fits"),
            Product("MASK", output_time, "/outputs/mask.fits"),
        ]

    processor_module.process = process
    processor_module.discover = discover
    monkeypatch.setitem(sys.modules, "discovery_processors", processor_module)
    config_path = tmp_path / "pipeline.toml"
    config_path.write_text(
        '''[worker]
database = "worker.sqlite3"

[functions.discovery]
processor = "discovery_processors:process"
discover_products = "discovery_processors:discover"
rule = "same_timestamp"
inputs = ["RAW"]

[functions.discovery.cadence]
type = "on_input"
product = "RAW"
'''
    )
    worker = ConfigWorker(config_path)
    pipeline = Pipeline(tmp_path / "worker.sqlite3")
    pipeline.add_product(Product("RAW", END, "/inputs/raw"))

    assert worker.run_once(END) == {("discovery", END): "complete"}
    assert calls == [
        ("process", "/inputs/raw", END),
        ("discover", END),
    ]
    assert [product.product_code for product in pipeline.candidate_outputs("discovery", END)] == [
        "CAL",
        "MASK",
    ]


def test_config_worker_supports_random_distributed_window(tmp_path, monkeypatch):
    calls = []
    processor_module = types.ModuleType("random_processors")
    processor_module.process = lambda *args: (
        calls.append(args[:-1]) or "/products/output.fits"
    )
    monkeypatch.setitem(sys.modules, "random_processors", processor_module)
    config_path = tmp_path / "pipeline.toml"
    config_path.write_text(
        '''[worker]
database = "worker.sqlite3"

[functions.random]
inputs = ["L1"]
output = "L2"
rule = "random_distributed_window"
window_seconds = 604800
count = 2
processor = "random_processors:process"

[functions.random.cadence]
type = "interval"
every_seconds = 86400
start_at = "2026-10-16T00:00:00Z"
'''
    )
    worker = ConfigWorker(config_path)
    pipeline = Pipeline(tmp_path / "worker.sqlite3")
    for day in (1, 2, 3):
        pipeline.add_product(
            Product("L1", END - timedelta(days=day), f"/inputs/{day}")
        )

    assert worker.run_once(END) == {("random", END): "complete"}
    assert len(calls) == 1
    assert len(calls[0]) == 2
    assert all(isinstance(path, str) for path in calls[0])


def test_sensor_cadence_registers_stable_files(tmp_path, monkeypatch):
    calls = []
    factory_calls = []
    processor_module = types.ModuleType("sensor_processors")
    processor_module.process = lambda *args: (
        calls.append(args) or "/products/output.fits"
    )

    def product_from_path(path):
        factory_calls.append(path)
        return Product("L1", END, str(path))

    processor_module.product_from_path = product_from_path
    monkeypatch.setitem(sys.modules, "sensor_processors", processor_module)
    sensor_directory = tmp_path / "incoming"
    sensor_directory.mkdir()
    incoming_file = sensor_directory / "new-file.dat"
    incoming_file.write_text("complete input")
    (sensor_directory / ".DS_Store").write_text("metadata")
    (sensor_directory / "ignore.txt").write_text("not a product")

    config_path = tmp_path / "pipeline.toml"
    config_path.write_text(
        f'''[worker]
database = "worker.sqlite3"

[functions.sensor_l2]
output = "L2"
processor = "sensor_processors:process"

[[functions.sensor_l2.rules]]
type = "same_timestamp"
inputs = ["L1"]

[functions.sensor_l2.cadence]
type = "sensor"
directory = "{sensor_directory}"
product_factory = "sensor_processors:product_from_path"
extension = ".dat"
'''
    )

    worker = ConfigWorker(config_path)
    pipeline = Pipeline(tmp_path / "worker.sqlite3")
    assert worker.run_once(END) == {}
    assert pipeline.products("L1") == []

    assert worker.run_once(END) == {("sensor_l2", END): "complete"}
    assert [product.path for product in pipeline.products("L1")] == [
        str(incoming_file)
    ]
    assert len(calls) == 1
    assert factory_calls == [incoming_file]

    restarted_worker = ConfigWorker(config_path)
    assert restarted_worker.run_once(END) == {}
    assert restarted_worker.run_once(END) == {}
    assert factory_calls == [incoming_file]


def test_concurrent_sensors_register_a_shared_file_once(tmp_path):
    database = tmp_path / "pipeline.sqlite3"
    sensor_path = tmp_path / "incoming" / "shared.dat"
    sensor_path.parent.mkdir()
    sensor_path.write_text("content")
    pipelines = [Pipeline(database), Pipeline(database)]
    products = [
        Product("L1", END, str(sensor_path)),
        Product("L1B", END, str(sensor_path)),
    ]
    start = Barrier(2)

    def register(index):
        start.wait()
        return pipelines[index].register_sensor_product(
            sensor_path, products[index]
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(register, range(2)))

    assert sum(product is not None for product in results) == 1
    assert len(pipelines[0].products()) == 1


def test_config_worker_runs_processor_in_a_separate_process(tmp_path):
    (tmp_path / "processor.py").write_text(
        "def process(*args):\n"
        "    output_time = args[-1]\n"
        "    return f'/products/{output_time:%Y%m%dT%H%M%SZ}.fits'\n"
    )
    config_path = tmp_path / "pipeline.toml"
    _write_config(
        config_path,
        cadence='type = "on_input"\nproduct = "L1"',
        processor="processor:process",
    )
    database = tmp_path / "worker.sqlite3"
    pipeline = Pipeline(database)
    pipeline.add_product(Product("L1", END, "/inputs/one"))
    pipeline.add_product(Product("L1B", END, "/inputs/two"))
    worker = ConfigWorker(config_path)

    worker.start()
    deadline = time.monotonic() + 5
    while pipeline.candidate_status("combine", END) != "complete" and time.monotonic() < deadline:
        time.sleep(0.01)
    assert pipeline.candidate_status("combine", END) == "complete"
    assert worker.stop(timeout=5)
    assert worker.join(timeout=0) == 0


def test_config_worker_starts_configured_number_of_processes(tmp_path):
    (tmp_path / "processor.py").write_text(
        "def process(*args):\n"
        "    output_time = args[-1]\n"
        "    return f'/products/{output_time:%Y%m%dT%H%M%SZ}.fits'\n"
    )
    config_path = tmp_path / "pipeline.toml"
    _write_config(
        config_path,
        cadence='type = "on_input"\nproduct = "L1"',
        processor="processor:process",
        max_workers=2,
    )
    worker = ConfigWorker(config_path)
    assert worker.max_workers == 2

    worker.start()
    assert len(worker._processes) == 2
    assert all(process.is_alive() for process in worker._processes)
    assert worker.stop(timeout=5)
    assert worker.join(timeout=0) == 0


def test_config_worker_process_handles_keyboard_interrupt(monkeypatch):
    def interrupt(self, stop_event):
        raise KeyboardInterrupt

    monkeypatch.setattr(ConfigWorker, "__init__", lambda self, config_path: None)
    monkeypatch.setattr(ConfigWorker, "run_forever", interrupt)
    _run_config_process("unused.toml", None)


def test_config_worker_process_runs_to_completion(monkeypatch):
    calls = []

    def run_forever(self, stop_event):
        calls.append(stop_event)

    monkeypatch.setattr(ConfigWorker, "__init__", lambda self, config_path: None)
    monkeypatch.setattr(ConfigWorker, "run_forever", run_forever)
    event = object()
    _run_config_process("unused.toml", event)
    assert calls == [event]


def test_config_worker_run_forever_exits_when_stop_event_is_set(tmp_path, monkeypatch):
    processor_module = types.ModuleType("test_processors")
    processor_module.process = _configured_worker_processor
    monkeypatch.setitem(sys.modules, "test_processors", processor_module)
    config_path = tmp_path / "pipeline.toml"
    _write_config(config_path, cadence='type = "on_input"\nproduct = "L1"')
    worker = ConfigWorker(config_path)
    calls = []

    class StopAfterFirstWait:
        def __init__(self):
            self.wait_count = 0

        def is_set(self):
            return False

        def wait(self, timeout):
            calls.append(timeout)
            self.wait_count += 1
            return self.wait_count == 1

    monkeypatch.setattr(worker, "run_once", lambda: calls.append("poll"))
    worker.run_forever(StopAfterFirstWait())
    assert calls == ["poll", worker.poll_interval]


def test_sensor_cadence_reports_missing_directory(tmp_path, monkeypatch):
    processor_module = types.ModuleType("sensor_processors")
    processor_module.process = _configured_worker_processor
    processor_module.product_from_path = lambda path: Product("L1", END, str(path))
    monkeypatch.setitem(sys.modules, "sensor_processors", processor_module)
    missing = tmp_path / "not-created"
    config_path = tmp_path / "pipeline.toml"
    config_path.write_text(
        f'''[worker]
database = "worker.sqlite3"

[functions.sensor_l2]
output = "L2"
processor = "sensor_processors:process"

[[functions.sensor_l2.rules]]
type = "same_timestamp"
inputs = ["L1"]

[functions.sensor_l2.cadence]
type = "sensor"
directory = "{missing}"
product_factory = "sensor_processors:product_from_path"
'''
    )
    with pytest.raises(NotADirectoryError, match="sensor directory"):
        ConfigWorker(config_path).run_once(END)


def test_dashboard_serves_recent_pipeline_status(tmp_path, caplog):
    caplog.set_level("DEBUG", logger="simplepipeline.dashboard")
    pipeline = Pipeline(tmp_path / "pipeline.sqlite3")
    rule = SameTimestampRule("combine", "L1", "L2")
    pipeline.run_candidate(rule, END - timedelta(seconds=1), _worker_processor)
    pipeline.add_product(Product("L1", END, "/inputs/current"))
    pipeline.run_candidate(rule, END, _worker_processor)

    dashboard = DashboardServer(pipeline.database, port=0)
    dashboard.start()
    try:
        host, port = dashboard.address
        with urlopen(f"http://{host}:{port}/api/status") as response:
            status = json.loads(response.read())
        with urlopen(f"http://{host}:{port}/api/status?refresh=1") as response:
            assert response.status == 200
        window_query = urlencode(
            {"start": END.isoformat(), "end": END.isoformat()}
        )
        with urlopen(f"http://{host}:{port}/api/status?{window_query}") as response:
            filtered_status = json.loads(response.read())
        with pytest.raises(HTTPError) as error:
            urlopen(f"http://{host}:{port}/api/status?start=not-a-timestamp")
        assert error.value.code == 400
        with urlopen(f"http://{host}:{port}/") as response:
            page = response.read().decode()
    finally:
        dashboard.close()

    assert status["summary"] == {
        "pending": 1,
        "processing": 0,
        "complete": 1,
        "failed": 0,
    }
    assert [item["status"] for item in status["candidates"]] == [
        "complete",
        "pending",
    ]
    assert [item["attempts"] for item in status["candidates"]] == [1, 0]
    assert "waiting for L1 at" in status["candidates"][1]["pending_reason"]
    assert status["products"][0]["product_code"] == "L2"
    assert "<title>simplepipeline dashboard</title>" in page
    assert 'id="start-time"' in page
    assert "Waiting for" in page
    assert len(filtered_status["candidates"]) == 1
    assert filtered_status["candidates"][0]["timestamp"] == END.isoformat(
        timespec="microseconds"
    ).replace("+00:00", "Z")
    assert filtered_status["summary"] == {
        "pending": 0,
        "processing": 0,
        "complete": 1,
        "failed": 0,
    }
    assert {product["product_code"] for product in filtered_status["products"]} == {
        "L1",
        "L2",
    }
    assert '"GET ' not in caplog.text


def test_dashboard_rejects_reversed_time_window_and_handles_missing_database(tmp_path):
    pipeline = Pipeline(tmp_path / "pipeline.sqlite3")
    with pytest.raises(ValueError, match="start must not be later"):
        read_dashboard_status(
            pipeline.database,
            start=END.isoformat(),
            end=(END - timedelta(days=1)).isoformat(),
        )
    one_sided = read_dashboard_status(
        pipeline.database, start=END.isoformat()
    )
    assert one_sided["candidates"] == []
    assert one_sided["products"] == []

    dashboard = DashboardServer(str(tmp_path / "missing.sqlite3"), port=0)
    dashboard.start()
    try:
        host, port = dashboard.address
        with pytest.raises(HTTPError) as missing_route:
            urlopen(f"http://{host}:{port}/missing")
        assert missing_route.value.code == 404
        with pytest.raises(HTTPError) as database_error:
            urlopen(f"http://{host}:{port}/api/status")
        assert database_error.value.code == 500
    finally:
        dashboard.close()


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"claim_timeout": timedelta(0)}, "claim_timeout"),
        ({"max_attempts": 0}, "max_attempts"),
        ({"max_attempts": True}, "max_attempts"),
        ({"output_version": "invalid"}, "semantic version"),
    ],
)
def test_run_candidate_rejects_invalid_options(tmp_path, kwargs, message):
    pipeline = Pipeline(tmp_path / "pipeline.sqlite3")
    rule = SameTimestampRule("validate", "L1", "L2")
    with pytest.raises(ValueError, match=message):
        pipeline.run_candidate(rule, END, _worker_processor, **kwargs)


def test_add_product_is_idempotent_and_rejects_conflicting_path(tmp_path):
    pipeline = Pipeline(tmp_path / "pipeline.sqlite3")
    product = Product("L1", END, "/inputs/one")
    first = pipeline.add_product(product)
    duplicate = pipeline.add_product(product)
    assert duplicate.product_id == first.product_id
    with pytest.raises(ValueError, match="already exists"):
        pipeline.add_product(Product("L1", END, "/inputs/different"))


def test_candidate_without_inputs_and_expired_claim_at_limit(tmp_path):
    pipeline = Pipeline(tmp_path / "pipeline.sqlite3")
    rule = SameTimestampRule("expired", "L1", "L2")
    assert pipeline.candidate_inputs("missing", END) == []
    assert pipeline.run_candidate(rule, END, _worker_processor, max_attempts=1) == "pending"
    pipeline.add_product(Product("L1", END, "/inputs/one"))
    assert pipeline.run_candidate(rule, END, _worker_processor, max_attempts=1) == "complete"

    timestamp = timestamp_text(END + timedelta(seconds=1))
    with sqlite3.connect(pipeline.database) as db:
        db.execute(
            "INSERT INTO candidates(rule_name, output_product_code, timestamp, status, "
            "claim_token, claimed_at, attempts) VALUES (?, ?, ?, 'pending', ?, ?, ?)",
            (
                rule.name,
                "L2",
                timestamp,
                "expired-token",
                "2020-01-01T00:00:00.000000Z",
                1,
            ),
        )
    assert pipeline.run_candidate(
        rule,
        END + timedelta(seconds=1),
        _worker_processor,
        claim_timeout=timedelta(seconds=1),
        max_attempts=1,
    ) == "failed"
    assert pipeline.candidate_status(rule.name, END + timedelta(seconds=1)) == "failed"


def test_dashboard_pending_reason_names_unmet_composite_rule(tmp_path):
    pipeline = Pipeline(tmp_path / "pipeline.sqlite3")
    rule = rules_module.CompositeRule(
        "combined",
        [
            SameTimestampRule("combined[1:same_timestamp]", "L1", "L2"),
            EvenlyDistributedWindowRule(
                "combined[2:evenly_distributed_window]",
                "L1B",
                "L2",
                window=timedelta(days=1),
                count=3,
            ),
        ],
    )
    pipeline.add_product(Product("L1", END, "/inputs/l1"))
    pipeline.add_product(
        Product("L1B", END - timedelta(hours=1), "/inputs/l1b-one")
    )

    assert pipeline.run_candidate(rule, END, _worker_processor) == "pending"
    status = read_dashboard_status(pipeline.database)
    pending_reason = status["candidates"][0]["pending_reason"]
    assert "combined[2:evenly_distributed_window]" in pending_reason
    assert "2 more L1B product(s)" in pending_reason
    assert "combined[1:same_timestamp]" not in pending_reason


def test_candidate_processor_empty_path_records_failure(tmp_path):
    pipeline = Pipeline(tmp_path / "pipeline.sqlite3")
    rule = SameTimestampRule("empty-output", "L1", "L2")
    pipeline.add_product(Product("L1", END, "/inputs/one"))
    with pytest.raises(ValueError, match="non-empty output path"):
        pipeline.run_candidate(rule, END, lambda inputs, timestamp: "")
    assert pipeline.candidate_status("empty-output", END) == "failed"


def test_candidate_reuses_existing_output_product(tmp_path):
    pipeline = Pipeline(tmp_path / "pipeline.sqlite3")
    rule = SameTimestampRule("existing-output", "L1", "L2")
    pipeline.add_product(Product("L1", END, "/inputs/one"))
    output = pipeline.add_product(Product("L2", END, "/outputs/existing"))
    assert pipeline.run_candidate(
        rule, END, lambda inputs, timestamp: "/outputs/not-used"
    ) == "complete"
    assert pipeline.products("L2") == [output]
    assert pipeline.candidate_inputs("existing-output", END)[0].product_code == "L1"
    assert pipeline.candidate_outputs("existing-output", END) == [output]


def test_candidate_registers_many_discovered_products(tmp_path):
    pipeline = Pipeline(tmp_path / "pipeline.sqlite3")
    rule = SameTimestampRule("many-outputs", "L1", "dynamic")
    pipeline.add_product(Product("L1", END, "/inputs/one"))
    output_time = END + timedelta(seconds=1)
    outputs = [
        Product("CAL", output_time, "/outputs/calibration.fits", version="1.2.0"),
        Product("MASK", END, "/outputs/mask.fits"),
    ]

    assert pipeline.run_candidate(
        rule,
        END,
        lambda inputs, timestamp: None,
        product_discovery=lambda inputs, timestamp: outputs,
    ) == "complete"
    assert pipeline.candidate_outputs("many-outputs", END) == [
        Product("CAL", output_time, "/outputs/calibration.fits", 2, "1.2.0"),
        Product("MASK", END, "/outputs/mask.fits", 3),
    ]
    assert [product.path for product in pipeline.candidate_inputs("many-outputs", END)] == [
        "/inputs/one"
    ]
    assert pipeline.candidate_status("many-outputs", END) == "complete"


def test_candidate_can_complete_with_no_discovered_products(tmp_path):
    pipeline = Pipeline(tmp_path / "pipeline.sqlite3")
    rule = SameTimestampRule("no-outputs", "L1", "dynamic")
    pipeline.add_product(Product("L1", END, "/inputs/one"))

    assert pipeline.run_candidate(
        rule,
        END,
        lambda inputs, timestamp: None,
        product_discovery=lambda inputs, timestamp: [],
    ) == "complete"
    assert pipeline.candidate_outputs("no-outputs", END) == []
    assert pipeline.candidate_status("no-outputs", END) == "complete"


def test_discovery_failure_registers_no_outputs(tmp_path):
    pipeline = Pipeline(tmp_path / "pipeline.sqlite3")
    rule = SameTimestampRule("discovery-fails", "L1", "dynamic")
    pipeline.add_product(Product("L1", END, "/inputs/one"))

    def discover(inputs, timestamp):
        yield Product("CAL", timestamp, "/outputs/calibration.fits")
        raise RuntimeError("failed to inspect outputs")

    with pytest.raises(RuntimeError, match="failed to inspect outputs"):
        pipeline.run_candidate(
            rule,
            END,
            lambda inputs, timestamp: None,
            product_discovery=discover,
        )
    assert pipeline.products("CAL") == []
    assert pipeline.candidate_outputs("discovery-fails", END) == []
    assert pipeline.candidate_status("discovery-fails", END) == "failed"


def test_product_timestamps_excludes_completed_and_exhausted_candidates(tmp_path):
    pipeline = Pipeline(tmp_path / "pipeline.sqlite3")
    rule = SameTimestampRule("dedupe", "L1", "L2")
    for timestamp in (END, END + timedelta(seconds=1)):
        pipeline.add_product(Product("L1", timestamp, f"/inputs/{timestamp.second}"))
    assert pipeline.run_candidate(rule, END, _worker_processor) == "complete"
    with pytest.raises(RuntimeError):
        pipeline.run_candidate(
            rule,
            END + timedelta(seconds=1),
            lambda inputs, timestamp: (_ for _ in ()).throw(RuntimeError("failed")),
            max_attempts=1,
        )
    assert pipeline.product_timestamps(
        "L1", excluding_rule="dedupe", max_attempts=1
    ) == []
    assert pipeline.product_timestamps("L1", excluding_rule=None) == [
        END,
        END + timedelta(seconds=1),
    ]


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("2026-10-16T00:00:00Z", END),
        ("2026-10-16T02:00:00+02:00", END),
    ],
)
def test_timestamp_helpers_normalize_to_utc(value, expected):
    assert normalize_timestamp(datetime.fromisoformat(value.replace("Z", "+00:00"))) == expected
    assert parse_timestamp(value) == expected
    assert timestamp_text(expected) == "2026-10-16T00:00:00.000000Z"


@pytest.mark.parametrize(
    ("code", "message"),
    [
        ("", "non-empty"),
        ("A" * 17, "at most 16"),
        (" L1", "whitespace"),
        ("L1 ", "whitespace"),
    ],
)
def test_product_code_validation_errors(code, message):
    with pytest.raises(ValueError, match=message):
        validate_product_code(code)


def test_product_and_rule_validation_errors():
    with pytest.raises(ValueError, match="path"):
        Product("L1", END, "")
    with pytest.raises(ValueError, match="rule name"):
        Rule("", "L1", "L2")
    with pytest.raises(ValueError, match="at least one"):
        SameTimestampRule("empty", [], "L2")
    with pytest.raises(ValueError, match="unique"):
        SameTimestampRule("duplicate", ["L1", "L1"], "L2")


def test_semantic_version_rejects_invalid_values_and_breaks_build_ties():
    for version in ("1.0", "01.0.0", "1.0.0-01", 1):
        with pytest.raises(ValueError):
            semantic_version_key(version)
    assert semantic_version_key("1.0.0+build.2") > semantic_version_key(
        "1.0.0+build.1"
    )
    with pytest.raises(ValueError, match="timezone-aware"):
        normalize_timestamp(datetime(2026, 10, 16))


def test_latest_version_products_keeps_each_code_latest_version():
    products = [
        Product("L1", END, "/old", version="1.0.0"),
        Product("L1", END + timedelta(days=1), "/new", version="2.0.0"),
        Product("L2", END, "/other"),
    ]
    assert [product.path for product in latest_version_products(products)] == [
        "/new",
        "/other",
    ]
    with pytest.raises(NotImplementedError):
        Rule("base", "L1", "L2").select_inputs([], END)


def test_window_rule_validation_and_exact_count_edges():
    with pytest.raises(ValueError, match="window must be positive"):
        EvenlyDistributedWindowRule(
            "bad", "L1", "L2", window=timedelta(0), count=1
        )
    with pytest.raises(ValueError, match="count must be positive"):
        EvenlyDistributedWindowRule(
            "bad", "L1", "L2", window=timedelta(days=1), count=0
        )
    rule = EvenlyDistributedWindowRule(
        "weekly", "L1", "L2", window=timedelta(days=1), count=2
    )
    products = [
        Product("L1", END - timedelta(hours=12), "/a"),
        Product("L1", END - timedelta(hours=12), "/b"),
    ]
    assert [p.path for p in rule.select_inputs(products, END)] == ["/a", "/b"]
    with pytest.raises(ValueError, match="timezone-aware"):
        rule.select_inputs(products, END.replace(tzinfo=None))


def test_random_window_validation_and_nonmatching_inputs():
    with pytest.raises(ValueError, match="window must be positive"):
        RandomDistributedWindowRule(
            "bad", "L1", "L2", window=timedelta(0), count=1
        )
    with pytest.raises(ValueError, match="count must be positive"):
        RandomDistributedWindowRule(
            "bad", "L1", "L2", window=timedelta(days=1), count=0
        )
    rule = RandomDistributedWindowRule(
        "random", "L1", "L2", window=timedelta(days=1), count=1
    )
    assert rule.select_inputs(
        [Product("L2", END - timedelta(hours=1), "/wrong")], END
    ) == []


def test_composite_rule_validates_and_deduplicates_inputs():
    one = SameTimestampRule("one", "L1", "L2")
    same_product = SameTimestampRule("two", "L1", "L2")
    composite = rules_module.CompositeRule("combined", [one, same_product])
    selected = composite.select_inputs(
        [Product("L1", END, "/input")], END
    )
    assert [product.path for product in selected] == ["/input"]
    with pytest.raises(ValueError, match="rule name"):
        rules_module.CompositeRule("", [one])
    with pytest.raises(ValueError, match="at least one"):
        rules_module.CompositeRule("empty", [])
    with pytest.raises(ValueError, match="same output"):
        rules_module.CompositeRule(
            "mixed", [one, SameTimestampRule("other", "L1", "L3")]
        )


def test_cli_main_without_dashboard(monkeypatch):
    calls = []

    class FakeWorker:
        database = "pipeline.sqlite3"
        joins = 0

        def __init__(self, config):
            calls.append(("init", config))

        def start(self):
            calls.append(("start",))

        def join(self):
            return 0

    monkeypatch.setattr(cli_module, "ConfigWorker", FakeWorker)
    monkeypatch.setattr(sys, "argv", ["simplepipeline", "pipeline.toml"])
    assert cli_module.main() == 0
    assert calls == [("init", "pipeline.toml"), ("start",)]


def test_cli_main_dashboard_verbose_and_keyboard_interrupt(monkeypatch):
    calls = []

    class FakeWorker:
        database = "pipeline.sqlite3"
        joins = 0

        def __init__(self, config):
            pass

        def start(self):
            calls.append("worker-start")

        def join(self):
            self.joins += 1
            if self.joins == 1:
                raise KeyboardInterrupt
            return 0

        def stop(self):
            calls.append("worker-stop")

    class FakeDashboard:
        def __init__(self, database, host, port):
            calls.append((database, host, port))

        def start(self):
            calls.append("dashboard-start")

        def close(self):
            calls.append("dashboard-close")

    monkeypatch.setattr(cli_module, "ConfigWorker", FakeWorker)
    monkeypatch.setattr(cli_module, "DashboardServer", FakeDashboard)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "simplepipeline",
            "pipeline.toml",
            "--dashboard",
            "--dashboard-host",
            "localhost",
            "--dashboard-port",
            "9000",
            "--verbose",
        ],
    )
    assert cli_module.main() == 0
    assert calls == [
        ("pipeline.sqlite3", "localhost", 9000),
        "worker-start",
        "dashboard-start",
        "worker-stop",
        "dashboard-close",
    ]


def test_cli_dashboard_start_failure_stops_worker(monkeypatch):
    calls = []

    class FakeWorker:
        database = "pipeline.sqlite3"

        def __init__(self, config):
            pass

        def start(self):
            calls.append("start")

        def stop(self):
            calls.append("stop")

        def join(self):
            calls.append("join")

    class BrokenDashboard:
        def __init__(self, database, host, port):
            pass

        def start(self):
            raise OSError("port busy")

    monkeypatch.setattr(cli_module, "ConfigWorker", FakeWorker)
    monkeypatch.setattr(cli_module, "DashboardServer", BrokenDashboard)
    monkeypatch.setattr(
        sys, "argv", ["simplepipeline", "pipeline.toml", "--dashboard"]
    )
    with pytest.raises(OSError, match="port busy"):
        cli_module.main()
    assert calls == ["start", "stop", "join"]


def test_cli_returns_nonzero_for_worker_failure(monkeypatch):
    class FailedWorker:
        database = "pipeline.sqlite3"

        def __init__(self, config):
            pass

        def start(self):
            pass

        def join(self):
            return 2

    monkeypatch.setattr(cli_module, "ConfigWorker", FailedWorker)
    monkeypatch.setattr(sys, "argv", ["simplepipeline", "pipeline.toml"])
    assert cli_module.main() == 1


def test_module_entrypoint_exits_with_cli_status(monkeypatch):
    monkeypatch.setattr(cli_module, "main", lambda: 7)
    with pytest.raises(SystemExit) as error:
        runpy.run_module("simplepipeline.__main__", run_name="__main__")
    assert error.value.code == 7


def _base_config_text():
    return (
        '[functions.f]\noutput = "L2"\ninputs = ["L1"]\n'
        'rule = "same_timestamp"\nprocessor = "test_processors:process"\n'
        '[functions.f.cadence]\ntype = "on_input"\n'
    )


@pytest.mark.parametrize(
    ("settings", "message"),
    [
        ({}, "output product code"),
        ({"output": "L2"}, "non-empty inputs"),
        ({"output": "L2", "inputs": ["L1"], "rule": "unknown"}, "unsupported rule"),
        (
            {
                "output": "L2",
                "inputs": ["L1", "L1B"],
                "rule": "evenly_distributed_window",
            },
            "exactly one input",
        ),
        (
            {
                "output": "L2",
                "inputs": ["L1"],
                "rule": "random_distributed_window",
                "window_seconds": "bad",
                "count": 1,
            },
            "window_seconds and count",
        ),
        (
            {
                "output": "L2",
                "rules": [],
            },
            "non-empty rules",
        ),
        (
            {
                "output": "L2",
                "rules": ["bad"],
            },
            "must be a table",
        ),
    ],
)
def test_rule_configuration_validation(settings, message):
    with pytest.raises(ValueError, match=message):
        config_module._make_rule("configured", settings)


@pytest.mark.parametrize(
    ("config_text", "message"),
    [
        ('worker = "bad"\n' + _base_config_text(), "\\[worker\\]"),
        (
            '[worker]\ndatabase = 1\n' + _base_config_text(),
            "database",
        ),
        (
            '[worker]\noutput_version = 3\n' + _base_config_text(),
            "worker.output_version",
        ),
        (
            '[worker]\noutput_version = "invalid"\n' + _base_config_text(),
            "semantic version",
        ),
        ('[worker]\nmax_attempts = 0\n' + _base_config_text(), "max_attempts"),
        ('[worker]\nmax_workers = 0\n' + _base_config_text(), "max_workers"),
        ('[worker]\nmax_workers = true\n' + _base_config_text(), "max_workers"),
        (
            '[worker]\npoll_interval_seconds = 0\n' + _base_config_text(),
            "poll_interval_seconds",
        ),
        (
            '[worker]\nclaim_timeout_seconds = 0\n' + _base_config_text(),
            "claim_timeout_seconds",
        ),
        ("functions = []\n", "at least one"),
        ("[functions]\nf = 1\n", "must be a TOML table"),
        ('[functions.f]\noutput = 7\n', "output product code"),
        (
            '[functions.f]\noutput = "L2"\ninputs = ["L1"]\nrule = "same_timestamp"\n',
            "processor",
        ),
    ],
)
def test_config_worker_rejects_invalid_top_level_settings(
    tmp_path, monkeypatch, config_text, message
):
    module = types.ModuleType("test_processors")
    module.process = _worker_processor
    monkeypatch.setitem(sys.modules, "test_processors", module)
    path = tmp_path / "invalid.toml"
    path.write_text(config_text)
    with pytest.raises(ValueError, match=message):
        ConfigWorker(path)


@pytest.mark.parametrize(
    ("function_settings", "message"),
    [
        (
            'output = "L2"\ninputs = ["L1"]\nrule = "same_timestamp"\n'
            'processor = "test_processors:process"\noutput_version = 3\n'
            '[functions.f.cadence]\ntype = "on_input"\n',
            "output_version",
        ),
        (
            'output = "L2"\ninputs = ["L1"]\nrule = "same_timestamp"\n'
            'processor = "test_processors:process"\n',
            "cadence",
        ),
        (
            'output = "L2"\ninputs = ["L1"]\nrule = "same_timestamp"\n'
            'processor = "test_processors:process"\n'
            '[functions.f.cadence]\ntype = "mystery"\n',
            "cadence.type",
        ),
        (
            'output = "L2"\ninputs = ["L1"]\nrule = "same_timestamp"\n'
            'processor = "test_processors:process"\n'
            '[functions.f.cadence]\ntype = "on_input"\nproduct = "L1B"\n',
            "trigger product",
        ),
        (
            'output = "L2"\ninputs = ["L1"]\nrule = "same_timestamp"\n'
            'processor = "test_processors:process"\n'
            '[functions.f.cadence]\ntype = "interval"\nevery_seconds = 0\n'
            'start_at = "2026-10-16T00:00:00Z"\n',
            "every_seconds must be positive",
        ),
        (
            'output = "L2"\ninputs = ["L1"]\nrule = "same_timestamp"\n'
            'processor = "test_processors:process"\n'
            '[functions.f.cadence]\ntype = "interval"\nevery_seconds = 1\n'
            'start_at = "not-time"\n',
            "interval cadence requires",
        ),
        (
            'output = "L2"\ninputs = ["L1"]\nrule = "same_timestamp"\n'
            'processor = "test_processors:process"\n'
            '[functions.f.cadence]\ntype = "sensor"\ndirectory = "incoming"\n'
            'product_factory = "test_processors:product_from_path"\nextension = "fits"\n',
            "extension",
        ),
        (
            'output = "L2"\ninputs = ["L1"]\nrule = "same_timestamp"\n'
            'processor = "test_processors:process"\n'
            '[functions.f.cadence]\ntype = "sensor"\ndirectory = "incoming"\n'
            'product_factory = "test_processors:product_from_path"\nproduct = "L1B"\n',
            "trigger product",
        ),
    ],
)
def test_config_worker_rejects_invalid_function_settings(
    tmp_path, monkeypatch, function_settings, message
):
    module = types.ModuleType("test_processors")
    module.process = _worker_processor
    module.product_from_path = lambda path: Product("L1", END, str(path))
    monkeypatch.setitem(sys.modules, "test_processors", module)
    path = tmp_path / "invalid.toml"
    path.write_text(
        '[worker]\ndatabase = "worker.sqlite3"\n[functions.f]\n'
        + function_settings
    )
    with pytest.raises(ValueError, match=message):
        ConfigWorker(path)


def test_processor_and_factory_references_are_validated(tmp_path, monkeypatch):
    with pytest.raises(ValueError, match="module:function"):
        config_module._load_processor("missing-colon", tmp_path)
    with pytest.raises(ValueError, match="invalid processor"):
        config_module._load_processor(":function", tmp_path)
    with pytest.raises(ValueError, match="product_factory"):
        config_module._load_product_factory("missing-colon", tmp_path)

    module = types.ModuleType("invalid_factory_module")
    module.factory = lambda path: "not a Product"
    monkeypatch.setitem(sys.modules, "invalid_factory_module", module)
    factory = config_module._load_product_factory(
        "invalid_factory_module:factory", tmp_path
    )
    with pytest.raises(TypeError, match="must return a Product"):
        factory(tmp_path / "input")


@pytest.mark.parametrize(
    ("value", "message"),
    [
        (None, "timezone"),
        ("not-a-date", "timezone"),
        ("2026-10-16T00:00:00", "timezone"),
    ],
)
def test_config_timestamp_parser_rejects_invalid_values(value, message):
    with pytest.raises(ValueError, match=message):
        config_module._parse_timestamp(value, "test timestamp")
