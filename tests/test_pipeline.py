from datetime import datetime, timedelta, timezone
import sqlite3
import sys
from threading import Event, Thread
import time
import types

import pytest

from simplepipeline import (
    ConfigWorker,
    DailyWorker,
    EvenlyDistributedWindowRule,
    Pipeline,
    Product,
)
from simplepipeline.config import _run_config_process
from simplepipeline.engine import daily_utc_candidates
from simplepipeline.models import semantic_version_key
from simplepipeline.rules import SameTimestampRule


UTC = timezone.utc
END = datetime(2026, 10, 16, tzinfo=UTC)


def _worker_processor(inputs, output_time):
    return "/products/output.fits"


def _write_config(
    path,
    *,
    cadence,
    processor="test_processors:process",
    output_version="0.0.0",
):
    path.write_text(
        f'''[worker]
database = "worker.sqlite3"
poll_interval_seconds = 1

[functions.combine]
inputs = ["L1", "L1B"]
output = "L2"
rule = "same_timestamp"
processor = "{processor}"
output_version = "{output_version}"

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


def test_config_worker_triggers_when_registered_inputs_share_timestamp(tmp_path, monkeypatch):
    calls = []
    processor_module = types.ModuleType("test_processors")
    processor_module.process = lambda inputs, output_time: (
        calls.append((inputs, output_time)) or "/products/output.fits"
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
    assert [(product.product_code, product.version) for product in calls[0][0]] == [
        ("L1", "2.0.0"),
        ("L1B", "0.0.0"),
    ]


def test_config_worker_runs_and_persists_fixed_interval_schedule(tmp_path, monkeypatch):
    calls = []
    processor_module = types.ModuleType("test_processors")
    processor_module.process = lambda inputs, output_time: (
        calls.append(output_time) or f"/products/{output_time:%S}.fits"
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


def test_config_worker_combines_multiple_rules(tmp_path, monkeypatch):
    calls = []
    processor_module = types.ModuleType("test_processors")
    processor_module.process = lambda inputs, output_time: (
        calls.append(inputs) or "/products/output.fits"
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
    assert [[product.path for product in inputs] for inputs in calls] == [[
        "/inputs/l1",
        "/inputs/l1b-early",
        "/inputs/l1b-late",
    ]]
    assert [
        product.path for product in pipeline.candidate_inputs("combine", END)
    ] == [
        "/inputs/l1",
        "/inputs/l1b-early",
        "/inputs/l1b-late",
    ]


def test_sensor_cadence_registers_stable_files(tmp_path, monkeypatch):
    calls = []
    processor_module = types.ModuleType("sensor_processors")
    processor_module.process = lambda inputs, output_time: (
        calls.append((inputs, output_time)) or "/products/output.fits"
    )
    processor_module.product_from_path = lambda path: Product(
        "L1", END, str(path)
    )
    monkeypatch.setitem(sys.modules, "sensor_processors", processor_module)
    sensor_directory = tmp_path / "incoming"
    sensor_directory.mkdir()
    incoming_file = sensor_directory / "new-file.dat"
    incoming_file.write_text("complete input")

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


def test_config_worker_runs_processor_in_a_separate_process(tmp_path):
    (tmp_path / "processor.py").write_text(
        "def process(inputs, output_time):\n"
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


def test_config_worker_process_handles_keyboard_interrupt(monkeypatch):
    def interrupt(self, stop_event):
        raise KeyboardInterrupt

    monkeypatch.setattr(ConfigWorker, "__init__", lambda self, config_path: None)
    monkeypatch.setattr(ConfigWorker, "run_forever", interrupt)
    _run_config_process("unused.toml", None)
