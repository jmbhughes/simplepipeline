# simplepipeline

A minimal Python scaffold for a generic, database-backed scientific data pipeline.
Product codes are case-sensitive strings of up to 16 characters. SQLite records
products, scheduled candidates, their status, and the exact input provenance for
completed outputs. Products have SemVer versions (default `0.0.0`). Input
selection uses only the highest version for each product code across all
registered timestamps.

## How it works

Files are registered as timestamped, versioned products in SQLite. A configured
worker watches for new inputs or follows a fixed interval, then applies each
function's selection rule and calls its `module:function` processor. Successful
outputs and their exact input provenance are recorded in the database; jobs
without enough inputs are retried on later polls.

## Configuration-driven worker

Pipeline functions, product codes, processor references, and cadence can be
declared in TOML. The worker polls registered products in SQLite for input
triggers, or runs candidates on a fixed interval. Register discovered files
with `Pipeline.add_product`; the worker uses their product codes and timestamps
to find work.

```sh
python -m simplepipeline pipeline.toml
```

The example [`pipeline.toml`](./pipeline.toml) configures an L2 function that
creates an output at each registered L1 timestamp. Its processor,
`demo:make_l2`, receives a list of input `Product` records and the candidate
timestamp, writes the output, and returns its path. Processor references use
`module:function`; the module must be importable from the configuration
directory or Python environment.

Each function requires a `[functions.<name>.cadence]` table. The `type` can be:

- **`on_input`**: schedule a candidate whenever a product of a listed input
  code is registered. The optional `product` selects which input code triggers
  candidates; it must appear in that function's rule inputs. If omitted, the
  first input product code is used. For example:

  ```toml
  [functions.l2.cadence]
  type = "on_input"
  product = "L1"
  ```

- **`interval`**: schedule candidates on a fixed interval starting at
  `start_at`. Set `every_seconds` to a positive number of seconds and `start_at`
  to a timezone-aware ISO-8601 timestamp (UTC recommended). The start time is
  included; missed intervals are scheduled after a restart. For example:

  ```toml
  [functions.daily_l2.cadence]
  type = "interval"
  every_seconds = 86400
  start_at = "2026-10-17T00:00:00Z"
  ```

Cadence determines candidate output timestamps; rules determine which inputs
are required for each candidate. All rules must select their inputs before the
processor runs. For example, this interval-scheduled function requires an L1
product at each candidate timestamp and 100 evenly distributed L1B products
from the preceding week:

```toml
[functions.daily_l2]
output = "L2"
processor = "my_processing:make_l2"
output_version = "1.2.0"

[[functions.daily_l2.rules]]
type = "same_timestamp"
inputs = ["L1"]

[[functions.daily_l2.rules]]
type = "evenly_distributed_window"
inputs = ["L1B"]
window_seconds = 604800
count = 100

[functions.daily_l2.cadence]
type = "interval"
every_seconds = 86400
start_at = "2026-10-17T00:00:00Z"
```

The existing single-rule configuration (`rule`, `inputs`, and any rule-specific
settings directly under the function) remains supported. `same_timestamp` rules
can require multiple input product codes at a matching timestamp, while
`evenly_distributed_window` accepts one input product code. When registering
files, pass `version="1.2.0"` to `Product`; the chooser
ignores older versions for that product code, even if an older-version file has
a timestamp that the newest version does not. Build metadata follows SemVer
precedence and is used only to break ties deterministically. Generated outputs
use `output_version` from TOML or the `run_candidate` keyword argument, defaulting
to `0.0.0`. Interval candidates are anchored at `start_at`; missed intervals are
scheduled on restart, and the next interval cursor is persisted in SQLite.
Incomplete candidates are retried each poll for either cadence. The worker uses
Python's `spawn` mode and runs until stopped with Ctrl+C.

The existing programmatic `DailyWorker` API remains available for daily
midnight-UTC schedules. Its processing claim expires after one day by default;
set `claim_timeout` longer than the longest expected job to prevent another
worker from reclaiming a still-running job. Set its `output_version` argument
to control the version recorded for generated products.

## Tests

```sh
python -m pip install -e '.[test]'
python -m pytest
```

## Extension points

`Rule.select_inputs` defines how a rule selects its inputs for an output time.
Implement additional rule classes for other windows, grouping strategies, or
input counts. Candidate scheduling is separate: use `daily_utc_candidates` or
supply timestamps from another cadence. Each rule name plus output timestamp
uniquely identifies a candidate.
