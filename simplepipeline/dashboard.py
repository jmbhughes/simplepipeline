from __future__ import annotations

import json
import logging
import sqlite3
import threading
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlsplit

from .models import normalize_timestamp, timestamp_text

LOGGER = logging.getLogger(__name__)

_DASHBOARD_HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>simplepipeline dashboard</title>
  <style>
    :root { color-scheme: light dark; font: 15px system-ui, sans-serif; }
    body { margin: 2rem auto; padding: 0 1rem; max-width: 1100px; }
    h1 { margin-bottom: .25rem; }
    .muted { opacity: .7; }
    .summary { display: flex; flex-wrap: wrap; gap: .75rem; margin: 1.5rem 0; }
    .card { border: 1px solid #8886; border-radius: 8px; padding: .8rem 1rem; min-width: 100px; }
    .card strong { display: block; font-size: 1.5rem; }
    .filters { display: flex; flex-wrap: wrap; align-items: end; gap: .75rem; margin-top: 1.25rem; }
    .filters label { display: grid; gap: .25rem; }
    button { padding: .45rem .75rem; }
    section { margin: 2rem 0; }
    .table-wrap { overflow-x: auto; }
    table { border-collapse: collapse; width: 100%; }
    th, td { text-align: left; padding: .55rem; border-bottom: 1px solid #8885; }
    td.path, td.error { overflow-wrap: anywhere; }
    .complete { color: #238636; }
    .failed { color: #cf222e; }
    .pending, .processing { color: #9a6700; }
    td.pending-reason { min-width: 18rem; overflow-wrap: anywhere; }
  </style>
</head>
<body>
  <h1>simplepipeline</h1>
  <div class="muted" id="updated">Loading status…</div>
  <form class="filters" id="time-window">
    <label>Start time <input type="datetime-local" id="start-time"></label>
    <label>End time <input type="datetime-local" id="end-time"></label>
    <button type="submit">Apply window</button>
    <button type="button" id="clear-window">Clear</button>
  </form>
  <div class="summary" id="summary"></div>
  <section>
    <h2>Average runtime by rule</h2>
    <div class="table-wrap"><table>
      <thead><tr><th>Rule</th><th>Average runtime (s)</th><th>Runs</th></tr></thead>
      <tbody id="runtime-summary"></tbody>
    </table></div>
  </section>
  <section>
    <h2>Recent candidates</h2>
    <div class="table-wrap"><table>
      <thead><tr><th>Function</th><th>Output</th><th>Timestamp</th><th>Status</th><th>Attempts</th><th>Waiting for</th><th>Error</th></tr></thead>
      <tbody id="candidates"></tbody>
    </table></div>
  </section>
  <section>
    <h2>Recent products</h2>
    <div class="table-wrap"><table>
      <thead><tr><th>Code</th><th>Version</th><th>Timestamp</th><th>Path</th></tr></thead>
      <tbody id="products"></tbody>
    </table></div>
  </section>
  <script>
    const el = (tag, text, className = "") => {
      const node = document.createElement(tag);
      node.textContent = text ?? "";
      if (className) node.className = className;
      return node;
    };
    const startInput = document.getElementById("start-time");
    const endInput = document.getElementById("end-time");
    let timeWindow = {start: "", end: ""};
    document.getElementById("time-window").addEventListener("submit", event => {
      event.preventDefault();
      timeWindow = {start: startInput.value, end: endInput.value};
      refresh();
    });
    document.getElementById("clear-window").addEventListener("click", () => {
      startInput.value = "";
      endInput.value = "";
      timeWindow = {start: "", end: ""};
      refresh();
    });
    const toUtc = value => value ? new Date(value).toISOString() : "";
    async function refresh() {
      try {
        const params = new URLSearchParams();
        if (timeWindow.start) params.set("start", toUtc(timeWindow.start));
        if (timeWindow.end) params.set("end", toUtc(timeWindow.end));
        const query = params.toString();
        const response = await fetch(`/api/status${query ? `?${query}` : ""}`, {cache: "no-store"});
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        const data = await response.json();
        document.getElementById("updated").textContent =
          `Updated ${new Date().toLocaleTimeString()} · refreshes every 3 seconds`;

        const summary = document.getElementById("summary");
        summary.replaceChildren();
        for (const [label, count] of Object.entries(data.summary)) {
          const card = el("div", "", "card");
          card.append(el("strong", String(count)), el("span", label));
          summary.append(card);
        }

        const runtimeSummary = document.getElementById("runtime-summary");
        runtimeSummary.replaceChildren();
        for (const item of data.runtime_summary) {
          const row = document.createElement("tr");
          row.append(el("td", item.rule_name));
          row.append(el("td", item.average_duration_seconds == null ? "—" : Number(item.average_duration_seconds).toFixed(3)));
          row.append(el("td", String(item.run_count)));
          runtimeSummary.append(row);
        }

        const candidates = document.getElementById("candidates");
        candidates.replaceChildren();
        for (const item of data.candidates) {
          const row = document.createElement("tr");
          for (const value of [item.function, item.output, item.timestamp]) row.append(el("td", value));
          row.append(el("td", item.status, item.status));
          row.append(el("td", String(item.attempts)));
          row.append(el("td", item.pending_reason, "pending-reason"));
          row.append(el("td", item.error, "error"));
          candidates.append(row);
        }

        const products = document.getElementById("products");
        products.replaceChildren();
        for (const item of data.products) {
          const row = document.createElement("tr");
          for (const value of [item.product_code, item.version, item.timestamp]) row.append(el("td", value));
          row.append(el("td", item.path, "path"));
          products.append(row);
        }
      } catch (error) {
        document.getElementById("updated").textContent = `Dashboard refresh failed: ${error}`;
      }
    }
    refresh();
    setInterval(refresh, 3000);
  </script>
</body>
</html>
"""


def _parse_time_bound(value: str | None, parameter: str) -> str | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return timestamp_text(normalize_timestamp(parsed))
    except ValueError as error:
        raise ValueError(
            f"{parameter} must be an ISO-8601 timestamp with a timezone"
        ) from error


def read_dashboard_status(
    database: str, *, start: str | None = None, end: str | None = None
) -> dict[str, Any]:
    start_time = _parse_time_bound(start, "start")
    end_time = _parse_time_bound(end, "end")
    if start_time is not None and end_time is not None and start_time > end_time:
        raise ValueError("start must not be later than end")

    candidate_filters = []
    product_filters = []
    parameters: list[str] = []
    if start_time is not None:
        candidate_filters.append("timestamp >= ?")
        product_filters.append("timestamp >= ?")
        parameters.append(start_time)
    if end_time is not None:
        candidate_filters.append("timestamp <= ?")
        product_filters.append("timestamp <= ?")
        parameters.append(end_time)
    candidate_where = (
        f" WHERE {' AND '.join(candidate_filters)}" if candidate_filters else ""
    )
    candidate_time_predicate = (
        f" AND {' AND '.join(candidate_filters)}" if candidate_filters else ""
    )
    product_where = (
        f" WHERE {' AND '.join(product_filters)}" if product_filters else ""
    )

    with sqlite3.connect(database, timeout=5) as connection:
        connection.row_factory = sqlite3.Row
        counts = connection.execute(
            "SELECT status, claim_token, COUNT(*) AS count "
            f"FROM candidates{candidate_where} GROUP BY status, claim_token",
            parameters,
        ).fetchall()
        candidates = connection.execute(
            "SELECT rule_name, output_product_code, timestamp, status, error, "
            "pending_reason, claim_token, attempts, started_at, finished_at, duration_seconds "
            "FROM candidates"
            f"{candidate_where} ORDER BY timestamp DESC, id DESC LIMIT 100",
            parameters,
        ).fetchall()
        products = connection.execute(
            "SELECT product_code, version, timestamp, path "
            f"FROM products{product_where} "
            "ORDER BY created_at DESC, id DESC LIMIT 100",
            parameters,
        ).fetchall()
        runtime_summary = connection.execute(
            "SELECT rule_name, ROUND(AVG(duration_seconds), 3) AS average_duration_seconds, "
            "COUNT(*) AS run_count FROM candidates "
            "WHERE status IN ('complete', 'failed') "
            "AND duration_seconds IS NOT NULL"
            f"{candidate_time_predicate} GROUP BY rule_name "
            "ORDER BY average_duration_seconds DESC",
            parameters,
        ).fetchall()

    summary = {"pending": 0, "processing": 0, "complete": 0, "failed": 0}
    for row in counts:
        if row["status"] == "pending" and row["claim_token"]:
            summary["processing"] += row["count"]
        else:
            summary[row["status"]] += row["count"]

    return {
        "summary": summary,
        "runtime_summary": [
            {
                "rule_name": row["rule_name"],
                "average_duration_seconds": row["average_duration_seconds"],
                "run_count": row["run_count"],
            }
            for row in runtime_summary
        ],
        "candidates": [
            {
                "function": row["rule_name"],
                "output": row["output_product_code"],
                "timestamp": row["timestamp"],
                "status": "processing" if row["claim_token"] else row["status"],
                "attempts": row["attempts"],
                "pending_reason": row["pending_reason"] or "",
                "error": row["error"] or "",
                "started_at": row["started_at"],
                "finished_at": row["finished_at"],
                "duration_seconds": row["duration_seconds"],
            }
            for row in candidates
        ],
        "products": [dict(row) for row in products],
    }


class DashboardServer:
    """Serve a read-only live view of a pipeline database."""

    def __init__(self, database: str, host: str = "127.0.0.1", port: int = 8765) -> None:
        database_path = database

        class DashboardHandler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                route = urlsplit(self.path).path
                if route == "/":
                    body = _DASHBOARD_HTML.encode("utf-8")
                    content_type = "text/html; charset=utf-8"
                elif route == "/api/status":
                    try:
                        query = parse_qs(urlsplit(self.path).query)
                        body = json.dumps(
                            read_dashboard_status(
                                database_path,
                                start=query.get("start", [None])[0],
                                end=query.get("end", [None])[0],
                            )
                        ).encode("utf-8")
                    except ValueError as error:
                        self.send_error(400, str(error))
                        return
                    except sqlite3.Error:
                        LOGGER.exception("Unable to read pipeline status for dashboard")
                        self.send_error(500, "Unable to read pipeline status")
                        return
                    content_type = "application/json; charset=utf-8"
                else:
                    self.send_error(404)
                    return

                self.send_response(200)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Cache-Control", "no-store")
                self.end_headers()
                self.wfile.write(body)

            def log_request(
                self, code: int | str = "-", size: int | str = "-"
            ) -> None:
                if self.command == "GET":
                    return
                super().log_request(code, size)

            def log_message(self, format: str, *args: object) -> None:
                LOGGER.debug("Dashboard: " + format, *args)

        self._server = ThreadingHTTPServer((host, port), DashboardHandler)
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            name="simplepipeline-dashboard",
            daemon=True,
        )

    @property
    def address(self) -> tuple[str, int]:
        host, port = self._server.server_address[:2]
        return str(host), int(port)

    def start(self) -> None:
        self._thread.start()
        LOGGER.info("Dashboard available at http://%s:%d", *self.address)

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        if self._thread.is_alive():
            self._thread.join()
