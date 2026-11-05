"""verity as the quality gate between silver and gold.

    python -m retail_lakehouse.gate bootstrap --through 2015-12-29
    python -m retail_lakehouse.gate run --week 2016-01-05 --run-id manual
    python -m retail_lakehouse.gate release --week 2016-01-12 --by luke --reason "store 52 opened"

verity compares two versions of a weekly fact table. The gate keeps its own:
``gate/favorita_weekly``, store x item x week net units (verity's Favorita
layout: ``week``, ``store_id``, ``product_id``, ``commodity_id`` (the family),
``state_id``, ``units``), built from silver with DuckDB. Each gate run
replaces one week's rows, which is one Delta version, and asks verity about
the previous version against the new one.

Each run writes a decision to ``control/gate/week_ending=.../decision-<time>.json``.
A week enters gold only if its latest decision approves it:

- ``PASS`` and ``PASS_WITH_EXPLANATION`` approve;
- ``INVESTIGATE``, ``DATA_CONTRACT_FAILURE`` and ``INCOMPLETE`` block: the
  week is quarantined;
- verity crashing or being locked is not a verdict: the run fails without a
  decision and is retried against the same table version pair;
- ``release`` records a person's approval for a quarantined week, with a
  name and a reason. verity's status is kept, not rewritten.

Rerunning a week whose rows are unchanged returns its latest decision without
a new table version or a new assessment. Weeks loaded before the gate existed
are written once by ``bootstrap`` and recorded as approved pre-gate history.

Runs in ``.venv-qc`` (verity, deltalake, duckdb).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from retail_lakehouse.landing import lake_root

FIRST_WEEK_ENDING = date(2013, 1, 8)  # week 1, as in verity's Favorita runs
APPROVING = {"PASS", "PASS_WITH_EXPLANATION"}
EXIT_STATUS = {0: None, 2: "INVESTIGATE", 3: "DATA_CONTRACT_FAILURE", 4: "INCOMPLETE"}
CONFIG = Path(__file__).resolve().parents[2] / "config/verity/favorita.yaml"


def week_number(week_ending: date) -> int:
    return (week_ending - FIRST_WEEK_ENDING).days // 7 + 1


def gate_table() -> str:
    return str(lake_root() / "gate/favorita_weekly")


def decisions_dir(week: date) -> Path:
    return lake_root() / "control/gate" / f"week_ending={week}"


def snapshot(through: date, only: date | None = None):
    """Gate rows from silver: one week (``only``) or every week up to ``through``."""
    import duckdb

    silver = lake_root() / "silver/favorita"
    where = f"s.week_ending = DATE '{only}'" if only else f"s.week_ending <= DATE '{through}'"
    con = duckdb.connect()
    con.sql("LOAD delta")
    return con.sql(f"""
        SELECT CAST(datediff('day', DATE '{FIRST_WEEK_ENDING}', s.week_ending) / 7 + 1 AS INTEGER) AS week,
               'S' || s.store_nbr AS store_id,
               st.state AS state_id,
               i.family AS commodity_id,
               'I' || s.item_nbr AS product_id,
               sum(s.unit_sales) AS units
        FROM delta_scan('{silver / "sales"}') s
        JOIN delta_scan('{silver / "items"}') i USING (item_nbr)
        JOIN delta_scan('{silver / "stores"}') st USING (store_nbr)
        WHERE {where}
        GROUP BY ALL
        HAVING sum(s.unit_sales) <> 0
        ORDER BY week, store_id, product_id
    """).to_arrow_table()


def content_hash(table) -> str:
    import pyarrow.csv as pcsv

    sink = __import__("io").BytesIO()
    pcsv.write_csv(table, sink)
    return hashlib.sha256(sink.getvalue()).hexdigest()


def latest_decision(week: date) -> dict[str, Any] | None:
    files = sorted(decisions_dir(week).glob("decision-*.json"))
    return json.loads(files[-1].read_text()) if files else None


def write_decision(week: date, **fields: Any) -> dict[str, Any]:
    decided_at = datetime.now(UTC)
    record = {"week_ending": str(week), "decided_at": decided_at.isoformat(timespec="microseconds"), **fields}
    folder = decisions_dir(week)
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"decision-{decided_at.strftime('%Y%m%dT%H%M%S%f')}.json"
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(json.dumps(record, indent=1) + "\n")
    tmp.rename(path)
    return record


def table_version() -> int:
    from deltalake import DeltaTable

    return DeltaTable(gate_table()).version()


def bootstrap(through: date, run_id: str) -> dict[str, Any]:
    """Write every week up to ``through`` as gate version 0, approved as pre-gate history."""
    from deltalake import DeltaTable, write_deltalake

    if DeltaTable.is_deltatable(gate_table()):
        return {"action": "unchanged", "version": table_version()}
    rows = snapshot(through)
    write_deltalake(gate_table(), rows, mode="error", name="favorita_weekly")
    version = table_version()
    weeks = sorted(set(rows["week"].to_pylist()))
    for number in weeks:
        week = FIRST_WEEK_ENDING + timedelta(weeks=number - 1)
        write_decision(
            week,
            approved=True,
            status="PRE_GATE",
            reason=f"history loaded before the gate went live (through {through})",
            gate_version=version,
            content_sha256=None,
            run_id=run_id,
        )
    return {"action": "bootstrapped", "version": version, "weeks": len(weeks), "rows": rows.num_rows}


def run(week: date, run_id: str) -> dict[str, Any]:
    """Gate one week: replace its rows in the gate table, assess, record the decision."""
    from deltalake import write_deltalake

    rows = snapshot(week, only=week)
    if rows.num_rows == 0:
        raise ValueError(f"silver has no sales for week ending {week}")
    digest = content_hash(rows)
    previous = latest_decision(week)
    if previous and previous.get("content_sha256") == digest:
        return {**previous, "action": "unchanged"}

    # A failed attempt leaves its table version behind; the retry assesses that
    # same pair rather than writing the week again and comparing it with itself.
    pending_path = decisions_dir(week) / "pending.json"
    pending = json.loads(pending_path.read_text()) if pending_path.exists() else None
    if pending and pending["content_sha256"] == digest and pending["after"] == table_version():
        before, after = pending["before"], pending["after"]
    else:
        before = table_version()
        write_deltalake(gate_table(), rows, mode="overwrite", predicate=f"week = {week_number(week)}")
        after = table_version()
        pending_path.parent.mkdir(parents=True, exist_ok=True)
        pending = {"before": before, "after": after, "content_sha256": digest}
        pending_path.write_text(json.dumps(pending) + "\n")
    out = lake_root() / "reports/gate"
    command = [
        str(Path(sys.executable).with_name("qc")),
        "weekly",
        "--uri", gate_table(),
        "--previous", str(before),
        "--current", str(after),
        "--config", str(CONFIG),
        "--store", str(lake_root() / "control/verity.db"),
        "--out", str(out),
        "--json",
    ]  # fmt: skip
    result = subprocess.run(command, capture_output=True, text=True, env=os.environ)
    if result.returncode not in EXIT_STATUS:
        # 1 is an execution error, 75 another run holding verity's lock: neither
        # is a verdict on the data, so the task fails and retries.
        raise RuntimeError(f"qc weekly exited {result.returncode}: {result.stderr.strip()[-2000:]}")
    weekly = json.loads(result.stdout)
    report = Path(weekly["report_dir"])
    machine = json.loads((report / "report.json").read_text())
    record = write_decision(
        week,
        approved=weekly["status"] in APPROVING,
        status=weekly["status"],
        reason="verity",
        gate_version=after,
        previous_version=before,
        content_sha256=digest,
        report=str(report),
        assessment_id=weekly.get("assessment_id"),
        likely_cause=(weekly.get("decision") or {}).get("likely_cause"),
        findings=summarize(machine),
        run_id=run_id,
    )
    pending_path.unlink(missing_ok=True)
    return {**record, "action": "assessed"}


REVIEW = {"UNEXPLAINED_ANOMALY", "HARD_FAILURE", "UNAVAILABLE_EVIDENCE"}  # verity report.md's review set
SUMMARY_KEYS = ("check", "disposition", "scope_type", "scope", "metric", "period", "impact")


def summarize(machine: dict[str, Any]) -> list[dict[str, Any]]:
    """What verity puts up for review, short enough for an alert.

    First what the blocking findings leave unnamed: a material revision of
    already-gated weeks, with its size (the finding itself carries impact 0),
    and the entities missing from the latest week, for each entity type whose
    loss verity rates material (verity reports them as events). Then the
    review findings, largest impact first.
    """
    history = machine.get("historical_revision") or {}
    revised = (
        [
            {
                "check": "historical_revision",
                "disposition": "MATERIAL",
                "scope_type": "history",
                "scope": "weeks already gated",
                "metric": "units",
                "period": None,
                "impact": history.get("raw_delta"),
            }
        ]
        if history.get("material")
        else []
    )
    material = {
        kind for kind, entry in (machine.get("missing_entities") or {}).items() if entry.get("material")
    }
    missing = [
        {
            "check": "missing",
            "disposition": "LATEST_WEEK_MISSING",
            "scope_type": e["entity_type"],
            "scope": e["entity_id"],
            "metric": "units",
            "period": None,
            "impact": -e["expected_value"],
        }
        for e in machine.get("events", [])
        if e.get("classification") == "LATEST_WEEK_MISSING" and e.get("entity_type") in material
    ]
    findings = [
        {key: f.get(key) for key in SUMMARY_KEYS}
        for f in machine.get("findings", [])
        if f.get("disposition") in REVIEW
    ]

    def largest_first(items):
        return sorted(items, key=lambda f: -abs(f["impact"] or 0))

    return (revised + largest_first(missing) + largest_first(findings))[:20]


def release(week: date, by: str, reason: str) -> dict[str, Any]:
    latest = latest_decision(week)
    if latest is None:
        raise SystemExit(f"no gate decision for week ending {week}")
    if latest["approved"]:
        raise SystemExit(f"week ending {week} is not quarantined (latest: {latest['status']})")
    return write_decision(
        week,
        approved=True,
        status=latest["status"],
        reason=f"released by {by}: {reason}",
        released_by=by,
        gate_version=latest.get("gate_version"),
        content_sha256=latest.get("content_sha256"),
        run_id="release",
    )


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = parser.add_subparsers(dest="command", required=True)
    boot = sub.add_parser("bootstrap")
    boot.add_argument("--through", type=date.fromisoformat, required=True)
    boot.add_argument("--run-id", default="manual")
    one = sub.add_parser("run")
    one.add_argument("--week", type=date.fromisoformat, required=True)
    one.add_argument("--run-id", default="manual")
    rel = sub.add_parser("release")
    rel.add_argument("--week", type=date.fromisoformat, required=True)
    rel.add_argument("--by", required=True)
    rel.add_argument("--reason", required=True)
    args = parser.parse_args(argv)
    if args.command == "bootstrap":
        result = bootstrap(args.through, args.run_id)
    elif args.command == "run":
        result = run(args.week, args.run_id)
    else:
        result = release(args.week, args.by, args.reason)
    sys.stdout.write(json.dumps(result, default=str) + "\n")


if __name__ == "__main__":
    main()
