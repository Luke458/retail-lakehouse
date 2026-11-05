"""The gate's bookkeeping: week numbers, decisions, releases. verity itself is not run here."""

from datetime import date

import pytest

from retail_lakehouse import gate

WEEK = date(2016, 1, 12)


@pytest.fixture(autouse=True)
def lake(tmp_path, monkeypatch):
    monkeypatch.setenv("LAKE_ROOT", str(tmp_path))
    return tmp_path


def test_week_numbers_match_veritys_favorita_runs():
    assert gate.week_number(date(2013, 1, 8)) == 1
    assert gate.week_number(date(2016, 1, 5)) == 157


def test_the_latest_decision_wins():
    gate.write_decision(WEEK, approved=True, status="PASS", reason="verity")
    gate.write_decision(WEEK, approved=False, status="INVESTIGATE", reason="verity")
    assert gate.latest_decision(WEEK)["status"] == "INVESTIGATE"
    assert gate.latest_decision(date(2016, 1, 19)) is None


def test_release_records_a_person_and_keeps_veritys_status():
    gate.write_decision(WEEK, approved=False, status="INVESTIGATE", reason="verity", content_sha256="abc")
    released = gate.release(WEEK, by="luke", reason="store opening confirmed")
    assert released["approved"] and released["status"] == "INVESTIGATE"
    assert released["released_by"] == "luke" and "store opening" in released["reason"]
    assert gate.latest_decision(WEEK) == released


def test_release_refuses_a_week_that_is_not_quarantined():
    with pytest.raises(SystemExit, match="no gate decision"):
        gate.release(WEEK, by="luke", reason="x")
    gate.write_decision(WEEK, approved=True, status="PASS", reason="verity")
    with pytest.raises(SystemExit, match="not quarantined"):
        gate.release(WEEK, by="luke", reason="x")


def test_only_passing_statuses_approve():
    assert gate.APPROVING == {"PASS", "PASS_WITH_EXPLANATION"}


def test_summarize_keeps_veritys_review_findings_largest_first():
    machine = {
        "findings": [
            {"check": "temporal", "disposition": "EXPLAINED", "scope": "S1", "impact": 900.0},
            {
                "check": "temporal",
                "disposition": "UNEXPLAINED_ANOMALY",
                "scope_type": "store_id",
                "scope": "S2",
                "metric": "units",
                "period": 160,
                "impact": -40.0,
            },
            {
                "check": "absence",
                "disposition": "HARD_FAILURE",
                "scope_type": "product_id",
                "scope": "I7",
                "metric": "units",
                "period": 160,
                "impact": 120.0,
            },
        ]
    }
    kept = [(f["check"], f["scope"]) for f in gate.summarize(machine)]
    assert kept == [("absence", "I7"), ("temporal", "S2")]
    assert gate.summarize({}) == []


def test_summarize_names_materially_missing_entities_first():
    machine = {
        "missing_entities": {"store": {"material": True}, "product": {"material": False}},
        "events": [
            {
                "classification": "LATEST_WEEK_MISSING",
                "entity_type": "store",
                "entity_id": "S44",
                "expected_value": 302515.4,
            },
            {
                "classification": "LATEST_WEEK_MISSING",
                "entity_type": "product",
                "entity_id": "I9",
                "expected_value": 10.0,
            },
            {
                "classification": "NEW_ENTITY",
                "entity_type": "store",
                "entity_id": "S99",
                "expected_value": 0.0,
            },
        ],
        "findings": [
            {
                "check": "temporal",
                "disposition": "UNEXPLAINED_ANOMALY",
                "scope": "Guayas",
                "impact": 900000.0,
            },
        ],
    }
    kept = [(f["check"], f["scope"], f["impact"]) for f in gate.summarize(machine)]
    assert kept == [("missing", "S44", -302515.4), ("temporal", "Guayas", 900000.0)]


def test_summarize_states_the_size_of_a_material_revision():
    machine = {
        "historical_revision": {"material": True, "raw_delta": -295512.066},
        "findings": [{"check": "historical_revision", "disposition": "UNEXPLAINED_ANOMALY", "impact": 0.0}],
    }
    first = gate.summarize(machine)[0]
    assert (first["check"], first["disposition"], first["impact"]) == (
        "historical_revision",
        "MATERIAL",
        -295512.066,
    )
    assert gate.summarize({"historical_revision": {"material": False, "raw_delta": 2058.0}}) == []
