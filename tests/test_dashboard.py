"""Dashboard tests using Streamlit's AppTest and an in-memory MongoDB (mongomock)."""
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import mongomock
import pytest
from streamlit.testing.v1 import AppTest

APP = str(Path(__file__).resolve().parents[1] / "dashboard" / "app.py")
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "producer"))


def make_long_records(n_polls=6, location="Manila", as_datetime=True):
    """Records shaped like producer.build_records output, as stored by the processor."""
    from producer import build_records

    base = datetime(2026, 10, 25, 2, 0, tzinfo=timezone.utc)  # 10:00 in Manila
    docs = []
    for i in range(n_polls):
        ts = base + timedelta(minutes=i)
        payload = {
            "location": {"name": location, "country": "Philippines"},
            "current": {"temp_c": 28.0 + i, "humidity": 70 + i, "wind_kph": 10.5 + i,
                        "precip_mm": 0.0 if i % 2 else 0.4, "condition": {"text": "Partly cloudy"}},
        }
        for rec in build_records(payload, timestamp=ts.isoformat()):
            rec["ingested_at"] = ts
            rec["timestamp"] = ts if as_datetime else rec["timestamp"]
            docs.append(rec)
    return docs


@pytest.fixture
def patched(monkeypatch):
    import pymongo

    client = mongomock.MongoClient(tz_aware=True)
    monkeypatch.setattr(pymongo, "MongoClient", lambda *a, **k: client)
    monkeypatch.setenv("MONGODB_URI", "mongodb://mongodb:27017")
    monkeypatch.setenv("MONGODB_DATABASE", "weather_db")
    monkeypatch.setenv("MONGODB_COLLECTION", "weather_data")
    monkeypatch.setenv("DASHBOARD_TIMEZONE", "Asia/Manila")
    monkeypatch.delenv("DASHBOARD_DEMO_MODE", raising=False)
    from streamlit import cache_data
    cache_data.clear()
    return client["weather_db"]["weather_data"]


def run():
    at = AppTest.from_file(APP, default_timeout=60).run()
    return at


def test_empty_collection_is_graceful(patched):
    at = run()
    assert not at.exception, at.exception
    assert any("no records yet" in m.value for m in at.markdown)


def test_connection_failure_is_graceful(monkeypatch):
    import pymongo
    from streamlit import cache_data

    def boom(*a, **k):
        raise pymongo.errors.ServerSelectionTimeoutError("no server")

    monkeypatch.setattr(pymongo, "MongoClient", boom)
    cache_data.clear()
    at = run()
    assert not at.exception, at.exception
    assert any("Could not connect to MongoDB" in m.value for m in at.markdown)


@pytest.mark.parametrize("as_datetime", [True, False])
def test_long_format_is_pivoted(patched, as_datetime):
    patched.insert_many(make_long_records(as_datetime=as_datetime))
    at = run()
    assert not at.exception, at.exception
    metrics = {m.label: m.value for m in at.metric}
    # newest poll i=5: temp 33.0, humidity 75, wind 15.5, precip 0.0
    assert metrics["Temperature"] == "33.0 °C"
    assert metrics["Humidity"] == "75%"
    assert metrics["Wind speed"] == "15.5 km/h"
    assert metrics["Rainfall, current"] == "0.0 mm"


def test_timestamps_are_converted_to_manila_time(patched):
    patched.insert_many(make_long_records())
    at = run()
    header = [m.value for m in at.markdown if "Updated" in m.value][0]
    assert "Oct 25, 10:05" in header  # 02:05 UTC + 8h


def test_missing_rainfall_shows_na(patched):
    docs = [d for d in make_long_records() if d["metric_type"] != "rainfall"]
    patched.insert_many(docs)
    at = run()
    assert not at.exception, at.exception
    assert {m.label: m.value for m in at.metric}["Rainfall, current"] == "n/a"


def test_incomplete_records_do_not_crash(patched):
    patched.insert_many([d for d in make_long_records() if d["metric_type"] == "humidity"])
    at = run()
    assert not at.exception, at.exception
    assert any("none contain" in m.value for m in at.markdown)


@pytest.mark.parametrize("page", ["Historical Data", "Analytics", "About Pipeline"])
def test_all_pages_render(patched, page):
    patched.insert_many(make_long_records(n_polls=10))
    at = run()
    at.sidebar.radio[0].set_value(page).run()
    assert not at.exception, at.exception


def test_demo_mode_is_explicit(patched, monkeypatch):
    monkeypatch.setenv("DASHBOARD_DEMO_MODE", "true")
    from streamlit import cache_data
    cache_data.clear()
    at = run()
    assert not at.exception, at.exception
    assert any("Demo data" in c.value for c in at.sidebar.caption)
