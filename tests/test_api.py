"""FastAPI 接口测试（需要 fastapi/httpx，随 requirements-dev.txt 安装）：

    pytest tests/test_api.py -v
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

fastapi = pytest.importorskip("fastapi")
from fastapi.testclient import TestClient  # noqa: E402

from app.config import Settings  # noqa: E402
from app.main import create_app  # noqa: E402


@pytest.fixture()
def client(tmp_path):
    settings = Settings(
        database_path=str(tmp_path / "api.db"),
        silence_interval_seconds=60.0,
    )
    with TestClient(create_app(settings)) as c:
        yield c


def test_submit_and_get_segment(client):
    resp = client.post("/events", json={
        "source": "gw-01", "event_id": "e1",
        "occurred_at": "2026-09-20T08:00:00Z", "severity": "warning",
    })
    assert resp.status_code == 200
    body = resp.json()
    assert body["deduplicated"] is False
    seg = body["segment"]
    assert seg["event_count"] == 1
    assert seg["revision"] == 1
    assert seg["max_severity"] == "warning"
    assert seg["started_at"] == seg["ended_at"]

    got = client.get(f"/segments/{seg['id']}")
    assert got.status_code == 200
    assert got.json() == seg


def test_duplicate_submission_keeps_count(client):
    payload = {
        "source": "gw-01", "event_id": "e1",
        "occurred_at": "2026-09-20T08:00:00Z", "severity": "info",
    }
    first = client.post("/events", json=payload).json()
    second = client.post("/events", json=payload).json()
    assert second["deduplicated"] is True
    assert second["segment"]["event_count"] == first["segment"]["event_count"] == 1


def test_late_out_of_order_event_bumps_revision(client):
    client.post("/events", json={
        "source": "gw-01", "event_id": "e1",
        "occurred_at": "2026-09-20T08:00:30Z", "severity": "info",
    })
    late = client.post("/events", json={
        "source": "gw-01", "event_id": "e2",
        "occurred_at": "2026-09-20T08:00:00Z", "severity": "error",
    }).json()
    seg = late["segment"]
    assert seg["revision"] == 2
    assert seg["max_severity"] == "error"
    assert seg["started_at"] == "2026-09-20T08:00:00+00:00"
    assert seg["ended_at"] == "2026-09-20T08:00:30+00:00"
    assert seg["event_count"] == 2


def test_list_segments_filter_by_source(client):
    for source, eid in [("a", "e1"), ("b", "e2"), ("a", "e3")]:
        client.post("/events", json={
            "source": source, "event_id": eid,
            "occurred_at": "2026-09-20T08:00:00Z", "severity": "info",
        })
    assert len(client.get("/segments").json()) == 2
    segs = client.get("/segments", params={"source": "a"}).json()
    assert len(segs) == 1
    assert segs[0]["event_count"] == 2


def test_invalid_severity_rejected(client):
    resp = client.post("/events", json={
        "source": "gw-01", "event_id": "e1",
        "occurred_at": "2026-09-20T08:00:00Z", "severity": "fatal-error",
    })
    assert resp.status_code == 422


def test_missing_field_rejected(client):
    resp = client.post("/events", json={"source": "gw-01", "event_id": "e1"})
    assert resp.status_code == 422


def test_get_missing_segment_404(client):
    assert client.get("/segments/9999").status_code == 404


def test_healthz(client):
    assert client.get("/healthz").json() == {"status": "ok"}
