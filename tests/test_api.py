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


def _post(client, source, event_id, occurred_at, severity="info"):
    return client.post("/events", json={
        "source": source, "event_id": event_id,
        "occurred_at": occurred_at, "severity": severity,
    })


def test_segment_changes_upsert_only_on_visible_change(client):
    _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z", "warning")
    _post(client, "gw-01", "e2", "2026-09-20T08:00:30Z", "warning")  # 扩展 end
    # 幂等重放与段内低级别事件（仅计数增加）均不产生变更
    _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z", "warning")
    _post(client, "gw-01", "e3", "2026-09-20T08:00:15Z", "debug")

    resp = client.get("/segment-changes")
    assert resp.status_code == 200
    body = resp.json()
    assert [c["change_id"] for c in body["changes"]] == [1, 2]
    assert all(c["change_type"] == "upsert" for c in body["changes"])
    assert body["changes"][0]["segment"]["revision"] == 1
    assert body["changes"][1]["segment"]["revision"] == 2
    assert body["changes"][1]["segment"]["event_count"] == 2
    assert body["next_after"] == 2

    # 逐页拉取与全量一致；空页回显 after
    ids, after = [], 0
    while True:
        page = client.get("/segment-changes",
                          params={"after": after, "limit": 1}).json()
        ids.extend(c["change_id"] for c in page["changes"])
        after = page["next_after"]
        if not page["changes"]:
            break
    assert ids == [1, 2]
    assert after == 2


def test_segment_changes_merge_tombstone(client):
    a = _post(client, "gw-02", "e1", "2026-09-20T08:00:00Z").json()
    b = _post(client, "gw-02", "e2", "2026-09-20T08:02:00Z").json()  # 超静默间隔 → 新段
    _post(client, "gw-02", "e3", "2026-09-20T08:01:00Z")             # 桥接两段

    changes = client.get("/segment-changes").json()["changes"]
    assert [c["change_type"] for c in changes] == [
        "upsert", "upsert", "merged", "upsert",
    ]
    tomb = changes[2]
    assert tomb["segment_id"] == b["segment"]["id"]
    assert tomb["merged_into"] == a["segment"]["id"]
    assert tomb["segment"] is None
    final = changes[3]
    assert final["segment_id"] == a["segment"]["id"]
    assert final["segment"]["event_count"] == 3
    assert final["segment"]["revision"] == 2


def test_segment_changes_params_validated(client):
    assert client.get("/segment-changes", params={"limit": 0}).status_code == 422
    assert client.get("/segment-changes", params={"limit": 1001}).status_code == 422
    assert client.get("/segment-changes", params={"after": -1}).status_code == 422


def test_healthz(client):
    assert client.get("/healthz").json() == {"status": "ok"}
