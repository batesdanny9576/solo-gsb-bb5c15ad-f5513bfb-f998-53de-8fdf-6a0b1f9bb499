"""事件归段核心逻辑（仅依赖标准库，便于独立测试）。

归段规则：
- 同一 source 下，事件 occurred_at 与某事件段的 [start, end] 相距不超过
  silence 秒（含段外前后各 silence 的扩展窗口）即归入该段；
- 乱序/迟到事件可能同时落在多个段的扩展窗口内，或桥接相邻段，
  此时把这些段合并为一段（链式传导，直到不再扩大）；
- (source, event_id) 唯一，重复提交直接返回原段，不改变任何计数；
- revision 初始为 1，仅当新事件导致段的起止边界或最高级别发生变化时递增，
  仅事件数增加不递增。
"""
from __future__ import annotations

import sqlite3
import time
from datetime import datetime, timezone

from . import severity as sev


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def _row_to_segment(row: sqlite3.Row) -> dict:
    return {
        "id": row["id"],
        "source": row["source"],
        "started_at": _iso(row["start_ts"]),
        "ended_at": _iso(row["end_ts"]),
        "max_severity": row["max_severity"],
        "event_count": row["event_count"],
        "revision": row["revision"],
    }


def get_segment(conn: sqlite3.Connection, segment_id: int) -> dict | None:
    row = conn.execute(
        "SELECT * FROM segments WHERE id = ?", (segment_id,)
    ).fetchone()
    return _row_to_segment(row) if row else None


def list_segments(conn: sqlite3.Connection, source: str | None = None) -> list[dict]:
    if source is None:
        rows = conn.execute("SELECT * FROM segments ORDER BY id").fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM segments WHERE source = ? ORDER BY id", (source,)
        ).fetchall()
    return [_row_to_segment(r) for r in rows]


def _find_merge_candidates(
    conn: sqlite3.Connection, source: str, occurred_ts: float, silence: float
) -> list[sqlite3.Row]:
    """找出应与新事件合并的所有段。

    新事件的初始窗口为 [occurred - silence, occurred + silence]；
    每并入一个段，窗口随该段边界继续向外扩展，循环直至收敛，
    从而正确处理一个迟到事件桥接多个段的链式情形。
    """
    lo, hi = occurred_ts - silence, occurred_ts + silence
    merged: dict[int, sqlite3.Row] = {}
    while True:
        rows = conn.execute(
            """
            SELECT * FROM segments
            WHERE source = ? AND start_ts <= ? AND end_ts >= ?
            ORDER BY id
            """,
            (source, hi, lo),
        ).fetchall()
        fresh = {r["id"]: r for r in rows if r["id"] not in merged}
        if not fresh:
            return [merged[k] for k in sorted(merged)]
        merged.update(fresh)
        lo = min([lo] + [r["start_ts"] - silence for r in fresh.values()])
        hi = max([hi] + [r["end_ts"] + silence for r in fresh.values()])


def ingest_event(
    conn: sqlite3.Connection,
    *,
    source: str,
    event_id: str,
    occurred_ts: float,
    severity: str,
    silence: float,
) -> dict:
    """写入一个事件并返回 {"deduplicated": bool, "segment": {...}}。

    调用方需持有事务（建议 BEGIN IMMEDIATE）并保证串行化。
    """
    now = time.time()

    dup = conn.execute(
        "SELECT segment_id FROM events WHERE source = ? AND event_id = ?",
        (source, event_id),
    ).fetchone()
    if dup:
        return {"deduplicated": True, "segment": get_segment(conn, dup["segment_id"])}

    candidates = _find_merge_candidates(conn, source, occurred_ts, silence)

    if not candidates:
        cur = conn.execute(
            """
            INSERT INTO segments
                (source, start_ts, end_ts, max_severity, event_count, revision,
                 created_at, updated_at)
            VALUES (?, ?, ?, ?, 1, 1, ?, ?)
            """,
            (source, occurred_ts, occurred_ts, severity, now, now),
        )
        segment_id = cur.lastrowid
    else:
        survivor = candidates[0]  # id 最小者作为保留段
        segment_id = survivor["id"]

        start_ts = min([occurred_ts] + [c["start_ts"] for c in candidates])
        end_ts = max([occurred_ts] + [c["end_ts"] for c in candidates])
        max_severity = sev.highest(severity, *[c["max_severity"] for c in candidates])
        event_count = 1 + sum(c["event_count"] for c in candidates)

        changed = (
            start_ts != survivor["start_ts"]
            or end_ts != survivor["end_ts"]
            or max_severity != survivor["max_severity"]
        )
        revision = max(c["revision"] for c in candidates) + (1 if changed else 0)

        others = [c["id"] for c in candidates[1:]]
        if others:
            marks = ",".join("?" * len(others))
            conn.execute(
                f"UPDATE events SET segment_id = ? WHERE segment_id IN ({marks})",
                [segment_id, *others],
            )
            conn.execute(f"DELETE FROM segments WHERE id IN ({marks})", others)

        conn.execute(
            """
            UPDATE segments
            SET start_ts = ?, end_ts = ?, max_severity = ?,
                event_count = ?, revision = ?, updated_at = ?
            WHERE id = ?
            """,
            (start_ts, end_ts, max_severity, event_count, revision, now, segment_id),
        )

    conn.execute(
        """
        INSERT INTO events (segment_id, source, event_id, occurred_at, severity, received_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (segment_id, source, event_id, occurred_ts, severity, now),
    )
    return {"deduplicated": False, "segment": get_segment(conn, segment_id)}
