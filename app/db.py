"""SQLite 连接与 schema 初始化（幂等，服务启动时自动执行）。"""
from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS segments (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    source        TEXT    NOT NULL,
    start_ts      REAL    NOT NULL,          -- 段内最早 occurred_at（epoch 秒）
    end_ts        REAL    NOT NULL,          -- 段内最晚 occurred_at（epoch 秒）
    max_severity  TEXT    NOT NULL,
    event_count   INTEGER NOT NULL,
    revision      INTEGER NOT NULL,
    created_at    REAL    NOT NULL,
    updated_at    REAL    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_segments_source ON segments (source, start_ts, end_ts);

CREATE TABLE IF NOT EXISTS events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    segment_id  INTEGER NOT NULL REFERENCES segments (id),
    source      TEXT    NOT NULL,
    event_id    TEXT    NOT NULL,
    occurred_at REAL    NOT NULL,            -- epoch 秒
    severity    TEXT    NOT NULL,
    received_at REAL    NOT NULL,
    UNIQUE (source, event_id)                -- 幂等去重键
);
CREATE INDEX IF NOT EXISTS idx_events_segment ON events (segment_id);

-- 段变更流：供下游增量同步，只增不改
CREATE TABLE IF NOT EXISTS segment_changes (
    change_id   INTEGER PRIMARY KEY AUTOINCREMENT,  -- 单调递增的变更序号
    segment_id  INTEGER NOT NULL,          -- upsert: 目标段；merged: 被删除段
    change_type TEXT    NOT NULL CHECK (change_type IN ('upsert', 'merged')),
    merged_into INTEGER,                   -- merged 时指向保留段 id，否则为 NULL
    revision    INTEGER NOT NULL,          -- upsert: 段的新 revision；merged: 被删段最后的 revision
    payload     TEXT,                      -- upsert: 变更后的段快照 JSON；merged: NULL
    created_at  REAL    NOT NULL
);
"""


def connect(db_path: str) -> sqlite3.Connection:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def init_db(db_path: str) -> None:
    """建表，幂等；Docker 启动时即完成数据库初始化。"""
    with connect(db_path) as conn:
        conn.executescript(SCHEMA)


@contextmanager
def session(db_path: str):
    """事务上下文：成功提交、异常回滚、保证关闭连接。"""
    conn = connect(db_path)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
