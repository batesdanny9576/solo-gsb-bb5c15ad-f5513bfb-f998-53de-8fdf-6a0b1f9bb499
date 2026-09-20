"""核心归段逻辑测试：仅依赖标准库，可直接运行：

    python3 -m unittest tests.test_service -v
"""
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.db import init_db, session  # noqa: E402
from app import service  # noqa: E402

SILENCE = 10.0


class ServiceTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "test.db")
        init_db(self.db_path)

    def tearDown(self):
        self.tmp.cleanup()

    def submit(self, source, event_id, occurred_ts, severity="info"):
        with session(self.db_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            return service.ingest_event(
                conn,
                source=source,
                event_id=event_id,
                occurred_ts=occurred_ts,
                severity=severity,
                silence=SILENCE,
            )

    def get(self, segment_id):
        with session(self.db_path) as conn:
            return service.get_segment(conn, segment_id)

    def test_first_event_creates_segment(self):
        r = self.submit("s1", "e1", 1000.0, "warning")
        self.assertFalse(r["deduplicated"])
        seg = r["segment"]
        self.assertEqual(seg["event_count"], 1)
        self.assertEqual(seg["revision"], 1)
        self.assertEqual(seg["max_severity"], "warning")
        self.assertEqual(seg["started_at"], seg["ended_at"])

    def test_duplicate_event_id_is_idempotent(self):
        r1 = self.submit("s1", "e1", 1000.0)
        r2 = self.submit("s1", "e1", 1000.0)
        self.assertTrue(r2["deduplicated"])
        seg = self.get(r1["segment"]["id"])
        self.assertEqual(seg["event_count"], 1)
        self.assertEqual(seg["revision"], 1)

    def test_same_event_id_different_source_not_duplicate(self):
        self.submit("s1", "e1", 1000.0)
        r = self.submit("s2", "e1", 1000.0)
        self.assertFalse(r["deduplicated"])
        self.assertEqual(r["segment"]["source"], "s2")

    def test_out_of_order_event_extends_start_and_bumps_revision(self):
        r1 = self.submit("s1", "e1", 1000.0)
        r2 = self.submit("s1", "e2", 995.0)  # 更早到达的乱序事件
        self.assertEqual(r2["segment"]["id"], r1["segment"]["id"])
        seg = r2["segment"]
        self.assertEqual(seg["event_count"], 2)
        self.assertEqual(seg["revision"], 2)  # 边界变化 → 递增
        self.assertEqual(seg["started_at"], "1970-01-01T00:16:35+00:00")  # 995.0

    def test_inner_event_lower_severity_keeps_revision(self):
        self.submit("s1", "e1", 1000.0, "error")
        r = self.submit("s1", "e2", 1010.0, "info")  # 扩展 end → rev=2
        self.assertEqual(r["segment"]["revision"], 2)
        r = self.submit("s1", "e3", 1005.0, "debug")  # 严格位于段内、级别更低
        self.assertEqual(r["segment"]["event_count"], 3)
        self.assertEqual(r["segment"]["revision"], 2)  # 边界与级别未变
        self.assertEqual(r["segment"]["max_severity"], "error")

    def test_inner_event_higher_severity_bumps_revision(self):
        self.submit("s1", "e1", 1000.0, "info")
        r = self.submit("s1", "e2", 1003.0, "critical")
        self.assertEqual(r["segment"]["revision"], 2)
        self.assertEqual(r["segment"]["max_severity"], "critical")

    def test_event_beyond_silence_creates_new_segment(self):
        r1 = self.submit("s1", "e1", 1000.0)
        r2 = self.submit("s1", "e2", 1000.0 + SILENCE + 0.1)
        self.assertNotEqual(r1["segment"]["id"], r2["segment"]["id"])

    def test_event_at_exact_silence_boundary_joins_segment(self):
        r1 = self.submit("s1", "e1", 1000.0)
        r2 = self.submit("s1", "e2", 1000.0 + SILENCE)
        self.assertEqual(r1["segment"]["id"], r2["segment"]["id"])

    def test_late_event_bridges_two_segments(self):
        r1 = self.submit("s1", "e1", 0.0)          # 段 A [0, 0]
        r2 = self.submit("s1", "e2", 11.0)         # 11 > 0+10 → 段 B [11, 11]
        self.assertNotEqual(r1["segment"]["id"], r2["segment"]["id"])
        r3 = self.submit("s1", "e3", 5.0)          # 桥接 A 与 B
        seg = r3["segment"]
        self.assertEqual(seg["id"], r1["segment"]["id"])
        self.assertEqual(seg["event_count"], 3)
        self.assertEqual(seg["revision"], 2)
        with session(self.db_path) as conn:
            self.assertEqual(len(service.list_segments(conn, "s1")), 1)

    def test_chain_merge_via_fixpoint(self):
        self.submit("s1", "e1", 0.0)               # 段 A
        self.submit("s1", "e2", 11.0)              # 段 B
        self.submit("s1", "e3", 22.0)              # 22 > 11+10 → 段 C
        self.submit("s1", "e4", 5.0)               # 合并 A+B → [0, 11]
        r = self.submit("s1", "e5", 16.0)          # 桥接 [0,11] 与 [22,22]
        seg = r["segment"]
        self.assertEqual(seg["event_count"], 5)
        with session(self.db_path) as conn:
            segs = service.list_segments(conn, "s1")
        self.assertEqual(len(segs), 1)
        self.assertTrue(segs[0]["ended_at"].startswith("1970-01-01T00:00:22"))

    def test_sources_are_isolated(self):
        self.submit("s1", "e1", 1000.0)
        r = self.submit("s2", "e2", 1001.0)  # 时间接近但 source 不同
        self.assertEqual(r["segment"]["event_count"], 1)
        with session(self.db_path) as conn:
            self.assertEqual(len(service.list_segments(conn)), 2)

    def test_revision_never_decreases_after_merge(self):
        self.submit("s1", "e1", 0.0, "info")
        self.submit("s1", "e2", 1.0, "critical")   # 段 A rev=2
        self.submit("s1", "e3", 21.0, "info")      # 21 > 1+10 → 段 B rev=1
        r = self.submit("s1", "e4", 11.0)          # 桥接 A、B
        self.assertGreaterEqual(r["segment"]["revision"], 3)


if __name__ == "__main__":
    unittest.main()
