"""Archive-disk offload: transfer, registration, restore and failure paths."""

import hashlib
import io
import json
import os
import shutil
import sqlite3
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import app as appmod  # noqa: E402


class ArchiveFlowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.mkdtemp(prefix="qc-archive-test-")
        cls.db_path = os.path.join(cls.tmp, "test.db")
        cls.archive_dir = os.path.join(cls.tmp, "archive")
        os.makedirs(cls.archive_dir, exist_ok=True)
        appmod.app.config.update(
            TESTING=True,
            DATABASE=cls.db_path,
            ARCHIVE_DIR=cls.archive_dir,
        )
        appmod.init_db()

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmp, ignore_errors=True)

    def setUp(self):
        conn = self.db()
        conn.executescript(
            "DELETE FROM determinations; DELETE FROM details; "
            "DELETE FROM summaries; DELETE FROM coils; DELETE FROM batches;"
        )
        conn.commit()
        conn.close()
        os.makedirs(self.archive_dir, exist_ok=True)
        for name in os.listdir(self.archive_dir):
            os.unlink(os.path.join(self.archive_dir, name))
        self.client = appmod.app.test_client()
        self.client.post("/batches", data={"batch_no": "B1"})
        self.bid = self.db().execute("SELECT id FROM batches").fetchone()["id"]

    # -- helpers -----------------------------------------------------------

    def db(self):
        conn = sqlite3.connect(appmod.app.config["DATABASE"])
        conn.row_factory = sqlite3.Row
        return conn

    def summary(self, coil, n, mn, p, mx):
        return self.client.post(
            f"/batch/{self.bid}/summary",
            data={"coil_no": coil, "point_count": n, "min_um": mn,
                  "p95_um": p, "max_um": mx},
            follow_redirects=True,
        )

    def detail(self, coil, raw):
        return self.client.post(
            f"/batch/{self.bid}/detail",
            content_type="multipart/form-data",
            data={"coil_no": coil, "readings": (io.BytesIO(raw), "c.csv")},
            follow_redirects=True,
        )

    def retire(self, did):
        return self.client.post(
            f"/batch/{self.bid}/detail/{did}/retire", follow_redirects=True)

    def activate(self, did):
        return self.client.post(
            f"/batch/{self.bid}/detail/{did}/activate", follow_redirects=True)

    def archive(self, did):
        return self.client.post(
            f"/batch/{self.bid}/detail/{did}/archive", follow_redirects=True)

    def only_detail_id(self):
        return self.db().execute("SELECT id FROM details").fetchone()["id"]

    def detail_row(self, did):
        return self.db().execute(
            "SELECT * FROM details WHERE id = ?", (did,)).fetchone()

    def archive_files(self):
        return sorted(os.listdir(self.archive_dir))

    def archive_payload(self, did):
        with open(os.path.join(self.archive_dir, f"detail-{did}.json")) as fh:
            return json.load(fh)

    @staticmethod
    def fingerprint(readings):
        canon = json.dumps([int(v) for v in readings], separators=(",", ":"))
        return hashlib.sha256(canon.encode("utf-8")).hexdigest()

    def make_retired_detail(self, raw=b"300\n100\n"):
        """A matching detail (uploaded active) that is then retired."""
        self.summary("C1", 2, 100, 300, 300)
        self.detail("C1", raw)
        did = self.only_detail_id()
        self.retire(did)
        return did

    # -- archiving ---------------------------------------------------------

    def test_archive_writes_file_and_clears_library(self):
        did = self.make_retired_detail()
        text = self.archive(did).get_data(as_text=True)
        self.assertIn("库内读数已清除", text)

        row = self.detail_row(did)
        self.assertEqual(row["archived"], 1)
        self.assertEqual(row["active"], 0)
        self.assertEqual(row["readings"], "")
        self.assertEqual(row["readings_sha256"], self.fingerprint([300, 100]))
        self.assertTrue(row["archived_at"])

        self.assertEqual(self.archive_files(), [f"detail-{did}.json"])
        payload = self.archive_payload(did)
        self.assertEqual(payload["detail_id"], did)
        self.assertEqual(payload["readings"], [300, 100])  # original order
        self.assertEqual(payload["point_count"], 2)
        self.assertEqual(
            (payload["min_um"], payload["p95_um"], payload["max_um"]),
            (100, 300, 300),
        )
        self.assertEqual(payload["readings_sha256"], row["readings_sha256"])

    def test_archive_requires_retired_detail(self):
        self.summary("C1", 2, 100, 300, 300)
        self.detail("C1", b"300\n100\n")
        did = self.only_detail_id()
        text = self.archive(did).get_data(as_text=True)
        self.assertIn("请先停用", text)
        row = self.detail_row(did)
        self.assertEqual(row["archived"], 0)
        self.assertNotEqual(row["readings"], "")
        self.assertEqual(self.archive_files(), [])

    def test_archive_retry_keeps_single_official_file(self):
        did = self.make_retired_detail()
        self.archive(did)
        text = self.archive(did).get_data(as_text=True)
        self.assertIn("无需重复转存", text)
        self.assertEqual(self.archive_files(), [f"detail-{did}.json"])

    def test_unregistered_leftover_file_does_not_block_retry(self):
        # Simulates a crash after the file write but before registration:
        # the library still holds the readings and a retry converges on the
        # same single file.
        did = self.make_retired_detail()
        appmod.write_archive_file(
            self.archive_dir,
            did,
            {"detail_id": did, "readings": [300, 100],
             "readings_sha256": self.fingerprint([300, 100])},
        )
        row = self.detail_row(did)
        self.assertEqual(row["archived"], 0)
        self.assertNotEqual(row["readings"], "")

        text = self.archive(did).get_data(as_text=True)
        self.assertIn("库内读数已清除", text)
        row = self.detail_row(did)
        self.assertEqual(row["archived"], 1)
        self.assertEqual(row["readings"], "")
        self.assertEqual(self.archive_files(), [f"detail-{did}.json"])

    def test_enable_during_write_keeps_library_readings(self):
        did = self.make_retired_detail()
        original = appmod.write_archive_file

        def interleave(*args, **kwargs):
            result = original(*args, **kwargs)
            self.activate(did)   # enabled mid-write ...
            self.retire(did)     # ... then retired again before registration
            return result

        appmod.write_archive_file = interleave
        try:
            text = self.archive(did).get_data(as_text=True)
        finally:
            appmod.write_archive_file = original
        self.assertIn("本次不清除库内读数", text)
        row = self.detail_row(did)
        self.assertEqual(row["archived"], 0)
        self.assertEqual(json.loads(row["readings"]), [300, 100])
        # The unregistered file stays on disk as a recovery copy.
        self.assertEqual(self.archive_files(), [f"detail-{did}.json"])

        # A fresh retry now registers cleanly and clears the library.
        self.archive(did)
        row = self.detail_row(did)
        self.assertEqual(row["archived"], 1)
        self.assertEqual(row["readings"], "")
        self.assertEqual(self.archive_files(), [f"detail-{did}.json"])

    # -- restore / enable --------------------------------------------------

    def test_activate_restores_readings_from_archive(self):
        did = self.make_retired_detail()
        self.archive(did)
        text = self.activate(did).get_data(as_text=True)
        self.assertIn("恢复并启用", text)
        row = self.detail_row(did)
        self.assertEqual(row["active"], 1)
        self.assertEqual(row["archived"], 0)
        self.assertEqual(json.loads(row["readings"]), [300, 100])

        self.client.post(
            f"/batch/{self.bid}/determine",
            data={"limit_um": 300},
            follow_redirects=True,
        )
        det = self.db().execute(
            "SELECT snapshot_json FROM determinations ORDER BY id DESC LIMIT 1"
        ).fetchone()
        kinds = {c["coil_no"]: c["kind"]
                 for c in json.loads(det["snapshot_json"])["coils"]}
        self.assertEqual(kinds, {"C1": "detail"})

    def test_activate_missing_archive_stays_retired(self):
        did = self.make_retired_detail()
        self.archive(did)
        os.unlink(os.path.join(self.archive_dir, f"detail-{did}.json"))
        text = self.activate(did).get_data(as_text=True)
        self.assertIn("缺档", text)
        row = self.detail_row(did)
        self.assertEqual((row["active"], row["archived"]), (0, 1))

    def test_activate_tampered_archive_stays_retired(self):
        did = self.make_retired_detail()
        self.archive(did)
        path = os.path.join(self.archive_dir, f"detail-{did}.json")
        payload = self.archive_payload(did)
        payload["readings"] = [100, 300]  # same values, wrong order
        with open(path, "w") as fh:
            json.dump(payload, fh)
        text = self.activate(did).get_data(as_text=True)
        self.assertIn("指纹", text)
        row = self.detail_row(did)
        self.assertEqual((row["active"], row["archived"]), (0, 1))

    def test_activate_checks_current_summary(self):
        did = self.make_retired_detail()
        self.archive(did)
        self.summary("C1", 3, 100, 300, 300)  # summary moves on
        text = self.activate(did).get_data(as_text=True)
        self.assertIn("与当前摘要不符", text)
        row = self.detail_row(did)
        self.assertEqual((row["active"], row["archived"]), (0, 1))

    # -- history -----------------------------------------------------------

    def test_history_determination_survives_archive(self):
        self.summary("C1", 2, 100, 300, 300)
        self.detail("C1", b"300\n100\n")
        did = self.only_detail_id()
        self.client.post(f"/batch/{self.bid}/determine", data={"limit_um": 300})
        det = self.db().execute(
            "SELECT * FROM determinations ORDER BY id DESC LIMIT 1"
        ).fetchone()
        before = det["snapshot_json"]

        self.retire(did)
        self.archive(did)

        page = self.client.get(
            f"/batch/{self.bid}/determination/{det['id']}"
        ).get_data(as_text=True)
        self.assertIn("明细 #", page)
        again = self.db().execute(
            "SELECT snapshot_json FROM determinations WHERE id = ?", (det["id"],)
        ).fetchone()
        self.assertEqual(again["snapshot_json"], before)


if __name__ == "__main__":
    unittest.main()
