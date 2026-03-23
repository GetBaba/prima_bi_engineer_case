"""Tests for the storage module, uses an in-memory SQLite database"""

import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from storage import init_db, upsert_assets

SCHEMA_PATH = str(Path(__file__).resolve().parent.parent / "sql" / "schema.sql")


def _make_record(**overrides) -> dict:
    """Helper to build a valid bi_assets record with sensible defaults."""
    base = {
        "asset_id": "rpt-001",
        "asset_name": "Test Report",
        "workspace_id": "ws-001",
        "workspace_name": "Test Workspace",
        "dataset_id": "ds-001",
        "owner": "test@company.com",
        "last_refresh_at": "2026-03-22T02:00:00Z",
        "last_updated": None,
        "views_last_30d": None,
        "last_viewed": None,
        "status": "SUCCESS",
        "source_system": "power_bi",
        "ingested_at": "2026-03-22T03:00:00Z",
    }
    base.update(overrides)
    return base


class TestInitDb:
    def test_creates_table(self, tmp_path):
        db = str(tmp_path / "test.db")
        init_db(db, schema_path=SCHEMA_PATH)

        with sqlite3.connect(db) as conn:
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='bi_assets'"
            )
            assert cursor.fetchone() is not None

    def test_idempotent(self, tmp_path):
        """Running init_db twice should not error."""
        db = str(tmp_path / "test.db")
        init_db(db, schema_path=SCHEMA_PATH)
        init_db(db, schema_path=SCHEMA_PATH)


class TestUpsertAssets:
    def test_insert_and_read_back(self, tmp_path):
        db = str(tmp_path / "test.db")
        init_db(db, schema_path=SCHEMA_PATH)

        record = _make_record()
        upsert_assets(db, [record])

        with sqlite3.connect(db) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute("SELECT * FROM bi_assets WHERE asset_id = 'rpt-001'").fetchone()

        assert row["asset_name"] == "Test Report"
        assert row["status"] == "SUCCESS"

    def test_upsert_updates_existing(self, tmp_path):
        """Second insert with same asset_id should update, not duplicate."""
        db = str(tmp_path / "test.db")
        init_db(db, schema_path=SCHEMA_PATH)

        upsert_assets(db, [_make_record(status="SUCCESS")])
        upsert_assets(db, [_make_record(status="FAILED")])

        with sqlite3.connect(db) as conn:
            rows = conn.execute("SELECT * FROM bi_assets").fetchall()
            assert len(rows) == 1  # no duplicate

            conn.row_factory = sqlite3.Row
            row = conn.execute("SELECT status FROM bi_assets WHERE asset_id = 'rpt-001'").fetchone()
            assert row["status"] == "FAILED"

    def test_multiple_records(self, tmp_path):
        db = str(tmp_path / "test.db")
        init_db(db, schema_path=SCHEMA_PATH)

        records = [
            _make_record(asset_id="rpt-001"),
            _make_record(asset_id="rpt-002", asset_name="Second Report"),
        ]
        upsert_assets(db, records)

        with sqlite3.connect(db) as conn:
            count = conn.execute("SELECT COUNT(*) FROM bi_assets").fetchone()[0]
            assert count == 2
