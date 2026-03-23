"""Tests for the transform module — pure functions, no mocking needed."""

import sys
from pathlib import Path

# Add src/ to the path so we can import the modules directly
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from transform import _derive_status, _get_latest_refresh, build_asset_record, transform

# _derive_status

class TestDeriveStatus:
    def test_no_dataset_id(self):
        assert _derive_status(None, True, []) == "NO_DATASET"

    def test_not_refreshable(self):
        assert _derive_status("ds-001", False, []) == "NOT_REFRESHABLE"

    def test_refresh_history_none_means_unknown(self):
        """None = API call failed, distinct from empty list."""
        assert _derive_status("ds-001", True, None) == "REFRESH_UNKNOWN"

    def test_empty_history_means_never_refreshed(self):
        assert _derive_status("ds-001", True, []) == "NEVER_REFRESHED"

    def test_completed_maps_to_success(self):
        history = [{"status": "Completed", "endTime": "2026-01-01T00:00:00Z"}]
        assert _derive_status("ds-001", True, history) == "SUCCESS"

    def test_failed_maps_to_failed(self):
        history = [{"status": "Failed", "endTime": "2026-01-01T00:00:00Z"}]
        assert _derive_status("ds-001", True, history) == "FAILED"

    def test_unrecognised_status_maps_to_unknown(self):
        history = [{"status": "Unknown", "endTime": "2026-01-01T00:00:00Z"}]
        assert _derive_status("ds-001", True, history) == "REFRESH_UNKNOWN"


# _get_latest_refresh

class TestGetLatestRefresh:
    def test_empty_list(self):
        assert _get_latest_refresh([]) is None

    def test_none_input(self):
        assert _get_latest_refresh(None) is None

    def test_picks_latest_by_end_time(self):
        history = [
            {"status": "Failed", "endTime": "2026-01-01T00:00:00Z"},
            {"status": "Completed", "endTime": "2026-03-01T00:00:00Z"},
        ]
        assert _get_latest_refresh(history)["status"] == "Completed"

    def test_falls_back_to_start_time(self):
        history = [{"status": "Unknown", "startTime": "2026-02-01T00:00:00Z"}]
        assert _get_latest_refresh(history)["startTime"] == "2026-02-01T00:00:00Z"

# build_asset_record

class TestBuildAssetRecord:
    def test_basic_success_record(self):
        report = {"id": "rpt-1", "name": "Sales", "datasetId": "ds-1"}
        workspace = {"id": "ws-1", "name": "Finance"}
        dataset = {"id": "ds-1", "isRefreshable": True, "configuredBy": "alice@co.com", "createdDate": "2025-01-01T00:00:00Z"}
        history = [{"status": "Completed", "endTime": "2026-03-22T02:00:00Z"}]

        record = build_asset_record(report, workspace, dataset, history, "2026-03-22T03:00:00Z")

        assert record["asset_id"] == "rpt-1"
        assert record["status"] == "SUCCESS"
        assert record["owner"] == "alice@co.com"
        assert record["last_refresh_at"] == "2026-03-22T02:00:00Z"
        assert record["views_last_30d"] is None  # not yet populated
        assert record["last_viewed"] is None
        assert record["last_updated"] == "2025-01-01T00:00:00Z"

    def test_no_dataset(self):
        report = {"id": "rpt-2", "name": "Ad-hoc"}
        workspace = {"id": "ws-1", "name": "Finance"}

        record = build_asset_record(report, workspace, None, [], "2026-03-22T03:00:00Z")

        assert record["status"] == "NO_DATASET"
        assert record["owner"] is None


# transform (end-to-end)

class TestTransform:
    def test_produces_correct_count(self):
        workspaces = [{"id": "ws-1", "name": "Test"}]
        reports = {"ws-1": [
            {"id": "r1", "name": "Report A", "datasetId": "ds-1"},
            {"id": "r2", "name": "Report B", "datasetId": "ds-2"},
            {"id": "r3", "name": "Report C"},
        ]}
        datasets = {"ws-1": [
            {"id": "ds-1", "isRefreshable": True, "configuredBy": "a@b.com", "createdDate": "2025-01-01T00:00:00Z"},
            {"id": "ds-2", "isRefreshable": False, "createdDate": "2025-06-01T00:00:00Z"},
        ]}
        refresh = {"ds-1": [{"status": "Completed", "endTime": "2026-01-01T00:00:00Z"}]}

        records = transform(workspaces, reports, datasets, refresh, "2026-03-22T00:00:00Z")

        assert len(records) == 3
        assert records[0]["status"] == "SUCCESS"
        assert records[1]["status"] == "NOT_REFRESHABLE"
        assert records[2]["status"] == "NO_DATASET"
