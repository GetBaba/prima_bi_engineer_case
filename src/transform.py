from __future__ import annotations

from typing import Optional

from logger import get_logger

logger = get_logger(__name__)

SOURCE_SYSTEM = "power_bi"

def _get_latest_refresh(refresh_history: Optional[list[dict]]) -> Optional[dict]:
    """Return the latest refresh entry based on endTime/startTime.

    We do not assume the API response is always ordered newest-first, so we always pick the latest based on timestamp.
    """
    if not refresh_history:
        return None

    return max(
        refresh_history,
        key=lambda x: x.get("endTime") or x.get("startTime") or "",
    )

def _derive_status(
    dataset_id: Optional[str],
    is_refreshable: bool,
    refresh_history: Optional[list[dict]],
) -> str:
    """Derive a normalised status value from dataset and refresh metadata.

    Status values:
      NOT_REFRESHABLE  — no linked dataset, or isRefreshable=False (live
                         DirectQuery, push datasets, Excel workbooks, etc.).
      REFRESH_UNKNOWN  — dataset is refreshable but history could not be
                         retrieved (permissions gap or unexpected API status).
      NEVER_REFRESHED  — dataset is refreshable, history was successfully
                         fetched, but no entries exist yet.
      SUCCESS          — most recent refresh status is "Completed".
      FAILED           — most recent refresh status is "Failed".
    """

    # Note that these are distinct failure modes: no dataset at all vs. dataset exists but isn't refreshable
    if not dataset_id:
        return "NOT_REFRESHABLE"
    if not is_refreshable:
        return "NOT_REFRESHABLE"

    if refresh_history is None:
        # None means the API call failed, we don't know the real state
        return "REFRESH_UNKNOWN"

    if not refresh_history:
        return "NEVER_REFRESHED"

    latest = _get_latest_refresh(refresh_history)
    raw_status = latest.get("status", "").lower()

    if raw_status == "completed":
        return "SUCCESS"

    if raw_status == "failed":
        return "FAILED"

    # Unrecognised status (e.g. "unknown", "inProgress"), don't assume absence
    return "REFRESH_UNKNOWN"

def build_asset_record(
    report: dict,
    workspace: dict,
    dataset: Optional[dict],
    refresh_history: Optional[list[dict]],
    ingested_at: str,
) -> dict:
    """Map raw Power BI API objects to a single bi_assets row.

    Args:
        report:          A single item from GET /groups/{id}/reports.
        workspace:       The parent group object from GET /groups.
        dataset:         The matching dataset object, or None if the report has
                         no datasetId or the dataset wasn't found.
        refresh_history: Refresh entries (newest first), or None if the history
                         could not be retrieved (permissions gap etc.).
        ingested_at:     ISO 8601 timestamp of this pipeline run.

    Returns:
        A dict whose keys match the bi_assets table schema exactly.
    """
    dataset_id: Optional[str] = report.get("datasetId")
    is_refreshable: bool = dataset.get("isRefreshable", False) if dataset else False

    # 'configuredBy' is the UPN of the user who last configured the dataset
    # Requires Dataset.Read.All permission. May be None on shared/external datasets
    owner: Optional[str] = dataset.get("configuredBy") if dataset else None

    # The API returns endTime as an ISO 8601 string
    # Fall back to startTime when endTime is absent (e.g. "in-progress" or "aborted refresh")
    last_refresh_at: Optional[str] = None
    latest_refresh = _get_latest_refresh(refresh_history)
    if latest_refresh:
        last_refresh_at = (
            latest_refresh.get("endTime") or latest_refresh.get("startTime")
        )

    return {
        "asset_id": report.get("id", ""),
        "asset_name": report.get("name", ""),
        "workspace_id": workspace.get("id", ""),
        "workspace_name": workspace.get("name", ""),
        "dataset_id": dataset_id,
        "owner": owner,
        "last_refresh_at": last_refresh_at,
        # These require the Admin API (getActivityEvents) — NULL for now
        "last_updated": dataset.get("createdDate") if dataset else None,
        "views_last_30d": None,
        "last_viewed": None,
        "status": _derive_status(dataset_id, is_refreshable, refresh_history),
        "source_system": SOURCE_SYSTEM,
        "ingested_at": ingested_at,
    }


def transform(
    workspaces: list[dict],
    reports_by_workspace: dict[str, list[dict]],
    datasets_by_workspace: dict[str, list[dict]],
    refresh_by_dataset: dict[str, list[dict] | None],
    ingested_at: str,
) -> list[dict]:
    """Transform all fetched Power BI metadata into a flat list of bi_asset rows.

    Iterates over every workspace → every report, looks up the parent dataset and
    its refresh history, and calls build_asset_record for each report.
    """
    records: list[dict] = []

    for workspace in workspaces:
        ws_id = workspace.get("id", "")
        reports = reports_by_workspace.get(ws_id, [])
        datasets = datasets_by_workspace.get(ws_id, [])

        # Index datasets by ID for O(1) lookup instead of O(n) scan per report
        dataset_map: dict[str, dict] = {d["id"]: d for d in datasets if d.get("id")}

        for report in reports:
            dataset_id = report.get("datasetId")
            dataset = dataset_map.get(dataset_id) if dataset_id else None

            # Prefer the confirmed dataset object's ID for the refresh lookup so we
            # don't accidentally use a dataset_id that belongs to a cross-workspace
            # dataset (which would not be in refresh_by_dataset for this workspace)
            
            lookup_id = dataset.get("id") if dataset is not None else dataset_id

            if not lookup_id:
                history = []
            elif dataset and dataset.get("isRefreshable", False):
                # Missing key should not silently become NEVER_REFRESHED
                history = refresh_by_dataset.get(lookup_id)
            else:
                history = []

            record = build_asset_record(
                report=report,
                workspace=workspace,
                dataset=dataset,
                refresh_history=history,
                ingested_at=ingested_at,
            )
            records.append(record)
            logger.debug(
                "Report '%s' → status=%s", record["asset_name"], record["status"]
            )

    logger.info("Transformed %d report(s) into bi_asset records", len(records))
    return records
