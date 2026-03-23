import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests

from config import DB_PATH, ROOT_DIR
from logger import get_logger
from storage import init_db, upsert_assets
from transform import transform

logger = get_logger(__name__)

FIXTURE_PATH = ROOT_DIR / "tests" / "fixtures" / "sample_api_response.json"


def _load_demo_data() -> tuple[list, dict, dict, dict]:
    """Load fixture data so the pipeline can run without Azure credentials."""
    logger.info("Running in demo mode — loading fixture data from %s", FIXTURE_PATH)
    data = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    return (
        data["workspaces"],
        data["reports_by_workspace"],
        data["datasets_by_workspace"],
        data["refresh_by_dataset"],
    )


def _fetch_from_api() -> tuple[list, dict, dict, dict]:
    """Fetch live data from the Power BI REST API."""
    # Deferred imports, these require Azure credentials which aren't needed in demo mode, so we only load them when actually calling the API
    from auth import get_access_token
    from powerbi_client import PowerBIClient

    token = get_access_token()
    client = PowerBIClient(token)

    workspaces = client.get_workspaces()
    if not workspaces:
        logger.warning(
            "No workspaces found — verify the service principal has been added to at least one workspace"
        )

    reports_by_workspace: dict[str, list[dict]] = {}
    datasets_by_workspace: dict[str, list[dict]] = {}
    refresh_by_dataset: dict[str, list[dict] | None] = {}

    for ws in workspaces:
        ws_id = ws.get("id")
        if not ws_id:
            logger.warning("Skipping workspace with missing ID: %s", ws)
            continue

        reports = client.get_reports(ws_id)
        reports_by_workspace[ws_id] = reports

        datasets = client.get_datasets(ws_id)
        datasets_by_workspace[ws_id] = datasets

        # Only fetch refresh history for datasets referenced by a report
        # and also flagged as refreshable. This avoids unnecessary API calls
        referenced_ids = {
            r.get("datasetId") for r in reports if r.get("datasetId")
        }
        for ds in datasets:
            ds_id = ds.get("id")
            if ds_id and ds_id in referenced_ids and ds.get("isRefreshable", False):
                refresh_by_dataset[ds_id] = client.get_refresh_history(
                    ws_id, ds_id
                )

    return workspaces, reports_by_workspace, datasets_by_workspace, refresh_by_dataset


def run(demo: bool = False) -> None:
    """Entry point for the Power BI metadata ingestion pipeline.

    Pipeline steps:
      1. Auth — acquire an OAuth2 Bearer token from Azure AD (skipped in demo mode)
      2. Fetch — pull workspaces, reports, datasets, and refresh history
      3. Transform — map raw API objects to the bi_assets schema
      4. Load — upsert records into SQLite
    """
    logger.info("Pipeline starting")
    ingested_at = datetime.now(timezone.utc).isoformat()

    try:
        if demo:
            workspaces, reports_by_workspace, datasets_by_workspace, refresh_by_dataset = _load_demo_data()
        else:
            workspaces, reports_by_workspace, datasets_by_workspace, refresh_by_dataset = _fetch_from_api()

        records = transform(
            workspaces=workspaces,
            reports_by_workspace=reports_by_workspace,
            datasets_by_workspace=datasets_by_workspace,
            refresh_by_dataset=refresh_by_dataset,
            ingested_at=ingested_at,
        )

        schema_path = str(ROOT_DIR / "sql" / "schema.sql")
        init_db(DB_PATH, schema_path=schema_path)

        if not records:
            logger.warning("No records to load — nothing was written to the database.")
            return

        upsert_assets(DB_PATH, records)

        total_reports = sum(len(v) for v in reports_by_workspace.values())
        total_datasets = sum(len(v) for v in datasets_by_workspace.values())
        total_refresh_calls = len(refresh_by_dataset)

        logger.info(
            "Pipeline complete — workspaces=%d, reports=%d, datasets=%d, refresh_calls=%d, assets_synced=%d",
            len(workspaces),
            total_reports,
            total_datasets,
            total_refresh_calls,
            len(records),
        )

    # To be specific, so catches API, DB, and file-system errors rather than just bare Exception
    except (requests.RequestException, sqlite3.Error, OSError, ValueError) as exc:
        logger.error("Pipeline failed: %s", exc, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    run(demo="--demo" in sys.argv)
