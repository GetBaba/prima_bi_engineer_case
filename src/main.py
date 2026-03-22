import sys
from datetime import datetime, timezone

from auth import get_access_token
from config import DB_PATH, ROOT_DIR
from logger import get_logger
from powerbi_client import PowerBIClient
from storage import init_db, upsert_assets
from transform import transform

logger = get_logger(__name__)

def run() -> None:
    """Entry point for the Power BI metadata ingestion pipeline.

    Pipeline steps:
      1. Auth — acquire an OAuth2 Bearer token from Azure AD
      2. Fetch — pull workspaces, reports, datasets, and refresh history
      3. Transform — map raw API objects to the bi_assets schema
      4. Load — upsert records into SQLite
    """
    logger.info("Pipeline starting")
    ingested_at = datetime.now(timezone.utc).isoformat()

    try:
        # Authentification
        token = get_access_token()
        client = PowerBIClient(token)

        # Fetch
        workspaces = client.get_workspaces()
        if not workspaces:
            logger.warning(
                "No workspaces found — verify the service principal has been added to at least one workspace"
            )

        reports_by_workspace: dict[str, list] = {}
        datasets_by_workspace: dict[str, list] = {}
        refresh_by_dataset: dict[str, list | None] = {}

        for ws in workspaces:
            ws_id = ws["id"]
            reports = client.get_reports(ws_id)
            reports_by_workspace[ws_id] = reports

            datasets = client.get_datasets(ws_id)
            datasets_by_workspace[ws_id] = datasets

            # Only fetch refresh history for datasets that are referenced by a report AND flagged as refreshable. Skipping non-refreshable datasets
            # avoids unnecessary API calls and potential HTTP 400 errors
            referenced_ids = {
                r["datasetId"] for r in reports if r.get("datasetId")
            }
            for ds in datasets:
                if ds["id"] in referenced_ids and ds.get("isRefreshable", False):
                    refresh_by_dataset[ds["id"]] = client.get_refresh_history(
                        ws_id, ds["id"]
                    )

        # Transform 
        records = transform(
            workspaces=workspaces,
            reports_by_workspace=reports_by_workspace,
            datasets_by_workspace=datasets_by_workspace,
            refresh_by_dataset=refresh_by_dataset,
            ingested_at=ingested_at,
        )

        # Load
        schema_path = str(ROOT_DIR / "sql" / "schema.sql")  # note maybe I should define and change to schema_path here and in config.py to avoid ROOT_DIR restriction
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

    except Exception as exc:
        logger.error("Pipeline failed: %s", exc, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    run()
