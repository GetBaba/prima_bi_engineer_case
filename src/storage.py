from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from logger import get_logger

logger = get_logger(__name__)

# Insert or update in a single statement. The pipeline is idempotent because re-running with the same asset_id updates the existing row rather than
# failing or inserting a duplicate. See also ingested_at
_UPSERT_SQL = """
INSERT INTO bi_assets (
    asset_id, asset_name, workspace_id, workspace_name, dataset_id,
    owner, last_refresh_at, last_updated, views_last_30d, last_viewed,
    status, source_system, ingested_at
) VALUES (
    :asset_id, :asset_name, :workspace_id, :workspace_name, :dataset_id,
    :owner, :last_refresh_at, :last_updated, :views_last_30d, :last_viewed,
    :status, :source_system, :ingested_at
)
ON CONFLICT(asset_id) DO UPDATE SET
    asset_name      = excluded.asset_name,
    workspace_id    = excluded.workspace_id,
    workspace_name  = excluded.workspace_name,
    dataset_id      = excluded.dataset_id,
    owner           = excluded.owner,
    last_refresh_at = excluded.last_refresh_at,
    last_updated    = excluded.last_updated,
    views_last_30d  = excluded.views_last_30d,
    last_viewed     = excluded.last_viewed,
    status          = excluded.status,
    source_system   = excluded.source_system,
    ingested_at     = excluded.ingested_at;
"""

def init_db(db_path: str, schema_path: str = "sql/schema.sql") -> None:
    """Create the database file and apply the schema if it doesn't exist yet."""
    schema = Path(schema_path).read_text(encoding="utf-8")
    with sqlite3.connect(db_path) as conn:
        conn.executescript(schema)
        conn.commit()
    logger.info("Database initialised at %s", db_path)


def upsert_assets(db_path: str, records: list[dict[str, Any]]) -> None:
    """Upsert a batch of bi_asset records into SQLite."""
    with sqlite3.connect(db_path) as conn:
        conn.executemany(_UPSERT_SQL, records)
        conn.commit()
    logger.info("Upserted %d record(s) into bi_assets", len(records))
