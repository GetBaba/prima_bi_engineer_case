-- bi_assets: one row per Power BI report.
-- asset_id is the report GUID — stable across runs, used as the upsert key.

CREATE TABLE IF NOT EXISTS bi_assets (
    asset_id        TEXT PRIMARY KEY,   -- Power BI report GUID
    asset_name      TEXT NOT NULL,      -- display name of the report
    workspace_id    TEXT,               -- GUID of the parent workspace / group
    workspace_name  TEXT,               -- display name of the parent workspace
    dataset_id      TEXT,               -- GUID of the linked dataset (nullable)
    owner           TEXT,               -- dataset configuredBy (UPN) — may be NULL
    last_refresh_at TEXT,               -- ISO 8601 endTime of the latest refresh
    status          TEXT,               -- SUCCESS | FAILED | NEVER_REFRESHED | NOT_REFRESHABLE | REFRESH_UNKNOWN
    source_system   TEXT,               -- always 'power_bi' for this pipeline
    ingested_at     TEXT NOT NULL       -- ISO 8601 timestamp of the pipeline run
);
