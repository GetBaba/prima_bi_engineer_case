from __future__ import annotations

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import POWER_BI_BASE_URL
from logger import get_logger

logger = get_logger(__name__)


class PowerBIClient:
    """Small wrapper around the Power BI REST API.

    Keeps the API calls in one place, retries transient failures,
    and returns raw JSON for the transform layer to handle.
    """

    def __init__(self, access_token: str) -> None:
        self._session = self._build_session(access_token)

    @staticmethod
    def _build_session(token: str) -> requests.Session:
        """Create a requests session with auth and retry settings."""
        session = requests.Session()
        session.headers.update({"Authorization": f"Bearer {token}"})
        retries = Retry(
            total=3,
            backoff_factor=1,  # 1s, 2s, 4s between retries
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        session.mount("https://", HTTPAdapter(max_retries=retries))
        return session

    def _get(self, path: str) -> dict:
        """Send a GET request to one Power BI API endpoint."""
        url = f"{POWER_BI_BASE_URL}{path}"
        logger.debug("GET %s", url)
        response = self._session.get(url, timeout=15)
        response.raise_for_status()
        return response.json()

    def _get_all(self, path: str) -> list[dict]:
        """Fetch all pages for a list endpoint"""
        items: list[dict] = []
        url: str | None = f"{POWER_BI_BASE_URL}{path}"

        while url:
            logger.debug("GET %s", url)
            response = self._session.get(url, timeout=15)
            response.raise_for_status()
            body = response.json()
            # see the `or []` instead of default, this catches the edge case where the API returns "value": null
            items.extend(body.get("value") or [])
            url = body.get("@odata.nextLink")

        return items

    def get_workspaces(self) -> list[dict]:
        """Get all workspaces visible to the service principal"""
        workspaces: list[dict] = self._get_all("/groups")
        logger.info("Fetched %d workspace(s)", len(workspaces))
        return workspaces

    def get_reports(self, workspace_id: str) -> list[dict]:
        """Get all reports in one workspace"""
        reports: list[dict] = self._get_all(f"/groups/{workspace_id}/reports")
        logger.info("Workspace %s — %d report(s)", workspace_id, len(reports))
        return reports

    def get_datasets(self, workspace_id: str) -> list[dict]:
        """Get all datasets in one workspace"""
        datasets: list[dict] = self._get_all(f"/groups/{workspace_id}/datasets")
        logger.info("Workspace %s — %d dataset(s)", workspace_id, len(datasets))
        return datasets

    def get_refresh_history(
        self, workspace_id: str, dataset_id: str, top: int = 1
    ) -> list[dict] | None:
        """Get recent refresh history for one dataset.

        Returns:
            list[dict]: refresh history records, or an empty list if no history exists
            None: if the refresh history could not be read
        """
        try:
            data = self._get(
                f"/groups/{workspace_id}/datasets/{dataset_id}"
                f"/refreshes?$top={top}"
            )
            return data.get("value", [])
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code in (400, 403, 404):
                logger.warning(
                    "Cannot read refresh history for dataset %s (HTTP %s) — "
                    "status will be recorded as REFRESH_UNKNOWN",
                    dataset_id,
                    exc.response.status_code,
                )
                return None
            raise