import requests

from config import CLIENT_ID, CLIENT_SECRET, POWER_BI_SCOPE, TOKEN_URL
from logger import get_logger

logger = get_logger(__name__)


def get_access_token() -> str:
    """Get an Azure AD access token using the client credentials flow.

    The pipeline authenticates as a service principal (app registration),
    which is better suited for automated jobs than user-based auth.

    Requirements:
    - Azure AD app registration with client secret
    - Power BI API permissions (Workspace.Read.All, Report.Read.All, Dataset.Read.All)
    - Service principal added to target workspaces (or granted tenant-wide access)

    Returns:
        Bearer access token (valid ~1 hour)
    """
    logger.info("Requesting access token from Azure AD")
    response = requests.post(
        TOKEN_URL,
        data={
            "grant_type": "client_credentials",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "scope": POWER_BI_SCOPE,
        },
        timeout=15,
    )
    response.raise_for_status()
    token: str = response.json()["access_token"]
    logger.info("Access token acquired")
    return token
