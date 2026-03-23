import requests

from config import POWER_BI_SCOPE, get_credentials
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
    tenant_id, client_id, client_secret = get_credentials()
    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"

    logger.info("Requesting access token from Azure AD")
    response = requests.post(
        token_url,
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": POWER_BI_SCOPE,
        },
        timeout=15,
    )
    response.raise_for_status()

    # .get() + validation instead of direct key access,
    # in case Azure returns a successful response without a token (potentially due to misconfiguration or permission issues)
    body = response.json()
    token = body.get("access_token")
    if not token:
        raise ValueError(
            f"Azure AD response did not contain an access_token: {list(body.keys())}"
        )
    logger.info("Access token acquired")
    return token
