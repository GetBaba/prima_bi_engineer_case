import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


def _require(key: str) -> str:
    """Read a mandatory environment variable, raising a clear error if absent."""
    value = os.environ.get(key)
    if not value:
        raise EnvironmentError(
            f"Required environment variable '{key}' is not set. "
            "Copy .env.example to .env and fill in your credentials."
        )
    return value


# Credentials
# These come from an Azure AD app registration (service principal) 
# See README.md for the setup steps
TENANT_ID: str = _require("AZURE_TENANT_ID")
CLIENT_ID: str = _require("AZURE_CLIENT_ID")
CLIENT_SECRET: str = _require("AZURE_CLIENT_SECRET")

# Storage
DB_PATH: str = os.getenv("DB_PATH", "bi_assets.db")

# Paths
# Resolves relative paths regardless of the working directory at runtime
ROOT_DIR: Path = Path(__file__).resolve().parent.parent

# Power BI API constants
POWER_BI_BASE_URL: str = "https://api.powerbi.com/v1.0/myorg"
TOKEN_URL: str = (
    f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
)
POWER_BI_SCOPE: str = "https://analysis.windows.net/powerbi/api/.default"
