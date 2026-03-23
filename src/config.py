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


# Credentials are read lazily, they are only validated when auth.py calls _require()
# This lets demo mode and tests run without an .env file
def get_credentials() -> tuple[str, str, str]:
    """Return (tenant_id, client_id, client_secret), validated."""
    return _require("AZURE_TENANT_ID"), _require("AZURE_CLIENT_ID"), _require("AZURE_CLIENT_SECRET")


DB_PATH: str = os.getenv("DB_PATH", "bi_assets.db")

ROOT_DIR: Path = Path(__file__).resolve().parent.parent

POWER_BI_BASE_URL: str = "https://api.powerbi.com/v1.0/myorg"
POWER_BI_SCOPE: str = "https://analysis.windows.net/powerbi/api/.default"
