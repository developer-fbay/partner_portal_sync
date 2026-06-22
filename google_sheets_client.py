"""Google Sheets API client for dealsheet sync."""

import base64
import json
import logging
import time
from urllib.parse import quote

import httpx
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

from config import Config

logger = logging.getLogger(__name__)

TOKEN_URL = "https://oauth2.googleapis.com/token"
SHEETS_SCOPE = "https://www.googleapis.com/auth/spreadsheets.readonly"


def _parse_pem(pem: str) -> bytes:
    """Strip PEM headers/newlines and base64-decode the key body."""
    lines = pem.strip().split("\n")
    body = "".join(line.strip() for line in lines if not line.startswith("-----"))
    return base64.b64decode(body)


def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def get_google_access_token() -> str:
    """Exchange a signed service-account JWT for a Google OAuth2 access token."""
    now = int(time.time())
    header = {"alg": "RS256", "typ": "JWT"}
    payload = {
        "iss": Config.GOOGLE_SERVICE_ACCOUNT_EMAIL,
        "scope": SHEETS_SCOPE,
        "aud": TOKEN_URL,
        "exp": now + 3600,
        "iat": now,
    }

    key_pem = Config.get_google_private_key_pem()
    key_data = _parse_pem(key_pem)
    private_key = serialization.load_der_private_key(key_data, password=None)

    signing_input = (
        f"{_b64url_encode(json.dumps(header, separators=(',', ':')).encode())}."
        f"{_b64url_encode(json.dumps(payload, separators=(',', ':')).encode())}"
    )
    signature = private_key.sign(
        signing_input.encode(),
        padding.PKCS1v15(),
        hashes.SHA256(),
    )
    jwt = f"{signing_input}.{_b64url_encode(signature)}"

    response = httpx.post(
        TOKEN_URL,
        data={
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "assertion": jwt,
        },
        timeout=30.0,
    )
    response.raise_for_status()
    return response.json()["access_token"]


def fetch_google_sheet() -> list:
    """Fetch raw cell values from the configured Google Sheet range."""
    token = get_google_access_token()
    sheet_id = Config.GOOGLE_SHEET_ID
    range_name = Config.GOOGLE_SHEET_RANGE
    url = (
        f"https://sheets.googleapis.com/v4/spreadsheets/{sheet_id}"
        f"/values/{quote(range_name, safe='')}"
    )

    response = httpx.get(
        url,
        headers={"Authorization": f"Bearer {token}"},
        timeout=60.0,
    )
    if response.status_code == 403:
        raise PermissionError(
            "Google Sheets returned 403 Forbidden. "
            f"Share the spreadsheet with {Config.GOOGLE_SERVICE_ACCOUNT_EMAIL} "
            "(Viewer access or higher). Also confirm Google Sheets API is enabled "
            f"for the service account project. Sheet ID: {sheet_id}, range: {range_name}"
        )
    response.raise_for_status()
    return response.json().get("values", [])
