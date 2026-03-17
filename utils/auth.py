"""
Authentication — Microsoft Fabric / Azure AD Service Principal token
"""

import requests
from fastapi import HTTPException
from config import TENANT_ID, CLIENT_ID, CLIENT_SECRET, FABRIC_SCOPE


def get_fabric_token() -> str:
    """
    Obtain an Azure AD bearer token scoped for Fabric / Power BI APIs
    using the configured Service Principal (client_credentials flow).

    Returns:
        Bearer token string.

    Raises:
        HTTPException 500 on auth failure.
    """
    try:
        resp = requests.post(
            f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token",
            data={
                "grant_type":    "client_credentials",
                "client_id":     CLIENT_ID,
                "client_secret": CLIENT_SECRET,
                "scope":         FABRIC_SCOPE,
            },
            timeout=30,
        )
        resp.raise_for_status()
        token = resp.json().get("access_token")
        if not token:
            raise ValueError("No access_token in response")
        print("✅ Fabric token obtained")
        return token

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get Fabric token: {str(e)}",
        )
