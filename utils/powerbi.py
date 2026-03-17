"""
utils/powerbi.py
────────────────
Trigger a Power BI dataset refresh via the Fabric / Power BI REST API.

The same Service Principal used for Fabric Lakehouse access also needs
the Power BI workspace "Member" or "Contributor" role AND the
"Dataset.ReadWrite.All" delegated permission (or the service principal
must be allowed in the tenant admin portal).

API used:
  POST https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshes

Docs:
  https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/refresh-dataset-in-group
"""

import requests
from fastapi import HTTPException
from utils.auth import get_fabric_token

_PBI_API = "https://api.powerbi.com/v1.0/myorg"


def trigger_powerbi_refresh(workspace_id: str, dataset_id: str) -> dict:
    """
    POST a refresh request for a Power BI dataset.

    Power BI processes the request asynchronously — a 202 response means
    the refresh was accepted and queued. Use get_refresh_status() to poll.

    Args:
        workspace_id: Fabric / Power BI workspace (group) GUID.
        dataset_id:   Power BI dataset GUID.

    Returns:
        dict with keys: status, request_id, message

    Raises:
        HTTPException on auth or API failure.
    """
    token = get_fabric_token()
    url   = f"{_PBI_API}/groups/{workspace_id}/datasets/{dataset_id}/refreshes"

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type":  "application/json",
    }

    # notifyOption: "MailOnFailure" | "MailOnCompletion" | "NoNotification"
    body = {"notifyOption": "NoNotification"}

    print(f"🔄 Triggering Power BI refresh: dataset={dataset_id} workspace={workspace_id}")
    resp = requests.post(url, json=body, headers=headers, timeout=30)

    if resp.status_code == 202:
        request_id = resp.headers.get("RequestId", "unknown")
        print(f"   ✅ PBI refresh accepted — RequestId={request_id}")
        return {
            "status":     "accepted",
            "request_id": request_id,
            "message":    "Power BI dataset refresh queued successfully.",
        }

    if resp.status_code == 400:
        # 400 often means a refresh is already in progress
        detail = resp.json().get("error", {}).get("message", resp.text)
        print(f"   ⚠️  PBI refresh 400: {detail}")
        return {
            "status":  "already_in_progress",
            "message": detail,
        }

    # Any other non-2xx is a real error
    raise HTTPException(
        status_code=resp.status_code,
        detail=f"Power BI refresh failed: {resp.text}",
    )


def get_refresh_history(workspace_id: str, dataset_id: str, top: int = 5) -> list[dict]:
    """
    Fetch the most recent refresh history entries for a dataset.
    Useful for checking whether the last scheduled refresh succeeded.

    Returns a list of refresh records (newest first).
    """
    token = get_fabric_token()
    url   = f"{_PBI_API}/groups/{workspace_id}/datasets/{dataset_id}/refreshes"

    headers = {"Authorization": f"Bearer {token}"}
    params  = {"$top": top}

    resp = requests.get(url, headers=headers, params=params, timeout=30)
    if resp.status_code != 200:
        print(f"⚠️  Could not fetch PBI refresh history: {resp.text}")
        return []

    return resp.json().get("value", [])
