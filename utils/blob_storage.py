"""
Azure Blob Storage utilities — download .twbx workbook files.
"""

import os
import tempfile
from fastapi import HTTPException
from azure.storage.blob import BlobServiceClient
from config import AZURE_STORAGE_CONNECTION_STRING, AZURE_BLOB_CONTAINER


def _get_client() -> BlobServiceClient:
    if not AZURE_STORAGE_CONNECTION_STRING:
        raise HTTPException(500, "AZURE_STORAGE_CONNECTION_STRING is not set.")
    try:
        return BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
    except Exception as e:
        raise HTTPException(500, f"Failed to create BlobServiceClient: {e}")


def download_twbx_from_blob(file_name: str, container_name: str = None) -> str:
    """
    Download a .twbx workbook from Azure Blob Storage.
    Returns the local temp file path.
    """
    if not container_name:
        container_name = AZURE_BLOB_CONTAINER

    print(f"📥 Downloading '{file_name}' from container '{container_name}'...")

    client      = _get_client()
    blob_client = client.get_blob_client(container=container_name, blob=file_name)

    try:
        props = blob_client.get_blob_properties()
        print(f"   File size: {props.size:,} bytes")
    except Exception as e:
        raise HTTPException(404, f"Blob '{file_name}' not found in '{container_name}': {e}")

    temp_dir   = tempfile.mkdtemp()
    local_path = os.path.join(temp_dir, file_name)

    data = blob_client.download_blob().readall()
    with open(local_path, "wb") as f:
        f.write(data)

    if os.path.getsize(local_path) == 0:
        raise HTTPException(500, "Downloaded file is empty.")

    print(f"   ✅ Saved to: {local_path}")
    return local_path


def list_twbx_files(container_name: str = None) -> list[str]:
    """Return names of all .twbx / .twb files in the container."""
    if not container_name:
        container_name = AZURE_BLOB_CONTAINER
    client = _get_client()
    return [
        b.name for b in client.get_container_client(container_name).list_blobs()
        if b.name.lower().endswith((".twbx", ".twb"))
    ]


# kept for compatibility — not used without refresh
def upload_blob(content, blob_name, container_name=None, overwrite=True):
    pass

def download_blob_text(blob_name, container_name=None):
    return None
