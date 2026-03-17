# Tableau → Microsoft Fabric Lakehouse API

Standalone FastAPI service that migrates Tableau workbooks from Azure Blob Storage
into Microsoft Fabric Lakehouses as Delta tables, with webhook-based refresh when
source data changes.

---

## Project Structure

```
tableau_lakehouse_api/
├── main.py                     ← FastAPI entry point  (port 8001)
├── requirements.txt
├── .env                        ← All configuration (fill this in)
├── config/
│   └── settings.py             ← Loads env vars
├── routes/
│   ├── migrate.py              ← POST /api/v1/lakehouse/migrate
│   ├── refresh.py              ← POST /api/v1/lakehouse/refresh  (webhook)
│   ├── status.py               ← GET  /api/v1/lakehouse/status
│   └── health.py               ← GET  /health
└── utils/
    ├── auth.py                 ← Fabric token (Service Principal)
    ├── blob_storage.py         ← Download .twbx + read/write state blob
    ├── file_handler.py         ← Parse TWB/TWBX XML (connections, tables, types)
    ├── hyper_reader.py         ← Read .hyper extract via pantab / tableauhyperapi
    ├── type_mapper.py          ← Tableau → Spark/Delta type conversion
    ├── datasource.py           ← Snowflake / Azure SQL / Databricks connectors
    ├── fabric.py               ← Fabric REST API + OneLake upload + Load Table
    └── lakehouse_state.py      ← State persistence (JSON blob in Azure Storage)
```

---

## Setup

```bash
# 1. Clone / copy this folder
cd tableau_lakehouse_api

# 2. Create virtual environment
python -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Fill in .env (see .env file for all variables)

# 5. Run
python main.py
# or:
uvicorn main:app --host 0.0.0.0 --port 8001 --reload
```

API docs at: http://localhost:8001/docs

---

## Endpoints

### POST /api/v1/lakehouse/migrate
Migrate a Tableau workbook from blob to a Fabric Lakehouse.

Auto-detects whether the workbook uses **Extracted** (.hyper) or **Live** data.

```json
{
    "file_name":      "Sales_Dashboard.twbx",
    "workspace_id":   "your-fabric-workspace-guid",
    "container_name": "tableau-raw",
    "password":       "source-db-password",
    "tables":         ["Orders", "Customers"]
}
```
- `password` — only needed for live sources (Snowflake / Azure SQL / Databricks)
- `tables` — optional; omit to migrate all tables in the workbook
- `container_name` — optional; defaults to `AZURE_BLOB_CONTAINER` in `.env`

---

### POST /api/v1/lakehouse/refresh
Webhook called by your source system when data changes.

```json
{
    "workbook_name": "Sales_Dashboard",
    "password":      "source-db-password",
    "tables":        ["Orders"]
}
```
- `workbook_name` — must match the workbook name from /migrate
- `tables` — optional; omit to refresh all tracked tables

---

### GET /api/v1/lakehouse/status
List all migrated workbooks and their state.

### GET /api/v1/lakehouse/status/{workbook_name}
Detail for one workbook + live table list from Fabric.

### DELETE /api/v1/lakehouse/state/{workbook_name}
Remove a state record (does NOT delete the Fabric Lakehouse).

---

## How the Refresh Trigger Works

The `/refresh` endpoint is a plain HTTP webhook. Configure each source system
to POST to it after a data change:

### Databricks
In your Databricks job, add a **Webhook** notification on job success:
```
URL:     http://<your-host>:8001/api/v1/lakehouse/refresh
Method:  POST
Headers: Content-Type: application/json
Body:    {"workbook_name": "Sales_Dashboard", "password": "your-token"}
```

### Azure SQL
1. Set up CDC or a SQL Agent Job that detects changes
2. Create an **Azure Logic App** with an HTTP action:
   - Method: POST
   - URI: `http://<your-host>:8001/api/v1/lakehouse/refresh`
   - Body: `{"workbook_name": "Sales_Dashboard", "password": "..."}`

### Snowflake
Create a Snowflake Task after your data pipeline:
```sql
CREATE OR REPLACE TASK notify_lakehouse
  WAREHOUSE = your_warehouse
  AFTER your_pipeline_task
AS
  SELECT SYSTEM$SEND_EMAIL(
    'integration_email',
    'lakehouse-trigger@yourcompany.com',
    'Refresh needed',
    '{"workbook_name": "Sales_Dashboard", "password": "..."}'
  );
```
Or use Snowflake's external function / REST integration to POST directly.

---

## State Storage

Migration state is stored as a single JSON blob:
- Container: `LAKEHOUSE_STATE_CONTAINER` (default: `tableau-lakehouse-state`)
- Blob name: `LAKEHOUSE_STATE_BLOB_NAME` (default: `lakehouse_state.json`)

The container is created automatically on first write if it does not exist.
Passwords are **never stored** — only connection metadata (server, schema, etc).

---

## Fabric Permissions Required

Your Service Principal needs:
- **Fabric workspace**: Member or Contributor role
- **Azure Storage account**: Storage Blob Data Contributor role

To grant Fabric workspace access via PowerShell:
```powershell
Add-PowerBIWorkspaceUser `
  -WorkspaceId "your-workspace-guid" `
  -UserPrincipalName "your-sp-app-id" `
  -UserAccessRight Member
```

---

## Data Flow

```
Azure Blob Storage (.twbx)
         │
         ▼
 FastAPI /migrate
         │
    ┌────┴────────────────────────┐
    │  Detect: Extracted or Live? │
    └────┬──────────────────┬─────┘
         │                  │
    Extracted            Live
    Read .hyper      Connect to source
    (pantab)         (Snowflake / Azure SQL
         │            / Databricks)
         │                  │
         └──────────┬───────┘
                    │
           Coerce types to
           Tableau metadata
                    │
           Create Lakehouse
          (named after workbook)
                    │
          Write each table as
           Delta table via
        OneLake + Load Table API
                    │
          Save state to blob
         (no passwords stored)
                    │
                    ▼
         Source system changes
                    │
     POST /api/v1/lakehouse/refresh
                    │
          Load state from blob
          Re-read from source
          Overwrite Delta tables
```
