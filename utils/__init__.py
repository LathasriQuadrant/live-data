"""Utils package — Tableau to Fabric Lakehouse"""

from .auth         import get_fabric_token
from .blob_storage import download_twbx_from_blob, upload_blob, download_blob_text
from .file_handler import get_workbook_name, extract_twb_from_twbx, extract_datasource_from_twb
from .hyper_reader import twbx_has_extract, read_hyper_tables
from .type_mapper  import coerce_dataframe_to_tableau_types, build_delta_schema
from .datasource   import (
    read_snowflake_table,
    read_sqlserver_table,
    read_databricks_table,
    test_snowflake_connection,
    test_sqlserver_connection,
    test_databricks_connection,
)
from .fabric import (
    create_lakehouse,
    write_table_to_lakehouse,
    get_lakehouse_by_name,
    get_lakehouse_tables,
)

__all__ = [
    "get_fabric_token",
    "download_twbx_from_blob",
    "upload_blob",
    "download_blob_text",
    "get_workbook_name",
    "extract_twb_from_twbx",
    "extract_datasource_from_twb",
    "twbx_has_extract",
    "read_hyper_tables",
    "coerce_dataframe_to_tableau_types",
    "build_delta_schema",
    "read_snowflake_table",
    "read_sqlserver_table",
    "read_databricks_table",
    "test_snowflake_connection",
    "test_sqlserver_connection",
    "test_databricks_connection",
    "create_lakehouse",
    "write_table_to_lakehouse",
    "get_lakehouse_by_name",
    "get_lakehouse_tables",
]

from .registry       import register, get, get_all, get_password, update_refresh_status, delete as delete_record
from .refresh_engine import refresh_migration
from .powerbi        import trigger_powerbi_refresh, get_refresh_history
