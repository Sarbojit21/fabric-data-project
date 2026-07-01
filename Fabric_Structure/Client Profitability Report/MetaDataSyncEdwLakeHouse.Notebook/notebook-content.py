# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "45614522-af60-4d44-aeeb-f66a7da63b13",
# META       "default_lakehouse_name": "EDW_LAKEHOUSE_CP",
# META       "default_lakehouse_workspace_id": "c8d75176-b949-4f7e-a658-b996603ec8c3",
# META       "known_lakehouses": [
# META         {
# META           "id": "45614522-af60-4d44-aeeb-f66a7da63b13"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import json
import notebookutils
import sempy.fabric as fabric
from sempy.fabric.exceptions import FabricHTTPException, WorkspaceNotFoundException
from IPython.display import display, HTML
import pandas as pd

workspace_id = spark.conf.get("trident.workspace.id")
lakehouse_id = spark.conf.get("trident.lakehouse.id")
# List of tables you want to refresh
tables_to_refresh = ["FactSalesCostActualOutBound","DimGLAccountsDepartment"]  # replace with your Delta table names

# Instantiate the client
client = fabric.FabricRestClient()

# Get SQL endpoint ID
sqlendpoint = client.get(
    f"/v1/workspaces/{workspace_id}/lakehouses/{lakehouse_id}"
).json()["properties"]["sqlEndpointProperties"]["id"]
# API endpoint for table-level metadata refresh
uri = f"v1/workspaces/{workspace_id}/sqlEndpoints/{sqlendpoint}/refreshMetadata"

# Payload to refresh specific tables
payload = {
    "tables": [{"name": tbl} for tbl in tables_to_refresh],
    "timeout": {"timeUnit": "Seconds", "value": "60"},
    "type":"Full"
}

try:
    response = client.post(uri, json=payload, lro_wait=True)
    sync_status = json.loads(response.text)

    # Convert to DataFrame for display
    df = pd.DataFrame(sync_status)

    for col in df.columns:
        if "DateTime" in col:
            df[col] = pd.to_datetime(df[col]).dt.strftime("%Y-%m-%d %H:%M:%S")

    display(HTML(df.to_html(index=False)))

except FabricHTTPException as fhe:
    print(f"Fabric API error: {fhe}")
except Exception as e:
    print(f"Unexpected error: {e}")

df = spark.sql("SELECT * FROM EDW_LAKEHOUSE.FactSalesCostActualOutBound LIMIT 1000")
display(df)

df = spark.sql("SELECT * FROM EDW_LAKEHOUSE.FactSalesCostActualOutBound LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
import notebookutils
import sempy.fabric as fabric
from sempy.fabric.exceptions import FabricHTTPException, WorkspaceNotFoundException
from IPython.display import display, HTML
import pandas as pd

workspace_id = spark.conf.get("trident.workspace.id")
lakehouse_id = spark.conf.get("trident.lakehouse.id")

# Instantiate the client
client = fabric.FabricRestClient()

# Get SQL endpoint ID
sqlendpoint = client.get(
    f"/v1/workspaces/{workspace_id}/lakehouses/{lakehouse_id}"
).json()["properties"]["sqlEndpointProperties"]["id"]

# URI for refresh
uri = f"v1/workspaces/{workspace_id}/sqlEndpoints/{sqlendpoint}/refreshMetadata"

# Payload
payload = {"timeout": {"timeUnit": "Seconds", "value": "60"}}

try:
    # API call
    response = client.post(uri, json=payload, lro_wait=True)
    sync_status = json.loads(response.text)  # This is a list of dicts

    # Convert list of dicts to DataFrame
    df = pd.DataFrame(sync_status)

    # Format date columns for better readability (optional)
    for col in df.columns:
        if "DateTime" in col:
            df[col] = pd.to_datetime(df[col]).dt.strftime("%Y-%m-%d %H:%M:%S")

    # Convert to HTML table with styling
    html_table = df.to_html(index=False, classes="dataframe", border=1)

    # Add CSS for nicer look
    styled_html = f"""
    <style>
        table.dataframe {{
            border-collapse: collapse;
            width: 100%;
            font-family: Arial, sans-serif;
            font-size: 14px;
        }}
        table.dataframe th, table.dataframe td {{
            border: 1px solid #ccc;
            padding: 8px;
            text-align: left;
        }}
        table.dataframe th {{
            background-color: #f4f4f4;
            font-weight: bold;
        }}
        table.dataframe tr:nth-child(even) {{
            background-color: #fafafa;
        }}
    </style>
    {html_table}
    """

    display(HTML(styled_html))

except Exception as e:
    print(e)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
