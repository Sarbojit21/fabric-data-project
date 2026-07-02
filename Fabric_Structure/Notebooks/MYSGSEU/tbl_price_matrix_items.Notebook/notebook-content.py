# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "aabf914c-0501-4c58-ba5b-4b0f05f4420f",
# META       "default_lakehouse_name": "SILVER",
# META       "default_lakehouse_workspace_id": "c8d75176-b949-4f7e-a658-b996603ec8c3",
# META       "known_lakehouses": [
# META         {
# META           "id": "aabf914c-0501-4c58-ba5b-4b0f05f4420f"
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

in_mode = "FULL"  # Replace with the actual IN_MODE value

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
from pyspark.sql.functions import concat_ws, expr, sha2, size, lit, col, array, struct, udf, current_timestamp, max as spark_max
from functools import reduce
from pyspark.sql.types import *
from pyspark.sql import *
from delta.tables import *
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# Define the schema for the order data
orderSchema = StructType([
    StructField("SYS_CHANGE_VERSION", StringType(), True),
    StructField("SYS_CHANGE_CREATION_VERSION", StringType(), True),
    StructField("SYS_CHANGE_OPERATION", StringType(), True),
    StructField("SYS_CHANGE_COLUMNS", StringType(), True),
    StructField("SYS_CHANGE_CONTEXT", StringType(), True),
    StructField("CT_PriceMatrixItemId", StringType(), True),
    StructField("PriceMatrixItemId", StringType(), True),
    StructField("PriceMatrixId", StringType(), True),
    StructField("PriceMatrixItemDesc", StringType(), True),
    StructField("PriceMatrixItemCost", StringType(), True),
    StructField("PriceMatrixItemSalesAccountCode", StringType(), True),
    StructField("PriceMatrixItemPlateType", StringType(), True),
    StructField("PriceMatrixItemOrder", StringType(), True),
    StructField("PriceMatrixItemKPITypeId", StringType(), True),
    StructField("PriceMatrixItemKPITurnaround", StringType(), True),
    StructField("ItemDeleted", StringType(), True),
    StructField("PriceMatrixItemAlias", StringType(), True),
    StructField("PriceMatrixItemMultiplier", StringType(), True),
    StructField("PriceMatrixItemSalesDepartmentCode", StringType(), True),
    StructField("PriceMatrixItemSalesTaxCode", StringType(), True),
    StructField("Independent", StringType(), True),
    StructField("Quantity", StringType(), True)
])
# Initialize Spark session
spark = SparkSession.builder \
    .appName("Bronze to Silver Merge") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

    # Define the schema
schema = StructType([
    StructField("SYS_CHANGE_VERSION", StringType(), True),
    StructField("SYS_CHANGE_OPERATION", StringType(), True),
    StructField("PriceMatrixItemId", StringType(), True),
    StructField("PriceMatrixId", StringType(), True),
    StructField("PriceMatrixItemDesc", StringType(), True),
    StructField("PriceMatrixItemCost", StringType(), True),
    StructField("PriceMatrixItemSalesAccountCode", StringType(), True),
    StructField("PriceMatrixItemPlateType", StringType(), True),
    StructField("PriceMatrixItemOrder", StringType(), True),
    StructField("PriceMatrixItemKPITypeId", StringType(), True),
    StructField("PriceMatrixItemKPITurnaround", StringType(), True),
    StructField("ItemDeleted", StringType(), True),
    StructField("PriceMatrixItemAlias", StringType(), True),
    StructField("PriceMatrixItemMultiplier", StringType(), True),
    StructField("PriceMatrixItemSalesDepartmentCode", StringType(), True),
    StructField("PriceMatrixItemSalesTaxCode", StringType(), True),
    StructField("Independent", StringType(), True),
    StructField("Quantity", StringType(), True)
])
# Create an empty DataFrame with the schema
df = spark.createDataFrame([], schema)
silver_path="abfss://Propelis_Production@onelake.dfs.fabric.microsoft.com/SILVER.Lakehouse/Tables/MYSGSEU/tbl_price_matrix_items"
silver_table = DeltaTable.forPath(spark, silver_path)
# Parameters
param = ""  # Replace with the actual PARAM value

# If-else logic to control the flow based on in_mode
if in_mode == "FULL":
    # Write the DataFrame as a Delta table
    df.write.format("delta").mode("overwrite").saveAsTable("MYSGSEU.tbl_price_matrix_items")
    bronze_Path ="abfss://Propelis_Production@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_price_matrix_items_FULL"
    # Load Delta tables correctly
    bronze_df = spark.read.format("delta").load(bronze_Path)
    
        # Merge data from Bronze Full to Silver
    silver_table.alias("target").merge(
        bronze_df.alias("source"),
        "target.PriceMatrixItemId = source.PriceMatrixItemId"
    ).whenMatchedUpdate(set={
        "SYS_CHANGE_VERSION": "source.SYS_CHANGE_VERSION",
        "SYS_CHANGE_OPERATION": "source.SYS_CHANGE_OPERATION",
        "PriceMatrixItemId": "source.PriceMatrixItemId",
        "PriceMatrixId": "source.PriceMatrixId",
        "PriceMatrixItemDesc": "source.PriceMatrixItemDesc",
        "PriceMatrixItemCost": "source.PriceMatrixItemCost",
        "PriceMatrixItemSalesAccountCode": "source.PriceMatrixItemSalesAccountCode",
        "PriceMatrixItemPlateType": "source.PriceMatrixItemPlateType",
        "PriceMatrixItemOrder": "source.PriceMatrixItemOrder",
        "PriceMatrixItemKPITypeId": "source.PriceMatrixItemKPITypeId",
        "PriceMatrixItemKPITurnaround": "source.PriceMatrixItemKPITurnaround",
        "ItemDeleted": "source.ItemDeleted",
        "PriceMatrixItemAlias": "source.PriceMatrixItemAlias",
        "PriceMatrixItemMultiplier": "source.PriceMatrixItemMultiplier",
        "PriceMatrixItemSalesDepartmentCode": "source.PriceMatrixItemSalesDepartmentCode",
        "PriceMatrixItemSalesTaxCode": "source.PriceMatrixItemSalesTaxCode",
        "Independent": "source.Independent",
        "Quantity": "source.Quantity"
    }).whenNotMatchedInsert(values={
        "SYS_CHANGE_VERSION": "source.SYS_CHANGE_VERSION",
        "SYS_CHANGE_OPERATION": "source.SYS_CHANGE_OPERATION",
        "PriceMatrixItemId": "source.PriceMatrixItemId",
        "PriceMatrixId": "source.PriceMatrixId",
        "PriceMatrixItemDesc": "source.PriceMatrixItemDesc",
        "PriceMatrixItemCost": "source.PriceMatrixItemCost",
        "PriceMatrixItemSalesAccountCode": "source.PriceMatrixItemSalesAccountCode",
        "PriceMatrixItemPlateType": "source.PriceMatrixItemPlateType",
        "PriceMatrixItemOrder": "source.PriceMatrixItemOrder",
        "PriceMatrixItemKPITypeId": "source.PriceMatrixItemKPITypeId",
        "PriceMatrixItemKPITurnaround": "source.PriceMatrixItemKPITurnaround",
        "ItemDeleted": "source.ItemDeleted",
        "PriceMatrixItemAlias": "source.PriceMatrixItemAlias",
        "PriceMatrixItemMultiplier": "source.PriceMatrixItemMultiplier",
        "PriceMatrixItemSalesDepartmentCode": "source.PriceMatrixItemSalesDepartmentCode",
        "PriceMatrixItemSalesTaxCode": "source.PriceMatrixItemSalesTaxCode",
        "Independent": "source.Independent",
        "Quantity": "source.Quantity"
    }).execute()
else:
    df.write.format("delta").mode("append").saveAsTable("MYSGSEU.tbl_price_matrix_items")
    
    source_path = "abfss://Propelis_Production@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_price_matrix_items_DELTA"
    source_df_delta = spark.read.format("delta").load(source_path)

    # Filter the source DataFrame for "D" operations and select distinct CT_Id
    filtered_source_df = source_df_delta.filter(source_df_delta["SYS_CHANGE_OPERATION"] == "D") \
    .select("CT_PriceMatrixItemId") \
    .distinct()

   # Perform the MERGE operation
    silver_table.alias("T") \
    .merge(filtered_source_df.alias("S"), "T.PriceMatrixItemId = S.CT_PriceMatrixItemId") \
    .whenMatchedDelete() \
    .execute()

    # Perform the transformation with all required columns
    transformed_df = source_df_delta.alias("a").join(
        source_df_delta.groupBy("PriceMatrixItemId")
        .agg(spark_max(col("SYS_CHANGE_VERSION").cast("bigint")).alias("SYS_CHANGE_VERSION"))
        .alias("b"),
        (col("a.PriceMatrixItemId") == col("b.PriceMatrixItemId")) & 
        (col("a.SYS_CHANGE_VERSION").cast("bigint") == col("b.SYS_CHANGE_VERSION"))
    ).where(col("a.SYS_CHANGE_OPERATION") != 'D') \
    .select(
         col("a.SYS_CHANGE_VERSION").alias("SYS_CHANGE_VERSION"),
         col("a.SYS_CHANGE_OPERATION").alias("SYS_CHANGE_OPERATION"),
         col("a.PriceMatrixItemId").alias("PriceMatrixItemId"),
         col("a.PriceMatrixId").alias("PriceMatrixId"),
         col("a.PriceMatrixItemDesc").alias("PriceMatrixItemDesc"),
         col("a.PriceMatrixItemCost").alias("PriceMatrixItemCost"),
         col("a.PriceMatrixItemSalesAccountCode").alias("PriceMatrixItemSalesAccountCode"),
         col("a.PriceMatrixItemPlateType").alias("PriceMatrixItemPlateType"),
         col("a.PriceMatrixItemOrder").alias("PriceMatrixItemOrder"),
         col("a.PriceMatrixItemKPITypeId").alias("PriceMatrixItemKPITypeId"),
         col("a.PriceMatrixItemKPITurnaround").alias("PriceMatrixItemKPITurnaround"),
         col("a.ItemDeleted").alias("ItemDeleted"),
         col("a.PriceMatrixItemAlias").alias("PriceMatrixItemAlias"),
         col("a.PriceMatrixItemMultiplier").alias("PriceMatrixItemMultiplier"),
         col("a.PriceMatrixItemSalesDepartmentCode").alias("PriceMatrixItemSalesDepartmentCode"),
         col("a.PriceMatrixItemSalesTaxCode").alias("PriceMatrixItemSalesTaxCode"),
         col("a.Independent").alias("Independent"),
         col("a.Quantity").alias("Quantity")
    ).distinct()

    # Perform the MERGE operation
    silver_table.alias("target").merge(
        transformed_df.alias("source"),
        "target.PriceMatrixItemId = source.PriceMatrixItemId"
    ).whenMatchedUpdate(set={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "PriceMatrixItemId": col("source.PriceMatrixItemId"),
        "PriceMatrixId": col("source.PriceMatrixId"),
        "PriceMatrixItemDesc": col("source.PriceMatrixItemDesc"),
        "PriceMatrixItemCost": col("source.PriceMatrixItemCost"),
        "PriceMatrixItemSalesAccountCode": col("source.PriceMatrixItemSalesAccountCode"),
        "PriceMatrixItemPlateType": col("source.PriceMatrixItemPlateType"),
        "PriceMatrixItemOrder": col("source.PriceMatrixItemOrder"),
        "PriceMatrixItemKPITypeId": col("source.PriceMatrixItemKPITypeId"),
        "PriceMatrixItemKPITurnaround": col("source.PriceMatrixItemKPITurnaround"),
        "ItemDeleted": col("source.ItemDeleted"),
        "PriceMatrixItemAlias": col("source.PriceMatrixItemAlias"),
        "PriceMatrixItemMultiplier": col("source.PriceMatrixItemMultiplier"),
        "PriceMatrixItemSalesDepartmentCode": col("source.PriceMatrixItemSalesDepartmentCode"),
        "PriceMatrixItemSalesTaxCode": col("source.PriceMatrixItemSalesTaxCode"),
        "Independent": col("source.Independent"),
        "Quantity": col("source.Quantity"),
    }).whenNotMatchedInsert(values={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "PriceMatrixItemId": col("source.PriceMatrixItemId"),
        "PriceMatrixId": col("source.PriceMatrixId"),
        "PriceMatrixItemDesc": col("source.PriceMatrixItemDesc"),
        "PriceMatrixItemCost": col("source.PriceMatrixItemCost"),
        "PriceMatrixItemSalesAccountCode": col("source.PriceMatrixItemSalesAccountCode"),
        "PriceMatrixItemPlateType": col("source.PriceMatrixItemPlateType"),
        "PriceMatrixItemOrder": col("source.PriceMatrixItemOrder"),
        "PriceMatrixItemKPITypeId": col("source.PriceMatrixItemKPITypeId"),
        "PriceMatrixItemKPITurnaround": col("source.PriceMatrixItemKPITurnaround"),
        "ItemDeleted": col("source.ItemDeleted"),
        "PriceMatrixItemAlias": col("source.PriceMatrixItemAlias"),
        "PriceMatrixItemMultiplier": col("source.PriceMatrixItemMultiplier"),
        "PriceMatrixItemSalesDepartmentCode": col("source.PriceMatrixItemSalesDepartmentCode"),
        "PriceMatrixItemSalesTaxCode": col("source.PriceMatrixItemSalesTaxCode"),
        "Independent": col("source.Independent"),
        "Quantity": col("source.Quantity"),
    }).execute()
    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
