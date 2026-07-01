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

in_mode="FULL"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


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
    StructField("PayCode", StringType(), True),
    StructField("PayLabel", StringType(), True),
    StructField("PayTypeKey", StringType(), True),
    StructField("PayTypeLabel", StringType(), True)
])


# Initialize Spark session
spark = SparkSession.builder \
    .appName("Bronze to Silver Merge") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Define the schema
schema = StructType([
    StructField("PayCode", StringType(), True),
    StructField("PayLabel", StringType(), True),
    StructField("PayTypeKey", StringType(), True),
    StructField("PayTypeLabel", StringType(), True)
])

# Create an empty DataFrame with the schema
df = spark.createDataFrame([], schema)
silver_path="abfss://Propelis_Production@onelake.dfs.fabric.microsoft.com/SILVER.Lakehouse/Tables/ULTIPRO/tbl_paytype"
silver_table = DeltaTable.forPath(spark, silver_path)

# Parameters
param = ""  # Replace with the actual PARAM value
if in_mode == "FULL":
    # Write the DataFrame as a Delta table
    # df.write.format("delta").mode("overwrite").saveAsTable("ULTIPRO.tbl_PayType")
    bronze_Path="abfss://Propelis_Production@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/Ultipro/_PayType"

    # Load Delta tables correctly
    bronze_df = spark.read.format("delta").load(bronze_Path)

# Merge data from Bronze Full to Silver
    silver_table.alias("target").merge(
        bronze_df.alias("source"),
        "target.PayCode = source.PayCode"
    ).whenMatchedUpdate(set={
        "PayCode": col("source.PayCode"),
        "PayLabel": col("source.PayLabel"),
        "PayTypeKey": col("source.PayTypeKey"),
        "PayTypeLabel": col("source.PayTypeLabel")
    }).whenNotMatchedInsert(values={
        "PayCode": col("source.PayCode"),
        "PayLabel": col("source.PayLabel"),
        "PayTypeKey": col("source.PayTypeKey"),
        "PayTypeLabel": col("source.PayTypeLabel")
    }).execute()

else:
    df.write.format("delta").mode("append").saveAsTable("ULTIPRO.tbl_PayType")
   # Define paths
    source_path = "abfss://Propelis_Production@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/Ultipro/_PayType"
    source_df_delta = spark.read.format("delta").load(source_path)
    
# Perform the MERGE operation
    silver_table.alias("target").merge(
        source_df_delta.alias("source"),
            "target.PayCode = source.PayCode"
            ).whenMatchedUpdate(set={
            "PayCode": col("source.PayCode"),
            "PayLabel": col("source.PayLabel"),
            "PayTypeKey": col("source.PayTypeKey"),
            "PayTypeLabel": col("source.PayTypeLabel")
        }).whenNotMatchedInsert(values={
            "PayCode": col("source.PayCode"),
            "PayLabel": col("source.PayLabel"),
            "PayTypeKey": col("source.PayTypeKey"),
            "PayTypeLabel": col("source.PayTypeLabel")
        }).execute()
            

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
