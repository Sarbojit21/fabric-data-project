# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


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
    StructField("Code", IntegerType(), True),
    StructField("ServiceType", StringType(), True),
    StructField("Active", BooleanType(), True),
    StructField("DateAdded", TimestampType(), True),
    StructField("AddedBy", StringType(), True),
    StructField("LastModifiedDate", TimestampType(), True),
    StructField("LastModifiedBy", StringType(), True),
    StructField("System", StringType(), True),
    StructField("ETL_BatchId", IntegerType(), True),
    StructField("ETL_ChangeTrackingOperation", StringType(), True)

])

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Bronze to Silver Merge") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Define the schema
schema = StructType([
    StructField("Code", IntegerType(), True),
    StructField("ServiceType", StringType(), True),
    StructField("Active", BooleanType(), True),
    StructField("DateAdded", TimestampType(), True),
    StructField("AddedBy", StringType(), True),
    StructField("LastModifiedDate", TimestampType(), True),
    StructField("LastModifiedBy", StringType(), True),
    StructField("System", StringType(), True),
    StructField("ETL_BatchId", IntegerType(), True),
    StructField("ETL_ChangeTrackingOperation", StringType(), True)

])
# Create an empty DataFrame with the schema
df = spark.createDataFrame([], schema)

silver_path="abfss://Propelis_Production@onelake.dfs.fabric.microsoft.com/SILVER.Lakehouse/Tables/MYSGSEU/Administration_TimeTrackingCategoriesCategories"
silver_table = DeltaTable.forPath(spark, silver_path)

# Parameters
param = ""  # RDeplace with the actual PARAM value
# If-else logic to control the flow based on in_mode
if in_mode == "FULL":
    # Write the Dataframe as a Delta table
    df.write.format("delta").mode("overwrite").saveAsTable("MYSGSEU.Administration_TimeTrackingCategories")
    bronze_Path="abfss://Propelis_Production@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/Administration_TimeTrackingCategories_FULL"

    # Load Delta tables correctly
    bronze_df = spark.read.format("delta").load(bronze_Path)

   # Merge data from Bronze Full to Silver
    silver_table.alias("target").merge(
           bronze_df.alias("source"),
           "target.Id = source.Id"
        ).whenMatchedUpdate(set={
        "Code": "source.Code",
        "ServiceType": "source.ServiceType",
        "Active": "source.Active",
        "DateAdded": "source.DateAdded",
        "AddedBy": "source.AddedBy",
        "LastModifiedDate": "source.LastModifiedDate",
        "LastModifiedBy": "source.LastModifiedBy",
        "System": "source.System",
        "ETL_BatchId": "source.ETL_BatchId",
        "ETL_ChangeTrackingOperation": "source.ETL_ChangeTrackingOperation"
        }).whenNotMatchedInsert(values={
        "Code": "source.Code",
        "ServiceType": "source.ServiceType",
        "Active": "source.Active",
        "DateAdded": "source.DateAdded",
        "AddedBy": "source.AddedBy",
        "LastModifiedDate": "source.LastModifiedDate",
        "LastModifiedBy": "source.LastModifiedBy",
        "System": "source.System",
        "ETL_BatchId": "source.ETL_BatchId",
        "ETL_ChangeTrackingOperation": "source.ETL_ChangeTrackingOperation"
        }).execute()
else:
    
    df.write.format("delta").mode("append").saveAsTable("MYSGSEU.Administration_TimeTrackingCategories")
    
    source_path = "abfss://Propelis_Production@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/Administration_TimeTrackingCategories_DELTA"
    source_df_delta = spark.read.format("delta").load(source_path)

    # Perform the MERGE operation
    silver_table.alias("target").merge(
        transformed_df.alias("source"),
        "target.Id = source.Id"
    ).whenMatchedUpdate(set={
        "Code": "source.Code",
        "ServiceType": "source.ServiceType",
        "Active": "source.Active",
        "DateAdded": "source.DateAdded",
        "AddedBy": "source.AddedBy",
        "LastModifiedDate": "source.LastModifiedDate",
        "LastModifiedBy": "source.LastModifiedBy",
        "System": "source.System",
        "ETL_BatchId": "source.ETL_BatchId",
        "ETL_ChangeTrackingOperation": "source.ETL_ChangeTrackingOperation"
    }).whenNotMatchedInsert(values={
        "Code": "source.Code",
        "ServiceType": "source.ServiceType",
        "Active": "source.Active",
        "DateAdded": "source.DateAdded",
        "AddedBy": "source.AddedBy",
        "LastModifiedDate": "source.LastModifiedDate",
        "LastModifiedBy": "source.LastModifiedBy",
        "System": "source.System",
        "ETL_BatchId": "source.ETL_BatchId",
        "ETL_ChangeTrackingOperation": "source.ETL_ChangeTrackingOperation"
    }).execute()   


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
