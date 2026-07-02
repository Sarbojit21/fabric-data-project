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
    StructField("CT_ProductionTaskId", StringType(), True),
    StructField("ProductionTaskId", StringType(), True),
    StructField("ProductionTaskName", StringType(), True),
    StructField("ProductionTaskGroup", StringType(), True),
    StructField("ProductionTaskAMilestone", StringType(), True),
    StructField("ProductionTaskApprovable", StringType(), True),
    StructField("ProductionTaskADeliverable", StringType(), True),
    StructField("ProductionTaskDeliverableDesc", StringType(), True),
    StructField("ProductionTaskCostingGroup", StringType(), True),
    StructField("SystemTaskId", StringType(), True),
    StructField("Outsource", StringType(), True),
    StructField("ProductionTaskADigitalDelivery", StringType(), True),
    StructField("SystemTaskCanBeAbandoned", StringType(), True),
    StructField("ApprovalSystem", StringType(), True),
    StructField("EstimatedDuration", StringType(), True),
    StructField("Deleted", StringType(), True)
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
    StructField("ProductionTaskId", StringType(), True),
    StructField("ProductionTaskName", StringType(), True),
    StructField("ProductionTaskGroup", StringType(), True),
    StructField("ProductionTaskAMilestone", StringType(), True),
    StructField("ProductionTaskApprovable", StringType(), True),
    StructField("ProductionTaskADeliverable", StringType(), True),
    StructField("ProductionTaskDeliverableDesc", StringType(), True),
    StructField("ProductionTaskCostingGroup", StringType(), True),
    StructField("SystemTaskId", StringType(), True),
    StructField("Outsource", StringType(), True),
    StructField("ProductionTaskADigitalDelivery", StringType(), True),
    StructField("SystemTaskCanBeAbandoned", StringType(), True),
    StructField("ApprovalSystem", StringType(), True),
    StructField("EstimatedDuration", StringType(), True),
    StructField("Deleted", StringType(), True)
])
# Create an empty DataFrame with the schema
df = spark.createDataFrame([], schema)

silver_path="abfss://Propelis_Production@onelake.dfs.fabric.microsoft.com/SILVER.Lakehouse/Tables/MYSGSEU/tbl_production_tasks"
silver_table = DeltaTable.forPath(spark, silver_path)
# Parameters
param = ""  # Replace with the actual PARAM value

# If-else logic to control the flow based on in_mode
if in_mode == "FULL":
    df.write.format("delta").mode("overwrite").saveAsTable("MYSGSEU.tbl_production_tasks")
    # Write the DataFrame as a Delta table
    bronze_Path ="abfss://Propelis_Production@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_production_tasks_FULL"
    # Load Delta tables correctly
    bronze_df = spark.read.format("delta").load(bronze_Path)
    
        # Merge data from Bronze Full to Silver
    silver_table.alias("target").merge(
        bronze_df.alias("source"),
        "target.ProductionTaskId = source.ProductionTaskId"
    ).whenMatchedUpdate(set={
        "SYS_CHANGE_VERSION": "source.SYS_CHANGE_VERSION",
        "SYS_CHANGE_OPERATION": "source.SYS_CHANGE_OPERATION",
        "ProductionTaskId": "source.ProductionTaskId",
        "ProductionTaskName": "source.ProductionTaskName",
        "ProductionTaskGroup": "source.ProductionTaskGroup",
        "ProductionTaskAMilestone": "source.ProductionTaskAMilestone",
        "ProductionTaskApprovable": "source.ProductionTaskApprovable",
        "ProductionTaskADeliverable": "source.ProductionTaskADeliverable",
        "ProductionTaskDeliverableDesc": "source.ProductionTaskDeliverableDesc",
        "ProductionTaskCostingGroup": "source.ProductionTaskCostingGroup",
        "SystemTaskId": "source.SystemTaskId",
        "Outsource": "source.Outsource",
        "ProductionTaskADigitalDelivery": "source.ProductionTaskADigitalDelivery",
        "SystemTaskCanBeAbandoned": "source.SystemTaskCanBeAbandoned",
        "ApprovalSystem": "source.ApprovalSystem",
        "EstimatedDuration": "source.EstimatedDuration",
        "Deleted": "source.Deleted"
    }).whenNotMatchedInsert(values={
        "SYS_CHANGE_VERSION": "source.SYS_CHANGE_VERSION",
        "SYS_CHANGE_OPERATION": "source.SYS_CHANGE_OPERATION",
        "ProductionTaskId": "source.ProductionTaskId",
        "ProductionTaskName": "source.ProductionTaskName",
        "ProductionTaskGroup": "source.ProductionTaskGroup",
        "ProductionTaskAMilestone": "source.ProductionTaskAMilestone",
        "ProductionTaskApprovable": "source.ProductionTaskApprovable",
        "ProductionTaskADeliverable": "source.ProductionTaskADeliverable",
        "ProductionTaskDeliverableDesc": "source.ProductionTaskDeliverableDesc",
        "ProductionTaskCostingGroup": "source.ProductionTaskCostingGroup",
        "SystemTaskId": "source.SystemTaskId",
        "Outsource": "source.Outsource",
        "ProductionTaskADigitalDelivery": "source.ProductionTaskADigitalDelivery",
        "SystemTaskCanBeAbandoned": "source.SystemTaskCanBeAbandoned",
        "ApprovalSystem": "source.ApprovalSystem",
        "EstimatedDuration": "source.EstimatedDuration",
        "Deleted": "source.Deleted"
    }).execute()
else:
    df.write.format("delta").mode("append").saveAsTable("MYSGSEU.tbl_production_tasks")
    
    source_path = "abfss://Propelis_Production@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_production_tasks_DELTA"
    source_df_delta = spark.read.format("delta").load(source_path)

    # Filter the source DataFrame for "D" operations and select distinct CT_Id
    filtered_source_df = source_df_delta.filter(source_df_delta["SYS_CHANGE_OPERATION"] == "D") \
    .select("CT_ProductionTaskId") \
    .distinct()

   # Perform the MERGE operation
    silver_table.alias("T") \
    .merge(filtered_source_df.alias("S"), "T.ProductionTaskId = S.CT_ProductionTaskId") \
    .whenMatchedDelete() \
    .execute()

    # Perform the transformation with all required columns
    transformed_df = source_df_delta.alias("a").join(
        source_df_delta.groupBy("ProductionTaskId")
        .agg(spark_max(col("SYS_CHANGE_VERSION").cast("bigint")).alias("SYS_CHANGE_VERSION"))
        .alias("b"),
        (col("a.ProductionTaskId") == col("b.ProductionTaskId")) & 
        (col("a.SYS_CHANGE_VERSION").cast("bigint") == col("b.SYS_CHANGE_VERSION"))
    ).where(col("a.SYS_CHANGE_OPERATION") != 'D') \
    .select(
           col("a.SYS_CHANGE_VERSION").alias("SYS_CHANGE_VERSION"),
           col("a.SYS_CHANGE_OPERATION").alias("SYS_CHANGE_OPERATION"),
           col("a.ProductionTaskId").alias("ProductionTaskId"),
           col("a.ProductionTaskName").alias("ProductionTaskName"),
           col("a.ProductionTaskGroup").alias("ProductionTaskGroup"),
           col("a.ProductionTaskAMilestone").alias("ProductionTaskAMilestone"),
           col("a.ProductionTaskApprovable").alias("ProductionTaskApprovable"),
           col("a.ProductionTaskADeliverable").alias("ProductionTaskADeliverable"),
           col("a.ProductionTaskDeliverableDesc").alias("ProductionTaskDeliverableDesc"),
           col("a.ProductionTaskCostingGroup").alias("ProductionTaskCostingGroup"),
           col("a.SystemTaskId").alias("SystemTaskId"),
           col("a.Outsource").alias("Outsource"),
           col("a.ProductionTaskADigitalDelivery").alias("ProductionTaskADigitalDelivery"),
           col("a.SystemTaskCanBeAbandoned").alias("SystemTaskCanBeAbandoned"),
           col("a.ApprovalSystem").alias("ApprovalSystem"),
           col("a.EstimatedDuration").alias("EstimatedDuration"),
           col("a.Deleted").alias("Deleted")
    ).distinct()

    # Perform the MERGE operation
    silver_table.alias("target").merge(
        transformed_df.alias("source"),
        "target.ProductionTaskId = source.ProductionTaskId"
    ).whenMatchedUpdate(set={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "ProductionTaskId": col("source.ProductionTaskId"),
        "ProductionTaskName": col("source.ProductionTaskName"),
        "ProductionTaskGroup": col("source.ProductionTaskGroup"),
        "ProductionTaskAMilestone": col("source.ProductionTaskAMilestone"),
        "ProductionTaskApprovable": col("source.ProductionTaskApprovable"),
        "ProductionTaskADeliverable": col("source.ProductionTaskADeliverable"),
        "ProductionTaskDeliverableDesc": col("source.ProductionTaskDeliverableDesc"),
        "ProductionTaskCostingGroup": col("source.ProductionTaskCostingGroup"),
        "SystemTaskId": col("source.SystemTaskId"),
        "Outsource": col("source.Outsource"),
        "ProductionTaskADigitalDelivery": col("source.ProductionTaskADigitalDelivery"),
        "SystemTaskCanBeAbandoned": col("source.SystemTaskCanBeAbandoned"),
        "ApprovalSystem": col("source.ApprovalSystem"),
        "EstimatedDuration": col("source.EstimatedDuration"),
        "Deleted": col("source.Deleted")
    }).whenNotMatchedInsert(values={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "ProductionTaskId": col("source.ProductionTaskId"),
        "ProductionTaskName": col("source.ProductionTaskName"),
        "ProductionTaskGroup": col("source.ProductionTaskGroup"),
        "ProductionTaskAMilestone": col("source.ProductionTaskAMilestone"),
        "ProductionTaskApprovable": col("source.ProductionTaskApprovable"),
        "ProductionTaskADeliverable": col("source.ProductionTaskADeliverable"),
        "ProductionTaskDeliverableDesc": col("source.ProductionTaskDeliverableDesc"),
        "ProductionTaskCostingGroup": col("source.ProductionTaskCostingGroup"),
        "SystemTaskId": col("source.SystemTaskId"),
        "Outsource": col("source.Outsource"),
        "ProductionTaskADigitalDelivery": col("source.ProductionTaskADigitalDelivery"),
        "SystemTaskCanBeAbandoned": col("source.SystemTaskCanBeAbandoned"),
        "ApprovalSystem": col("source.ApprovalSystem"),
        "EstimatedDuration": col("source.EstimatedDuration"),
        "Deleted": col("source.Deleted")
    }).execute()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
