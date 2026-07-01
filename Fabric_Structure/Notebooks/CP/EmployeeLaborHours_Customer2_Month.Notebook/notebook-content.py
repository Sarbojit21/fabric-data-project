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

in_mode = "FULL" 

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
from pyspark.sql import Window
from pyspark.sql.functions import row_number

orderschema = StructType([
    StructField("DataSource", StringType(), True),
    StructField("MonthBeginDate", DateType(), True),
    StructField("EmployeeNumber", StringType(), True),
    StructField("EmployeeName", StringType(), True),
    StructField("Customer2", StringType(), True),
    StructField("LaborHours", DoubleType(), True),
    StructField("MDSSimplifiedId", StringType(), True),
    StructField("MDSPortfolioId", StringType(), True)
])

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Bronze to Silver Merge") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

schema = StructType([
    StructField("DataSource", StringType(), True),
    StructField("MonthBeginDate", DateType(), True),
    StructField("EmployeeNumber", StringType(), True),
    StructField("EmployeeName", StringType(), True),
    StructField("Customer2", StringType(), True),
    StructField("LaborHours", DoubleType(), True),
    StructField("MDSSimplifiedId", StringType(), True),
    StructField("MDSPortfolioId", StringType(), True)
])
    # Define the schema


df = spark.createDataFrame([], schema)

silver_path="abfss://Propelis_Production@onelake.dfs.fabric.microsoft.com/SILVER.Lakehouse/Tables/CP/employeelaborhours_customer2_month"
silver_table = DeltaTable.forPath(spark, silver_path)
# Parameters
param = ""  # Replace with the actual PARAM value


# If-else logic to control the flow based on in_mode
if in_mode == "FULL":
    # Write the DataFrame as a Delta table
    df.write.format("delta").mode("overwrite").saveAsTable("CP.EmployeeLaborHours_Customer2_Month")
    bronze_Path ="abfss://Propelis_Production@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/CP/EmployeeLaborHours_Customer2_Month"
    # Load Delta tables correctly
    bronze_df = spark.read.format("delta").load(bronze_Path)
  
# Merge data from Bronze Full to Silver with only the specified columns
    silver_table.alias("target").merge(
    bronze_df.alias("source"),
    "target.MDSPortfolioId= source.MDSPortfolioId"
).whenMatchedUpdate(set={
    "DataSource": col("source.DataSource"),
    "MonthBeginDate": col("source.MonthBeginDate"),
    "EmployeeNumber": col("source.EmployeeNumber"),
    "EmployeeName": col("source.EmployeeName"),
    "Customer2": col("source.Customer2"),
    "LaborHours": col("source.LaborHours"),
    "MDSSimplifiedId": col("source.MDSSimplifiedId"),
    "MDSPortfolioId": col("source.MDSPortfolioId")
}).whenNotMatchedInsert(values={
    "DataSource": col("source.DataSource"),
    "MonthBeginDate": col("source.MonthBeginDate"),
    "EmployeeNumber": col("source.EmployeeNumber"),
    "EmployeeName": col("source.EmployeeName"),
    "Customer2": col("source.Customer2"),
    "LaborHours": col("source.LaborHours"),
    "MDSSimplifiedId": col("source.MDSSimplifiedId"),
    "MDSPortfolioId": col("source.MDSPortfolioId")
}).execute()

else:
    df.write.format("delta").mode("append").saveAsTable("CP.EmployeeLaborHours_Customer2_Month")
    
    source_path = "abfss://Propelis_Production@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/CP/EmployeeLaborHours_Customer2_Month"
    source_df_delta = spark.read.format("delta").load(source_path)
    window_spec = Window.partitionBy("MDSPortfolioId").orderBy(col("MonthBeginDate").desc())

    source_df_delta_dedup = source_df_delta.withColumn("row_num", row_number().over(window_spec)) \
                                       .filter("row_num = 1") \
                                       .drop("row_num")
    # Perform the MERGE operation
    silver_table.alias("target").merge(
    source_df_delta_dedup.alias("source"),
    "target.MDSPortfolioId= source.MDSPortfolioId"
).whenMatchedUpdate(set={
    "DataSource": col("source.DataSource"),
    "MonthBeginDate": col("source.MonthBeginDate"),
    "EmployeeNumber": col("source.EmployeeNumber"),
    "EmployeeName": col("source.EmployeeName"),
    "Customer2": col("source.Customer2"),
    "LaborHours": col("source.LaborHours"),
    "MDSSimplifiedId": col("source.MDSSimplifiedId"),
    "MDSPortfolioId": col("source.MDSPortfolioId")
}).whenNotMatchedInsert(values={
    "DataSource": col("source.DataSource"),
    "MonthBeginDate": col("source.MonthBeginDate"),
    "EmployeeNumber": col("source.EmployeeNumber"),
    "EmployeeName": col("source.EmployeeName"),
    "Customer2": col("source.Customer2"),
    "LaborHours": col("source.LaborHours"),
    "MDSSimplifiedId": col("source.MDSSimplifiedId"),
    "MDSPortfolioId": col("source.MDSPortfolioId")
}).execute()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT count(*) FROM SILVER.CP.employeelaborhours_customer2_month")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.load("abfss://Propelis_Production@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/CP/EmployeeLaborHours_Customer2_Month")
df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
