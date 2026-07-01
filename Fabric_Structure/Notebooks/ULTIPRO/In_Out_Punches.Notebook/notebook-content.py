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

# Define the schema for the order data
orderSchema = StructType([
    StructField("EmployeeNumber", StringType(), True),
    StructField("EmployeeName", StringType(), True),
    StructField("PlantCode", StringType(), True),
    StructField("PlantName", StringType(), True),
    StructField("DepartmentCode", StringType(), True),
    StructField("DepartmentName", StringType(), True),
    StructField("WageTypeCode", StringType(), True),
    StructField("WageTypeName", StringType(), True),
    StructField("JobCode", StringType(), True),
    StructField("JobTitle", StringType(), True),
    StructField("SupervisorName", StringType(), True),
    StructField("WorkDate", StringType(), True),
    StructField("DailyEEPunchOrder", StringType(), True),
    StructField("InPunchDateTime", StringType(), True),
    StructField("OutPunchDateTime", StringType(), True),
    StructField("PayCode", StringType(), True),
    StructField("PayLabel", StringType(), True),
    StructField("RegularHours", StringType(), True),
    StructField("Overtime1Hours", StringType(), True),
    StructField("DoubleTimeHours", StringType(), True),
    StructField("HolidayWorkedHours", StringType(), True)
])


# Initialize Spark session
spark = SparkSession.builder \
    .appName("Bronze to Silver Merge") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Define the schema
schema = StructType([
    StructField("EmployeeNumber", StringType(), True),
    StructField("EmployeeName", StringType(), True),
    StructField("PlantCode", StringType(), True),
    StructField("PlantName", StringType(), True),
    StructField("DepartmentCode", StringType(), True),
    StructField("DepartmentName", StringType(), True),
    StructField("WageTypeCode", StringType(), True),
    StructField("WageTypeName", StringType(), True),
    StructField("JobCode", StringType(), True),
    StructField("JobTitle", StringType(), True),
    StructField("SupervisorName", StringType(), True),
    StructField("WorkDate", StringType(), True),
    StructField("DailyEEPunchOrder", StringType(), True),
    StructField("InPunchDateTime", StringType(), True),
    StructField("OutPunchDateTime", StringType(), True),
    StructField("PayCode", StringType(), True),
    StructField("PayLabel", StringType(), True),
    StructField("RegularHours", StringType(), True),
    StructField("Overtime1Hours", StringType(), True),
    StructField("DoubleTimeHours", StringType(), True),
    StructField("HolidayWorkedHours", StringType(), True)
])

# Create an empty DataFrame with the schema
df = spark.createDataFrame([], schema)

silver_path="abfss://Propelis_Production@onelake.dfs.fabric.microsoft.com/SILVER.Lakehouse/Tables/ULTIPRO/tbl_in_out_punches"
silver_table = DeltaTable.forPath(spark, silver_path)

# Parameters
param = ""  # Replace with the actual PARAM value
# If-else logic to control the flow based on in_mode
if in_mode == "FULL":
    # Write the DataFrame as a Delta table
    df.write.format("delta").mode("overwrite").saveAsTable("ULTIPRO.tbl_In_Out_Punches")
    bronze_Path="abfss://Propelis_Production@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/Ultipro/_In_Out_Punches"

    # Load Delta tables correctly
    bronze_df = spark.read.format("delta").load(bronze_Path)

# Merge data from Bronze Full to Silver
    silver_table.alias("target").merge(
        bronze_df.alias("source"),
        # "target.EmployeeNumber = source.EmployeeNumber"
        """
        target.EmployeeNumber = source.EmployeeNumber AND
        target.WorkDate = source.WorkDate AND
        target.DailyEEPunchOrder = source.DailyEEPunchOrder
        """
    ).whenMatchedUpdate(set={
        "EmployeeNumber": col("source.EmployeeNumber"),
        "EmployeeName": col("source.EmployeeName"),
        "PlantCode": col("source.PlantCode"),
        "PlantName": col("source.PlantName"),
        "DepartmentCode": col("source.DepartmentCode"),
        "DepartmentName": col("source.DepartmentName"),
        "WageTypeCode": col("source.WageTypeCode"),
        "WageTypeName": col("source.WageTypeName"),
        "JobCode": col("source.JobCode"),
        "JobTitle": col("source.JobTitle"),
        "SupervisorName": col("source.SupervisorName"),
        "WorkDate": col("source.WorkDate"),
        "DailyEEPunchOrder": col("source.DailyEEPunchOrder"),
        "InPunchDateTime": col("source.InPunchDateTime"),
        "OutPunchDateTime": col("source.OutPunchDateTime"),
        "PayCode": col("source.PayCode"),
        "PayLabel": col("source.PayLabel"),
        "RegularHours": col("source.RegularHours"),
        "Overtime1Hours": col("source.Overtime1Hours"),
        "DoubleTimeHours": col("source.DoubleTimeHours"),
        "HolidayWorkedHours": col("source.HolidayWorkedHours")
    }).whenNotMatchedInsert(values={
        "EmployeeNumber": col("source.EmployeeNumber"),
        "EmployeeName": col("source.EmployeeName"),
        "PlantCode": col("source.PlantCode"),
        "PlantName": col("source.PlantName"),
        "DepartmentCode": col("source.DepartmentCode"),
        "DepartmentName": col("source.DepartmentName"),
        "WageTypeCode": col("source.WageTypeCode"),
        "WageTypeName": col("source.WageTypeName"),
        "JobCode": col("source.JobCode"),
        "JobTitle": col("source.JobTitle"),
        "SupervisorName": col("source.SupervisorName"),
        "WorkDate": col("source.WorkDate"),
        "DailyEEPunchOrder": col("source.DailyEEPunchOrder"),
        "InPunchDateTime": col("source.InPunchDateTime"),
        "OutPunchDateTime": col("source.OutPunchDateTime"),
        "PayCode": col("source.PayCode"),
        "PayLabel": col("source.PayLabel"),
        "RegularHours": col("source.RegularHours"),
        "Overtime1Hours": col("source.Overtime1Hours"),
        "DoubleTimeHours": col("source.DoubleTimeHours"),
        "HolidayWorkedHours": col("source.HolidayWorkedHours")
    }).execute()

else:
    df.write.format("delta").mode("append").saveAsTable("ULTIPRO.tbl_In_Out_Punches")
   # Define paths
    source_path = "abfss://Propelis_Production@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/Ultipro/_In_Out_Punches"
    source_df_delta = spark.read.format("delta").load(source_path)
    
# Perform the MERGE operation
    silver_table.alias("target").merge(
        source_df_delta.alias("source"),
        # "target.EmployeeNumber = source.EmployeeNumber"
        """
        target.EmployeeNumber = source.EmployeeNumber AND
        target.WorkDate = source.WorkDate AND
        target.DailyEEPunchOrder = source.DailyEEPunchOrder
        """
        ).whenMatchedUpdate(set={
           "EmployeeNumber": col("source.EmployeeNumber"),
           "EmployeeName": col("source.EmployeeName"),
           "PlantCode": col("source.PlantCode"),
           "PlantName": col("source.PlantName"),
           "DepartmentCode": col("source.DepartmentCode"),
           "DepartmentName": col("source.DepartmentName"),
           "WageTypeCode": col("source.WageTypeCode"),
           "WageTypeName": col("source.WageTypeName"),
           "JobCode": col("source.JobCode"),
           "JobTitle": col("source.JobTitle"),
           "SupervisorName": col("source.SupervisorName"),
           "WorkDate": col("source.WorkDate"),
           "DailyEEPunchOrder": col("source.DailyEEPunchOrder"),
           "InPunchDateTime": col("source.InPunchDateTime"),
           "OutPunchDateTime": col("source.OutPunchDateTime"),
           "PayCode": col("source.PayCode"),
           "PayLabel": col("source.PayLabel"),
           "RegularHours": col("source.RegularHours"),
           "Overtime1Hours": col("source.Overtime1Hours"),
           "DoubleTimeHours": col("source.DoubleTimeHours"),
           "HolidayWorkedHours": col("source.HolidayWorkedHours")
       }).whenNotMatchedInsert(values={
           "EmployeeNumber": col("source.EmployeeNumber"),
           "EmployeeName": col("source.EmployeeName"),
           "PlantCode": col("source.PlantCode"),
           "PlantName": col("source.PlantName"),
           "DepartmentCode": col("source.DepartmentCode"),
           "DepartmentName": col("source.DepartmentName"),
           "WageTypeCode": col("source.WageTypeCode"),
           "WageTypeName": col("source.WageTypeName"),
           "JobCode": col("source.JobCode"),
           "JobTitle": col("source.JobTitle"),
           "SupervisorName": col("source.SupervisorName"),
           "WorkDate": col("source.WorkDate"),
           "DailyEEPunchOrder": col("source.DailyEEPunchOrder"),
           "InPunchDateTime": col("source.InPunchDateTime"),
           "OutPunchDateTime": col("source.OutPunchDateTime"),
           "PayCode": col("source.PayCode"),
           "PayLabel": col("source.PayLabel"),
           "RegularHours": col("source.RegularHours"),
           "Overtime1Hours": col("source.Overtime1Hours"),
           "DoubleTimeHours": col("source.DoubleTimeHours"),
           "HolidayWorkedHours": col("source.HolidayWorkedHours")
       }).execute()
	   
            

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
