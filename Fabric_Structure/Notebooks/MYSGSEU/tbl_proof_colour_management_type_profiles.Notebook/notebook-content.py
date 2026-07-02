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
# META         },
# META         {
# META           "id": "5db3d583-e11f-4ac4-9781-65ee3ee820a0"
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
    StructField("CT_PCMTypeProfileId", StringType(), True),
    StructField("PCMTypeProfileId", StringType(), True),
    StructField("PCMTypeId", StringType(), True),
    StructField("PCMTypeProfileName", StringType(), True),
    StructField("ProfileInUse", StringType(), True),
    StructField("Profile", StringType(), True),
    StructField("Device", StringType(), True),
    StructField("Substrate", StringType(), True),
    StructField("GMGServer", StringType(), True),
    StructField("FileFormat", StringType(), True),
    StructField("SurfaceReversePrint", StringType(), True),
    StructField("WorkflowFolderName", StringType(), True),
    StructField("SpotColourLibrary1", StringType(), True),
    StructField("SpotColourLibrary2", StringType(), True),
    StructField("ProofSingleCurve", StringType(), True),
    StructField("ProofAutomaticCurve", StringType(), True),
    StructField("SpotColourDGPrediction", StringType(), True),
    StructField("OriginalProfile", StringType(), True),
    StructField("ProfileNotes", StringType(), True),
    StructField("WFAvailable", StringType(), True),
    StructField("PlateData", StringType(), True),
    StructField("MinDot", StringType(), True),
    StructField("LABValues", StringType(), True),
    StructField("PlateTintReference", StringType(), True),
    StructField("FingerPrintDotShapes", StringType(), True),
    StructField("ScreenSet", StringType(), True),
    StructField("HD", StringType(), True),
    StructField("Resolution", StringType(), True),
    StructField("PlateSingleCurve", StringType(), True),
    StructField("PlateAutomatic", StringType(), True),
    StructField("PlateMakingDGC", StringType(), True),
    StructField("ConfigurationSet", StringType(), True),
    StructField("PlateNotes", StringType(), True),
    StructField("Brand", StringType(), True),
    StructField("Variety", StringType(), True),
    StructField("FingerprintReports", StringType(), True),
    StructField("ProfileStatus", StringType(), True),
    StructField("SiteId", StringType(), True)
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
    StructField("PCMTypeProfileId", StringType(), True),
    StructField("PCMTypeId", StringType(), True),
    StructField("PCMTypeProfileName", StringType(), True),
    StructField("ProfileInUse", StringType(), True),
    StructField("Profile", StringType(), True),
    StructField("Device", StringType(), True),
    StructField("Substrate", StringType(), True),
    StructField("GMGServer", StringType(), True),
    StructField("FileFormat", StringType(), True),
    StructField("SurfaceReversePrint", StringType(), True),
    StructField("WorkflowFolderName", StringType(), True),
    StructField("SpotColourLibrary1", StringType(), True),
    StructField("SpotColourLibrary2", StringType(), True),
    StructField("ProofSingleCurve", StringType(), True),
    StructField("ProofAutomaticCurve", StringType(), True),
    StructField("SpotColourDGPrediction", StringType(), True),
    StructField("OriginalProfile", StringType(), True),
    StructField("ProfileNotes", StringType(), True),
    StructField("WFAvailable", StringType(), True),
    StructField("PlateData", StringType(), True),
    StructField("MinDot", StringType(), True),
    StructField("LABValues", StringType(), True),
    StructField("PlateTintReference", StringType(), True),
    StructField("FingerPrintDotShapes", StringType(), True),
    StructField("ScreenSet", StringType(), True),
    StructField("HD", StringType(), True),
    StructField("Resolution", StringType(), True),
    StructField("PlateSingleCurve", StringType(), True),
    StructField("PlateAutomatic", StringType(), True),
    StructField("PlateMakingDGC", StringType(), True),
    StructField("ConfigurationSet", StringType(), True),
    StructField("PlateNotes", StringType(), True),
    StructField("Brand", StringType(), True),
    StructField("Variety", StringType(), True),
    StructField("FingerprintReports", StringType(), True),
    StructField("ProfileStatus", StringType(), True),
    StructField("SiteId", StringType(), True)
])
# Create an empty DataFrame with the schema
df = spark.createDataFrame([], schema)

silver_path="abfss://Propelis_Production@onelake.dfs.fabric.microsoft.com/SILVER.Lakehouse/Tables/MYSGSEU/tbl_proof_colour_management_type_profiles"
silver_table = DeltaTable.forPath(spark, silver_path)
# Parameters
param = ""  # Replace with the actual PARAM value

# If-else logic to control the flow based on in_mode
if in_mode == "FULL":
    # Write the DataFrame as a Delta table
    df.write.format("delta").mode("overwrite").saveAsTable("MYSGSEU.tbl_proof_colour_management_type_profiles")
    bronze_Path ="abfss://Propelis_Production@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_proof_colour_management_type_profiles_FULL"
    # Load Delta tables correctly
    bronze_df = spark.read.format("delta").load(bronze_Path)
     
        # Merge data from Bronze Full to Silver
    silver_table.alias("target").merge(
           bronze_df.alias("source"),
           "target.PCMTypeProfileId = source.PCMTypeProfileId"
        ).whenMatchedUpdate(set={
        "SYS_CHANGE_VERSION": "source.SYS_CHANGE_VERSION",
        "SYS_CHANGE_OPERATION": "source.SYS_CHANGE_OPERATION",
        "PCMTypeProfileId": "source.PCMTypeProfileId",
        "PCMTypeId": "source.PCMTypeId",
        "PCMTypeProfileName": "source.PCMTypeProfileName",
        "ProfileInUse": "source.ProfileInUse",
        "Profile": "source.Profile",
        "Device": "source.Device",
        "Substrate": "source.Substrate",
        "GMGServer": "source.GMGServer",
        "FileFormat": "source.FileFormat",
        "SurfaceReversePrint": "source.SurfaceReversePrint",
        "WorkflowFolderName": "source.WorkflowFolderName",
        "SpotColourLibrary1": "source.SpotColourLibrary1",
        "SpotColourLibrary2": "source.SpotColourLibrary2",
        "ProofSingleCurve": "source.ProofSingleCurve",
        "ProofAutomaticCurve": "source.ProofAutomaticCurve",
        "SpotColourDGPrediction": "source.SpotColourDGPrediction",
        "OriginalProfile": "source.OriginalProfile",
        "ProfileNotes": "source.ProfileNotes",
        "WFAvailable": "source.WFAvailable",
        "PlateData": "source.PlateData",
        "MinDot": "source.MinDot",
        "LABValues": "source.LABValues",
        "PlateTintReference": "source.PlateTintReference",
        "FingerPrintDotShapes": "source.FingerPrintDotShapes",
        "ScreenSet": "source.ScreenSet",
        "HD": "source.HD",
        "Resolution": "source.Resolution",
        "PlateSingleCurve": "source.PlateSingleCurve",
        "PlateAutomatic": "source.PlateAutomatic",
        "PlateMakingDGC": "source.PlateMakingDGC",
        "ConfigurationSet": "source.ConfigurationSet",
        "PlateNotes": "source.PlateNotes",
        "Brand": "source.Brand",
        "Variety": "source.Variety",
        "FingerprintReports": "source.FingerprintReports",
        "ProfileStatus": "source.ProfileStatus",
        "SiteId": "source.SiteId"
        }).whenNotMatchedInsert(values={
        "SYS_CHANGE_VERSION": "source.SYS_CHANGE_VERSION",
        "SYS_CHANGE_OPERATION": "source.SYS_CHANGE_OPERATION",
        "PCMTypeProfileId": "source.PCMTypeProfileId",
        "PCMTypeId": "source.PCMTypeId",
        "PCMTypeProfileName": "source.PCMTypeProfileName",
        "ProfileInUse": "source.ProfileInUse",
        "Profile": "source.Profile",
        "Device": "source.Device",
        "Substrate": "source.Substrate",
        "GMGServer": "source.GMGServer",
        "FileFormat": "source.FileFormat",
        "SurfaceReversePrint": "source.SurfaceReversePrint",
        "WorkflowFolderName": "source.WorkflowFolderName",
        "SpotColourLibrary1": "source.SpotColourLibrary1",
        "SpotColourLibrary2": "source.SpotColourLibrary2",
        "ProofSingleCurve": "source.ProofSingleCurve",
        "ProofAutomaticCurve": "source.ProofAutomaticCurve",
        "SpotColourDGPrediction": "source.SpotColourDGPrediction",
        "OriginalProfile": "source.OriginalProfile",
        "ProfileNotes": "source.ProfileNotes",
        "WFAvailable": "source.WFAvailable",
        "PlateData": "source.PlateData",
        "MinDot": "source.MinDot",
        "LABValues": "source.LABValues",
        "PlateTintReference": "source.PlateTintReference",
        "FingerPrintDotShapes": "source.FingerPrintDotShapes",
        "ScreenSet": "source.ScreenSet",
        "HD": "source.HD",
        "Resolution": "source.Resolution",
        "PlateSingleCurve": "source.PlateSingleCurve",
        "PlateAutomatic": "source.PlateAutomatic",
        "PlateMakingDGC": "source.PlateMakingDGC",
        "ConfigurationSet": "source.ConfigurationSet",
        "PlateNotes": "source.PlateNotes",
        "Brand": "source.Brand",
        "Variety": "source.Variety",
        "FingerprintReports": "source.FingerprintReports",
        "ProfileStatus": "source.ProfileStatus",
        "SiteId": "source.SiteId"
        }).execute()
else:
    df.write.format("delta").mode("append").saveAsTable("MYSGSEU.tbl_proof_colour_management_type_profiles")
    
    source_path = "abfss://Propelis_Production@onelake.dfs.fabric.microsoft.com/BRONZE.Lakehouse/Tables/MYSGSEU/tbl_proof_colour_management_type_profiles_DELTA"
    source_df_delta = spark.read.format("delta").load(source_path)

    # Filter the source DataFrame for "D" operations and select distinct CT_Id
    filtered_source_df = source_df_delta.filter(source_df_delta["SYS_CHANGE_OPERATION"] == "D") \
    .select("CT_PCMTypeProfileId") \
    .distinct()

   # Perform the MERGE operation
    silver_table.alias("T") \
    .merge(filtered_source_df.alias("S"), "T.PCMTypeProfileId = S.CT_PCMTypeProfileId") \
    .whenMatchedDelete() \
    .execute()

    # Perform the transformation with all required columns
    transformed_df = source_df_delta.alias("a").join(
        source_df_delta.groupBy("PCMTypeProfileId")
        .agg(spark_max(col("SYS_CHANGE_VERSION").cast("bigint")).alias("SYS_CHANGE_VERSION"))
        .alias("b"),
        (col("a.PCMTypeProfileId") == col("b.PCMTypeProfileId")) & 
        (col("a.SYS_CHANGE_VERSION").cast("bigint") == col("b.SYS_CHANGE_VERSION"))
    ).where(col("a.SYS_CHANGE_OPERATION") != 'D') \
    .select(
        col("a.SYS_CHANGE_VERSION").alias("SYS_CHANGE_VERSION"),
        col("a.SYS_CHANGE_OPERATION").alias("SYS_CHANGE_OPERATION"),
        col("a.PCMTypeProfileId").alias("PCMTypeProfileId"),
        col("a.PCMTypeId").alias("PCMTypeId"),
        col("a.PCMTypeProfileName").alias("PCMTypeProfileName"),
        col("a.ProfileInUse").alias("ProfileInUse"),
        col("a.Profile").alias("Profile"),
        col("a.Device").alias("Device"),
        col("a.Substrate").alias("Substrate"),
        col("a.GMGServer").alias("GMGServer"),
        col("a.FileFormat").alias("FileFormat"),
        col("a.SurfaceReversePrint").alias("SurfaceReversePrint"),
        col("a.WorkflowFolderName").alias("WorkflowFolderName"),
        col("a.SpotColourLibrary1").alias("SpotColourLibrary1"),
        col("a.SpotColourLibrary2").alias("SpotColourLibrary2"),
        col("a.ProofSingleCurve").alias("ProofSingleCurve"),
        col("a.ProofAutomaticCurve").alias("ProofAutomaticCurve"),
        col("a.SpotColourDGPrediction").alias("SpotColourDGPrediction"),
        col("a.OriginalProfile").alias("OriginalProfile"),
        col("a.ProfileNotes").alias("ProfileNotes"),
        col("a.WFAvailable").alias("WFAvailable"),
        col("a.PlateData").alias("PlateData"),
        col("a.MinDot").alias("MinDot"),
        col("a.LABValues").alias("LABValues"),
        col("a.PlateTintReference").alias("PlateTintReference"),
        col("a.FingerPrintDotShapes").alias("FingerPrintDotShapes"),
        col("a.ScreenSet").alias("ScreenSet"),
        col("a.HD").alias("HD"),
        col("a.Resolution").alias("Resolution"),
        col("a.PlateSingleCurve").alias("PlateSingleCurve"),
        col("a.PlateAutomatic").alias("PlateAutomatic"),
        col("a.PlateMakingDGC").alias("PlateMakingDGC"),
        col("a.ConfigurationSet").alias("ConfigurationSet"),
        col("a.PlateNotes").alias("PlateNotes"),
        col("a.Brand").alias("Brand"),
        col("a.Variety").alias("Variety"),
        col("a.FingerprintReports").alias("FingerprintReports"),
        col("a.ProfileStatus").alias("ProfileStatus"),
        col("a.SiteId").alias("SiteId")

    ).distinct()

    # Perform the MERGE operation
    silver_table.alias("target").merge(
        transformed_df.alias("source"),
        "target.PCMTypeProfileId = source.PCMTypeProfileId"
    ).whenMatchedUpdate(set={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "PCMTypeProfileId": col("source.PCMTypeProfileId"),
        "PCMTypeId": col("source.PCMTypeId"),
        "PCMTypeProfileName": col("source.PCMTypeProfileName"),
        "ProfileInUse": col("source.ProfileInUse"),
        "Profile": col("source.Profile"),
        "Device": col("source.Device"),
        "Substrate": col("source.Substrate"),
        "GMGServer": col("source.GMGServer"),
        "FileFormat": col("source.FileFormat"),
        "SurfaceReversePrint": col("source.SurfaceReversePrint"),
        "WorkflowFolderName": col("source.WorkflowFolderName"),
        "SpotColourLibrary1": col("source.SpotColourLibrary1"),
        "SpotColourLibrary2": col("source.SpotColourLibrary2"),
        "ProofSingleCurve": col("source.ProofSingleCurve"),
        "ProofAutomaticCurve": col("source.ProofAutomaticCurve"),
        "SpotColourDGPrediction": col("source.SpotColourDGPrediction"),
        "OriginalProfile": col("source.OriginalProfile"),
        "ProfileNotes": col("source.ProfileNotes"),
        "WFAvailable": col("source.WFAvailable"),
        "PlateData": col("source.PlateData"),
        "MinDot": col("source.MinDot"),
        "LABValues": col("source.LABValues"),
        "PlateTintReference": col("source.PlateTintReference"),
        "FingerPrintDotShapes": col("source.FingerPrintDotShapes"),
        "ScreenSet": col("source.ScreenSet"),
        "HD": col("source.HD"),
        "Resolution": col("source.Resolution"),
        "PlateSingleCurve": col("source.PlateSingleCurve"),
        "PlateAutomatic": col("source.PlateAutomatic"),
        "PlateMakingDGC": col("source.PlateMakingDGC"),
        "ConfigurationSet": col("source.ConfigurationSet"),
        "PlateNotes": col("source.PlateNotes"),
        "Brand": col("source.Brand"),
        "Variety": col("source.Variety"),
        "FingerprintReports": col("source.FingerprintReports"),
        "ProfileStatus": col("source.ProfileStatus"),
        "SiteId": col("source.SiteId")
    }).whenNotMatchedInsert(values={
        "SYS_CHANGE_VERSION": col("source.SYS_CHANGE_VERSION"),
        "SYS_CHANGE_OPERATION": col("source.SYS_CHANGE_OPERATION"),
        "PCMTypeProfileId": col("source.PCMTypeProfileId"),
        "PCMTypeId": col("source.PCMTypeId"),
        "PCMTypeProfileName": col("source.PCMTypeProfileName"),
        "ProfileInUse": col("source.ProfileInUse"),
        "Profile": col("source.Profile"),
        "Device": col("source.Device"),
        "Substrate": col("source.Substrate"),
        "GMGServer": col("source.GMGServer"),
        "FileFormat": col("source.FileFormat"),
        "SurfaceReversePrint": col("source.SurfaceReversePrint"),
        "WorkflowFolderName": col("source.WorkflowFolderName"),
        "SpotColourLibrary1": col("source.SpotColourLibrary1"),
        "SpotColourLibrary2": col("source.SpotColourLibrary2"),
        "ProofSingleCurve": col("source.ProofSingleCurve"),
        "ProofAutomaticCurve": col("source.ProofAutomaticCurve"),
        "SpotColourDGPrediction": col("source.SpotColourDGPrediction"),
        "OriginalProfile": col("source.OriginalProfile"),
        "ProfileNotes": col("source.ProfileNotes"),
        "WFAvailable": col("source.WFAvailable"),
        "PlateData": col("source.PlateData"),
        "MinDot": col("source.MinDot"),
        "LABValues": col("source.LABValues"),
        "PlateTintReference": col("source.PlateTintReference"),
        "FingerPrintDotShapes": col("source.FingerPrintDotShapes"),
        "ScreenSet": col("source.ScreenSet"),
        "HD": col("source.HD"),
        "Resolution": col("source.Resolution"),
        "PlateSingleCurve": col("source.PlateSingleCurve"),
        "PlateAutomatic": col("source.PlateAutomatic"),
        "PlateMakingDGC": col("source.PlateMakingDGC"),
        "ConfigurationSet": col("source.ConfigurationSet"),
        "PlateNotes": col("source.PlateNotes"),
        "Brand": col("source.Brand"),
        "Variety": col("source.Variety"),
        "FingerprintReports": col("source.FingerprintReports"),
        "ProfileStatus": col("source.ProfileStatus"),
        "SiteId": col("source.SiteId")
    }).execute()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
