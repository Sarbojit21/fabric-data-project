-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "synapse_pyspark"
-- META   },
-- META   "dependencies": {
-- META     "warehouse": {
-- META       "default_warehouse": "298cd444-fb22-4489-a6f9-4843f505fceb",
-- META       "known_warehouses": [
-- META         {
-- META           "id": "298cd444-fb22-4489-a6f9-4843f505fceb",
-- META           "type": "Lakewarehouse"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

-- MAGIC %%pyspark
-- MAGIC from delta.tables import DeltaTable
-- MAGIC 
-- MAGIC dt = DeltaTable.forPath(spark, "abfss://Propelis_Production@onelake.dfs.fabric.microsoft.com/SILVER.Lakehouse/Tables/MYSGSEU/OPEN_ORDERS_SNAPSHOT")
-- MAGIC 
-- MAGIC dt.delete("1=1")
-- MAGIC 
-- MAGIC print("Table truncated")

-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }
