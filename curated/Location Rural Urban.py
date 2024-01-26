# Databricks notebook source
from pyspark.sql import *
from pyspark.sql.functions import *

# COMMAND ----------

use_temp_paths= True
if(use_temp_paths):
    spark.conf.set(
        "fs.azure.account.key.aniketinternallearning.dfs.core.windows.net",
        dbutils.secrets.get(scope="aniketghodindekeyvault",key="aniketinternallearningaccesskey")
    )
else:
    spark.conf.unset(
        "fs.azure.account.key.aniketinternallearning.dfs.core.windows.net"
     )

# COMMAND ----------

sourceShare = 'enriched'
sinkShare = 'curated'
input_folder_name = 'dimension'
input_folder_name1 = 'organisationUnitGroups' 
output_folder_name = 'Location_Rural_Urban'
input_folder_path = f"abfss://{sourceShare}@aniketinternallearning.dfs.core.windows.net/{input_folder_name}"
output_folder_path = f"abfss://{sinkShare}@aniketinternallearning.dfs.core.windows.net/{output_folder_name}"
input_folder_path1 =  f"abfss://{sourceShare}@aniketinternallearning.dfs.core.windows.net/{input_folder_name1}"

# COMMAND ----------

df_dimension = spark.read.format("delta").load(input_folder_path)

# COMMAND ----------

df_dimension = df_dimension.filter(col('name')==lit('Location Rural/Urban'))

# COMMAND ----------

df_organisationUnitGroups = spark.read.format("delta").load(input_folder_path1)

# COMMAND ----------

df_Location_Rural_Urban = df_dimension.join(df_organisationUnitGroups,df_dimension.items_id==df_organisationUnitGroups.id,"left").select(df_organisationUnitGroups['name'],'organisationUnits_id')

# COMMAND ----------

df_Location_Rural_Urban.write.format("parquet").option("mergeSchema", "true").option("overwriteSchema", "true").mode("Overwrite").save(output_folder_path)
