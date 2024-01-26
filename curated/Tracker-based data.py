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
input_folder_name1 = 'dataElementGroups' 
output_folder_name = 'Tracker-based_data'
input_folder_path = f"abfss://{sourceShare}@aniketinternallearning.dfs.core.windows.net/{input_folder_name}"
output_folder_path = f"abfss://{sinkShare}@aniketinternallearning.dfs.core.windows.net/{output_folder_name}"
input_folder_path1 =  f"abfss://{sourceShare}@aniketinternallearning.dfs.core.windows.net/{input_folder_name1}"

# COMMAND ----------

df_dimension = spark.read.format("delta").load(input_folder_path)

# COMMAND ----------

df_dimension = df_dimension.filter(col('name')==lit('Tracker-based data'))

# COMMAND ----------

df_dataElementGroups = spark.read.format("delta").load(input_folder_path1)

# COMMAND ----------

df_Tracker_based_data = df_dimension.join(df_dataElementGroups,df_dimension.items_id==df_dataElementGroups.id,"left").select(df_dataElementGroups['name'],'dataElements_id').distinct()

# COMMAND ----------

display(df_Tracker_based_data)

# COMMAND ----------

df_Tracker_based_data.write.format("parquet").option("mergeSchema", "true").option("overwriteSchema", "true").mode("Overwrite").save(output_folder_path)
