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
input_folder_name1 = 'categoryOptions' 
output_folder_name = 'HIV_age'
input_folder_path = f"abfss://{sourceShare}@aniketinternallearning.dfs.core.windows.net/{input_folder_name}"
output_folder_path = f"abfss://{sinkShare}@aniketinternallearning.dfs.core.windows.net/{output_folder_name}"
input_folder_path1 =  f"abfss://{sourceShare}@aniketinternallearning.dfs.core.windows.net/{input_folder_name1}"

# COMMAND ----------

df_dimension = spark.read.format("delta").load(input_folder_path)

# COMMAND ----------

df_dimension = df_dimension.filter(col('name')==lit('HIV age'))

# COMMAND ----------

df_categoryOptions = spark.read.format("delta").load(input_folder_path1)

# COMMAND ----------

df_HIV_age= df_dimension.join(df_categoryOptions,df_dimension.items_id==df_categoryOptions.id,"left").select(df_categoryOptions['name'],'categoryOptionCombos_id')

# COMMAND ----------

display(df_HIV_age)

# COMMAND ----------

df_HIV_age.write.format("parquet").option("mergeSchema", "true").option("overwriteSchema", "true").mode("Overwrite").save(output_folder_path)
