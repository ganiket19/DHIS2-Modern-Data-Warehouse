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
input_folder_name1 = 'categoryOptionGroups' 
output_folder_name = 'Donor'
input_folder_name2 = 'categoryOptions'
input_folder_path = f"abfss://{sourceShare}@aniketinternallearning.dfs.core.windows.net/{input_folder_name}"
output_folder_path = f"abfss://{sinkShare}@aniketinternallearning.dfs.core.windows.net/{output_folder_name}"
input_folder_path1 =  f"abfss://{sourceShare}@aniketinternallearning.dfs.core.windows.net/{input_folder_name1}"
input_folder_path2 = f"abfss://{sourceShare}@aniketinternallearning.dfs.core.windows.net/{input_folder_name2}"

# COMMAND ----------

df_dimension = spark.read.format("delta").load(input_folder_path)

# COMMAND ----------

df_dimension = df_dimension.filter(col('name')==lit('Donor'))

# COMMAND ----------

df_categoryOptionGroups = spark.read.format("delta").load(input_folder_path1)

# COMMAND ----------

df_categoryOptions = spark.read.format("delta").load(input_folder_path2)

# COMMAND ----------

df_donor = (df_dimension.join(df_categoryOptionGroups,df_dimension.items_id==df_categoryOptionGroups.id,"left")      .join(df_categoryOptions,df_categoryOptionGroups.categoryOptions_id==df_categoryOptions.id,'left').select(df_categoryOptionGroups['name'],df_categoryOptions['categoryOptionCombos_id']))

# COMMAND ----------

display(df_donor)

# COMMAND ----------

df_donor.write.format("parquet").option("mergeSchema", "true").option("overwriteSchema", "true").mode("Overwrite").save(output_folder_path)
