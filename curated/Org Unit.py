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
input_folder_name = 'orgunit'
output_folder_name = 'OrganisationUnits'
input_folder_path = f"abfss://{sourceShare}@aniketinternallearning.dfs.core.windows.net/{input_folder_name}"
output_folder_path = f"abfss://{sinkShare}@aniketinternallearning.dfs.core.windows.net/{output_folder_name}"

# COMMAND ----------

df = spark.read.format("delta").load(input_folder_path)

# COMMAND ----------

df_orgnit = (df.withColumn('long',
                                 when(df.coordinates == None,df.coordinates)
                                 .when(df.coordinates.isNull(),df.coordinates)
                                 .otherwise(substring_index(df.coordinates, ',', 1)))
                .withColumn('lat',
                                 when(df.coordinates == None,df.coordinates)
                                 .when(df.coordinates.isNull(),df.coordinates)
                                 .otherwise(substring_index(df.coordinates, ',', -1))))

# COMMAND ----------

display(df_orgnit)

# COMMAND ----------

df_orgnit = df_orgnit.select('code','name','id','shortName','path','level','parent_id','lat','long')

# COMMAND ----------

df_orgnit.write.format("parquet").option("mergeSchema", "true").option("overwriteSchema", "true").mode("Overwrite").save(output_folder_path)
