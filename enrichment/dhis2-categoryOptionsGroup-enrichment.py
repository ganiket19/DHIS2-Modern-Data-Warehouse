# Databricks notebook source
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

sourceShare = 'raw'
sinkShare = 'enriched'
input_file_name = 'categoryOptionGroups.json'
input_folder_name = 'categoryOptionGroups'
output_folder_name = 'categoryOptionGroups'
input_folder_path = f"abfss://{sourceShare}@aniketinternallearning.dfs.core.windows.net/{input_folder_name}/{input_file_name}"
output_folder_path = f"abfss://{sinkShare}@aniketinternallearning.dfs.core.windows.net/{output_folder_name}"

# COMMAND ----------

dfInputPy = spark.read.format("json").load(input_folder_path)

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

#Flatten array of structs and structs
def flatten(df):
   # compute Complex Fields (Lists and Structs) in Schema   
   complex_fields = dict([(field.name, field.dataType)
                             for field in df.schema.fields
                             if type(field.dataType) == ArrayType or  type(field.dataType) == StructType])
   while len(complex_fields)!=0:
      col_name=list(complex_fields.keys())[0]
      print ("Processing :"+col_name+" Type : "+str(type(complex_fields[col_name])))
    
      # if StructType then convert all sub element to columns.
      # i.e. flatten structs
      if (type(complex_fields[col_name]) == StructType):
         expanded = [col(col_name+'.'+k).alias(col_name+'_'+k) for k in [ n.name for n in  complex_fields[col_name]]]
         df=df.select("*", *expanded).drop(col_name)
    
      # if ArrayType then add the Array Elements as Rows using the explode function
      # i.e. explode Arrays
      elif (type(complex_fields[col_name]) == ArrayType):    
         df=df.withColumn(col_name,explode_outer(col_name))
    
      # recompute remaining Complex Fields in Schema       
      complex_fields = dict([(field.name, field.dataType)
                             for field in df.schema.fields
                             if type(field.dataType) == ArrayType or  type(field.dataType) == StructType])
   return df

df=flatten(dfInputPy)
df.printSchema()

# COMMAND ----------

df_select = df.select('categoryOptionGroups_id','categoryOptionGroups_name','categoryOptionGroups_categoryOptions_id').distinct()

# COMMAND ----------

column = {
    "categoryOptionGroups_id":"id",
    "categoryOptionGroups_name":"name",
    "categoryOptionGroups_categoryOptions_id":"categoryOptions_id"
}
for key, value in column.items():
    df_select = df_select.withColumnRenamed(key,value)

# COMMAND ----------

(df_select
     .write
     .format("delta")
     .option("mergeSchema", "true")
     .option("overwriteSchema", "true")
     .mode("Overwrite")
     .save(output_folder_path))
