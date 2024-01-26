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
input_file_name = '*.json'
input_folder_name = 'dataElement-values'
output_folder_name = 'dataElement-values'
input_folder_path = f"abfss://{sourceShare}@aniketinternallearning.dfs.core.windows.net/{input_folder_name}/{input_file_name}"
output_folder_path = f"abfss://{sinkShare}@aniketinternallearning.dfs.core.windows.net/{output_folder_name}"

# COMMAND ----------

dfInputPy = spark.read.format("json").load(input_folder_path)

# COMMAND ----------

import pyspark.sql.functions as F
dfInputPy = dfInputPy.withColumn("size", F.size(F.col("dataValues")))

# COMMAND ----------

if dfInputPy.filter("size >= 1").count() == 0:
    print('null')
    dbutils.notebook.exit("No Data Found")

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

df_select = df.select('dataValues_dataElement','dataValues_period','dataValues_orgUnit','dataValues_categoryOptionCombo','dataValues_attributeOptionCombo','dataValues_value','dataValues_storedBy','dataValues_created','dataValues_lastUpdated','dataValues_comment','dataValues_followup').distinct()

# COMMAND ----------

df_select = (df_select.withColumn('dataValues_lastUpdated',to_timestamp(df_select.dataValues_lastUpdated.substr(1, 19)))
                      .withColumn('dataValues_created',to_timestamp(df_select.dataValues_created.substr(1, 19))))               

# COMMAND ----------

column = {
    "dataValues_dataElement":"dataElement_id",
    "dataValues_period":"period",
    "dataValues_orgUnit":"orgUnit",
    "dataValues_categoryOptionCombo":"categoryOptionCombo_id",
    "dataValues_attributeOptionCombo":"attributeOptionCombo_id",
    "dataValues_value":"value",
    "dataValues_storedBy":"storedBy",
    "dataValues_created":"created",
    "dataValues_lastUpdated":"lastUpdated",
    "dataValues_comment":"comment",
    "dataValues_followup":"followup"
}
for key, value in column.items():
    df_select = df_select.withColumnRenamed(key,value)

# COMMAND ----------


from delta.tables import *
if(DeltaTable.isDeltaTable(spark, output_folder_path)):
    print("delta table found")
    isIncremental = True
else: 
    print("delta table not found, setting to not incremental!")
    isIncremental = False

if(isIncremental is False):
    (df_select
     .write
     .format("delta")
     .option("mergeSchema", "true")
     .option("overwriteSchema", "true")
     .mode("Overwrite")
     .save(output_folder_path))
else:
    deltaTable = DeltaTable.forPath(spark, output_folder_path)
    deltaTable.alias("target").merge(
        df_select.alias("source"),"target.dataElement_id=source.dataElement_id and target.categoryOptionCombo_id = source.categoryOptionCombo_id and source.attributeOptionCombo_id=target.attributeOptionCombo_id and source.period = target.period and source.orgUnit = target.orgUnit"
     ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

