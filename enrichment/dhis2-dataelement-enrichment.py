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
input_file_name = 'dataelement.json'
input_folder_name = 'dataelement'
output_folder_name = 'dataelement'
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

df_select = df.select('dataElements_id','dataElements_lastUpdated','dataElements_href','dataElements_created','dataElements_name','dataElements_shortName','dataElements_code','dataElements_displayFormName','dataElements_displayName','dataElements_displayShortName','dataElements_externalAccess','dataElements_dimensionItem','dataElements_translations_locale','dataElements_translations_property','dataElements_translations_value','dataElements_aggregationType','dataElements_publicAccess','dataElements_description','dataElements_valueType','dataElements_domainType','dataElements_formName','dataElements_displayDescription','dataElements_zeroIsSignificant','dataElements_url','dataElements_favorite','dataElements_optionSetValue','dataElements_dimensionItemType').distinct()

# COMMAND ----------

display(df_select)

# COMMAND ----------

df_select = df_select.withColumn('translated',concat_ws('_',df_select.dataElements_translations_property,df_select.dataElements_translations_locale))

# COMMAND ----------

pivotDF = df_select.groupBy('dataElements_id','dataElements_lastUpdated','dataElements_href','dataElements_created','dataElements_name','dataElements_shortName','dataElements_code','dataElements_displayFormName','dataElements_displayName','dataElements_displayShortName','dataElements_externalAccess','dataElements_dimensionItem','dataElements_aggregationType','dataElements_publicAccess','dataElements_description','dataElements_valueType','dataElements_domainType','dataElements_formName','dataElements_displayDescription','dataElements_zeroIsSignificant','dataElements_url','dataElements_favorite','dataElements_optionSetValue','dataElements_dimensionItemType').pivot("translated").agg(first("dataElements_translations_value"))

# COMMAND ----------

pivotDF.drop('')

# COMMAND ----------

column = {
    "dataElements_id":"id",
    "dataElements_lastUpdated":"lastUpdated",
    "dataElements_href":"href",
    "dataElements_created":"created",
    "dataElements_name":"name",
    "dataElements_shortName":"shortName",
    "dataElements_code":"code",
    "dataElements_displayFormName":"displayFormName",
    "dataElements_displayName":"displayName",
    "dataElements_displayShortName":"displayShortName",
    "dataElements_externalAccess":"externalAccess",
    "dataElements_dimensionItem":"dimensionItem",
    "dataElements_aggregationType":"aggregationType",
    "dataElements_publicAccess":"publicAccess",
    "dataElements_description":"description",
    "dataElements_valueType":"valueType",
    "dataElements_domainType":"domainType",
    "dataElements_formName":"formName",
    "dataElements_displayDescription":"displayDescription",
    "dataElements_zeroIsSignificant":"zeroIsSignificant",
    "dataElements_url":"url",
    "dataElements_favorite":"favorite",
    "dataElements_optionSetValue":"optionSetValue",
    "dataElements_dimensionItemType":"dimensionItemType"
}
for key, value in column.items():
    pivotDF = pivotDF.withColumnRenamed(key,value)

# COMMAND ----------

(pivotDF
     .write
     .format("delta")
     .option("mergeSchema", "true")
     .option("overwriteSchema", "true")
     .mode("Overwrite")
     .save(output_folder_path))
