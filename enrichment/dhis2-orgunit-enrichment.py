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
input_file_name = 'orgunit.json'
input_folder_name = 'orgunit'
output_folder_name = 'orgunit'
input_folder_path = f"abfss://{sourceShare}@aniketinternallearning.dfs.core.windows.net/{input_folder_name}/{input_file_name}"
output_folder_path = f"abfss://{sinkShare}@aniketinternallearning.dfs.core.windows.net/{output_folder_name}"

# COMMAND ----------

df = spark.read.format("json").load(input_folder_path)

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
      elif (type(complex_fields[col_name]) == ArrayType and col_name != 'organisationUnits_geometry_coordinates'):    
         df=df.withColumn(col_name,explode_outer(col_name))
        
      elif (type(complex_fields[col_name]) == ArrayType and col_name  == 'organisationUnits_geometry_coordinates'): 
         df=df.withColumn(col_name,concat_ws(",",col(col_name)))
    
      # recompute remaining Complex Fields in Schema       
      complex_fields = dict([(field.name, field.dataType)
                             for field in df.schema.fields
                             if type(field.dataType) == ArrayType or  type(field.dataType) == StructType])
   return df

df=flatten(df)
df.printSchema()

# COMMAND ----------

df_select =( df.select('organisationUnits_id','organisationUnits_lastUpdated','organisationUnits_href','organisationUnits_level','organisationUnits_created','organisationUnits_name','organisationUnits_shortName','organisationUnits_code','organisationUnits_leaf','organisationUnits_path','organisationUnits_displayFormName','organisationUnits_displayName','organisationUnits_displayShortName','organisationUnits_favorite','organisationUnits_externalAccess','organisationUnits_openingDate','organisationUnits_dimensionItem','organisationUnits_parent_id','organisationUnits_translations_locale','organisationUnits_translations_property','organisationUnits_translations_value','organisationUnits_geometry_coordinates')
            .withColumn('organisationUnits_geometry_coordinates',when(df.organisationUnits_level != 4,None).otherwise(df.organisationUnits_geometry_coordinates))
            .distinct())

# COMMAND ----------

display(df_select)

# COMMAND ----------

df_select = df_select.withColumn('translated',concat_ws('_',df_select.organisationUnits_translations_property,df_select.organisationUnits_translations_locale))

# COMMAND ----------

df_select.count()

# COMMAND ----------

pivotDF = df_select.groupBy('organisationUnits_id','organisationUnits_lastUpdated','organisationUnits_href','organisationUnits_level','organisationUnits_created','organisationUnits_name','organisationUnits_shortName','organisationUnits_code','organisationUnits_leaf','organisationUnits_path','organisationUnits_displayFormName','organisationUnits_displayName','organisationUnits_displayShortName','organisationUnits_favorite','organisationUnits_externalAccess','organisationUnits_openingDate','organisationUnits_dimensionItem','organisationUnits_parent_id','organisationUnits_geometry_coordinates').pivot("translated").agg(first("organisationUnits_translations_value"))

# COMMAND ----------

pivotDF.drop('')

# COMMAND ----------

column = {
    "organisationUnits_id":"id",
    "organisationUnits_lastUpdated":"lastUpdated",
    "organisationUnits_href":"href",
    "organisationUnits_level":"level",
    "organisationUnits_created":"created",
    "organisationUnits_name":"name",
    "organisationUnits_shortName":"shortName",
    "organisationUnits_code":"code",
    "organisationUnits_leaf":"leaf",
    "organisationUnits_path":"path",
    "organisationUnits_displayFormName":"displayFormName",
    "organisationUnits_displayName":"displayName",
    "organisationUnits_displayShortName":"displayShortName",
    "organisationUnits_favorite":"favorite",
    "organisationUnits_externalAccess":"externalAccess",
    "organisationUnits_openingDate":"openingDate",
    "organisationUnits_dimensionItem":"dimensionItem",
    "organisationUnits_parent_id":"parent_id",
    "organisationUnits_geometry_coordinates":"coordinates"
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
