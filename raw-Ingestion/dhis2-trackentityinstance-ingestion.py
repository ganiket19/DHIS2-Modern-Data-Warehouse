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

# MAGIC %pip install pip install azure-storage-blob

# COMMAND ----------

import requests
from requests.auth import HTTPBasicAuth
from azure.storage.blob import BlobServiceClient
import json
url = "https://play.dhis2.org/2.38.1.1/api/organisationUnits?paging=false&fields=*"
response = requests.get(url, auth=HTTPBasicAuth('admin', 'district'))
json_data = json.loads(response.content)

# COMMAND ----------

program_json = json_data['organisationUnits'][0]['programs']
print(program_json)

# COMMAND ----------

program  = []
for programs in program_json:
    id = programs.get('id',{})
    program.append(id)

# COMMAND ----------

blob_connection_string = dbutils.secrets.get(scope="aniketghodindekeyvault",key="aniketinternallearningconstring")
blob_service_client = BlobServiceClient.from_connection_string(conn_str=blob_connection_string)

# COMMAND ----------

sourceShare = 'raw'
output_path = 'trackedentityinstances'

# COMMAND ----------

wm_value = spark.sql("SELECT SUBSTRING(CAST(max(lastUpdated) as string),1,10) as date,SUBSTRING(CAST(max(lastUpdated) as string),12,23) as time from delta.`abfss://enriched@aniketinternallearning.dfs.core.windows.net/trackedentityinstances`")

# COMMAND ----------

from pyspark.sql.functions import *
vm_value = wm_value.withColumn('datetime',concat(col('date'),lit('T'),col('time'))).drop('date','time').first()[0]

# COMMAND ----------

for id in program:
    url = f"https://play.dhis2.org/2.38.1.1/api/trackedEntityInstances?ouMode=ALL&includeDeleted=true&fields=*&skipPaging=true&program={id}&lastUpdatedStartDate={vm_value}"
    response = requests.get(url, auth=HTTPBasicAuth('admin', 'district'))
    try:
        json_data = json.dumps(response.json())
        json_file_name = id+".json"
        upload_blob_client_json = blob_service_client.get_blob_client(container='raw',blob=f'{output_path}/{json_file_name}')
        upload_blob_client_json.upload_blob(json_data, overwrite=True)
    except Exception as e:
        print(str(e))
        print(response)
        print(id)
        pass
