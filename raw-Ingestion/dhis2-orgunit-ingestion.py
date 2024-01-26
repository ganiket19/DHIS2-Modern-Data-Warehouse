# Databricks notebook source
# MAGIC %pip install pip install azure-storage-blob

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

from azure.storage.blob import BlobServiceClient
import requests
from requests.auth import HTTPBasicAuth
import json
url = "https://play.dhis2.org/2.38.1.1/api/organisationUnits?paging=false&fields=*"
response = requests.get(url, auth=HTTPBasicAuth('admin', 'district'))
json_data = json.dumps(response.json())

# COMMAND ----------

blob_connection_string = dbutils.secrets.get(scope="aniketghodindekeyvault",key="aniketinternallearningconstring")
blob_service_client = BlobServiceClient.from_connection_string(conn_str=blob_connection_string)

# COMMAND ----------

sourceShare = 'raw'
json_file_name = 'orgunit.json'
output_path = 'orgunit'

# COMMAND ----------

upload_blob_client_json = blob_service_client.get_blob_client(container='raw',blob=f'{output_path}/{json_file_name}')
upload_blob_client_json.upload_blob(json_data, overwrite=True)

# COMMAND ----------


