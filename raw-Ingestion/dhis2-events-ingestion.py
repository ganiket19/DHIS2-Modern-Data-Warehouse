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
# MAGIC %pip install pip install asyncio
# MAGIC %pip install pip install aiohttp

# COMMAND ----------

import requests
from requests.auth import HTTPBasicAuth
from azure.storage.blob import BlobServiceClient
import json
url = "https://play.dhis2.org/2.38.1.1/api/organisationUnits?paging=false"
response = requests.get(url, auth=HTTPBasicAuth('admin', 'district'))
json_data = json.loads(response.content)

# COMMAND ----------

org_unit_id  = []
for orgunit in json_data['organisationUnits']:
    id = orgunit.get('id',{})
    org_unit_id.append(id)
print(org_unit_id)
print(len(org_unit_id))

# COMMAND ----------

sourceShare = 'raw'
output_path = 'events'


# COMMAND ----------

wm_value = spark.sql("SELECT SUBSTRING(CAST(max(lastUpdated) as string),1,10) as date,SUBSTRING(CAST(max(lastUpdated) as string),12,23) as time from delta.`abfss://enriched@aniketinternallearning.dfs.core.windows.net/events`")

# COMMAND ----------

from pyspark.sql.functions import *
vm_value = wm_value.withColumn('datetime',concat(col('date'),lit('T'),col('time'))).drop('date','time').first()[0]

# COMMAND ----------

print(vm_value)

# COMMAND ----------

blob_connection_string = dbutils.secrets.get(scope="aniketghodindekeyvault",key="aniketinternallearningconstring")
blob_service_client = BlobServiceClient.from_connection_string(conn_str=blob_connection_string)

# COMMAND ----------

import aiohttp
import asyncio

async def main():
    session_timeout =   aiohttp.ClientTimeout(total=None,sock_connect=1300,sock_read=1300)
    async with aiohttp.ClientSession(timeout=session_timeout) as session:
        tasks = []
        for id in org_unit_id:
            task = asyncio.ensure_future(get_org_unit_id(session, id))
            tasks.append(task)
        view_counts = await asyncio.gather(*tasks)


async def get_org_unit_id(session,id):
    url = f"https://play.dhis2.org/2.38.1.1/api/events?&orgUnit={id}&skipPaging=true&includeDeleted=true&fields=*&totalPages=true&lastUpdatedStartDate={vm_value}"
    async with session.get(url, auth=aiohttp.BasicAuth('admin', 'district'),ssl=False) as response:
        result_data = await response.json()
        json_data = json.dumps(result_data)
        json_file_name = id+".json"
        upload_blob_client_json = blob_service_client.get_blob_client(container='raw',blob=f'{output_path}/{json_file_name}')
        upload_blob_client_json.upload_blob(json_data, overwrite=True)
await main()
