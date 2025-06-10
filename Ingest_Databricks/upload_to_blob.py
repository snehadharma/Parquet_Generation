
from azure.storage.blob import BlobServiceClient
from azure.identity import ClientSecretCredential
from pathlib import Path
import os
import pandas as pd
from dotenv import load_dotenv

load_dotenv()


def get_parquet_path():
    # Get path to hi.txt
    current_file = Path(__file__)
    file_path = current_file.parent.parent / 'Query_Databricks' / 'equipment_list.parquet'
    
    return file_path

def auth_active_directory():
   token_credential = ClientSecretCredential(
      tenant_id=os.getenv("TENANT_ID"),
      client_id=os.getenv("CLIENT_ID"),
      client_secret=os.getenv("CLIENT_SECRET")
   )

   return token_credential

def blob_upload(sourceFileToUpload, container, targetFilePath):
   account_url = os.getenv("ACCOUNT_URL")
   creds = auth_active_directory()

   blob_service_client = BlobServiceClient(account_url, credential=creds)
   blob_client = blob_service_client.get_blob_client(container=container, blob=targetFilePath)

   # Upload the created file
   with open(file=sourceFileToUpload, mode="rb") as data:
      blob_client.upload_blob(data, overwrite=True)

# df = pd.read_parquet(get_parquet_path())
# print(df)

blob_upload(get_parquet_path(), container="dropzone", targetFilePath="elt_v2_dropzone/dev/kmt_sneha/source_data/equipment_list.parquet")

# need target file path to upload parquet files to, 
# need tenant id, client id, client secret <- credentials , account url for blob service 
# need container container = req[requestConstants.DATABRICKSSETTINGS][requestConstants.CONTAINER]  ?