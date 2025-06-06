import os, uuid
from azure.storage.blob import BlobServiceClient
from azure.identity import ClientSecretCredential


def auth_active_directory():
   token_credential = ClientSecretCredential(
      tenant_id=os.getenv("TENANT_ID"),
      client_id=os.getenv("CLIENT_ID"),
      client_secret=os.getenv("CLIENT_SECRET")
   )


   # Instantiate a BlobServiceClient using a token credential
   from azure.storage.blob import BlobServiceClient
   return token_credential

   # Get account information for the Blob Service
   account_info = blob_service_client.get_service_properties()

def UploadToBlob(sourceFileToUpload, container, targetFilePath):
   account_url = "https://kcazncdevdl.blob.core.windows.net/"
   creds = auth_active_directory()
   blob_service_client = BlobServiceClient(account_url, credential=creds)
   container_name = container

   blob_client = blob_service_client.get_blob_client(container=container_name, blob=targetFilePath)

   # Upload the created file
   with open(file=sourceFileToUpload, mode="rb") as data:
      blob_client.upload_blob(data, overwrite=True)

def __init__():
   FILE_PATH = current_dir = os.path.dirname(os.path.abspath(__file__)) + '\\equipment_list.parquet'
   UploadToBlob(FILE_PATH, container=None, targetFilePath=None)