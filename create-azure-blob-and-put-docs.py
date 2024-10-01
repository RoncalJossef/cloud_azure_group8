from azure.identity import AzureCliCredential
from azure.storage.blob import BlobServiceClient
from azure.mgmt.storage import StorageManagementClient

import uuid
import os
import argparse


def create_bucket(blob_service_client, bucket_name):
    print("Creating Bucket")

    # Create a unique name for the container
    container_name = str(uuid.uuid4())

    # Create the container
    return blob_service_client.create_container(container_name)

# Function to write files to S3
def write_files(container_storage, directory, bucket):
    for filename in os.listdir(directory):
        if filename.endswith(".pdf"):  # Check if the file is a PDF
            file_path = os.path.join(directory, filename)
            with open(file_path, 'rb') as file:
                print(f"Uploading {filename} to bucket {bucket}...")
                container_storage.upload_blob(name=filename, data=file)
                print(f"{filename} uploaded successfully.")

def main(bucket_name, local_dir):
    # Acquire a credential object using CLI-based authentication.
    credential = AzureCliCredential()

    blob_service_client = BlobServiceClient(
        account_url="https://hessogroup8.blob.core.windows.net",
        credential=credential,
        )
    container_storage = create_bucket(blob_service_client, bucket_name)
    write_files(container_storage, local_dir, bucket_name)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload PDF files to an Azure Blob Service")
    parser.add_argument("--bucket_name", help="The name of the Azure Blob to which the files will be uploaded")
    parser.add_argument("--local_path", help="The name of the folder to put the pdf files")
    args = parser.parse_args()
    main(args.bucket_name, args.local_path)

    