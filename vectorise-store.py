import configparser
from langchain_community.document_loaders import PyPDFDirectoryLoader
from langchain.text_splitter import RecursiveCharacterTextSplitter
from azure.search.documents.indexes import SearchIndexClient
from azure.search.documents.indexes.models import SearchIndex, SimpleField, SearchableField, ComplexField
from azure.core.credentials import AzureKeyCredential
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
from azure.core.exceptions import ResourceNotFoundError
import os
import requests
import json

## Read the config.ini file
config = configparser.ConfigParser()
config.read('config.ini')

## Container name
container_name = "41142b5e-653c-40a8-91a1-66c518edcf07"

def create_index(client, index_name):
    try:
        client.get_index(index_name)
        print(f"Index '{index_name}' already exists.")
    except ResourceNotFoundError:
        index = SearchIndex(
            name=index_name,
            fields=[
                SimpleField(name="id", type="Edm.String", key=True),
                SearchableField(name="name", type="Edm.String"),
                SearchableField(name="description", type="Edm.String"),
                SimpleField(name="embedding", type="Collection(Edm.Double)")  # Corrected to SimpleField
            ]
        )
        client.create_index(index)
        print(f"Index '{index_name}' created.")
    except Exception as e:
        print(f"Error occurred while creating or accessing index: {str(e)}")


## Download documents from Azure Blob Storage
def download_documents(blob_service_client, local_dir):
    # Get the container client
    container_client = blob_service_client.get_container_client(container=container_name)

    # Download each PDF file from the blob storage
    for blob in container_client.list_blobs():
        if blob.name.endswith(".pdf"):
            blob_client = container_client.get_blob_client(blob)
            download_path = os.path.join(local_dir, blob.name)

            # Download each PDF file
            with open(download_path, "wb") as download_file:
                download_file.write(blob_client.download_blob().readall())

            print(f"Downloaded: {blob.name}")

## Split documents into chunks
def split_text(docs, chunk_size, chunk_overlap):
    text_splitter = RecursiveCharacterTextSplitter(chunk_size=chunk_size, chunk_overlap=chunk_overlap)
    chunks = text_splitter.split_documents(docs)
    return chunks

## Generate embeddings using Azure OpenAI REST API
def generate_embeddings(text, openai_endpoint, api_key):
    url = f"{openai_endpoint}/openai/deployments/text-embedding-ada-002/embeddings?api-version=2022-12-01"
    
    headers = {
        "Content-Type": "application/json",
        "api-key": api_key
    }
    
    data = {
        "input": text
    }

    response = requests.post(url, headers=headers, json=data)

    if response.status_code == 200:
        return response.json()["data"][0]["embedding"]
    else:
        print(f"Error in embedding request: {response.status_code}, {response.text}")
        return None

## Index the documents and embeddings into Azure Cognitive Search
def index_documents(client, index_name, docs, embeddings):
    search_client = client.get_search_client(index_name)

    documents = []
    for i, (doc, embedding) in enumerate(zip(docs, embeddings)):
        document = {
            "id": str(i),
            "name": doc.metadata['source'],
            "description": doc.page_content,
            "embedding": embedding  # Store the embedding
        }
        documents.append(document)

    # Upload documents in batches
    result = search_client.upload_documents(documents=documents)
    print(f"Documents uploaded: {result}")

## Main function
def main():
    # Read configurations from the config.ini file
    bucket_name = config.get('azure', 'bucket_name')
    endpoint = config.get('azure', 'endpoint')
    index_name = config.get('azure', 'index_name')
    api_key = config.get('azure', 'api_key')
    openai_api_key = config.get('openai', 'openai_api_key')
    openai_endpoint = config.get('openai', 'openai_endpoint')
    local_path = config.get('local', 'local_path')

    # Use the API key provided as a command-line argument
    credential = AzureKeyCredential(api_key)

    # Initialize the index client
    index_client = SearchIndexClient(endpoint=f"https://{endpoint}.search.windows.net", credential=credential)

    # Initialize the blob service client with DefaultAzureCredential
    blob_service_client = BlobServiceClient(
        account_url=f"https://{bucket_name}.blob.core.windows.net",
        credential=DefaultAzureCredential(),  # Use DefaultAzureCredential for Blob access
    )

    # Download documents from blob storage
    download_documents(blob_service_client, local_path)

    # Load PDF files from the local directory
    loader = PyPDFDirectoryLoader(local_path)
    docs = loader.load()

    # Split the documents into chunks
    print('Start chunking')
    chunks = split_text(docs, 1000, 100)
    print(chunks[1])

    # Generate embeddings using Azure OpenAI REST API
    print('Generating embeddings...')
    embeddings = []
    for chunk in chunks:
        embedding = generate_embeddings(chunk.page_content, openai_endpoint, openai_api_key)
        if embedding:
            embeddings.append(embedding)

    # Create the search index if it doesn't exist
    create_index(index_client, index_name)

    # Index the document chunks and embeddings into Azure Cognitive Search
    print('Start indexing')
    index_documents(index_client, index_name, chunks, embeddings)
    print('Indexing completed')

## Entry point of the script
if __name__ == "__main__":
    main()
