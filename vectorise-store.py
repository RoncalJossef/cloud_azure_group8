from langchain_community.document_loaders import PyPDFDirectoryLoader
from langchain.text_splitter import RecursiveCharacterTextSplitter
from azure.search.documents.indexes import SearchIndexClient
from azure.search.documents.indexes.models import SearchIndex, SimpleField, SearchableField
# from langchain_community.embeddings import BedrockEmbeddings
# from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth
# from langchain_community.vectorstores import OpenSearchVectorSearch
from azure.identity import AzureCliCredential
from azure.storage.blob import BlobServiceClient
import os
import argparse


## Container name
container_name = "f9a1317d-792f-4aa1-a8ca-750851bba299"


## Create Index in Opensearch
def create_index(client, index_name):
    # Define the index schema
    index = SearchIndex(
        name=index_name,
        fields=[
            SimpleField(name="id", type="Edm.String", key=True),
            SearchableField(name="name", type="Edm.String"),
            SimpleField(name="description", type="Edm.String")
        ]
    )

    # Create the index
    client.create_index(index)
    print("Index created.")



## Load docs from S3
def download_documents(blob_service_client,local_dir):
    # Download the blob to a local file
    container_client = blob_service_client.get_container_client(container= container_name) 

    for blob in container_client.list_blobs():
        if blob.name.endswith(".pdf"):
            blob_client = container_client.get_blob_client(blob)
            download_path = os.path.join(local_dir, blob.name)

            # Download each PDF file
            with open(download_path, "wb") as download_file:
                download_file.write(blob_client.download_blob().readall())

            print(f"Downloaded: {blob.name}")

   


## Split pages/text into chunks
def split_text(docs, chunk_size, chunk_overlap):
    text_splitter = RecursiveCharacterTextSplitter(chunk_size=chunk_size, chunk_overlap=chunk_overlap)
    chunks = text_splitter.split_documents(docs)   

    return chunks
 
# ## Generate embeddings 
# def generate_embeddings(bedrock_client, chunks):
#     embeddings_model = BedrockEmbeddings(model_id="amazon.titan-embed-text-v1", client=bedrock_client)
#     chunks_list=[chunk.page_content for chunk in chunks]
#     embeddings = embeddings_model.embed_documents(chunks_list)
#     return embeddings

# # Store generated embeddings into an OpenSearch index.
# def store_embeddings(embeddings, texts, meta_data, host, awsauth, index_name):  
    
#     docsearch = OpenSearchVectorSearch.from_embeddings(
#         embeddings,
#         texts,
#         meta_data,
#         opensearch_url=f'https://{host}:443',
#         http_auth=awsauth,
#         use_ssl=True,
#         verify_certs=True,
#         connection_class=RequestsHttpConnection,
#         index_name=index_name,
#         bulk_size=1000
# )

#     return docsearch


# # Func to do both generating and storing embeddings
# def generate_store_embeddings(bedrock_client, chunks,awsauth,index_name):
#     embeddings_model = BedrockEmbeddings(model_id="amazon.titan-embed-text-v1", client=bedrock_client)
#     docsearch = OpenSearchVectorSearch.from_documents(
#         chunks,
#         embeddings_model,
#         opensearch_url=f'https://{host}:443',
#         http_auth=awsauth,
#         use_ssl=True,
#         verify_certs=True,
#         connection_class=RequestsHttpConnection,
#         index_name=index_name,
#         bulk_size=1000
# )

#     return docsearch



## main 
def main(bucket_name, endpoint, index_name, local_path):

    # Acquire a credential object using CLI-based authentication.
    credential = AzureCliCredential()

    # Initialize the index client
    index_client = SearchIndexClient(endpoint=f"https://azuregroup8.search.windows.net",
                                  credential=credential)

    blob_service_client = BlobServiceClient(
        account_url="https://hessogroup8.blob.core.windows.net",
        credential=credential,
        )

    download_documents(blob_service_client,local_path)
    loader= PyPDFDirectoryLoader(local_path)
    docs = loader.load()
    print('Start chunking')
    chunks = split_text(docs, 1000, 100)
    print(chunks[1])
    create_index(index_client,index_name)
    print('Start vectorising')
    # embeddings= generate_embeddings(bedrock_client, chunks)
    # print(embeddings[1])
    # texts = [chunk.page_content for chunk in chunks]
    #  # Prepare metadata for each chunk
    # meta_data = [{'source': chunk.metadata['source'], 'page': chunk.metadata['page'] + 1} for chunk in chunks]
    # print('Start storing')
    # store_embeddings(embeddings, texts, meta_data ,endpoint, awsauth,index_name)
    # print('End storing')


   

  




if __name__== "__main__":
    parser = argparse.ArgumentParser(description="Process PDF documents and store their embeddings.")
    parser.add_argument("--bucket_name", help="The S3 bucket name where documents are stored")
    # parser.add_argument("--endpoint", help="The OpenSearch service endpoint")
    # parser.add_argument("--index_name", help="The name of the OpenSearch index")
    parser.add_argument("--local_path", help="local path")
    args = parser.parse_args()
    main(args.bucket_name, args.local_path)
