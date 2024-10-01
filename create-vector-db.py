
import argparse
import os
from azure.identity import AzureCliCredential
from azure.mgmt.search import SearchManagementClient
from azure.mgmt.search.models import SearchService

location = "eastus"
resource_group_name = "learning"

def main(collection_name):
    # Initialize the Search Management Client
    credential = AzureCliCredential()
    subscription_id = os.environ["AZURE_SUBSCRIPTION_ID"]
    search_client = SearchManagementClient(credential, subscription_id)

    # Create the Search Service
    search_service = SearchService(
        location=location,
        sku={"name": "standard", "tier": "standard"},  # Choose the appropriate SKU
        hosting_mode="default",
        replica_count=1,
        partition_count=1
    )

    # Create the search service
    result = search_client.services.begin_create_or_update(
        resource_group_name=resource_group_name,
        search_service_name=collection_name,
        service=search_service
    ).result()

    print(f"Search Service '{result.name}' created in '{result.location}'.")



if __name__== "__main__":
    parser = argparse.ArgumentParser(description="Create collection")
    parser.add_argument("--collection_name", help="The name of the collection")
    args = parser.parse_args()
    main(args.collection_name)
