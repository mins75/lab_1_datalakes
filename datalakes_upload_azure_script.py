import os
import zipfile
import random
from azure.storage.filedatalake import (
    DataLakeServiceClient,
)
import sys
# Import the needed management objects from the libraries. The azure.common library
# is installed automatically with the other libraries.
from azure.identity import AzureCliCredential
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.storage import StorageManagementClient

# Acquire a credential object using CLI-based authentication.
credential = AzureCliCredential()

def create_container(subscription_id):
    # Obtain the management object for resources.
    resource_client = ResourceManagementClient(credential, subscription_id)

    RESOURCE_GROUP_NAME = "lab1_datalakes"
    LOCATION = "francecentral"# "centralus"

    # Step 1: Provision the resource group.
    rg_result = resource_client.resource_groups.create_or_update(RESOURCE_GROUP_NAME,
        { "location": LOCATION })
    #print(f"Provisioned resource group {rg_result.name}")

    # Step 2: Provision the storage account, starting with a management object.
    storage_client = StorageManagementClient(credential, subscription_id)
    STORAGE_ACCOUNT_NAME = f"pythonazurestorage{random.randint(1,100000):05}"
    availability_result = storage_client.storage_accounts.check_name_availability(
        { "name": STORAGE_ACCOUNT_NAME }
    )

    if not availability_result.name_available:
        #print(f"Storage name {STORAGE_ACCOUNT_NAME} is already in use. Try another name.")
        exit()
    # The name is available, so provision the account
    poller = storage_client.storage_accounts.begin_create(RESOURCE_GROUP_NAME, STORAGE_ACCOUNT_NAME,
        {
            "location" : LOCATION,
            "kind": "StorageV2",
            "sku": {"name": "Standard_LRS"}
        }
    )

    account_result = poller.result()
    #print(f"Provisioned storage account {account_result.name}")

    # Step 3: Retrieve the account's primary access key and generate a connection string.
    keys = storage_client.storage_accounts.list_keys(RESOURCE_GROUP_NAME, STORAGE_ACCOUNT_NAME)
    print(f"Primary key for storage account: {keys.keys[0].value}")

    conn_string = f"DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName={STORAGE_ACCOUNT_NAME};AccountKey={keys.keys[0].value}"
    #print(f"Connection string: {conn_string}")

    # Step 4: Provision the blob container in the account (this call is synchronous)
    #CONTAINER_NAME = "blob-container-01"
    #container = storage_client.blob_containers.create(RESOURCE_GROUP_NAME, STORAGE_ACCOUNT_NAME, CONTAINER_NAME, {})

    # The fourth argument is a required BlobContainer object, but because we don't need any
    # special values there, so we just pass empty JSON.

    #print(f"Provisioned blob container {container.name}")

    print("Created a new azure storage : ", STORAGE_ACCOUNT_NAME)

    return STORAGE_ACCOUNT_NAME, keys.keys[0].value

def upload_folder_contents(filesystem_client, local_folder_path, remote_folder_path):
    # Create the remote folder if it doesn't exist
    directory_client = filesystem_client.get_directory_client(remote_folder_path)
    directory_client.create_directory()

    # Traverse the local folder and upload files recursively
    for root, dirs, files in os.walk(local_folder_path):
        for file in files:
            local_file_path = os.path.join(root, file)
            remote_file_path = os.path.join(remote_folder_path, os.path.relpath(local_file_path, local_folder_path))

            if local_file_path.endswith('.zip'):
                # Checks if the file is a zip one, then unzip it
                unzip_and_upload_folder(filesystem_client, local_file_path, remote_file_path)
            else:
                # Upload the file to Azure Data Lake Storage
                print("Uploading file: {}".format(local_file_path))
                file_client = filesystem_client.get_file_client(remote_file_path)
                file_client.create_file()

                with open(local_file_path, 'rb') as local_file:
                    # Upload it
                    file_client.upload_data(local_file)

def unzip_and_upload_folder(filesystem_client, local_zip_path, remote_folder_path):
    # Extract the zip file to a temporary folder
    temp_folder = "./temp"
    with zipfile.ZipFile(local_zip_path, 'r') as zip_ref:
        zip_ref.extractall(temp_folder)

    # Upload the extracted folder recursively
    upload_folder_contents(filesystem_client, temp_folder, remote_folder_path)

    # Clean up the temporary folder
    for root, dirs, files in os.walk(temp_folder, topdown=False):
        for name in files:
            os.remove(os.path.join(root, name))
        for name in dirs:
            os.rmdir(os.path.join(root, name))
    os.rmdir(temp_folder)

def run(subscription_id):
    container_info = create_container(subscription_id)
    account_name, account_key = container_info[0], container_info[1]

    # Set up the service client with the credentials from the environment variables
    service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
        "https",
        account_name
    ), credential=account_key)

    # Generate a random name for testing purpose
    fs_name = "testfs{}".format(random.randint(1, 1000))
    print("Generating a test filesystem named '{}'.".format(fs_name))

    # Create the filesystem
    filesystem_client = service_client.create_file_system(file_system=fs_name)

    # Local folder path to upload
    local_folder_path = "data"

    # Remote folder path in Azure Data Lake Storage
    remote_folder_path = "data"

    # Invoke the function to upload the folder contents
    try:
        upload_folder_contents(filesystem_client, local_folder_path, remote_folder_path)
    finally:
        print("Done")

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Error: subscription_id argument is required.")
        print("Usage: python datalakes_upload_azure_script.py <subscription_id>")
        sys.exit(1)

    subscription_id = sys.argv[1]
    run(subscription_id)
