# lab_1_datalakes

To launch the script : `python datalakes_upload_azure_script.py <subscription_id>`

Needs the subscription_id from Azure available with `az login`


## Script

- It will create a resource group on Azure, provision it.
- Get both the account name and account key to run the script to upload files on Azure
- Upload each file if possible, if not (.zip) it will unzip it and extract the files and upload them on Azure

## Example

After running the script, we will see on Azure that we have created the resource group

![image](https://github.com/mins75/lab_1_datalakes/assets/94439376/41a2afed-26ae-4027-a1d9-d13de6795776)

Within this resource group, we will have our storage account :

![image](https://github.com/mins75/lab_1_datalakes/assets/94439376/edf9e590-ae55-461b-92b8-63c760d0ca55)

And within a created container, we will have our files :

![image](https://github.com/mins75/lab_1_datalakes/assets/94439376/36d9d043-bcb8-4b24-8874-0c6eb6144fc7)


## Annex 

Here are the two sources that helped me to create mine :

https://github.com/Azure/azure-sdk-for-python/blob/main/sdk/storage/azure-storage-file-datalake/samples/datalake_samples_upload_download.py

https://learn.microsoft.com/fr-fr/azure/developer/python/sdk/examples/azure-sdk-example-storage?tabs=cmd
