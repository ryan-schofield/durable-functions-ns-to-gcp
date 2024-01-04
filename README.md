# durable-functions-ns-to-gcp

This is sample code used to download files from a NetSuite file cabinet, upload those files to Azure Storage, and then copy the files to Google Cloud Storage. The Azure Durable Functions framework is used to split the download and upload into separate functions and allow for the creation of multiple function instances with a single HTTP request.

## required app settings

- **AzureWebJobsStorage**: This is the connection string for Azure Storage, which is used by Azure Functions for various functionalities like managing triggers, logging function executions and reading from and writing to Azure Storage.
- **GCP_CREDS**: This is a JSON string that contains the credentials for connecting to Google Cloud Platform. It includes details like the project ID, private key, client email, and various URLs for authentication and certificates.
- **NETSUITE_ACCOUNT**: This is the account id for the NetSuite instance that the application interacts with.
- **NETSUITE_CONSUMER_KEY**: This is the consumer key for the NetSuite service account, used for authentication.
- **NETSUITE_CONSUMER_SECRET**: This is the consumer secret for the NetSuite service account, used for authentication.
- **NETSUITE_TOKEN_KEY**: This is the token key for the NetSuite account, used for authentication.
- **NETSUITE_TOKEN_SECRET**: This is the token secret for the NetSuite service account, used for authentication.

## function_app.py

This Python script is the main entry point for an Azure Durable Functions application that includes two activity functions: `upload_netsuite_file` and `copy_blob_to_gcp`.

1. **Imports**: The script imports necessary modules such as `azure.functions` and `azure.durable_functions`. It also imports the blueprints for the `upload_netsuite_file` and `copy_blob_to_gcp` activity functions.

2. **Application Creation**: The script creates an instance of `DFApp` with the HTTP authorization level set to `FUNCTION`. This means that the HTTP trigger requires a function key.

3. **Function Registration**: The script registers the blueprints for the two activity functions with the application.

4. **HTTP Trigger**: The script defines an HTTP trigger function `http_start` that starts a new orchestration. The function takes an HTTP request and a durable client as input. It retrieves the body of the request and the name of the orchestration function from the request, starts a new instance of the orchestration function with the request body as input, and returns a check status response for the orchestration.

5. **Orchestration Trigger**: The script defines an orchestration function `durable_client_orchestrator` that calls an activity function for each item in the input. The function takes a `DurableOrchestrationContext` as input. It retrieves the name of the activity function and the input for the activity function from the context. If the input is a dictionary, it converts it to a list. It then calls the activity function for each item in the input, waits for all the activity functions to complete using `context.task_all`, and returns the results. This allows multiple function activities to be called and run concurrently.


## activity_upload_netsuite_file.py

This Python script downloads a file from a NetSuite file cabinet and then uploads the file to Azure Blob Storage. It uses the Azure Functions and Durable Functions libraries to handle the serverless function logic, and the NetSuite SDK library to interact with the NetSuite SOAP API.

1. **NetSuite Connection**: The script establishes a connection to NetSuite using the `NetSuiteConnection` class from the `netsuitesdk` library. The connection details are fetched from environment variables related to a NetSuite service account.

2. **File Download**: The script logs the start of the file download process from NetSuite using the `internal_id` (in this case, retrieved from the NetSuite `File` object). The file is retrieved using the `get` method of `nc.files`, where `nc` is the NetSuite connection instance. The folder name and file name are cleaned up using the `name_cleanup` function and the file path is constructed.

3. **File Upload**: If the file content is not empty, the script logs the start of the file upload process to Azure Blob Storage. It creates a `BlobServiceClient` from the connection string stored in the `AzureWebJobsStorage` environment variable. It then gets a `BlobClient` for the specified file path and uploads the file content to the blob, overwriting any existing blob.

4. **Response**: The script constructs a response indicating the success of the file upload process and returns it as a JSON string. If the file content is empty, it returns a response indicating that the file contains no data.

### Example Request Body

``` json
{
    "func_name": "upload_netsuite_file",
    "reqbody": [
        {
            "azure_container_name": "myazureblobcontainer",
            "azure_path_prefix": "my_directory/my_subdirectory",
            "internal_id": "123"
        },
        {
            "azure_container_name": "myazureblobcontainer",
            "azure_path_prefix": "my_directory/my_subdirectory",
            "internal_id": "456"
        }
    ]
}
```

## activity_copy_blob_to_gcp.py

This Python script copies a blob from Azure Blob Storage to Google Cloud Platform (GCP) Storage. 

1. **Inputs**: The function takes a dictionary `context` as input, which contains details like the Azure container name, Azure blob name, GCP project ID, GCP bucket name, and GCP blob name. If the GCP blob name is excluded, it will use the Azure blob name when writing the file.

2. **Azure Connection**: The function retrieves the Azure connection string from the `AzureWebJobsStorage` environment variable and the `container_name` and `azure_blob_name` from the `context` dictionary.

3. **GCP Connection**: The function retrieves the GCP credentials from the `GCP_CREDS` environment variable and creates a GCP storage client. It then gets the GCP bucket where the blob will be copied.

4. **Azure Stream Creation**: The function creates a stream to download the blob from Azure Blob Storage using the `create_azure_stream` function.

5. **Blob Copy**: The function copies the blob from Azure Blob Storage to GCP Storage in chunks. It uploads each chunk to a temporary location in GCP Storage, and then composes these chunks into a single blob in the destination location. If there are more than 32 chunks, it composes them into a temporary blob first to avoid hitting the limit of 32 components for a composed blob in GCP.

6. **Response**: The function constructs a response indicating the success of the blob copy process and returns it as a dictionary.

### Example Request Body

``` json
{
    "func_name": "copy_blob_to_gcp",
    "reqbody": [
        {
            "azure_container_name": "myazureblobcontainer",
            "gcp_project_id": "my-gcp-project",
            "gcp_bucket_name": "my-gcp-bucket",
            "azure_blob_name": "my_azure_blob_dir/my_file.txt"
        }
    ]
}
```
