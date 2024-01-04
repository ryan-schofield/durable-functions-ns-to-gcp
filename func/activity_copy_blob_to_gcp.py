import json
import logging
import os

from pathlib import PurePosixPath
from uuid import uuid4

import azure.functions as func
import azure.durable_functions as df

from azure.storage.blob import BlobServiceClient
from google.cloud import storage
from google.oauth2 import service_account
from tempfile import NamedTemporaryFile
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)
from google.api_core.exceptions import TooManyRequests

gcp_bp = df.Blueprint()


@gcp_bp.activity_trigger(input_name="context")
def copy_blob_to_gcp(context: dict) -> func.HttpResponse:
    logging.info("Python HTTP trigger function processed a request.")

    azure_connection_string = os.environ["AzureWebJobsStorage"]
    container_name = context.get("azure_container_name")
    azure_blob_name = context.get("azure_blob_name")

    gcp_creds = json.loads(os.environ["GCP_CREDS"])
    project_id = context.get("gcp_project_id")
    bucket_name = context.get("gcp_bucket_name")
    gcp_blob_name = context.get("gcp_blob_name", azure_blob_name)

    gcp_storage_client = create_gcp_storage_client(gcp_creds, project_id)
    gcp_bucket = gcp_storage_client.bucket(bucket_name)

    stream = create_azure_stream(
        azure_connection_string, container_name, azure_blob_name
    )

    full_path = PurePosixPath(gcp_blob_name)
    parent_path = PurePosixPath(full_path.parent)
    root_path = parent_path.parts[0]
    subdir = PurePosixPath(*parent_path.parts[1:])
    stem = full_path.stem

    destination = gcp_bucket.blob(gcp_blob_name)
    sources = []
    for idx, chunk in enumerate(stream.chunks()):
        # gcp service account provided by the client only has list and create permissions
        # upload_chunk allows streaming from azure which then can be composed as a single file without updates or deletes
        gcp_chunk_name = f"{root_path}/chunks/{subdir}/{stem}/{stem}_{uuid4()}"
        gcp_blob = gcp_bucket.blob(gcp_chunk_name)
        logging.info(
            f"Uploading chunk {idx + 1} ({len(chunk)} bytes) to {gcp_blob_name}."
        )
        upload_chunk(gcp_blob, chunk)
        sources.append(gcp_blob)

        if len(sources) == 32:
            logging.info(f"Composing {len(sources)} chunks.")
            gcp_compose_name = (
                f"{root_path}/chunks/{subdir}/{stem}/compose/{stem}_{uuid4()}"
            )
            temp_compose = gcp_bucket.blob(gcp_compose_name)
            temp_compose.compose(sources)
            sources = [temp_compose]

    destination.compose(sources)

    response = f"Data uploaded to {gcp_blob_name} in bucket {bucket_name}."
    logging.info(response)

    return {"response": response}


def create_azure_stream(conn_str, container_name, azure_blob_name):
    """
    Creates a stream to download a blob from Azure Storage.

    This function creates a BlobServiceClient from the provided connection string,
    gets a ContainerClient for the specified container, and then gets a BlobClient
    for the specified blob. It then returns a download stream for the blob.

    Args:
        conn_str (str): The connection string for the Azure Storage account.
        container_name (str): The name of the container in the Azure Storage account.
        azure_blob_name (str): The name of the blob to download.

    Returns:
        azure.storage.blob._download.StorageStreamDownloader: A stream downloader for the blob.
    """
    logger = logging.getLogger(
        "azure.core.pipeline.policies.http_logging_policy"
    ).setLevel(logging.WARNING)
    blob_service_client = BlobServiceClient.from_connection_string(
        conn_str=conn_str, logger=logger
    )
    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(azure_blob_name)

    return blob_client.download_blob()


def create_gcp_storage_client(gcp_creds, project_id):
    gcp_storage_credentials = service_account.Credentials.from_service_account_info(
        gcp_creds
    )
    return storage.Client(project=project_id, credentials=gcp_storage_credentials)


@retry(
    stop=stop_after_attempt(10),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    retry=retry_if_exception_type(TooManyRequests),
)
def upload_chunk(gcp_blob, chunk):
    """
    Uploads a chunk of data to a Google Cloud Platform (GCP) blob.

    This function writes the chunk to a temporary file and then uploads
    that file to the specified GCP blob. The size of the upload is determined
    by the size of the chunk.

    Args:
        gcp_blob (google.cloud.storage.blob.Blob): The GCP blob to which the chunk will be uploaded.
        chunk (bytes): The chunk of data to be uploaded.

    """
    with NamedTemporaryFile() as temp_file:
        chunk_size = len(chunk)
        temp_file.write(chunk)
        temp_file.flush()
        temp_file.seek(0)
        gcp_blob.upload_from_file(temp_file, size=chunk_size)
