import json
import logging
import os

import azure.functions as func
import azure.durable_functions as df

from netsuitesdk import NetSuiteConnection
from azure.storage.blob import BlobServiceClient


ns_bp = df.Blueprint()


@ns_bp.activity_trigger(input_name="context")
def upload_netsuite_file(context: dict) -> func.HttpResponse:
    internal_id = context.get("internal_id")
    container_name = context.get("azure_container_name")
    path_prefix = context.get("azure_path_prefix", "Netsuite/file_cabinet")

    nc = NetSuiteConnection(
        account=os.environ["NETSUITE_ACCOUNT"],
        consumer_key=os.environ["NETSUITE_CONSUMER_KEY"],
        consumer_secret=os.environ["NETSUITE_CONSUMER_SECRET"],
        token_key=os.environ["NETSUITE_TOKEN_KEY"],
        token_secret=os.environ["NETSUITE_TOKEN_SECRET"],
        caching=False,
    )
    logging.info("Connected to NetSuite.")

    logging.info(f"Downloading file {internal_id} from NetSuite.")
    file = nc.files.get(internalId=internal_id)
    folder = name_cleanup(file.folder["name"])
    fname = name_cleanup(file.name)
    fpath = f"{path_prefix}/{folder}/{fname}"
    content = file.content
    logging.info(f"File {fpath} downloaded.")

    if content:
        logging.info(f"Uploading file {fpath} to blob.")
        logger = logging.getLogger(
            "azure.core.pipeline.policies.http_logging_policy"
        ).setLevel(logging.WARNING)
        blob_service_client = BlobServiceClient.from_connection_string(
            conn_str=os.environ["AzureWebJobsStorage"], logger=logger
        )
        blob = blob_service_client.get_blob_client(container_name, fpath)
        blob.upload_blob(content, overwrite=True)

        response = {"response": f"File {fpath} uploaded."}
        logging.info(response["response"])

        return json.dumps(response, indent=4)
    else:
        response_str = f"File {internal_id} ({fname}) contains no data."
        response = json.dumps({"response": response_str}, indent=4)
        logging.warn(response_str)

        return response


def name_cleanup(file_name: str) -> str:
    file_name.replace("\\", "/").replace("\t", "").replace(" : ", "/").replace(
        "./", "/"
    )
    if file_name.endswith("."):
        file_name = file_name[:-1]

    return file_name
