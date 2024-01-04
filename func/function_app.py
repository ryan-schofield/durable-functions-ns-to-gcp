import azure.functions as func
import azure.durable_functions as df

from activity_upload_netsuite_file import ns_bp
from activity_copy_blob_to_gcp import gcp_bp

app = df.DFApp(http_auth_level=func.AuthLevel.FUNCTION)

app.register_functions(ns_bp)
app.register_functions(gcp_bp)


@app.route(route="orchestrators/{functionName}")
@app.durable_client_input(client_name="client")
async def http_start(req: func.HttpRequest, client):
    req_body = req.get_json()
    function_name = req.route_params.get("functionName")
    instance_id = await client.start_new(function_name, None, req_body)

    return client.create_check_status_response(req, instance_id)


@app.orchestration_trigger(context_name="context")
def durable_client_orchestrator(context: df.DurableOrchestrationContext):
    activity_function_name = context.get_input()["func_name"]
    orch_input = context.get_input()["reqbody"]
    if isinstance(orch_input, dict):
        orch_input = [orch_input]
    input_batch = [context.call_activity(activity_function_name, f) for f in orch_input]
    result = yield context.task_all(input_batch)

    return result
