import argparse
import base64
import json
import os
import re
from distutils.util import strtobool
from typing import Optional

from google.cloud import aiplatform
from loguru import logger

from src.trigger.utils import wait_pipeline_until_complete


def cf_handler(event: dict) -> aiplatform.PipelineJob:
    """Handle the Pub/Sub event and make a call to trigger the KFP pipeline.

    Args:
        event (dict): Dictionary containing data specific to this type of event.
            The `@type` field maps to
            `type.googleapis.com/google.pubsub.v1.PubsubMessage`. The `data` field
            maps to the PubsubMessage data in a base64-encoded string. The
            `attributes` field maps to the Pubsub attributes if any is present.

    Returns:
        aiplatform.PipelineJob: Pipeline job that is triggered as result
    """
    event["data"] = base64.b64decode(event["data"]).decode("utf-8")
    event["data"] = json.loads(event["data"])

    return trigger_pipeline_from_payload(event)


def trigger_pipeline_from_payload(payload: dict) -> aiplatform.PipelineJob:
    """Triggers a pipeline from a payload, a pipeline definition, and env variables.

    Args:
        payload (dict): Payload containing attributes and data for the pipeline.

    Returns:
        aiplatform.PipelineJob: Pipeline job that is triggered as result
    """
    payload = convert_payload(payload)
    logger.debug(f"data_version: {payload['data']['data_version']}.")
    env = get_env()

    return trigger_pipeline(
        project_id=env["project_id"],
        location=env["location"],
        template_path=payload["attributes"]["template_path"],
        parameter_values=payload["data"],
        pipeline_root=env["pipeline_root"],
        service_account=env["service_account"],
        enable_caching=payload["attributes"]["enable_caching"],
        mode=env["mode"],
    )


def trigger_pipeline(
    project_id: str,
    location: str,
    template_path: str,
    parameter_values: dict,
    pipeline_root: str,
    service_account: str,
    enable_caching: Optional[bool] = None,
    mode: Optional[str] = None,
) -> aiplatform.PipelineJob:
    """Trigger the Vertex pipeline run.

    Args:
        project_id (str): GCP project ID where to run the Vertex pipeline.
        location (str): GCP location where to run the Vertex pipeline.
        template_path (str): Local or GCS path where containing the serialised KFP
            pipeline definition (JSON or YAML).
        parameter_values (dict): Dictionary containing the input parameters for the
            pipeline run.
        pipeline_root (str): GCS path to use as the pipeline root (for passing
            metadata / artifacts within the pipeline).
        service_account (str): Email address of the GCP service account used to
            run the pipeline in Vertex.
        enable_caching (Optional[bool], optional): Whether to enable caching of the
            pipeline components if component + inputs are the same. Defaults to None
            (enable caching, except where disabled at a component level).
        mode (Optional[str], optional): If `mode` = "run", monitor the job results
            until completion, otherwise just submit the job. Defaults to None.

    Returns:
        aiplatform.PipelineJob: Pipeline job that is triggered as result
    """
    # Initialise API client
    aiplatform.init(project=project_id, location=location)

    # Instantiate PipelineJob object
    pipeline_job = aiplatform.pipeline_jobs.PipelineJob(
        display_name="pipeline-execution",
        template_path=template_path,
        pipeline_root=pipeline_root,
        parameter_values=parameter_values,
        enable_caching=enable_caching,
    )

    # Execute pipeline in Vertex and optionally wait until completion
    if mode == "run":
        pipeline_job.run(service_account=service_account)
        wait_pipeline_until_complete(job=pipeline_job)
    else:
        pipeline_job.submit(service_account=service_account)
    return pipeline_job


def convert_payload(payload: dict) -> dict:
    """Converts the payload to the desired format.

    Parse enable_caching from str to bool and add defaults to missing fields.

    Args:
        payload (dict): Payload from Pub/Sub message.

    Returns:
        dict: Payload in desired format
    """
    # Make copy to not edit the original payload
    payload = payload.copy()

    # if missing, set to empty dict
    payload["data"] = payload.get("data", {})

    # if enable_caching is not None, convert to bool from str
    if "enable_caching" in payload["attributes"] and payload["attributes"][
        "enable_caching"
    ].lower() in ["true", "false"]:
        payload["attributes"]["enable_caching"] = bool(
            strtobool(payload["attributes"]["enable_caching"])
        )
    else:
        payload["attributes"]["enable_caching"] = None

    # if VERTEX_PROJECT_ID is set and the payload has no value, set it there
    env_value = os.environ.get("VERTEX_PROJECT_ID")
    if env_value is not None and "project_id" not in payload["data"]:
        payload["data"]["project_id"] = env_value

    # if VERTEX_LOCATION is set and the payload has no value, set it there
    env_value = os.environ.get("VERTEX_LOCATION")
    if env_value is not None and "project_location" not in payload["data"]:
        payload["data"]["project_location"] = env_value

    # if TEMPLATE_BASE_PATH is set, overwrite the one in payload
    env_value = os.environ.get("TEMPLATE_BASE_PATH")
    if env_value is not None:
        path = f"{env_value}/{payload['attributes']['template_path']}"
        payload["data"]["template_path"] = path

    # if MODEL_FILE_PATH is set, overwrite the one in payload
    env_value = os.environ.get("MODEL_FILE_PATH")
    if env_value is not None and "model_file" in payload["data"]:
        payload["data"]["model_file"] = env_value

    # add MONITORING_EMAIL_ADDRESS to payload
    env_value = os.environ.get("MONITORING_EMAIL_ADDRESS", "")
    env_value = re.sub(r"\s", "", env_value)
    payload["data"]["email_notification_recipients"] = env_value.split(",")

    return payload


def get_env() -> dict:
    """Returns the environment variables as a dictionary.

    Returns:
        dict: Environment variables
    """
    project_id = os.environ.get("VERTEX_PROJECT_ID")
    location = os.environ.get("VERTEX_LOCATION")
    pipeline_root = os.environ.get("VERTEX_PIPELINE_ROOT")
    service_account = os.environ.get("VERTEX_SA_EMAIL")
    mode = os.environ.get("VERTEX_TRIGGER_MODE") or None

    return {
        "project_id": project_id,
        "location": location,
        "pipeline_root": pipeline_root,
        "service_account": service_account,
        "mode": mode,
    }


def get_args(args: list[str] = None) -> argparse.Namespace:
    """Get args from command line args.

    Args:
        args (list[str], optional): Command line arguments submitted to
            src.trigger.main. Defaults to None.

    Returns:
        argparse.Namespace: Parsed arguments
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--payload", type=str, help="Path to payload json file.")
    parser.add_argument(
        "--data-version",
        type=str,
        help=(
            "Version of the input data to use in the training pipeline. "
            "It must follow `%Y%m%dT%H%M%S` format (e.g. 20230521T190000)."
        ),
    )
    return parser.parse_args(args)


def sandbox_run() -> Optional[aiplatform.PipelineJob]:
    """Trigger a Vertex pipeline.

    Returns:
        Optional[aiplatform.PipelineJob]: Pipeline job that is triggered as result
    """
    args = get_args()

    with open(args.payload, "r") as f:
        payload = json.load(f)

    if args.data_version and args.data_version != "":
        if re.match(r"\d{8}T\d{6}", args.data_version):
            payload["data"]["data_version"] = args.data_version
        else:
            logger.warning(
                "Argument `data-version` is in the wrong format. It will be ignored."
            )

    return trigger_pipeline_from_payload(payload)


if __name__ == "__main__":
    sandbox_run()
