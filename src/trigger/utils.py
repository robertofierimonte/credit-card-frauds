import time

import google.auth
import google.auth.transport.requests
import google.oauth2.id_token
import requests
from google.cloud.aiplatform import PipelineJob
from loguru import logger

# _block_until_complete wait times
_JOB_WAIT_TIME = 60
_LOG_WAIT_TIME = 5
_MAX_WAIT_TIME = 60 * 1
_WAIT_TIME_MULTIPLIER = 1

_PIPELINE_SUCCESS_STATES = set(["PIPELINE_STATE_SUCCEEDED"])
_PIPELINE_COMPLETE_STATES = set(
    ["PIPELINE_STATE_FAILED", "PIPELINE_STATE_CANCELLED", "PIPELINE_STATE_PAUSED"]
)


def _get_gcp_token():
    """Get GCP token for authentication."""
    # Get credentials
    credentials, project = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    # Get token
    auth_req = google.auth.transport.requests.Request()
    credentials.refresh(auth_req)
    return credentials.token, project


def _get_pipeline_state(job_id: str, location: str = "europe-west2"):
    """Get pipeline state.

    Args:
        job_id (str): Vertex pipeline job ID.
        location (str, optional): GCP location of the job. Defaults to
            "europe-west2".
    """
    token, project = _get_gcp_token()
    name = f"projects/{project}/locations/{location}/pipelineJobs/{job_id}"
    url = f"https://{location}-aiplatform.googleapis.com/v1/{name}"

    headers = {
        "content-type": "application/json; charset=utf-8",
        "Authorization": f"Bearer {token}",
    }

    response = requests.get(url, headers=headers)
    pipeline_state = response.json()["state"]
    return pipeline_state


def wait_pipeline_until_complete(job: PipelineJob, location: str = "europe-west2"):
    """Wait until pipeline is complete.

    Args:
        job (PipelineJob): Vertex pipeline job.
        location (str, optional): GCP location of the job. Defaults to
            "europe-west2".

    Raises:
        RuntimeError: If the pipline fails or if it times out.
    """
    # Wait until pipeline is complete
    log_wait = _LOG_WAIT_TIME
    time_snapshot_1 = time.time()
    state = None

    while state not in _PIPELINE_COMPLETE_STATES:

        time_snapshot_2 = time.time()
        state = _get_pipeline_state(job.job_id, location=location)

        if time_snapshot_2 - time_snapshot_1 >= log_wait:

            logger.info(f"Pipeline {job.name} is {state}")
            log_wait = min(log_wait * _WAIT_TIME_MULTIPLIER, _MAX_WAIT_TIME)
            time_snapshot_1 = time_snapshot_2

        time.sleep(_JOB_WAIT_TIME)

        if state in _PIPELINE_SUCCESS_STATES:
            logger.info(f"Pipeline {job.name} succeeded.")
            return
        elif state in _PIPELINE_COMPLETE_STATES:
            logger.info(f"Pipeline {job.name} is {state}.")
            raise RuntimeError(f"Pipeline {job.name} is {state}.")

    raise RuntimeError(f"Timed out waiting for job {job.name} to finish.")
