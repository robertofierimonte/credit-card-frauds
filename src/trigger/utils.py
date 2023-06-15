import time

import google.auth
import google.auth.transport.requests
import google.oauth2.id_token
import requests
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
    args:
        job_id: Job ID
        location: GCP location
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


def wait_pipeline_until_complete(job_id: str, location: str = "europe-west2"):

    """Wait until pipeline is complete.
    args:
        job_id: Job ID
        location: GCP location
    """

    # Wait until pipeline is complete
    log_wait = _LOG_WAIT_TIME
    time_snapshot_1 = time.time()
    state = None

    while state not in _PIPELINE_COMPLETE_STATES:

        time_snapshot_2 = time.time()
        state = _get_pipeline_state(job_id)

        if time_snapshot_2 - time_snapshot_1 >= log_wait:

            logger.info(f"Pipeline {job_id} is {state}")
            log_wait = min(log_wait * _WAIT_TIME_MULTIPLIER, _MAX_WAIT_TIME)
            time_snapshot_1 = time_snapshot_2

        time.sleep(_JOB_WAIT_TIME)

        if state in _PIPELINE_SUCCESS_STATES:
            logger.info(f"Pipeline {job_id} succeeded.")
            return
        elif state in _PIPELINE_COMPLETE_STATES:
            logger.info(f"Pipeline {job_id} is {state}.")
            raise RuntimeError(f"Pipeline {job_id} is {state}.")

    raise RuntimeError(f"Timed out waiting for job {job_id} to finish.")
