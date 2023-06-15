from pathlib import Path

from kfp.v2.dsl import component

from src.components.dependencies import CICD_TRIGGER_IMAGE_NAME, LOGURU, REQUESTS


@component(
    base_image=CICD_TRIGGER_IMAGE_NAME,
    packages_to_install=[LOGURU, REQUESTS],
    output_component_file=str(Path(__file__).with_suffix(".yaml")),
)
def trigger_cicd_pipeline() -> None:
    """Trigger a Bitbucket CI/CD pipeline through the API."""
    import base64
    import json
    import os

    import requests
    from loguru import logger

    workspace = os.environ.get("BITBUCKET_WORKSPACE")
    repo_slug = os.environ.get("BITBUCKET_REPO_SLUG")
    url = (
        f"https://api.bitbucket.org/2.0/repositories/{workspace}/{repo_slug}/pipelines/"
    )

    bearer_token = base64.b64decode(os.environ.get("BITBUCKET_CICD_TOKEN")).decode(
        "utf-8"
    )
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {bearer_token}",
    }

    payload = json.dumps(
        {
            "target": {
                "ref_type": "branch",
                "type": "pipeline_ref_target",
                "ref_name": "master",
            }
        }
    )

    response = requests.request("POST", url, headers=headers, data=payload)

    logger.info(
        json.dumps(
            json.loads(response.text), sort_keys=True, indent=4, separators=(",", ": ")
        )
    )
