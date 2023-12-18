from kfp.dsl import component

from src.components.dependencies import PIPELINE_IMAGE_NAME


@component(base_image=PIPELINE_IMAGE_NAME)
def update_version_alias(
    model_id: str,
    project_id: str,
    project_location: str,
    version_aliases: list,
    model_version: str = None,
) -> str:
    """Update the version aliases of a Vertex AI model.

    Args:
        model_id (str): The ID (name) of the model.
        project_id (str): GCP Project ID where the model is stored.
        project_location (str): Location where the model is stored.
        version_aliases (list): List of version aliases to be added to the model.
        model_version (str, optional): Version alias of the model to update.
            Defaults to None.

    Raises:
        RuntimeError: If the model is not found.
        RuntimeError: If the update request fails.

    Returns:
        str: Resource name of the updated model.
    """
    import re

    import google.auth
    import google.auth.transport.requests
    import google.oauth2.id_token
    import requests
    from google.api_core.exceptions import NotFound
    from google.cloud import aiplatform
    from loguru import logger
    from requests.exceptions import HTTPError

    from src.utils.logging import setup_logger

    setup_logger()

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

    token, _ = _get_gcp_token()
    logger.info("Correctly retried default application credentials.")

    try:
        model = aiplatform.Model(
            model_name=model_id,
            location=project_location,
            project=project_id,
            version=model_version,
        )

        model_name = model.versioned_resource_name
        logger.info(
            f"Model display name: {model.display_name}, "
            f"model resource name: {model_name}, "
            f"model URI: {model.uri}, "
            f"version id: {model.version_id}."
        )
    except NotFound:
        msg = (
            f"No model found with name {model_name} "
            f"(project {project_id}, location {project_location})."
        )
        logger.error(msg)
        raise RuntimeError(msg)

    url = f"https://{project_location}-aiplatform.googleapis.com/v1"
    url += f"/{model_name}:mergeVersionAliases"

    headers = {
        "content-type": "application/json; charset=utf-8",
        "Authorization": f"Bearer {token}",
    }
    version_aliases = [
        re.sub(r"[^0-9a-z\-]", "", alias.lower().replace("_", "-"))
        for alias in version_aliases
    ]
    payload = {"versionAliases": version_aliases}

    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
    except HTTPError:
        msg = f"Failed to update version aliases: {response.text}"
        logger.error(msg)
        raise RuntimeError(msg)

    return model_name
