from typing import NamedTuple

from kfp.dsl import Input, Model, component

from src.components.dependencies import PIPELINE_IMAGE_NAME


@component(base_image=PIPELINE_IMAGE_NAME)
def upload_model(
    model_id: str,
    display_name: str,
    serving_container_image_uri: str,
    project_id: str,
    project_location: str,
    model: Input[Model],
    labels: str,
    description: str,
    is_default_version: bool,
    serving_container_params: dict = None,
    version_description: str = None,
    version_aliases: list = [],
    model_name: str = None,
) -> NamedTuple("Outputs", [("model_resource_name", str)]):
    """Upload a model from GCS to the Vertex AI model registry.

    Args:
        model_id (str): The ID (name) of the model.
        display_name (str): The display name of the model. The name
        serving_container_image_uri (str): The URI of the model serving container.
            Must come from the GCP Container Registry or Artifact Registry.
        project_id (str): GCP Project ID where the model will be saved.
        project_location (str): Location where the model will be saved.
        model (Input[Model]): Model to be uploaded.
        labels (str): JSON-serialised dict[str, str] of labels with user-defined
            metadata to organise the model.
        description (str): Description of the model.
        is_default_version (bool): When set to True, the newly uploaded model version
            will automatically have alias "default" included. When set to False, the
            "default" alias will not be moved.
        serving_container_params (dict, optional): Parameters of the model serving
            container.
        version_description (str, optional): Description of the version of the model
            being uploaded. Defaults to None.
        version_aliases (list, optional): User provided version aliases so that a model
            version can be referenced via alias instead of auto-generated version ID.
            Defaults to [].
        model_name (str, optional):

    Returns:
        str: Resource name of the exported model.
    """
    import json
    import re

    from google.api_core.exceptions import NotFound
    from google.cloud.aiplatform import Model
    from loguru import logger

    from src.utils.logging import setup_logger

    setup_logger()

    # The URI expects a folder containing the model binaries
    model_uri = model.uri.rsplit("/", 1)[0]

    # If a model with the same id exists, use it as the parent model
    try:
        result_model = Model(
            model_name=model_id, project=project_id, location=project_location
        )
        parent_model = result_model.resource_name
    except (NotFound, ValueError):
        logger.info("Parent model not found.")
        parent_model = None

    if version_aliases == []:
        version_aliases = None
    if serving_container_params is None:
        serving_container_params = {}

    labels = json.loads(labels)
    if model_name is not None:
        labels["algorithm"] = model_name
    for k, v in labels.items():
        labels[k] = re.sub(r"[^0-9a-z\-]", "", v.lower().replace("_", "-"))

    logger.debug(f"Version aliases: {version_aliases}")
    logger.debug(f"Labels: {labels}")
    logger.debug(f"Serving container params: {serving_container_params}")

    logger.info("Uploading model to model registry.")
    aip_model = Model.upload(
        model_id=model_id,
        project=project_id,
        location=project_location,
        display_name=display_name,
        parent_model=parent_model,
        version_aliases=version_aliases,
        is_default_version=is_default_version,
        serving_container_image_uri=serving_container_image_uri,
        artifact_uri=model_uri,
        description=description,
        version_description=version_description,
        labels=labels,
        sync=True,
        **serving_container_params,
    )
    logger.info(f"Uploaded model {aip_model}.")

    model_resource_name = aip_model.versioned_resource_name
    return (model_resource_name,)
