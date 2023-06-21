from pathlib import Path

from kfp.v2.dsl import Input, Model, component

from src.components.dependencies import GOOGLE_CLOUD_AIPLATFORM, LOGURU, PYTHON


@component(
    base_image=PYTHON,
    packages_to_install=[GOOGLE_CLOUD_AIPLATFORM, LOGURU],
    output_component_file=str(Path(__file__).with_suffix(".yaml")),
)
def upload_model(
    model_id: str,
    display_name: str,
    serving_container_image_uri: str,
    project_id: str,
    project_location: str,
    model: Input[Model],
    labels: dict,
    description: str,
    is_default_version: bool,
    version_description: str,
    version_alias: str = None,
) -> str:
    """Upload a model from GCS to the Vertex AI model registry.

    Args:
        model_id (str): The ID (name) of the model.
        display_name (str): The display name of the model. The name
        serving_container_image_uri (str): The URI of the model serving container.
            Must come from the GCP Container Registry or Artifact Registry.
        project_id (str): GCP Project ID where the model will be saved.
        project_location (str): Location where the model will be saved.
        model (Input[Model]): Model to be uploaded.
        labels (dict): Labels with user-defined metadata to organise the model.
        description (str): Description of the model.
        is_default_version (bool): When set to True, the newly uploaded model version
            will automatically have alias "default" included. When set to False, the
            "default" alias will not be moved
        version_description (str): Description of the version of the model being
            uploaded.
        version_alias (str, optional): User provided version alias so that a model
            version can be referenced via alias instead of auto-generated version ID.
            Defaults to None.

    Returns:
        str: Resource name of the exported model
    """
    from google.api_core.exceptions import NotFound
    from google.cloud.aiplatform import Model
    from loguru import logger

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

    if version_alias is not None:
        version_alias = [version_alias]

    logger.debug(f"Labels: {labels}")
    logger.info("Uploading model to model registry.")
    model = Model.upload(
        model_id=model_id,
        project=project_id,
        location=project_location,
        display_name=display_name,
        parent_model=parent_model,
        version_aliases=version_alias,
        is_default_version=is_default_version,
        serving_container_image_uri=serving_container_image_uri,
        artifact_uri=model_uri,
        description=description,
        version_description=version_description,
        labels=labels,
        sync=True,
    )
    logger.info(f"Uploaded model {model}.")
    return model.resource_name
