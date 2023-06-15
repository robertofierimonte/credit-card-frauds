from pathlib import Path

from kfp.v2.dsl import Model, Output, component

from src.components.dependencies import GOOGLE_CLOUD_AIPLATFORM, LOGURU, PYTHON


@component(
    base_image=PYTHON,
    packages_to_install=[GOOGLE_CLOUD_AIPLATFORM, LOGURU],
    output_component_file=str(Path(__file__).with_suffix(".yaml")),
)
def lookup_model(
    model_name: str,
    project_id: str,
    project_location: str,
    model: Output[Model],
    model_label: str = None,
    fail_on_model_not_found: bool = False,
) -> str:
    """Fetch a Vertex AI model from the model registry given its name and version.

    Args:
        model_name (str): The ID (name) of the model.
        project_id (str): GCP Project ID where the model is stored.
        project_location (str): Location where the model is stored.
        model (Output[Model]): The fetched model as a KFP Model object. This
            parameter will be passed automatically by the orchestrator.
        model_label (str, optional): Version alias of the model. Defaults to None.
        fail_on_model_not_found (bool, optional): If set to True, raise an error
            if the model is not found. Defaults to False.

    Raises:
        RuntimeError: If the given model is not found and `fail_on_model_not_found`
            is True.

    Returns:
        str: Resource name of the fetched model. Empty string if a model is not
            found and `fail_on_model_not_found` is False
    """
    from google.api_core.exceptions import NotFound
    from google.cloud.aiplatform import Model
    from loguru import logger

    model_resource_name = ""
    try:
        target_model = Model(
            model_name=model_name,
            project=project_id,
            location=project_location,
            version=model_label,
        )
        model_resource_name = target_model.resource_name
        logger.info(
            f"Model display name: {target_model.display_name}, "
            f"model resource name: {model_resource_name}, "
            f"model URI: {target_model.uri}, "
            f"version id: {target_model.version_id}."
        )
        model.uri = model_resource_name
        model.metadata["resourceName"] = model_resource_name

    except NotFound:
        model.uri = None
        logger.warning(
            f"No model found with name {model_name} "
            f"(project {project_id}, location {project_location})."
        )
        if fail_on_model_not_found:
            msg = "Failed as model not found."
            logger.error(msg)
            raise RuntimeError(msg)

    return model_resource_name
