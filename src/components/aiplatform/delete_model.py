from kfp.dsl import component

from src.components.dependencies import GOOGLE_CLOUD_AIPLATFORM, LOGURU, PYTHON


@component(base_image=PYTHON, packages_to_install=[GOOGLE_CLOUD_AIPLATFORM, LOGURU])
def delete_model(
    model_id: str, project_id: str, project_location: str, model_label: str = None
) -> None:
    """Delete a Vertex AI model from the model registry given its name and version.

    Args:
        model_name (str): The ID (name) of the model.
        project_id (str): GCP Project ID where the model is stored.
        project_location (str): Location where the model is stored.
        model_label (str, optional): Version alias of the model. Defaults to None.

    Raises:
        RuntimeError: If the given model is not found.
    """
    from google.api_core.exceptions import NotFound
    from google.cloud.aiplatform import Model
    from loguru import logger

    try:
        model = Model(
            model_name=model_id,
            project=project_id,
            location=project_location,
            version=model_label,
        )
        logger.info(f"Found model {model_id}, version {model_label}.")

        model.delete()
        logger.info(f"Model {model_id}, version {model_label} deleted.")

    except NotFound:
        msg = f"No model found with name {model_id}, version {model_label}."
        logger.error(msg)
        raise RuntimeError(msg)
