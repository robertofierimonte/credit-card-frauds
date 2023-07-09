from typing import NamedTuple

from kfp.dsl import Model, Output, component

from src.components.dependencies import GOOGLE_CLOUD_AIPLATFORM, LOGURU, PYTHON


@component(
    base_image=PYTHON,
    packages_to_install=[GOOGLE_CLOUD_AIPLATFORM, LOGURU],
)
def export_model(
    model_id: str,
    project_id: str,
    project_location: str,
    model: Output[Model],
    model_label: str = None,
    model_file_name: str = None,
) -> NamedTuple("Outputs", [("labels", dict)]):
    """Export a Vertex AI model from the model registry to GCS.

    Args:
        model_name (str): The ID (name) of the model.
        project_id (str): GCP Project ID where the model is stored.
        project_location (str): Location where the model is stored.
        model (Output[Model]): The exported model as a KFP Model object. This
            parameter will be passed automatically by the orchestrator.
        model_label (str, optional): Version alias of the model. Defaults to None.
        model_file_name (str, optional): File name of the model inside the model folder.
            Defaults to None.
    """
    from google.cloud.aiplatform import Model
    from loguru import logger

    model_to_be_exported = Model(
        model_name=model_id,
        project=project_id,
        location=project_location,
        version=model_label,
    )
    result = model_to_be_exported.export_model(
        export_format_id="custom-trained", artifact_destination=model.uri, sync=True
    )

    model.path = result["artifactOutputUri"]
    if model_file_name:
        model.path += f"/{model_file_name}"
    model.metadata["resourceName"] = model_id
    # model.metadata["model_labels"] = model_to_be_exported.labels["model_label"]
    logger.info(f"Exported model to {model.path}.")

    return (model_to_be_exported.labels,)
