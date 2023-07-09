from kfp.dsl import component

from src.components.dependencies import GOOGLE_CLOUD_AIPLATFORM, LOGURU, PYTHON


@component(base_image=PYTHON, packages_to_install=[GOOGLE_CLOUD_AIPLATFORM, LOGURU])
def deploy_model(
    model_id: str,
    endpoint_id: str,
    project_location: str,
    project_id: str,
    dataset_id: str,
    model_display_name: str = None,
    endpoint_display_name: str = None,
    model_label: str = None,
    monitoring: bool = True,
) -> None:
    from google.api_core.exceptions import NotFound
    from google.cloud import aiplatform
    from google.cloud.aiplatform import (  # model_monitoring,
        Endpoint,
        Model,
        ModelDeploymentMonitoringJob,
    )
    from loguru import logger

    aiplatform.init(project=project_id, location=project_location)

    if model_display_name is None:
        model_display_name = model_id
    if endpoint_display_name is None:
        endpoint_display_name = endpoint_id

    try:
        model = Model(
            model_name=model_id,
            project=project_id,
            location=project_location,
            version=model_label,
        )
        logger.info(f"Found model {model_id}, version {model_label}.")
    except NotFound:
        msg = f"No model found with name {model_id}, version {model_label}."
        logger.error(msg)
        raise RuntimeError(msg)

    try:
        endpoint = Endpoint(endpoint_name=endpoint_id)
        logger.info(f"Found existing endpoint {endpoint_id}.")
        endpoint.undeploy_all()
        logger.info("Undeployed all models from existing endpoint.")

        for monitoring_job in ModelDeploymentMonitoringJob.list():
            if monitoring_job.name == endpoint.name:
                ModelDeploymentMonitoringJob.delete(monitoring_job)
        logger.info("Deleted all monitoring jobs from existing endpoint.")
    except NotFound:
        endpoint = Endpoint.create(
            display_name=endpoint_display_name,
            enable_request_response_logging=True,
            request_response_logging_sampling_rate=1.0,
            request_response_logging_bq_destination_table=(
                f"bq://{project_id}.{dataset_id}.endpoint_logging"
            ),
        )
        logger.info(f"Created endpoint {endpoint_display_name}.")

    model.deploy(endpoint=endpoint, deployed_model_display_name=model_display_name)
    logger.info(f"Deployed model {model_display_name} to ")
