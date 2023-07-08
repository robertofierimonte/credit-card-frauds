from kfp.dsl import component

from src.components.dependencies import GOOGLE_CLOUD_AIPLATFORM, LOGURU, PYTHON


@component(base_image=PYTHON, packages_to_install=[GOOGLE_CLOUD_AIPLATFORM, LOGURU])
def deploy_model(
    model_id: str,
    endpoint_id: str,
    model_location: str,
    project_id: str,
    model_display_name: str = None,
    endpoint_display_name: str = None,
    model_label: str = None,
    monitoring: bool = True,
) -> None:
    from google.cloud import aiplatform
    from google.cloud.aiplatform import (
        Endpoint,
        Model,
        ModelDeploymentMonitoringJob,
        model_monitoring,
    )
    from google.cloud.core.exceptions import NotFound
    from loguru import logger

    aiplatform.init(project=project_id, location=model_location)

    if model_display_name is None:
        model_display_name = model_id
    if endpoint_display_name is None:
        endpoint_display_name = endpoint_id

    try:
        model = Model(
            model_name=model_id,
            project=project_id,
            location=model_location,
        )
        logger.info(f"Found model {model_display_name}, version {model_label}.")
    except NotFound:
        msg = (
            f"No model found with name {model_id} "
            f"(project {project_id}, location {model_location})."
        )
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
        )
        logger.info(f"Created endpoint {endpoint_display_name}.")

    model.deploy(endpoint=endpoint, deployed_model_display_name=model_display_name)
    logger.info(f"Deployed model {model_display_name} to ")
