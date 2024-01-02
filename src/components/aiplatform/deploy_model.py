from kfp.dsl import component

from src.components.dependencies import PIPELINE_IMAGE_NAME


@component(base_image=PIPELINE_IMAGE_NAME)
def deploy_model(
    model_id: str,
    endpoint_id: str,
    project_location: str,
    project_id: str,
    dataset_id: str,
    model_display_name: str = None,
    endpoint_display_name: str = None,
    model_version: str = None,
    monitoring: bool = True,
    monitoring_config: dict = None,
    monitoring_email_recipients: list = None,
) -> None:
    """Deploy a ML model from the Vertex model registry to an online endpoint.

    Args:
        model_id (str): The ID (name) of the model.
        endpoint_id (str): The ID of the endpoint on which the model will be deployed.
        project_location (str): Location where the model is stored.
        project_id (str): GCP Project ID where the model is stored.
        dataset_id (str): Bigquery dataset ID that will be used to log the endpoint
            predictions if model monitoring is enabled.
        model_display_name (str, optional): The display name of the model within
            the endpoint. If not provided, fallback to `model_id`. Defaults to None.
        endpoint_display_name (str, optional): The display name of the endpoint.
            If not provided, fallback to `endpoint_id`. Defaults to None.
        model_version (str, optional): Version alias of the model. Defaults to None.
        monitoring (bool, optional): Whether to enable model monitoring for the
            endpoint. Defaults to True.
        monitoring_config (dict, optional): Dict containing the model monitoring
            configuration. Defaults to None.

    Raises:
        RuntimeError: If the `model_id` is not found in the model registry.
    """
    from google.api_core.exceptions import NotFound
    from google.cloud import aiplatform
    from google.cloud.aiplatform import (
        Endpoint,
        Model,
        ModelDeploymentMonitoringJob,
        model_monitoring,
    )
    from loguru import logger

    from src.utils.logging import setup_logger

    setup_logger()

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
            version=model_version,
        )
        logger.info(f"Found model {model_id}, version {model_version}.")
    except NotFound:
        msg = f"No model found with name {model_id}, version {model_version}."
        logger.error(msg)
        raise RuntimeError(msg)

    try:
        endpoint = Endpoint(endpoint_name=endpoint_id)
        logger.info(f"Found existing endpoint {endpoint.name}, ID {endpoint_id}.")
        endpoint.undeploy_all()
        logger.info("Undeployed all models from existing endpoint.")

        for monitoring_job in ModelDeploymentMonitoringJob.list():
            if monitoring_job.name == endpoint.name:
                ModelDeploymentMonitoringJob.delete(monitoring_job)
        logger.info("Deleted all monitoring jobs from existing endpoint.")
    except NotFound:
        endpoint = Endpoint.create(
            display_name=endpoint_display_name,
            endpoint_id=endpoint_id,
            enable_request_response_logging=True,
            request_response_logging_sampling_rate=1.0,
            request_response_logging_bq_destination_table=(
                f"bq://{project_id}.{dataset_id}_{model.labels['data_version']}.endpoint_logging"  # noqa: E501
            ),
        )
        logger.info(f"Created endpoint {endpoint_display_name}, ID: {endpoint_id}.")

    model.deploy(endpoint=endpoint, deployed_model_display_name=model_display_name)
    logger.info(f"Deployed model {model_id} to endpoint {endpoint_display_name}.")

    if monitoring is True:
        skew_config = model_monitoring.SkewDetectionConfig(
            data_source=f"bq://{project_id}.{dataset_id}_{model.labels['data_version']}.training",  # noqa: E501
            skew_thresholds=monitoring_config["skew_thresholds"],
            attribute_skew_thresholds=monitoring_config["attribute_skew_thresholds"],
            target_field=monitoring_config["target_column"],
        )
        drift_config = model_monitoring.DriftDetectionConfig(
            drift_thresholds=monitoring_config["drift_thresholds"],
            attribute_drift_thresholds=monitoring_config["attribute_drift_thresholds"],
        )
        # explanation_config = model_monitoring.ExplanationConfig()
        objective_config = model_monitoring.ObjectiveConfig(
            skew_detection_config=skew_config,
            drift_detection_config=drift_config,
            # explanation_config=explanation_config,
        )

        # Create sampling configuration.
        sampling_config = model_monitoring.RandomSampleConfig(sample_rate=1.0)

        # Create schedule configuration.
        schedule_config = model_monitoring.ScheduleConfig(monitor_interval=6)

        # Create alerting configuration.
        alerting_config = model_monitoring.EmailAlertConfig(
            user_emails=monitoring_email_recipients, enable_logging=True
        )

        # Create the monitoring job.
        _ = aiplatform.ModelDeploymentMonitoringJob.create(
            display_name=f"monitoring-job-{endpoint_display_name}",
            logging_sampling_strategy=sampling_config,
            schedule_config=schedule_config,
            alert_config=alerting_config,
            objective_configs=objective_config,
            project=project_id,
            location=project_location,
            endpoint=endpoint.resource_name,
        )
        logger.info(f"Enabled model monitoring for endpoint {endpoint_display_name}.")
