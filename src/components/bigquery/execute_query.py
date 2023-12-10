from kfp.dsl import component

from src.components.dependencies import PIPELINE_IMAGE_NAME


@component(base_image=PIPELINE_IMAGE_NAME)
def execute_query(
    query: str,
    bq_client_project_id: str,
    dataset_location: str = "europe-west2",
    query_job_config: dict = {},
) -> None:
    """Run a BQ query.

    Args:
        query (str): SQL query to execute.
        bq_client_project_id (str): Project ID that will be used by the BQ client.
        dataset_location (str): BQ dataset location.
        query_job_config (dict): Dict containing optional parameters required
            by the bq query operation. No need to specify destination param.
            Defaults to {}. See available parameters here:
            https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.job.QueryJobConfig.html

    Returns:
        GoogleCloudError: If an error is raised by the operation.
    """
    from google.cloud import bigquery
    from google.cloud.exceptions import GoogleCloudError
    from loguru import logger

    from src.utils.logging import setup_logger

    setup_logger()

    job_config = bigquery.QueryJobConfig(**query_job_config)

    bq_client = bigquery.client.Client(
        project=bq_client_project_id, location=dataset_location
    )
    query_job = bq_client.query(query, job_config=job_config)

    try:
        _ = query_job.result()
        logger.info("BQ query executed.")
    except GoogleCloudError as e:
        logger.error(e)
        logger.error(query_job.error_result)
        logger.error(query_job.errors)
        raise e
