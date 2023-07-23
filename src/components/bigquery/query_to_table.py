from kfp.dsl import component

from src.components.dependencies import GOOGLE_CLOUD_BIGQUERY, LOGURU, PYTHON


@component(
    base_image=PYTHON,
    packages_to_install=[GOOGLE_CLOUD_BIGQUERY, LOGURU],
)
def bq_query_to_table(
    query: str,
    bq_client_project_id: str,
    destination_project_id: str,
    dataset_id: str,
    table_id: str,
    dataset_location: str = "europe-west2",
    query_job_config: dict = {},
) -> None:
    """Run query and create a new BQ table.

    Args:
        query (str): SQL query to execute, results are saved in a BQ table.
        bq_client_project_id (str): Project ID that will be used by the BQ client.
        destination_project_id (str): Project ID where BQ table will be created.
        dataset_id (str): Dataset ID where BQ table will be created.
        table_id (str): Table name (without project ID and dataset ID) that
            will be created.
        dataset_location (str): BQ dataset location.
        query_job_config (dict): Dict containing optional parameters required
            by the bq query operation. No need to specify destination param.
            Defaults to {}.
            See available parameters here
            https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.job.QueryJobConfig.html

    Raises:
        GoogleCloudError: If an error is raised by the operation.
    """
    from google.cloud import bigquery
    from google.cloud.exceptions import GoogleCloudError
    from loguru import logger

    if (dataset_id is not None) and (table_id is not None):
        dest_table_ref = f"{destination_project_id}.{dataset_id}.{table_id}"
    else:
        dest_table_ref = None
    job_config = bigquery.QueryJobConfig(destination=dest_table_ref, **query_job_config)

    bq_client = bigquery.client.Client(
        project=bq_client_project_id, location=dataset_location
    )
    query_job = bq_client.query(query, job_config=job_config)

    try:
        _ = query_job.result()
        logger.info(f"BQ table {dest_table_ref} created.")
    except GoogleCloudError as e:
        logger.error(e)
        logger.error(query_job.error_result)
        logger.error(query_job.errors)
        raise e
