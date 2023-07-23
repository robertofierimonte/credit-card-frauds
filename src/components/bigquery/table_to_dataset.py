from typing import NamedTuple, Optional

from kfp.dsl import Dataset, Output, component

from src.components.dependencies import GOOGLE_CLOUD_BIGQUERY, LOGURU, PYTHON


@component(
    base_image=PYTHON,
    packages_to_install=[GOOGLE_CLOUD_BIGQUERY, LOGURU],
)
def bq_table_to_dataset(
    bq_client_project_id: str,
    source_project_id: str,
    dataset_id: str,
    table_name: str,
    dataset: Output[Dataset],
    destination_gcs_uri: Optional[str] = None,
    dataset_location: str = "europe-west2",
    extract_job_config: Optional[dict] = None,
    skip_if_exists: bool = True,
    file_pattern: Optional[str] = None,
) -> NamedTuple("Outputs", [("dataset_gcs_prefix", str), ("dataset_gcs_uri", list)]):
    """Extract BQ table in GCS.

    Args:
        bq_client_project_id (str): Project ID that will be used by the BQ client.
        source_project_id (str): Project id from where BQ table will be extracted.
        dataset_id (str): Dataset ID from where the BQ table will be extracted.
        table_name (str): Table name (without project ID and dataset ID) from
            where the BQ table will be extracted.
        dataset (Output[Dataset]): Output dataset artifact generated by the operation,
            this parameter will be passed automatically by the orchestrator.
        dataset_location (str): BQ dataset location. Defaults to "europe-west2".
        extract_job_config (Optional[dict], optional): Dict containing optional
            parameters required by the bq extract operation. Defaults to None.
            See available parameters here
            https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.job.ExtractJobConfig.html # noqa
        skip_if_exists (bool): If True, skip extracting the dataset if the
            output resource already exists.
        file_pattern (Optional[str], optional): File pattern to append to the
            output files (e.g. `.csv`). Defaults to None.
        destination_gcs_uri (Optional[str], optional): GCS URI to use for
            saving query results. Defaults to None.

    Returns:
        NamedTuple (str, list): Output dataset directory and its GCS uri

    Raises:
        GoogleCloudError: If an error is raised by the operation.
    """
    import os
    from pathlib import Path

    from google.cloud import bigquery
    from google.cloud.exceptions import GoogleCloudError
    from loguru import logger

    # Set uri of output dataset if destination_gcs_uri is provided
    if destination_gcs_uri:
        dataset.uri = destination_gcs_uri

    logger.info(f"Checking if destination exists: {dataset.path}.")
    if Path(dataset.path).exists() and skip_if_exists:
        logger.warning("Destination already exists, skipping table extraction.")
        return

    full_table_id = f"{source_project_id}.{dataset_id}.{table_name}"
    table = bigquery.table.Table(table_ref=full_table_id)

    if extract_job_config is None:
        extract_job_config = {}
    job_config = bigquery.job.ExtractJobConfig(**extract_job_config)
    client = bigquery.client.Client(
        project=bq_client_project_id, location=dataset_location
    )

    # If file_pattern is provided, join dataset.uri with file_pattern
    dataset_uri = dataset.uri
    if file_pattern is not None:
        dataset_uri = os.path.join(dataset_uri, file_pattern)
    dataset_directory = os.path.dirname(dataset_uri)

    logger.info(f"Extract table {table} to {dataset_uri}.")
    extract_job = client.extract_table(
        table,
        dataset_uri,
        job_config=job_config,
    )

    try:
        result = extract_job.result()
        logger.info(f"Table extracted, result: {result}.")
    except GoogleCloudError as e:
        logger.error(e)
        logger.error(extract_job.error_result)
        logger.error(extract_job.errors)
        raise e

    return (dataset_directory, [dataset_uri])
