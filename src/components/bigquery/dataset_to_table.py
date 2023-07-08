from kfp.dsl import Artifact, Input, component

from src.components.dependencies import GOOGLE_CLOUD_BIGQUERY, LOGURU, PYTHON


@component(
    base_image=PYTHON,
    packages_to_install=[GOOGLE_CLOUD_BIGQUERY, LOGURU],
)
def dataset_to_bq_table(
    bq_client_project_id: str,
    destination_project_id: str,
    dataset_id: str,
    table_name: str,
    dataset: Input[Artifact],
    dataset_location: str = "europe-west2",
    dataset_format: str = "csv",
) -> None:
    """Load datasets in JSONL / CSV / Parquet format from GCS to BQ.

    Args:
        bq_client_project_id (str): Project ID that will be used by the BQ client.
        destination_project_id (str): BQ project ID where the dataset will be loaded.
        dataset_id (str): BQ dataset ID where the dataset will be loaded.
        table_name (str): BQ table name (without project ID and dataset ID)
            where the dataset will be loaded.
        dataset (input[Artifact]): Dataset that will be loaded into BQ.
        dataset_location (str, optional): Location of the GCS bucket containing
            the dataset. Defaults to "europe-west2".
        dataset_format (str, optional): Format of the dataset, must be one of
            `csv`, `parquet`, `jsonl`. Defaults to "csv".
    """
    from google.cloud import bigquery
    from loguru import logger

    client = bigquery.Client(project=bq_client_project_id)

    table_id = f"{destination_project_id}.{dataset_id}.{table_name}"
    logger.info(f"Loading data from GCS location: {dataset.uri}.")
    logger.info(f"Destination table in BQ: {table_id}.")

    if dataset_format == "csv":
        job_config = bigquery.LoadJobConfig(
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
        )
    elif dataset_format == "parquet":
        job_config = bigquery.LoadJobConfig(
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.SourceFormat.PARQUET,
        )
    elif dataset_format == "jsonl":
        job_config = bigquery.LoadJobConfig(
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        )
    else:
        logger.error(f"Dataset format {dataset_format} not supported.")

    if "gcsOutputDirectory" in dataset.metadata:
        # Override in case of batch predictions
        data_location = (
            f"{dataset.metadata['gcsOutputDirectory']}/predictions.results-*"
        )
    else:
        data_location = dataset.uri

    # Make an API request
    load_job = client.load_table_from_uri(
        source_uris=data_location,
        destination=table_id,
        location=dataset_location,
        job_config=job_config,
    )

    # Wait for the job to complete
    job_result = load_job.result()

    if job_result.done():
        logger.info("BQ load job complete.")
    else:
        logger.error(job_result.exception())
        raise RuntimeError(job_result.exception())
