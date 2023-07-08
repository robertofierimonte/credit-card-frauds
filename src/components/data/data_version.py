from kfp.dsl import component

from src.components.dependencies import GOOGLE_CLOUD_BIGQUERY, LOGURU, PYTHON


@component(
    base_image=PYTHON,
    packages_to_install=[GOOGLE_CLOUD_BIGQUERY, LOGURU],
)
def get_data_version(
    payload_data_version: str,
    project_id: str,
    dataset_id: str,
    dataset_location: str = "europe-west2",
) -> str:
    """Get data version to use in the pipeline.

    Args:
        payload_data_version (str): Data version provided in the payload file.
        project_id (str): Bigquery project ID.
        dataset_id (str): Bigquery dataset ID. This function will look for the
            most recent BQ dataset that has the pattern of
            {dataset_id}_%Y%m%dT%H%M%S.
        dataset_location (str, optional): Bigquery dataset location.
            Defaults to "europe-west2".
    """
    import re

    from google.cloud import bigquery
    from loguru import logger

    if payload_data_version == "":
        bq_client = bigquery.client.Client(
            project=project_id, location=dataset_location
        )
        datasets = [d.dataset_id for d in list(bq_client.list_datasets())]
        matches = [
            re.search(rf"(?<={dataset_id}_)(\d{{8}}T\d{{6}})", d) for d in datasets
        ]
        versions = sorted([m.group(0) for m in matches if m is not None])
        logger.debug(f"Found {len(versions)} versions of the data.")

        try:
            res = versions[-1]
            logger.info(f"Most recent data version retrieved: {res}.")
            return res
        except IndexError as e:
            logger.error(
                f"No datasets matching the expected pattern in project {project_id}."
            )
            raise e
    else:
        logger.info(f"Data version {payload_data_version} provided in payload.")
        return payload_data_version
