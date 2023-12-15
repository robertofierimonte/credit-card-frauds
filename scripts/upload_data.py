import argparse
import os
import re
from datetime import datetime
from pathlib import Path

import holidays
import numpy as np
import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import Conflict
from loguru import logger

# Get files to upload
_DATA_FOLDER = Path(__file__).parent.parent / "data"
_TRANSACTIONS_FILE = _DATA_FOLDER / "credit_card_transactions-ibm_v2.csv"
_USERS_FILE = _DATA_FOLDER / "sd254_users.csv"
_CARDS_FILE = _DATA_FOLDER / "sd254_cards.csv"

if __name__ == "__main__":
    # Parse the arguments
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--data-version",
        type=str,
        required=False,
        default="",
        help="version of the data to be uploaded",
    )
    args = parser.parse_args()

    # Define the data version
    data_version = None
    if args.data_version and args.data_version != "":
        # If a correct timestamp is provided, use it as the data version number
        if re.match(r"\d{8}T\d{6}", args.data_version):
            data_version = args.data_version
            logger.info(f"Provided data version: {data_version}.")
        else:
            logger.warning(
                "Argument `data-version` is in the wrong format. It will be ignored."
            )
    if data_version is None:
        # Set the version number of the data asset to the current UTC time
        logger.info("Data version not provided, setting to current timestamp...")
        data_version = datetime.now().strftime("%Y%m%dT%H%M%S")

    # Get project info and set dataset name
    project_name = os.environ.get("VERTEX_PROJECT_ID")
    project_location = os.environ.get("VERTEX_LOCATION")

    # Upload the data to BQ

    # Create the BQ client
    bq_client = bigquery.Client(project=project_name, location=project_location)

    # Create the dataset if doesn't exist
    dataset_name = f"{project_name}.credit_card_frauds_{data_version}"
    dataset = bigquery.Dataset(dataset_name)
    dataset.location = project_location
    try:
        dataset = bq_client.create_dataset(dataset)
        logger.info(
            f"Created BQ dataset {dataset_name} in location {project_location}."
        )

        # Create a table to store US holidays
        job_config_holidays = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema=[
                bigquery.SchemaField("date", bigquery.enums.SqlTypeNames.DATE),
                bigquery.SchemaField("name", bigquery.enums.SqlTypeNames.STRING),
            ],
        )

        holidays_us = holidays.US(years=np.arange(1980, 2036))
        df_holidays = pd.DataFrame(
            data=holidays_us.items(),
            columns=["date", "name"],
        )
        table_id = f"{dataset_name}.holidays"
        job = bq_client.load_table_from_dataframe(
            df_holidays, destination=table_id, job_config=job_config_holidays
        )
        job.result()
        table = bq_client.get_table(table_id)
        logger.info(
            f"Loaded {table.num_rows} rows and {len(table.schema)} columns "
            f"of US holidays data into {table_id}."
        )

        # Load the data into the dataset
        job_config_data = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=True,
        )
        job_config_pandas = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV, autodetect=True
        )

        with open(_TRANSACTIONS_FILE, "rb") as f:
            table_id = f"{dataset_name}.transactions"
            job = bq_client.load_table_from_file(
                f, destination=table_id, job_config=job_config_data
            )
            job.result()
            table = bq_client.get_table(table_id)
            logger.info(
                f"Loaded {table.num_rows} rows and {len(table.schema)} columns "
                f"of transactions data into {table_id}."
            )

        with open(_USERS_FILE, "rb") as f:
            df = pd.read_csv(f)
            df["User"] = df.index
            table_id = f"{dataset_name}.users"
            job = bq_client.load_table_from_dataframe(
                df, destination=table_id, job_config=job_config_pandas
            )
            job.result()
            table = bq_client.get_table(table_id)
            logger.info(
                f"Loaded {table.num_rows} rows and {len(table.schema)} columns "
                f"of users data into {table_id}."
            )

        with open(_CARDS_FILE, "rb") as f:
            table_id = f"{dataset_name}.cards"
            job = bq_client.load_table_from_file(
                f, destination=table_id, job_config=job_config_data
            )
            job.result()
            table = bq_client.get_table(table_id)
            logger.info(
                f"Loaded {table.num_rows} rows and {len(table.schema)} columns "
                f"of cards data into {table_id}."
            )

        logger.info("Data upload complete.")

    except Conflict:
        logger.warning(
            f"BQ dataset {dataset_name} in location {project_location} "
            "already exists. Skipping creation."
        )

    # # Upload the data to GCS

    # # Create the GCS client
    # pipeline_root = os.environ.get("VERTEX_PIPELINE_ROOT")
    # pipeline_root = pipeline_root.replace("gs://", "")
    # gcs_client = storage.Client(project=project_name)

    # # Check if pipeline root bucket exist, otherwise create it.
    # bucket_name = pipeline_root.split("/")[0]
    # bucket = storage.Bucket(client=gcs_client, name=bucket_name)
    # if not bucket.exists():
    #     bucket.create()
    #     logger.info(
    #         f"Created GCS bucket gs://{bucket.name} in location {project_location}."
    #     )

    # gcs_path = f"{pipeline_root.split('/')[1:]}/data/{data_version}"

    # # Check if transactions blob exists. If not, upload the transactions file.
    # transactions_blob = storage.Blob(f"{gcs_path}/transactions.csv", bucket=bucket)
    # if not transactions_blob.exists():
    #     transactions_blob.upload_from_filename(_TRANSACTIONS_FILE)
    #     logger.info(f"Uploaded transactions data to gs://{gcs_path}/transactions.csv .")

    # # Check if users blob exists. If not, upload the users file.
    # users_blob = storage.Blob(f"{gcs_path}/users.csv", bucket=bucket)
    # if not users_blob.exists():
    #     users_blob.upload_from_filename(_USERS_FILE)
    #     logger.info(f"Uploaded users data to gs://{gcs_path}/users.csv .")

    # # Check if cards blob exists. If not, upload the cards file.
    # cards_blob = storage.Blob(f"{gcs_path}/cards.csv", bucket=bucket)
    # if not cards_blob.exists():
    #     cards_blob.upload_from_filename(_CARDS_FILE)
    #     logger.info(f"Uploaded cards data to gs://{gcs_path}/cards.csv .")
