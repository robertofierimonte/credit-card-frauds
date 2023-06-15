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
    dataset_name = f"{project_name}.credit_card_frauds_{data_version}"

    # Create the BQ client
    client = bigquery.Client(project=project_name, location=project_location)

    # Create the dataset if doesn't exist
    dataset = bigquery.Dataset(dataset_name)
    dataset.location = project_location
    try:
        dataset = client.create_dataset(dataset)
        logger.info(
            f"Created BQ dataset {dataset_name} in location {project_location}."
        )
    except Conflict:
        logger.warning(
            f"BQ dataset {dataset_name} in location {project_location} "
            "already exists. Skipping creation."
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
    job = client.load_table_from_dataframe(
        df_holidays, destination=table_id, job_config=job_config_holidays
    )
    job.result()
    table = client.get_table(table_id)
    logger.info(
        f"Loaded {table.num_rows} rows and {len(table.schema)} columns "
        f"of US holidays data into {table_id}."
    )

    # Load the data into the dataset
    data_folder = Path(__file__).parent.parent / "data"
    transactions_file = data_folder / "credit_card_transactions-ibm_v2.csv"
    users_file = data_folder / "sd254_users.csv"
    cards_file = data_folder / "sd254_cards.csv"

    job_config_data = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV, skip_leading_rows=1, autodetect=True
    )
    job_config_pandas = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV, autodetect=True
    )

    with open(transactions_file, "rb") as f:
        table_id = f"{dataset_name}.transactions"
        job = client.load_table_from_file(
            f, destination=table_id, job_config=job_config_data
        )
        job.result()
        table = client.get_table(table_id)
        logger.info(
            f"Loaded {table.num_rows} rows and {len(table.schema)} columns "
            f"of transactions data into {table_id}."
        )

    with open(users_file, "rb") as f:
        df = pd.read_csv(f)
        df["User"] = df.index
        table_id = f"{dataset_name}.users"
        job = client.load_table_from_dataframe(
            df, destination=table_id, job_config=job_config_pandas
        )
        job.result()
        table = client.get_table(table_id)
        logger.info(
            f"Loaded {table.num_rows} rows and {len(table.schema)} columns "
            f"of users data into {table_id}."
        )

    with open(cards_file, "rb") as f:
        table_id = f"{dataset_name}.cards"
        job = client.load_table_from_file(
            f, destination=table_id, job_config=job_config_data
        )
        job.result()
        table = client.get_table(table_id)
        logger.info(
            f"Loaded {table.num_rows} rows and {len(table.schema)} columns "
            f"of cards data into {table_id}."
        )

    logger.info("Data upload complete.")
