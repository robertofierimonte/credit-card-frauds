import json
import os
import pathlib
import warnings

# from google_cloud_pipeline_components.experimental.custom_job.utils import (
#     create_custom_training_job_op_from_component,
# )
from kfp.v2 import compiler, dsl

from src.base.utilities import generate_query, read_json
from src.components.bigquery import bq_table_to_dataset, execute_query
from src.components.data import get_data_version

warnings.filterwarnings("ignore", category=FutureWarning)

PIPELINE_TAG = os.getenv("PIPELINE_TAG", "untagged")
PIPELINE_NAME = f"frauds-training-pipeline-{PIPELINE_TAG}"


@dsl.pipeline(name=PIPELINE_NAME, description="Credit card frauds training Pipeline")
def training_pipeline(
    project_id: str,
    project_location: str,
    dataset_id: str,
    dataset_location: str,
    data_version: str,
    pipeline_files_gcs_path: str,
):
    """Credit card frauds classification training pipeline.

    Steps:
    1. Extract input data from BQ
    2. Train the model via Vertex AI custom training job
    3. Evaluate the model
    4. Upload the model to the Vertex AI model registry

    Args:
        project_id (str): GCP project ID where the pipeline will run.
        project_location (str): GCP location whe the pipeline will run.
        dataset_id (str): Bigquery dataset used to store all the staging datasets.
        dataset_location (str): Location of the BQ staging dataset.
        pipeline_files_gcs_path (str): GCS path where the pipeline files are located.
        data_version (str): Optional. Empty or a specific timestamp in
            `%Y%m%dT%H%M%S format.
    """
    queries_folder = pathlib.Path(__file__).parent / "queries"
    config_folder = pathlib.Path(__file__).parent.parent / "configuration"
    config_params = read_json(config_folder / "params.json")

    features = "`" + "`,\n`".join(f for f in config_params["features"]) + "`"

    data_version = (
        get_data_version(
            payload_data_version=data_version,
            project_id=project_id,
            dataset_id=dataset_id,
            dataset_location=dataset_location,
        )
        .set_display_name("Get data version")
        .set_caching_options(True)
    )

    dataset_name = f"{project_id}.{dataset_id}_{data_version.output}"
    transactions_table = f"{dataset_name}.transactions"
    users_table = f"{dataset_name}.users"
    cards_table = f"{dataset_name}.cards"
    holidays_table = f"{dataset_name}.holidays"
    preprocessed_table = f"{dataset_name}.preprocessed"
    train_set_table = f"{dataset_name}.training"
    valid_set_table = f"{dataset_name}.validation"
    test_set_table = f"{dataset_name}.testing"

    preprocessing_query = generate_query(
        queries_folder / "q_preprocessing.sql",
        transactions_table=transactions_table,
        users_table=users_table,
        cards_table=cards_table,
        holidays_table=holidays_table,
        preprocessed_table=preprocessed_table,
        fraud_delay_seconds=(config_params["fraud_delay_days"] * 24 * 60 * 60),
        features=features,
    )

    query_job_config = json.dumps(dict(use_query_cache=True))

    preprocess_data = (
        execute_query(
            query=preprocessing_query,
            bq_client_project_id=project_id,
            query_job_config=query_job_config,
        )
        .set_display_name("Preprocess input data")
        .set_caching_options(True)
    )

    train_valid_test_query = generate_query(
        queries_folder / "q_train_valid_test_split.sql",
        source_table=preprocessed_table,
        valid_size=0.15,
        test_size=0.15,
        training_table=train_set_table,
        validation_table=valid_set_table,
        testing_table=test_set_table,
    )

    train_valid_test = (
        execute_query(
            train_valid_test_query,
            bq_client_project_id=project_id,
            query_job_config=query_job_config,
        )
        .after(preprocess_data)
        .set_display_name("Train / validation / test split")
        .set_caching_options(True)
    )

    extract_training_data = (
        bq_table_to_dataset(
            bq_client_project_id=project_id,
            source_project_id=project_id,
            dataset_id=f"{dataset_id}_{data_version.output}",
            table_name=train_set_table.rsplit(".", 1)[1],
            dataset_location=dataset_location,
            file_pattern="file_*",
            extract_job_config=json.dumps(dict(destination_format="PARQUET")),
        )
        .after(train_valid_test)
        .set_display_name("Extract training data")
        .set_caching_options(True)
    )

    extract_validation_data = (
        bq_table_to_dataset(
            bq_client_project_id=project_id,
            source_project_id=project_id,
            dataset_id=f"{dataset_id}_{data_version.output}",
            table_name=valid_set_table.rsplit(".", 1)[1],
            dataset_location=dataset_location,
            file_pattern="file_*",
            extract_job_config=json.dumps(dict(destination_format="PARQUET")),
        )
        .after(train_valid_test)
        .set_display_name("Extract validation data")
        .set_caching_options(True)
    )

    extract_test_data = (
        bq_table_to_dataset(
            bq_client_project_id=project_id,
            source_project_id=project_id,
            dataset_id=f"{dataset_id}_{data_version.output}",
            table_name=test_set_table.rsplit(".", 1)[1],
            dataset_location=dataset_location,
            file_pattern="file_*",
            extract_job_config=json.dumps(dict(destination_format="PARQUET")),
        )
        .after(train_valid_test)
        .set_display_name("Extract test data")
        .set_caching_options(True)
    )

    # train = (
    #     train_model(
    #         training_data=train_test_data.outputs["output_train_dataset"],
    #         target_column=config_params["target_column"],
    #         model_params=json.dumps(config_params["model_params"]),
    #         model_file_name=config_params["model_file_name"],
    #     )
    #     .after(train_test_data)
    #     .set_display_name("Train model")
    # )

    # evaluate = evaluate_model(
    #     test_data=train_test_data.outputs["output_test_dataset"],
    #     target_column=config_params["target_column"],
    #     model=train.outputs["model"],
    # ).set_display_name("Evaluate model")

    # upload = upload_model(
    #     model_id=config_params["model_name"],
    #     display_name=config_params["model_name"],
    #     serving_container_image_uri=PIPELINE_IMAGE_NAME,
    #     project_id=project_id,
    #     project_location=project_location,
    #     model=train.outputs["model"],
    #     labels=json.dumps(
    #         dict(
    #             train_pipeline_timestamp=f"{current_timestamp.output}",
    #             pipeline_tag=PIPELINE_TAG,
    #         )
    #     ),
    #     description="Taxi fare prediction model",
    #     is_default_version=True,
    #     version_description="Taxi fare prediction model",
    # ).set_display_name("Upload model")


def compile():
    compiler.Compiler().compile(
        pipeline_func=training_pipeline,
        package_path="training.json",
        type_check=False,
    )


if __name__ == "__main__":
    compile()
