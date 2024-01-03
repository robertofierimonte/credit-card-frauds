import json
import os
import warnings
from pathlib import Path

from google_cloud_pipeline_components.v1.custom_job.utils import (
    create_custom_training_job_from_component,
)
from google_cloud_pipeline_components.v1.vertex_notification_email import (
    VertexNotificationEmailOp,
)
from kfp import compiler, dsl
from kfp.dsl.types.type_utils import InconsistentTypeWarning

from src.base.utilities import generate_query, read_yaml
from src.components.aiplatform import (
    export_model,
    lookup_model,
    update_version_alias,
    upload_model,
)
from src.components.bigquery import bq_table_to_dataset, execute_query
from src.components.data import get_data_version
from src.components.dependencies import PIPELINE_IMAGE_NAME
from src.components.helpers import get_current_time
from src.components.model import (
    compare_champion_challenger,
    compare_models,
    train_evaluate_model,
)

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=InconsistentTypeWarning, append=True)

PIPELINE_TAG = os.getenv("PIPELINE_TAG")
VERTEX_PIPELINE_FILES_GCS_PATH = os.getenv("VERTEX_PIPELINE_FILES_GCS_PATH")
PIPELINE_NAME = f"frauds-training-pipeline-{PIPELINE_TAG}"

ENVIRONMENT = os.environ.get("ENVIRONMENT")
BRANCH_NAME = os.environ.get("CURRENT_BRANCH")
COMMIT_HASH = os.environ.get("CURRENT_COMMIT")
RELEASE_TAG = os.environ.get("CURRENT_TAG")
SERVICE_ACCOUNT = os.environ.get("VERTEX_SA_EMAIL")


train_job = create_custom_training_job_from_component(
    component_spec=train_evaluate_model,
    machine_type="n1-standard-32",
    service_account=SERVICE_ACCOUNT,
    replica_count=1,
)


@dsl.pipeline(name=PIPELINE_NAME, description="Credit card frauds training Pipeline")
def training_pipeline(
    project_id: str,
    project_location: str,
    dataset_id: str,
    dataset_location: str,
    data_version: str,
    create_replace_tables: bool,
    skip_bq_extract_if_exists: bool,
    email_notification_recipients: list,
):
    """Credit card frauds classification training pipeline.

    Steps:
    1. Extract and process input data from BQ
    2. Train the models via Vertex AI custom training job
    3. Evaluate the models
    4. Upload the model to the Vertex AI model registry

    Args:
        project_id (str): GCP project ID where the pipeline will run.
        project_location (str): GCP location whe the pipeline will run.
        dataset_id (str): Bigquery dataset used to store all the staging datasets.
        dataset_location (str): Location of the BQ staging dataset.
        data_version (str): Specific timestamp in `%Y%m%dT%H%M%S format.
        create_replace_tables (bool): Whether to replace the staging tables if they
            already exist.
        skip_bq_extract_if_exists (bool): Whether to skip the BQ extract step to GCS if
            the output target already exists.
        email_notification_recipients (list): List of email addresses that will be
            notified upon completion (whether successful or not) of the pipeline.
    """
    notify_email_task = VertexNotificationEmailOp(
        recipients=email_notification_recipients
    )

    queries_folder = Path(__file__).parent / "queries"
    config_folder = Path(__file__).parent.parent / "configuration"
    config_params = read_yaml(config_folder / "params.yaml")
    serving_container_params = read_yaml(config_folder / "serving_container.yaml")

    models = config_params["models"]
    features = "`" + "`,\n`".join(f for f in config_params["features"]) + "`"

    with dsl.ExitHandler(notify_email_task, name="Notify pipeline result"):
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

        current_timestamp = (
            get_current_time(
                timestamp=dsl.PIPELINE_JOB_CREATE_TIME_UTC_PLACEHOLDER,
                format_str="%Y%m%d%H%M%S",
            )
            .set_display_name("Format current timestamp")
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

        models_gcs_folder_path = f"{VERTEX_PIPELINE_FILES_GCS_PATH}/models"

        preprocessing_query = generate_query(
            queries_folder / "q_preprocessing.sql",
            transactions_table=transactions_table,
            users_table=users_table,
            cards_table=cards_table,
            holidays_table=holidays_table,
            preprocessed_table=preprocessed_table,
            fraud_delay_seconds=(config_params["fraud_delay_days"] * 24 * 60 * 60),
            features=features,
            create_replace_tables=create_replace_tables,
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
            create_replace_tables=create_replace_tables,
        )

        train_valid_test = (
            execute_query(
                query=train_valid_test_query,
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
                extract_job_config=dict(destination_format="PARQUET"),
                skip_if_exists=skip_bq_extract_if_exists,
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
                extract_job_config=dict(destination_format="PARQUET"),
                skip_if_exists=skip_bq_extract_if_exists,
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
                extract_job_config=dict(destination_format="PARQUET"),
                skip_if_exists=skip_bq_extract_if_exists,
            )
            .after(train_valid_test)
            .set_display_name("Extract test data")
            .set_caching_options(True)
        )

        lookup_champion_model = (
            lookup_model(
                model_name="credit-card-frauds",
                project_id=project_id,
                project_location=project_location,
                model_version="champion",
            )
            .set_display_name("Lookup champion model")
            .set_caching_options(True)
        )

        model_labels = json.dumps(
            dict(
                environment=ENVIRONMENT,
                branch_name=BRANCH_NAME,
                commit_hash=COMMIT_HASH,
                release_tag=RELEASE_TAG,
                pipeline_id=dsl.PIPELINE_JOB_ID_PLACEHOLDER,
                pipeline_timestamp=f"{current_timestamp.output}",
                data_version=f"{data_version.output}",
            )
        )

        with dsl.ParallelFor(items=models, name="Train and evaluate models") as item:
            train = (
                train_job(
                    training_data=extract_training_data.outputs["dataset"],
                    validation_data=extract_validation_data.outputs["dataset"],
                    test_data=extract_test_data.outputs["dataset"],
                    target_column=config_params["target_column"],
                    model_name=item,
                    models_params=config_params["models_params"],
                    fit_args=config_params["fit_args"],
                    data_processing_args=config_params["data_processing_args"],
                    model_gcs_folder_path=models_gcs_folder_path,
                    # Training wrapper specific arguments
                    project=project_id,
                    location=project_location,
                )
                .after(extract_training_data, extract_validation_data)
                .set_display_name("Train and evaluate model")
                .set_caching_options(True)
            )

            upload = (
                upload_model(
                    model_id="credit-card-frauds",
                    display_name="credit-card-frauds",
                    serving_container_image_uri=PIPELINE_IMAGE_NAME,
                    serving_container_params=serving_container_params,
                    project_id=project_id,
                    project_location=project_location,
                    model=train.outputs["model"],
                    labels=model_labels,
                    description="Credit card frauds model",
                    is_default_version=False,
                    version_description="Credit card frauds model",
                    model_name=item,
                )
                .after(train)
                .set_display_name("Upload model")
                .set_caching_options(True)
            )

        compare_candidates = (
            compare_models(
                test_data=extract_test_data.outputs["dataset"],
                target_column=config_params["target_column"],
                metric_to_optimise="average_precision",
                higher_is_better=True,
                models=dsl.Collected(train.outputs["model"]),
                model_resource_names=dsl.Collected(
                    upload.outputs["model_resource_name"]
                ),
            )
            .set_display_name("Select best model")
            .set_caching_options(False)
        )

        with dsl.If(
            lookup_champion_model.outputs["Output"] != "", "Champion model exists"
        ):
            export_champion_model = (
                export_model(
                    model_id="credit-card-frauds",
                    project_id=project_id,
                    project_location=project_location,
                    model_version="champion",
                    model_file_name="model.joblib",
                )
                .set_display_name("Export champion model")
                .set_caching_options(False)
            )

            _ = (
                compare_champion_challenger(
                    test_data=extract_test_data.outputs["dataset"],
                    target_column=config_params["target_column"],
                    challenger_model=compare_candidates.outputs["best_model"],
                    champion_model=export_champion_model.outputs["model"],
                    metric_to_optimise="average_precision",
                    absolute_threshold=0.1,
                    higher_is_better=True,
                )
                .set_display_name("Compare challenger to champion")
                .set_caching_options(False)
            )

        _ = (
            update_version_alias(
                model_id="credit-card-frauds",
                project_id=project_id,
                project_location=project_location,
                model_version=compare_candidates.outputs["best_model_version"],
                version_aliases=["challenger"],
            )
            .set_display_name("Label challenger model")
            .set_caching_options(False)
        )


def compile():
    compiler.Compiler().compile(
        pipeline_func=training_pipeline,
        package_path="training.json",
        type_check=False,
    )


if __name__ == "__main__":
    compile()
