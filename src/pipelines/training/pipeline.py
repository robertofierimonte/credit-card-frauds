import os
import warnings
from pathlib import Path

from google_cloud_pipeline_components.v1.custom_job.utils import (
    create_custom_training_job_op_from_component,
)
from google_cloud_pipeline_components.v1.vertex_notification_email import (
    VertexNotificationEmailOp,
)
from kfp import compiler, dsl
from kfp.components.types.type_utils import InconsistentTypeWarning

from src.base.utilities import generate_query, read_json
from src.components.aiplatform import export_model, lookup_model, upload_model
from src.components.bigquery import bq_table_to_dataset, execute_query
from src.components.data import get_data_version
from src.components.dependencies import PIPELINE_IMAGE_NAME
from src.components.helpers import get_current_time
from src.components.model import (
    compare_champion_challenger,
    compare_models,
    evaluate_model,
    train_evaluate_model,
)

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=InconsistentTypeWarning, append=True)

DEVELOPMENT_STAGE = os.getenv("DEVELOPMENT_STAGE", "no_development_stage")
BRANCH_NAME = os.getenv("CURRENT_BRANCH", "no_branch")
COMMIT_HASH = os.getenv("CURRENT_COMMIT", "no_commit")
PIPELINE_FILES_GCS_PATH = os.getenv("PIPELINE_FILES_GCS_PATH")
PIPELINE_NAME = f"frauds-training-pipeline-{BRANCH_NAME}-{COMMIT_HASH}"


train_job = create_custom_training_job_op_from_component(
    component_spec=train_evaluate_model,
    machine_type="n1-standard-32",
    replica_count=1,
)


@dsl.pipeline(name=PIPELINE_NAME, description="Credit card frauds training Pipeline")
def training_pipeline(
    project_id: str,
    project_location: str,
    dataset_id: str,
    dataset_location: str,
    data_version: str,
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
        email_notification_recipients (list): List of email addresses that will be
            notified upon completion (whether successful or not) of the pipeline.
    """
    notify_email_task = VertexNotificationEmailOp(
        recipients=email_notification_recipients
    )

    queries_folder = Path(__file__).parent / "queries"
    config_folder = Path(__file__).parent.parent / "configuration"
    config_params = read_json(config_folder / "params.json")

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
                timestamp="{{$.pipeline_job_create_time_utc}}",
                format_str="%Y%m%d%H%M%S",
            )
            .set_display_name("Format current timestamp")
            .set_caching_options(False)
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

        models_gcs_folder_path = f"{PIPELINE_FILES_GCS_PATH}/models/{COMMIT_HASH}"

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

        query_job_config = dict(use_query_cache=True)

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
            )
            .after(train_valid_test)
            .set_display_name("Extract test data")
            .set_caching_options(True)
        )

        lookup_champion_model = (
            lookup_model(
                model_name="credit-card-frauds-champion",
                project_id=project_id,
                project_location=project_location,
            )
            .set_display_name("Lookup champion model")
            .set_caching_options(True)
        )

        with dsl.ParallelFor(items=models, name="Train and evaluate models") as item:
            train = (
                train_job(
                    training_data=extract_training_data.outputs["dataset"],
                    validation_data=extract_validation_data.outputs["dataset"],
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

            evaluate = (
                evaluate_model(
                    test_data=extract_test_data.outputs["dataset"],
                    target_column=config_params["target_column"],
                    model=train.outputs["model"],
                )
                .after(train)
                .set_display_name("Evaluate model on test set")
                .set_caching_options(True)
            )

            upload = (
                upload_model(
                    model_id="credit-card-frauds",
                    display_name="credit-card-frauds",
                    serving_container_image_uri=PIPELINE_IMAGE_NAME,
                    project_id=project_id,
                    project_location=project_location,
                    model=train.outputs["model"],
                    pipeline_timestamp=f"{current_timestamp.output}",
                    data_version=f"{data_version.output}",
                    labels=dict(
                        development_stage=DEVELOPMENT_STAGE,
                        branch_name=BRANCH_NAME,
                        commit_hash=COMMIT_HASH,
                    ),
                    description="Credit card frauds model",
                    is_default_version=False,
                    version_description="Credit card frauds model",
                    model_name=item,
                )
                .after(train)
                .set_display_name("Upload model")
            )

        compare_candidates = (
            compare_models(
                test_data=extract_test_data.outputs["dataset"],
                target_column=config_params["target_column"],
                metric_to_optimise="Average Precision",
                higher_is_better=True,
                models=dsl.Collected(train.outputs["model"]),
            )
            .set_display_name("Select best model")
            .set_caching_options(True)
        )

        with dsl.Condition(
            lookup_champion_model.outputs["Output"] != "", "Champion model exists"
        ):
            export_champion_model = (
                export_model(
                    model_id="credit-card-frauds-champion",
                    project_id=project_id,
                    project_location=project_location,
                    model_file_name="model.joblib",
                )
                .set_display_name("Export champion model")
                .set_caching_options(True)
            )

            challenge_champion_model = (
                compare_champion_challenger(
                    test_data=extract_test_data.outputs["dataset"],
                    target_column=config_params["target_column"],
                    challenger_model=compare_candidates.outputs["best_model"],
                    champion_model=export_champion_model.outputs["model"],
                    metric_to_optimise="Average Precision",
                    absolute_threshold=0.1,
                    higher_is_better=True,
                )
                .set_display_name("Compare challenger to champion")
                .set_caching_options(True)
            )

        upload_challenger = (
            upload_model(
                model_id="credit-card-frauds-challenger",
                display_name="credit-card-frauds-challenger",
                serving_container_image_uri=PIPELINE_IMAGE_NAME,
                project_id=project_id,
                project_location=project_location,
                model=compare_candidates.outputs["best_model"],
                pipeline_timestamp=f"{current_timestamp.output}",
                data_version=f"{data_version.output}",
                labels=dict(
                    development_stage=DEVELOPMENT_STAGE,
                    branch_name=BRANCH_NAME,
                    commit_hash=COMMIT_HASH,
                ),
                description="Credit card frauds challenger model",
                is_default_version=True,
            )
            .set_display_name("Upload challenger model")
            .set_caching_options(True)
        )


def compile():
    compiler.Compiler().compile(
        pipeline_func=training_pipeline,
        package_path="training.json",
        type_check=False,
    )


if __name__ == "__main__":
    compile()
