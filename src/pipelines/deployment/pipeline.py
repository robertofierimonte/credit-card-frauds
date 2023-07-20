import os
from pathlib import Path

from google_cloud_pipeline_components.v1.vertex_notification_email import (
    VertexNotificationEmailOp,
)
from kfp import compiler, dsl

from src.base.utilities import read_json
from src.components.aiplatform import deploy_model, export_model, upload_model
from src.components.dependencies import PIPELINE_IMAGE_NAME
from src.components.helpers import merge_dicts

COMMIT_TAG = os.getenv("CURRENT_TAG", "no_tag")
PIPELINE_FILES_GCS_PATH = os.getenv("PIPELINE_FILES_GCS_PATH")
PIPELINE_NAME = f"frauds-deployment-pipeline-{COMMIT_TAG}"


@dsl.pipeline(name=PIPELINE_NAME, description="Credit card frauds deployment Pipeline")
def deployment_pipeline(
    project_id: str,
    project_location: str,
    dataset_id: str,
    dataset_location: str,
    data_version: str,
    email_notification_recipients: list,
):
    """_summary_

    Args:
        project_id (str): _description_
        project_location (str): _description_
        dataset_id (str): _description_
        dataset_location (str): _description_
        data_version (str): _description_
        email_notification_recipients (list): _description_
    """

    config_folder = Path(__file__).parent.parent / "configuration"
    serving_container_params = read_json(config_folder / "serving_container.json")

    notify_email_task = VertexNotificationEmailOp(
        recipients=email_notification_recipients
    )
    with dsl.ExitHandler(notify_email_task, name="Notify pipeline result"):

        export = (
            export_model(
                model_id="credit-card-frauds-challenger",
                project_id=project_id,
                project_location=project_location,
                model_file_name="model.joblib",
            )
            .set_display_name("Export challenger")
            .set_caching_options(True)
        )

        merge_labels = (
            merge_dicts(
                dict1=export.outputs["labels"], dict2=dict(commit_tag=COMMIT_TAG)
            )
            .after(export)
            .set_display_name("Add git tag to challenger labels")
            .set_caching_options(True)
        )

        upload = (
            upload_model(
                model=export.outputs["model"],
                model_id="credit-card-frauds-champion",
                display_name="credit-card-frauds-champion",
                serving_container_image_uri=PIPELINE_IMAGE_NAME,
                serving_container_params=serving_container_params,
                project_id=project_id,
                project_location=project_location,
                labels=merge_labels.output,
                description="Credit card frauds champion model",
                is_default_version=True,
            )
            .after(export)
            .set_display_name("Crown challenger to champion")
            .set_caching_options(True)
        )

        deploy = (
            deploy_model(
                model_id="credit-card-frauds-champion",
                endpoint_id="credit-card-frauds-endpoint",
                project_id=project_id,
                dataset_id=dataset_id,
                project_location=project_location,
            )
            .after(upload)
            .set_display_name("Deploy new champion")
            .set_caching_options(True)
        )


def compile():
    compiler.Compiler().compile(
        pipeline_func=deployment_pipeline,
        package_path="deployment.json",
        type_check=False,
    )


if __name__ == "__main__":
    compile()
