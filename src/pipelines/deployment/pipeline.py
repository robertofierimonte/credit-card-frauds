import os
import warnings
from pathlib import Path

from google_cloud_pipeline_components.v1.vertex_notification_email import (
    VertexNotificationEmailOp,
)
from kfp import compiler, dsl
from kfp.dsl.types.type_utils import InconsistentTypeWarning

from src.base.utilities import read_json
from src.components.aiplatform import deploy_model, export_model, upload_model
from src.components.dependencies import PIPELINE_IMAGE_NAME

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=InconsistentTypeWarning, append=True)

PIPELINE_TAG = os.environ.get("PIPELINE_TAG")
PIPELINE_NAME = f"frauds-deployment-pipeline-{PIPELINE_TAG}"


@dsl.pipeline(name=PIPELINE_NAME, description="Credit card frauds deployment Pipeline")
def deployment_pipeline(
    project_id: str,
    project_location: str,
    dataset_id: str,
    email_notification_recipients: list,
):
    """Credit card frauds classification deployment pipeline.

    Args:
        project_id (str): GCP project ID where the pipeline will run.
        project_location (str): GCP location whe the pipeline will run.
        dataset_id (str): Bigquery dataset used to store all the staging datasets.
        email_notification_recipients (list): List of email addresses that will be
            notified upon completion (whether successful or not) of the pipeline.
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

        replace = (
            upload_model(
                model=export.outputs["model"],
                model_id="credit-card-frauds-champion",
                display_name="credit-card-frauds-champion",
                serving_container_image_uri=PIPELINE_IMAGE_NAME,
                serving_container_params=serving_container_params,
                project_id=project_id,
                project_location=project_location,
                labels=export.outputs["labels"],
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
            .after(replace)
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
