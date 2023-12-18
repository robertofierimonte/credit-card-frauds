import os
import warnings
from pathlib import Path

from google_cloud_pipeline_components.v1.vertex_notification_email import (
    VertexNotificationEmailOp,
)
from kfp import compiler, dsl
from kfp.dsl.types.type_utils import InconsistentTypeWarning

from src.base.utilities import read_yaml
from src.components.aiplatform import deploy_model, export_model, update_version_alias

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=InconsistentTypeWarning, append=True)

if os.environ.get("CURRENT_TAG") != "no-tag":
    MODEL_TAG = os.environ.get("CURRENT_TAG")
else:
    MODEL_TAG = os.environ.get("CURRENT_BRANCH")
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
    monitoring_config = read_yaml(config_folder / "endpoint_monitoring.yaml")

    notify_email_task = VertexNotificationEmailOp(
        recipients=email_notification_recipients
    )
    with dsl.ExitHandler(notify_email_task, name="Notify pipeline result"):

        export = (
            export_model(
                model_id="credit-card-frauds",
                model_version="challenger",
                project_id=project_id,
                project_location=project_location,
                model_file_name="model.joblib",
            )
            .set_display_name("Export challenger")
            .set_caching_options(True)
        )

        deploy = (
            deploy_model(
                model_id="credit-card-frauds",
                model_version="challenger",
                endpoint_id="7894561234",
                endpoint_display_name="credit-card-frauds-endpoint",
                project_id=project_id,
                dataset_id=dataset_id,
                project_location=project_location,
                monitoring=True,
                monitoring_config=monitoring_config,
                monitoring_email_recipients=email_notification_recipients,
            )
            .after(export)
            .set_display_name("Deploy challenger to endpoint")
            .set_caching_options(True)
        )

        replace = (
            update_version_alias(
                model_id="credit-card-frauds",
                model_version="challenger",
                version_aliases=["-challenger", "champion", MODEL_TAG],
                project_id=project_id,
                project_location=project_location,
            )
            .after(deploy)
            .set_display_name("Update challenger alias to champion")
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
