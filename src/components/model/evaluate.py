from kfp.dsl import Artifact, Dataset, Input, Metrics, Model, Output, component

from src.components.dependencies import PIPELINE_IMAGE_NAME


@component(base_image=PIPELINE_IMAGE_NAME)
def evaluate_model(
    test_data: Input[Dataset],
    target_column: str,
    model: Input[Model],
    predictions: Output[Dataset],
    test_metrics: Output[Metrics],
    metrics_artifact: Output[Artifact],
) -> None:
    """Evaluate a trained model on test data and report goodness metrics.

    Args:
        test_data (Input[Dataset]): Evaluation data as a KFP Dataset object.
        target_column (str): Column containing the target column for classification.
        model (Input[Model]): Input trained model as a KFP Model object.
        predictions (Output[Dataset]): Model predictions including input columns
            as a KFP Dataset object. This parameter will be passed automatically
            by the orchestrator.
        test_metrics (Output[Metrics]): Output metrics for the trained model. This
            parameter will be passed automatically by the orchestrator and it
            can be referred to by clicking on the component's execution in
            the pipeline.
        metrics_artifact (Output[Artifact]): Output metrics Artifact for the trained
            model. This parameter will be passed automatically by the orchestrator.
    """
    import joblib
    import pandas as pd
    from loguru import logger

    from src.base.model import evaluate_model
    from src.utils.logging import setup_logger

    setup_logger()

    classifier = joblib.load(model.path)

    df_test = pd.read_parquet(test_data.path)
    df_test = df_test.drop(columns=["transaction_id"])
    y_test = df_test.pop(target_column)
    logger.info(f"Loaded test data, shape {df_test.shape}.")

    testing_metrics, _, _ = evaluate_model(classifier, df_test, y_test)
    logger.info("Evaluation completed.")
    for k, v in testing_metrics.items():
        if k != "precision_recall_curve":
            test_metrics.log_metric(k, v)
