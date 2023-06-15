from pathlib import Path

from kfp.v2.dsl import Artifact, Dataset, Input, Metrics, Model, Output, component

from src.components.dependencies import JOBLIB, PANDAS, PYTHON, SCIKIT_LEARN


@component(
    base_image=PYTHON,
    packages_to_install=[SCIKIT_LEARN, PANDAS, JOBLIB],
    output_component_file=str(Path(__file__).with_suffix(".yaml")),
)
def evaluate_model(
    test_data: Input[Dataset],
    target_column: str,
    model: Input[Model],
    predictions: Output[Dataset],
    metrics: Output[Metrics],
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
        metrics (Output[Metrics]): Output metrics for the trained model. This
            parameter will be passed automatically by the orchestrator and it
            can be referred to by clicking on the component's execution in
            the pipeline.
        metrics_artifact (Output[Artifact]): Output metrics Artifact for the trained
            model. This parameter will be passed automatically by the orchestrator.
    """
    import joblib
    import pandas as pd
    from sklearn.metrics import accuracy_score

    dtc = joblib.load(model.path)

    df_test = pd.read_csv(test_data.path)
    y = df_test.pop(target_column)

    preds = dtc.predict(df_test)
    df_test["pred"] = preds

    df_test.to_csv(predictions.path, index=False)

    metrics_df = pd.DataFrame({"accuracy": [accuracy_score(y, preds)]})
    metrics_df.to_csv(metrics_artifact.path)

    for k, v in metrics_df.T[0].to_dict().items():
        metrics.log_metric(k, v)
