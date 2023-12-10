from kfp.dsl import Dataset, Input, Model, Output, component

from src.components.dependencies import PIPELINE_IMAGE_NAME


@component(base_image=PIPELINE_IMAGE_NAME)
def predict_model(
    input_data: Input[Dataset],
    model: Input[Model],
    predictions: Output[Dataset],
) -> None:
    """Use a trained model to make batch prediction on new data.

    Args:
        input_data (Input[Dataset]): Testing data as a KFP Dataset object.
        model (Input[Model]): Input trained model as a KFP Model object.
        predictions (Output[Dataset]): Model predictions including input columns
            as a KFP Dataset object. This parameter will be passed automatically
            by the orchestrator.
    """
    import joblib
    import pandas as pd

    from src.utils.logging import setup_logger

    setup_logger()

    dtc = joblib.load(model.path)

    df_test = pd.read_csv(input_data.path)

    preds = dtc.predict(df_test)
    df_test["pred"] = preds
    df_test.to_csv(predictions.path, index=False)
