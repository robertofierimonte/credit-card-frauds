from pathlib import Path

from kfp.v2.dsl import Dataset, Input, Model, Output, component

from src.components.dependencies import PIPELINE_IMAGE_NAME


@component(
    base_image=PIPELINE_IMAGE_NAME,
    output_component_file=str(Path(__file__).with_suffix(".yaml")),
)
def train_model(
    training_data: Input[Dataset],
    target_column: str,
    model_params: dict,
    model_file_name: str,
    model: Output[Model],
) -> None:
    """Train a DecisionTreeClassifier model on the training data.

    Args:
        training_data (Input[Dataset]): Training data as a KFP Dataset object.
        target_column (str): Column containing the target column for classification.
        model_params (dict): Parameters of the model.
        model_file_name (str): Name of the files where to save the serialised model.
        model (Output[Model]): Output model as a KFP Model object, this parameter
            will be passed automatically by the orchestrator. The .path
            attribute is the location of the joblib file in GCS.
    """
    import os
    from pathlib import Path

    import joblib
    import pandas as pd
    from loguru import logger

    from src.base.model import train_model

    df_train = pd.read_parquet(training_data.path)
    df_train = df_train.drop("transaction_id")
    y = df_train.pop(target_column)

    classifier = train_model()

    model.path = model.path + f"/{model_file_name}"
    model_dir = Path(model.path).parent.absolute()
    os.makedirs(model_dir, exist_ok=True)

    logger.info(f"Saving model to {model.path}.")
    joblib.dump(classifier, model.path)
