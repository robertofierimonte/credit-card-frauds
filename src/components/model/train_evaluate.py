from pathlib import Path

from kfp.v2.dsl import Dataset, Input, Metrics, Model, Output, component

from src.components.dependencies import PIPELINE_IMAGE_NAME


@component(
    base_image=PIPELINE_IMAGE_NAME,
    output_component_file=str(Path(__file__).with_suffix(".yaml")),
)
def train_evaluate_model(
    training_data: Input[Dataset],
    validation_data: Input[Dataset],
    target_column: str,
    model_name: str,
    train_metrics: Output[Metrics],
    valid_metrics: Output[Metrics],
    model: Output[Model],
    models_params: dict = {},
    fit_args: dict = {},
    data_processing_args: dict = {},
) -> None:
    """Train a classification model on the training data.

    Args:
        training_data (Input[Dataset]): Training data as a KFP Dataset object.
        validation_data (Input[Dataset]): Validation data (used to prevent overfitting)
            as a KFP Dataset object.
        target_column (str): Column containing the target column for classification.
        model_name (str): Name of the classifier that will be trained. Must be one of
            'logistic_regression', 'sgd_classifier', 'random_forest', 'lightgbm',
            'xgboost'.
        models_params (dict): Hyperparameters of the model. Default to an empty dict.
        fit_args (dict): Arguments used when fitting the model.
            Default to an empty dict.
        data_processing_args (dict): Arguments used when running extra processing on
            the data (such as scaling or oversampling). Default to an empty dict.
        train_metrics (Output[Metrics]):
        valid_metrics (Output[Metrics]):
        model (Output[Model]): Output model as a KFP Model object, this parameter
            will be passed automatically by the orchestrator. The .path
            attribute is the location of the joblib file in GCS.
    """
    import os
    from pathlib import Path

    import pandas as pd
    from lightgbm import LGBMClassifier
    from loguru import logger
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.linear_model import LogisticRegression, SGDClassifier
    from xgboost import XGBClassifier

    from src.base.model import evaluate_model, train_model

    df_train = pd.read_parquet(training_data.path)
    df_train = df_train.drop(columns=["transaction_id"])
    y_train = df_train.pop(target_column)
    logger.info(f"Loaded training data, shape {df_train.shape}.")

    df_valid = pd.read_parquet(validation_data.path)
    df_valid = df_valid.drop(columns=["transaction_id"])
    y_valid = df_valid.pop(target_column)
    logger.info(f"Loaded evaluation data, shape {df_valid.shape}.")

    use_eval_set = False
    model_params = models_params.get(model_name, {})
    if model_name == "logistic_regression":
        classifier = LogisticRegression(random_state=42, **model_params)
    elif model_name == "sgd_classifier":
        classifier = SGDClassifier(random_state=42, **model_params)
    elif model_name == "random_forest":
        classifier = RandomForestClassifier(random_state=42, **model_params)
    elif model_name == "lightgbm":
        classifier = LGBMClassifier(random_state=42, **model_params)
        use_eval_set = True
    elif model_name == "xgboost":
        classifier = XGBClassifier(
            use_label_encoder=False, random_state=42, **model_params
        )
        use_eval_set = True
    else:
        msg = (
            "`model_name` must be one of 'logistic_regression', 'sgd_classifier', "
            "'random_forest', 'lightgbm', 'xgboost'."
        )
        logger.error(msg)
        raise ValueError(msg)

    logger.info(f"Training model {model_name}.")
    classifier, training_metrics = train_model(
        classifier,
        X_train=df_train,
        y_train=y_train,
        X_valid=df_valid,
        y_valid=y_valid,
        use_eval_set=use_eval_set,
        fit_args=fit_args.get(model_name, {}),
        **data_processing_args,
    )
    logger.info("Training completed.")
    logger.debug(f"Type of classifier: {type(classifier)}.")
    logger.debug(f"Classifier: {classifier}.")
    for k, v in training_metrics.items():
        if k != "Precision Recall Curve":
            train_metrics.log_metric(k, v)

    validation_metrics, _, _ = evaluate_model(classifier, df_valid, y_valid)
    logger.info("Evaluation completed.")
    for k, v in validation_metrics.items():
        if k != "Precision Recall Curve":
            valid_metrics.log_metric(k, v)

    logger.debug(f"Type of classifier: {type(classifier)}.")
    logger.debug(f"Classifier: {classifier}.")
    model.path = model.path + f"/{model_name}"
    model_dir = Path(model.path).parent.absolute()
    os.makedirs(model_dir, exist_ok=True)

    # logger.info(f"Saving model to {model.path}.")
    # joblib.dump(classifier, model.path)
