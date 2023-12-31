from kfp.dsl import Artifact, Dataset, Input, Metrics, Model, Output, component

from src.components.dependencies import MATPLOTLIB, PIPELINE_IMAGE_NAME


@component(base_image=PIPELINE_IMAGE_NAME, packages_to_install=[MATPLOTLIB])
def train_evaluate_model(
    training_data: Input[Dataset],
    validation_data: Input[Dataset],
    test_data: Input[Dataset],
    target_column: str,
    model_name: str,
    train_metrics: Output[Metrics],
    valid_metrics: Output[Metrics],
    test_metrics: Output[Metrics],
    valid_pr_curve: Output[Artifact],
    test_pr_curve: Output[Artifact],
    model: Output[Model],
    models_params: dict = {},
    fit_args: dict = {},
    data_processing_args: dict = {},
    model_gcs_folder_path: str = None,
) -> None:
    """Train a classification model on the training data.

    Args:
        training_data (Input[Dataset]): Training data as a KFP Dataset object.
        validation_data (Input[Dataset]): Validation data (used to prevent overfitting)
            as a KFP Dataset object.
        test_data (Input[Dataset]): Evaluation data as a KFP Dataset object.
        target_column (str): Column containing the target column for classification.
        model_name (str): Name of the classifier that will be trained. Must be one of
            'logistic_regression', 'sgd_classifier', 'random_forest', 'lightgbm',
            'xgboost'.
        train_metrics (Output[Metrics]): Output metrics for the trained model
            on the training data. This parameter will be passed automatically
            by the orchestrator and it can be referred to by clicking on the
            component's execution in the pipeline.
        valid_metrics (Output[Metrics]): Output metrics for the trained model
            on the validation data. This parameter will be passed automatically
            by the orchestrator and it can be referred to by clicking on the
            component's execution in the pipeline.
        test_metrics (Output[Metrics]): Output metrics for the trained model
            on the test data. This parameter will be passed automatically
            by the orchestrator and it can be referred to by clicking on the
            component's execution in the pipeline.
        valid_pr_curve (Output[Artifact]): The output file for precision-recall plot
            on the validation data as a KFP Artifact object. This parameter will
            be passed automatically by the orchestrator.
        test_pr_curve (Output[Artifact]): The output file for precision-recall plot
            on the test data as a KFP Artifact object. This parameter will
            be passed automatically by the orchestrator.
        model (Output[Model]): Output model as a KFP Model object, this parameter
            will be passed automatically by the orchestrator. The .path
            attribute is the location of the joblib file in GCS.
        models_params (dict, optional): Hyperparameters of the model. Default to
            an empty dict.
        fit_args (dict, optional): Arguments used when fitting the model.
            Default to an empty dict.
        data_processing_args (dict, optional): Arguments used when running extra
            processing on the data (such as scaling or oversampling). Default
            to an empty dict.
        model_gcs_folder_path (str, optional): GCS path where to save the trained model
            and metrics artifacts. If not provided, use the default path of the
            component. Defaults to None.
    """
    from pathlib import Path

    import joblib
    import pandas as pd
    from lightgbm import LGBMClassifier
    from loguru import logger
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.linear_model import LogisticRegression, SGDClassifier
    from xgboost import XGBClassifier

    from src.base.model import evaluate_model, train_model
    from src.base.visualisation import plot_precision_recall_curve
    from src.utils.logging import setup_logger

    setup_logger()

    df_train = pd.read_parquet(training_data.path)
    df_train = df_train.drop(columns=["transaction_id"])
    y_train = df_train.pop(target_column)
    logger.info(f"Loaded training data, shape {df_train.shape}.")

    df_valid = pd.read_parquet(validation_data.path)
    df_valid = df_valid.drop(columns=["transaction_id"])
    y_valid = df_valid.pop(target_column)
    logger.info(f"Loaded evaluation data, shape {df_valid.shape}.")

    df_test = pd.read_parquet(test_data.path)
    df_test = df_test.drop(columns=["transaction_id"])
    y_test = df_test.pop(target_column)
    logger.info(f"Loaded test data, shape {df_test.shape}.")

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
    for k, v in training_metrics.items():
        if k != "precision_recall_curve":
            train_metrics.log_metric(k, v)

    validation_metrics, _, _ = evaluate_model(classifier, df_valid, y_valid)
    for k, v in validation_metrics.items():
        if k != "precision_recall_curve":
            valid_metrics.log_metric(k, v)

    testing_metrics, _, _ = evaluate_model(classifier, df_test, y_test)
    for k, v in testing_metrics.items():
        if k != "precision_recall_curve":
            test_metrics.log_metric(k, v)

    logger.info("Evaluation completed.")

    if model_gcs_folder_path is not None:
        model_gcs_folder_path = model_gcs_folder_path.replace("gs://", "/gcs/")
        model.path = model_gcs_folder_path
        valid_pr_curve.path = model_gcs_folder_path
        test_pr_curve.path = model_gcs_folder_path

    model.path = f"{model.path}/{model_name}/model.joblib"
    model_dir = Path(model.path).parent.absolute()
    model_dir.mkdir(parents=True, exist_ok=True)

    joblib.dump(classifier, model.path)
    logger.info(f"Saved model to {model.path}.")

    valid_pr_curve.path = (
        f"{valid_pr_curve.path}/precision_recall_curve_validation_{model_name}.png"
    )
    valid_pr_curve_dir = Path(valid_pr_curve.path).parent.absolute()
    valid_pr_curve_dir.mkdir(parents=True, exist_ok=True)

    _ = plot_precision_recall_curve(
        model=classifier,
        model_name=model_name,
        X=df_valid,
        y=y_valid,
        save_path=valid_pr_curve.path,
    )
    logger.info(f"Saved validation PR curve to {valid_pr_curve.path}.")

    test_pr_curve.path = (
        f"{test_pr_curve.path}/precision_recall_curve_test_{model_name}.png"
    )
    test_pr_curve_dir = Path(test_pr_curve.path).parent.absolute()
    test_pr_curve_dir.mkdir(parents=True, exist_ok=True)

    _ = plot_precision_recall_curve(
        model=classifier,
        model_name=model_name,
        X=df_test,
        y=y_test,
        save_path=test_pr_curve.path,
    )
    logger.info(f"Saved test PR curve to {test_pr_curve.path}.")
