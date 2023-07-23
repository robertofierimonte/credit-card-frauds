from typing import Optional, Tuple

import numpy as np
from imblearn.over_sampling import RandomOverSampler
from loguru import logger
from sklearn.base import ClassifierMixin
from sklearn.exceptions import NotFittedError
from sklearn.metrics import (
    average_precision_score,
    f1_score,
    fbeta_score,
    precision_recall_curve,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import MinMaxScaler, StandardScaler
from sklearn.utils.validation import check_is_fitted


def calculate_precision_top_k(
    y: np.ndarray, prediction_probabilities: np.ndarray, k: int = 200
) -> float:
    """Calculate the Precision-top-k (P@k) metric.

    Args:
        y (numpy.array): True classification labels
        prediction_probabilities (numpy.array): Predicted fraud probabilities
            for each transaction in y
        k (int): Optional, default value is 200. Number of samples for measuring
            precision, where samples are the k highest rated transactions for
            fraud by the model

    Returns:
        float: Score for the P@k metric
    """
    # Indexes for the top k highest predicted fraudulent transactions
    ix_top_k = np.argsort(prediction_probabilities)[::-1][0:k]
    # True labels for the highest predicted fraudulent transactions
    true_labels_top_k = y[ix_top_k]
    # Precision-top-k measure
    precision_top_k = np.sum(true_labels_top_k) / k

    return precision_top_k


def evaluate_model(
    trained_classifier: ClassifierMixin,
    X: np.ndarray,
    y: np.ndarray,
) -> Tuple[dict, np.ndarray, np.ndarray]:
    """Evaluates metrics for a given classifier and dataset.

    Args:
        trained-classifier: Any trained classification model with sklearn type
            "predict" and "predict_proba" methods.
        X (numpy.array): Input features to the model.
        y (numpy.array): Target data.

    Returns:
        dict: Dictionary containing all evaluation results on the data.
            The metrics are as follows:
            precision (float): Precision score on the data.
            recall (float): Recall score on the data.
            f1 (float): F1-score on the data.
            f2 (float): F2-score on the data.
            f05 (float): F0.5-score on the data.
            average_precision (float): Average precision score on the data.
            precision_top_k (float): Precision evaluated on the 200 samples with the
                highest predicted probability.
            precision_recall_curve: tuple with 3 elements:
                numpy.array: Precision score for different decision thresholds
                    on the data.
                numpy.array: Recall score for different decision thresholds
                    on the data.
                numpy.array: All the decision thresholds corresponding
                    to "precision" and "recall".
        numpy.array: Model classification for each entry in the data
        numpy.array: Model prediction for the probability of class 1 (fraud)
            for each entry in the data
    """
    # Evaluate test error
    predictions = trained_classifier.predict(X)
    prediction_probabilities = trained_classifier.predict_proba(X)[:, 1]
    precision = precision_score(y, predictions)
    recall = recall_score(y, predictions)
    f1 = f1_score(y, predictions)
    f2 = fbeta_score(y, predictions, beta=2)
    f05 = fbeta_score(y, predictions, beta=0.5)
    average_precision = average_precision_score(y, prediction_probabilities)
    precision_top_k = calculate_precision_top_k(y, prediction_probabilities)
    auc_roc = roc_auc_score(y, prediction_probabilities)
    precisions, recalls, thresholds = precision_recall_curve(
        y, prediction_probabilities
    )

    logger.info(f"Precision: {precision}.")
    logger.info(f"Recall: {recall}.")
    logger.info(f"F1 Score: {f1}.")
    logger.info(f"F2 Score: {f2}.")
    logger.info(f"F0.5 Score: {f05}.")
    logger.info(f"Average Precision: {average_precision}.")
    logger.info(f"Precision Top k: {precision_top_k}.")
    logger.info(f"AUC ROC: {auc_roc}.")
    logger.info(f"Number of correctly predicted frauds: {np.sum(y[predictions==1])}.")
    logger.info(f"Number of predicted frauds: {np.sum(predictions)}.")
    logger.info(f"Total number of frauds: {np.sum(y)}.")

    # Collect all test metrics in one list
    metrics = {
        "Precision": precision,
        "Recall": recall,
        "F1 Score": f1,
        "F2 Score": f2,
        "F0.5 Score": f05,
        "Average Precision": average_precision,
        "Precision Top 200": precision_top_k,
        "ROC AUC": auc_roc,
        "Precision Recall Curve": [precisions, recalls, thresholds],
    }

    return metrics, predictions, prediction_probabilities


def train_model(
    classifier: ClassifierMixin,
    X_train: np.ndarray,
    y_train: np.ndarray,
    X_valid: Optional[np.ndarray] = None,
    y_valid: Optional[np.ndarray] = None,
    data_standardization: str = "standard",
    data_sampling: str = "none",
    rose_upsampled_minority_proportion: float = 0.01,
    rose_random_state: int = 42,
    rose_shrinkage: int = 1,
    upsampling_coefficient: int = 2,
    use_eval_set: bool = True,
    fit_args: dict = {},
) -> Tuple[ClassifierMixin, dict]:
    """Train any sklearn-type classifier and calculate cross-validation metrics.

    Args:
        classifier: any sklearn-type classifier with fit, predict and
            predict_proba methods.
        X_train (numpy.array): training data features.
        y_train (numpy.array): training data target variable.
        X_valid (numpy.array, optional): validation data features. Defaults to None.
        y_valid (numpy.array, optional): validation data target variable.
            Defaults to None.
        data_standardization (str): Optional, default "standard". Used to select the
            mode for data scaling. Options: "standard", "min_max", "none".
        data_sampling (str): Optional, default "none". Used to select data sampling
            strategy, e.g., upsampling of minority class. Options: "none", "rose",
            "upsampling_with_duplicates".
        rose_upsampled_minority_proportion (float): Optional, default 0.01. The
            desired proportion of the minority class after upsampling. Only used if
            data_sampling="rose".
        rose_random_state (int): Optional, default 42. For controlling the
            randomization of the ROSE algorithm. Only used if data_sampling="rose".
        rose_shrinkage (float): Optional, default 1. Must be non-negative. For ROSE,
            this parameter controls the shrinkage applied to the covariance matrix,
            when a smoothed bootstrap is generated. Value 0 corresponds to adding
            duplicates of original data. Larger positive values correspond to
            increasingly bigger random noise in the generated samples. Only used if
            data_sampling="rose".
        upsampling_coefficient (int): Optional, default 2. Number of times to
            duplicate the minority class data points. Only used if
            data_sampling="upsampling_with_duplicates".
        use_eval_set (boolean): Optional, default True. If True, model fitting
            is done using an evaluation set. Also, the best model is chosen.
        fit_args (dict): Dictionary of optional arguments for model fitting.

    Returns:
        ClassifierMixin: Trained classifier model
        dict: Model performance metrics on the training data
    """
    # Scale data to mean zero and unit variance
    if data_standardization == "standard":
        scaler = StandardScaler()
    # Scale data to [0,1] interval
    elif data_standardization == "min_max":
        scaler = MinMaxScaler()
    # No scaling
    elif data_standardization == "none":
        scaler = None
    else:
        msg = (
            "`data_standardization` parameter not correctly set! "
            "It should have one of the following values: 'standard', 'min_max', "
            "'none'."
        )
        logger.error(msg)
        raise ValueError(msg)

    if scaler is not None:
        # Standardize X_train and X_valid. However, use only X_train for fitting
        X_train = scaler.fit_transform(X_train)
        X_valid_sc = scaler.transform(X_valid)

    if data_sampling == "none":
        logger.info("Using no upsampling strategy.")

    elif data_sampling == "rose":
        logger.info(
            f"Using rose upsampling strategy with shrinkage {rose_shrinkage} "
            f"and minority proportion {rose_upsampled_minority_proportion}."
        )
        # Upsample minority class in the training set

        # Sampling strategy is the relative size of the minority class to the majority
        # class, rose_upsampled_minority_proportion gives the desired final proportion
        # of the minority class in the whole data set
        my_sampling_strategy = rose_upsampled_minority_proportion / (
            1 - rose_upsampled_minority_proportion
        )
        ros = RandomOverSampler(
            sampling_strategy=my_sampling_strategy,
            random_state=rose_random_state,
            shrinkage=rose_shrinkage,
        )
        X_train, y_train = ros.fit_resample(X_train, y_train)
    elif data_sampling == "upsampling_with_duplicates":
        logger.info(
            "Using resampling upsampling strategy "
            f"with coefficient {upsampling_coefficient}."
        )
        # Upsample minority class in the training set
        new_values = np.tile(
            X_train[y_train == 1, :].copy(), (upsampling_coefficient, 1)
        )
        X_train = np.append(X_train, new_values, axis=0)
        y_train = np.append(
            y_train, np.ones(upsampling_coefficient * np.sum(y_train == 1))
        )
    else:
        msg = (
            "`data_sampling` parameter not correctly set! "
            "It should have one of the following values: 'none', 'rose', "
            "'upsampling_with_duplicates'."
        )
        logger.error(msg)
        raise ValueError(msg)

    # Fit model
    if use_eval_set is True and X_valid is not None and y_valid is not None:
        logger.debug("Using validation set to guide training.")
        classifier.fit(X_train, y_train, eval_set=[(X_valid_sc, y_valid)], **fit_args)
    else:
        logger.debug("Not using validation set.")
        classifier.fit(X_train, y_train, **fit_args)

    try:
        check_is_fitted(classifier)
        if scaler is not None:
            check_is_fitted(scaler)
    except NotFittedError as err:
        msg = "The scaler and/or the classifier are not fit!"
        logger.error(msg)
        raise err

    # Evaluate model performance on training set
    (
        train_metrics,
        _,
        _,
    ) = evaluate_model(classifier, X_train, y_train)

    if scaler is not None:
        classifier = Pipeline(steps=[("scaler", scaler), ("classifier", classifier)])
        logger.info("Saved classifier as pipeline.")

    return classifier, train_metrics
