from copy import deepcopy
from typing import Optional, Tuple

import numpy as np
from imblearn.over_sampling import RandomOverSampler
from loguru import logger
from sklearn.base import BaseEstimator
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


def initialize_metrics():
    """Create empty lists for collecting model performance data.

    Returns:
        dict: Dictionary of empty lists for later collecting model performance data.
            Metrics to be collected: precision, recall, f1, f2, f0.5, average precision,
            precision_top_k, roc_auc, precision-recall-curve
    """
    all_performance_metrics = {
        "precision": [],
        "recall": [],
        "f1": [],
        "f2": [],
        "f0.5": [],
        "average_precision": [],
        "precision_top_k": [],
        "roc_auc": [],
        "precision_recall_curve": [],
    }

    return all_performance_metrics


def get_standardized_train_and_test_sets(
    X: np.ndarray,
    y: np.ndarray,
    train_index: np.ndarray,
    test_index: np.ndarray,
    scaler: Optional[StandardScaler] = None,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Get train and test sets.

    Also, perform data scaling if required. The scaler used for the training set is also
    applied to the test set, thus returning a standardized test set. However,
    e.g. with StandardScaler, the test set may not have exactly zero mean and unit
    variance as it is scaled only based on the scaler learned from the training data
    (the distribution of the test set should not be known at training time).

    Args:
        X (numpy.array): training data
        y (numpy.array): target data for modelling
        train_index (numpy.array): indexes from X,y to use for training
        test_index (numpy.array): indexes from X,y to use for testing
        scaler (StandardScaler, optional): for performing data standardization

    Returns:
        numpy.array: Resampled data (features) for training.
        numpy.array: Standardized data (features) for testing.
        numpy.array: Resampled data for training.
        numpy.array: Target data for testing.
    """
    logger.info("Using no upsampling strategy.")
    # Get train and test sets
    X_train, X_test = X[train_index], X[test_index]
    y_train, y_test = y[train_index], y[test_index]

    if scaler:
        # Standardize X_train and X_test. However, use only X_train for fitting
        X_train = scaler.fit_transform(X_train)
        X_test = scaler.transform(X_test)

    return X_train, X_test, y_train, y_test


def get_resampled_train_and_test_sets(
    X: np.ndarray,
    y: np.ndarray,
    train_index: np.ndarray,
    test_index: np.ndarray,
    upsampling_coefficient: int,
    scaler: Optional[StandardScaler] = None,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Get train and test sets.

    This method adds duplicate copies of the minority class such that the number of
    duplicates is given by the number of original samples times upsampling_coefficient.
    Also, perform data scaling if required.

    Args:
        X (numpy.array): training data
        y (numpy.array): target data for modelling
        train_index (numpy.array): indexes from X,y to use for training
        test_index (numpy.array): indexes from X,y to use for testing
        scaler (StandardScaler, optional): for performing data standardization
        upsampling_coefficient (int): Number of times to duplicate the minority
            class data points.

    Returns:
        numpy.array: Resampled data (features) for training.
        numpy.array: Standardized data (features) for testing.
        numpy.array: Resampled data for training.
        numpy.array: Target data for testing.
    """
    logger.info(
        "Using resampling upsampling strategy "
        f"with coefficient {upsampling_coefficient}."
    )
    # Get train and test sets
    X_train, X_test = X[train_index], X[test_index]
    y_train, y_test = y[train_index], y[test_index]

    if scaler:
        # Standardize X_train and X_test. However, use only X_train for fitting
        X_train = scaler.fit_transform(X_train)
        X_test = scaler.transform(X_test)

    # Upsample minority class in the training set
    new_values = np.tile(X_train[y_train == 1, :].copy(), (upsampling_coefficient, 1))
    X_train = np.append(X_train, new_values, axis=0)
    y_train = np.append(y_train, np.ones(upsampling_coefficient * np.sum(y_train == 1)))

    return X_train, X_test, y_train, y_test


def get_rose_train_and_test_sets(
    X: np.ndarray,
    y: np.ndarray,
    train_index: np.ndarray,
    test_index: np.ndarray,
    rose_upsampled_minority_proportion: float,
    rose_shrinkage: float,
    rose_random_state: int = 42,
    scaler: Optional[StandardScaler] = None,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Get train and test sets.

    This method also upsamples the minority class by creating new instances by
    randomly perturbing original minority class instances.

    Args:
        X (numpy.array): training data
        y (numpy.array): target data for modelling
        train_index (numpy.array): indexes from X,y to use for training
        test_index (numpy.array): indexes from X,y to use for testing
        scaler (StandardScaler, optional): for performing data standardization
        rose_upsampled_minority_proportion (float): The desired proportion of the
            minority class after upsampling.
        rose_random_state (int): For controlling the randomization of the algorithm.
        rose_shrinkage (float): Must be non-negative. Parameter controlling the
            shrinkage applied to the covariance matrix, when a smoothed bootstrap is
            generated. Value 0 corresponds to adding duplicates of original data.
            Larger positive values correspond to increasingly bigger
            random noise in the generated samples.

    Returns:
        numpy.array: Resampled data (features) for training.
        numpy.array: Standardized data (features) for testing.
        numpy.array: Resampled data for training.
        numpy.array: Target data for testing.
    """
    logger.info(
        f"Using rose upsampling strategy with shrinkage {rose_shrinkage} "
        f"and minority proportion {rose_upsampled_minority_proportion}."
    )
    # Get train and test sets
    X_train, X_test = X[train_index], X[test_index]
    y_train, y_test = y[train_index], y[test_index]

    if scaler:
        # Standardize X_train and X_test. However, use only X_train for fitting
        X_train = scaler.fit_transform(X_train)
        X_test = scaler.transform(X_test)

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

    return X_train, X_test, y_train, y_test


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


def evaluate_train_and_test_error(
    trained_classifier: BaseEstimator,
    X_train: np.ndarray,
    X_test: np.ndarray,
    y_train: np.ndarray,
    y_test: np.ndarray,
) -> Tuple[dict, dict, np.ndarray, np.ndarray]:
    """Evaluates training and test error for a given classifier and dataset.

    Args:
        trained-classifier: Any trained classification model with sklearn type
            "predict" and "predict_proba" methods.
        X_train (numpy.array): Standardized data (features) for training.
        X_test (numpy.array): Standardized data (features) for testing.
        y_train (numpy.array): Target data for training.
        y_test (numpy.array): Target data for testing.

    Returns:
        dict: Dictionary containing all evaluation results on the test set.
            The metrics are as follows:
            precision_test (float): Precision score on the test set.
            recall_test (float): Recall score on the test set.
            f1_test (float): F1-score on the test set.
            f2_test (float): F2-score on the test set.
            f05_test (float): F0.5-score on the test set.
            average_precision_test (float): Average precision score on the test set.
            precision (numpy.array): Precision score for different decision thresholds
                on the test set.
            recall (numpy.array): Recall score for different decision thresholds
                on the test set.
            thresholds (numpy.array): All the decision thresholds corresponding
                to "precision" and "recall".
        dict: Dictionary containing the same metrics evaluated on the training set.
        numpy.array: Model classification for each entry in the test set
        numpy.array: Model prediction for the probability of class 1 (fraud)
            for each entry in the test set
    """
    # Evaluate training error
    train_predictions = trained_classifier.predict(X_train)
    train_prediction_probabilities = trained_classifier.predict_proba(X_train)[:, 1]
    precision_train = precision_score(y_train, train_predictions)
    recall_train = recall_score(y_train, train_predictions)
    f1_train = f1_score(y_train, train_predictions)
    f2_train = fbeta_score(y_train, train_predictions, beta=2)
    f05_train = fbeta_score(y_train, train_predictions, beta=0.5)
    average_precision_train = average_precision_score(
        y_train, train_prediction_probabilities
    )
    precision_top_k_train = calculate_precision_top_k(
        y_train, train_prediction_probabilities
    )
    auc_roc_train = roc_auc_score(y_train, train_prediction_probabilities)
    precision_tr, recall_tr, thresholds_tr = precision_recall_curve(
        y_train, train_prediction_probabilities
    )

    logger.info(f"Training precision: {precision_train}.")
    logger.info(f"Training recall: {recall_train}.")
    logger.info(f"Training f1: {f1_train}.")
    logger.info(f"Training f2: {f2_train}.")
    logger.info(f"Training f0.5: {f05_train}.")
    logger.info(f"Training average_precision: {average_precision_train}.")
    logger.info(f"Training P@k: {precision_top_k_train}.")
    logger.info(f"Training AUC ROC: {auc_roc_train}.")

    # Evaluate test error
    test_predictions = trained_classifier.predict(X_test)
    test_prediction_probabilities = trained_classifier.predict_proba(X_test)[:, 1]
    precision_test = precision_score(y_test, test_predictions)
    recall_test = recall_score(y_test, test_predictions)
    f1_test = f1_score(y_test, test_predictions)
    f2_test = fbeta_score(y_test, test_predictions, beta=2)
    f05_test = fbeta_score(y_test, test_predictions, beta=0.5)
    average_precision_test = average_precision_score(
        y_test, test_prediction_probabilities
    )
    precision_top_k_test = calculate_precision_top_k(
        y_test, test_prediction_probabilities
    )
    auc_roc_test = roc_auc_score(y_test, test_prediction_probabilities)
    precision, recall, thresholds = precision_recall_curve(
        y_test, test_prediction_probabilities
    )

    logger.info(f"Test precision: {precision_test}.")
    logger.info(f"Test recall: {recall_test}.")
    logger.info(f"Test f1: {f1_test}.")
    logger.info(f"Test f2: {f2_test}.")
    logger.info(f"Test f0.5: {f05_test}.")
    logger.info(f"Test average_precision: {average_precision_test}.")
    logger.info(f"Test P@k: {precision_top_k_test}.")
    logger.info(f"Test AUC ROC: {auc_roc_test}.")

    logger.info(
        f"Number of correctly predicted frauds: {np.sum(y_test[test_predictions==1])}."
    )
    logger.info(f"Number of predicted frauds: {np.sum(test_predictions)}.")
    logger.info(f"Total number of frauds: {np.sum(y_test)}.")

    # Collect all test metrics in one list
    test_metrics = {
        "precision": precision_test,
        "recall": recall_test,
        "f1": f1_test,
        "f2": f2_test,
        "f0.5": f05_test,
        "average_precision": average_precision_test,
        "precision_top_k": precision_top_k_test,
        "roc_auc": auc_roc_test,
        "precision_recall_curve": [precision, recall, thresholds],
    }

    train_metrics = {
        "precision": precision_train,
        "recall": recall_train,
        "f1": f1_train,
        "f2": f2_train,
        "f0.5": f05_train,
        "average_precision": average_precision_train,
        "precision_top_k": precision_top_k_train,
        "roc_auc": auc_roc_train,
        "precision_recall_curve": [precision_tr, recall_tr, thresholds_tr],
    }

    return test_metrics, train_metrics, test_predictions, test_prediction_probabilities


def append_metrics(test_metrics: dict, all_performance_metrics: dict) -> dict:
    """Add performance metrics to corresponding lists.

    Args:
        test_metrics (dict): Dictionary containing all evaluation results
            on the test set.
        all_performance_metrics (dict): Dictionary of lists for collecting
            performance measures.

    Returns:
        dict: An updated dictionary of lists for collecting performance measures.
    """
    for key in all_performance_metrics.keys():
        all_performance_metrics[key].append(test_metrics[key])

    return all_performance_metrics


def cross_validation_mean(all_performance_metrics: dict) -> dict:
    """Calculate mean value for the performance metrics across cross-validation folds.

    Args:
        all_performance_metrics (dict): Dictionary containing all evaluation results
            on the test set.

    Returns:
        dict: Contains all cross-validation results:
            mean_test_rmse (float): Mean of root-mean-squared errors
                on all CV test sets.
            mean_balanced_accuracy (float): Mean of balanced accuracy scores
                on all CV test sets.
            mean_precision (float): Mean of precision scores on all CV test sets.
            mean_recall (float): Mean of recall scores on all CV test sets.
            mean_f1 (float): Mean of F1-scores on all CV test sets.
            mean_average_precision (float): Mean of average precision scores
                on all CV test sets.
    """
    # Print and save results
    cross_validation_results = {}
    for key in all_performance_metrics.keys():
        if key != "precision_recall_curve":
            cv_mean = np.mean(all_performance_metrics[key])
            logger.info(f"Mean test {key}: {cv_mean}.")
            cross_validation_results[key] = cv_mean

    return cross_validation_results


def train_model(
    classifier: BaseEstimator,
    model_name: str,
    all_splits: list,
    X: np.ndarray,
    y: np.ndarray,
    models_folder: str,
    data_standardization: str = "standard",
    data_sampling: str = "none",
    rose_upsampled_minority_proportion: float = 0.01,
    rose_random_state: int = 42,
    rose_shrinkage: int = 1,
    upsampling_coefficient: int = 2,
    use_eval_set: bool = True,
    fit_params: dict = {},
    run_tags: dict = {},
) -> Tuple[list, dict, dict]:
    """Train any sklearn-type classifier and calculate cross-validation metrics.

    Args:
        classifier: any sklearn-type classifier with fit, predict and
            predict_proba methods
        all_splits (list): Contains train and test set indices for splitting the data.
        X (numpy.array): training data
        y (numpy.array): target data for modelling
        models_folder (str): folder where to save the models
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
        fit_params (dict): Dictionary of optional parameters for model fitting.
        run_tags (dict): Dictionary of tags to apply to the trained model.

    Returns:
        list: All trained classifier models
        dict: A dictionary for collecting performance measures.
        dict: Dictionary containing all cross-validation results
    """
    logger.debug(f"Classifier: {classifier}.")

    # Initialise performance metrics
    all_performance_metrics = initialize_metrics()

    # Collect all trained classifiers to a list
    all_classifiers = []

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

    # TRAINING AND EVALUATION
    for count, (train_index, test_index) in enumerate(all_splits):

        logger.info(f"CV FOLD {count+1}:")
        if data_sampling == "none":
            X_train, X_test, y_train, y_test = get_standardized_train_and_test_sets(
                X=X, y=y, train_index=train_index, test_index=test_index, scaler=scaler
            )
        elif data_sampling == "rose":
            X_train, X_test, y_train, y_test = get_rose_train_and_test_sets(
                X=X,
                y=y,
                train_index=train_index,
                test_index=test_index,
                scaler=scaler,
                rose_upsampled_minority_proportion=rose_upsampled_minority_proportion,
                rose_random_state=rose_random_state,
                rose_shrinkage=rose_shrinkage,
            )
        elif data_sampling == "upsampling_with_duplicates":
            X_train, X_test, y_train, y_test = get_resampled_train_and_test_sets(
                x=X,
                y=y,
                train_index=train_index,
                test_index=test_index,
                scaler=scaler,
                upsampling_coefficient=upsampling_coefficient,
            )
        else:
            msg = (
                "`data_sampling` parameter not correctly set! "
                "It should have one of the following values: 'none', 'rose', "
                "'upsampling_with_duplicates'."
            )
            logger.error(msg)
            raise Exception(msg)

        # Fit model
        if use_eval_set:
            classifier.fit(X_train, y_train, eval_set=[(X_test, y_test)], **fit_params)
        else:
            classifier.fit(X_train, y_train, **fit_params)

        # Append trained classifier to the list containing all the classifiers
        all_classifiers.append(deepcopy(classifier))

        # Evaluate model performance on training and test sets
        (
            test_metrics,
            train_metrics,
            test_predictions,
            test_prediction_probabilities,
        ) = evaluate_train_and_test_error(classifier, X_train, X_test, y_train, y_test)

        # Append all test evaluations to all_performance_metrics
        all_performance_metrics = append_metrics(test_metrics, all_performance_metrics)

    # Return the classifier trained on the longer split
    final_classifier = all_classifiers[-1]
    if scaler is not None:
        final_classifier = Pipeline(
            steps=[("scaler", scaler), ("classifier", final_classifier)]
        )

    # Register and save model
    model_metadata = {
        "training_params": {
            "data_standardization": data_standardization,
            "data_sampling": data_sampling,
            "rose_upsampled_minority_proportion": rose_upsampled_minority_proportion,
            "rose_random_state": rose_random_state,
            "rose_shrinkage": rose_shrinkage,
            "upsampling_coefficient": upsampling_coefficient,
        }
    }
    try:
        check_is_fitted(final_classifier)
        if scaler is not None:
            check_is_fitted(scaler)
    except NotFittedError as err:
        msg = "The scaler and/or the final classifier are not fit!"
        logger.error(msg)
        raise err

    # Calculate average result across all CV folds
    cross_validation_results = cross_validation_mean(all_performance_metrics)

    return all_classifiers, all_performance_metrics, cross_validation_results
