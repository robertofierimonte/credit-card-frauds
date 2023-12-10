from typing import List

from kfp.dsl import Dataset, Input, Model, Output, component

from src.components.dependencies import PIPELINE_IMAGE_NAME


@component(base_image=PIPELINE_IMAGE_NAME)
def compare_models(
    test_data: Input[Dataset],
    target_column: str,
    models: Input[List[Model]],
    best_model: Output[Model],
    metric_to_optimise: str,
    higher_is_better: bool = True,
) -> list:
    """Compare a collection of candidate models and return the best model.

    Args:
        test_data (Input[Dataset]): Evaluation data as a KFP Dataset object.
        target_column (str): Column containing the target column for classification.
        models (Input[List[Model]]): Collection of candidate models as a list of
            KFP Model objects.
        best_model(Output[Model]): Best model as a KFP Model object, this parameter
            will be passed automatically by the orchestrator. The .path
            attribute is the location of the joblib file in GCS.
        metric_to_optimise (str): Metric to use to determine which model is better.
        higher_is_better (bool, optional): Whether higher values of
            `metric_to_optimise` mean that a model is better. Defaults to True.

    Returns:
        list: Values of `metric_to_optimise` for all the candidate models.
    """
    import joblib
    import numpy as np
    import pandas as pd
    from loguru import logger

    from src.base.model import evaluate_model
    from src.utils.logging import setup_logger

    setup_logger()

    candidates = [joblib.load(m.path) for m in models]
    logger.debug(f"Len(candidates): {len(candidates)}")
    logger.debug(f"Candidates: {candidates}")

    df_test = pd.read_parquet(test_data.path)
    df_test = df_test.drop(columns=["transaction_id"])
    y_test = df_test.pop(target_column)
    logger.info(f"Loaded test data, shape {df_test.shape}.")

    res_m, _, _ = zip(*(evaluate_model(c, df_test, y_test) for c in candidates))
    metrics = [m[metric_to_optimise] for m in res_m]
    logger.info("Evaluation completed.")

    if higher_is_better is True:
        best_idx = np.argmax(metrics)
    else:
        best_idx = np.argmin(metrics)

    best_candidate = models[best_idx]
    best_model.path = best_candidate.path
    best_model.uri = best_candidate.uri
    best_model.metadata = best_candidate.metadata

    return metrics
