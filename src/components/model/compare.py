from typing import List

from kfp.dsl import Dataset, Input, Model, Output, component

from src.components.dependencies import PIPELINE_IMAGE_NAME


@component(
    base_image=PIPELINE_IMAGE_NAME,
)
def compare_models(
    test_data: Input[Dataset],
    target_column: str,
    models: Input[List[Model]],
    best_model: Output[Model],
    metric_to_optimise: str,
    higher_is_better: bool = True,
) -> list:
    """_summary_

    Args:
        test_data (Input[Dataset]): _description_
        target_column (str): _description_
        models (Input[Model]): _description_
        metric_to_optimise (str): _description_
        higher_is_better (bool, optional): _description_. Defaults to True.

    Returns:
        NamedTuple:
    """
    import joblib
    import numpy as np
    import pandas as pd
    from loguru import logger

    from src.base.model import evaluate_model

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
