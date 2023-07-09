from typing import NamedTuple

from kfp.dsl import Dataset, Input, Model, component

from src.components.dependencies import PIPELINE_IMAGE_NAME


@component(
    base_image=PIPELINE_IMAGE_NAME,
)
def compare_champion_challenger(
    test_data: Input[Dataset],
    target_column: str,
    challenger_model: Input[Model],
    champion_model: Input[Model],
    metric_to_optimise: str,
    absolute_threshold: float = 0.0,
    higher_is_better: bool = True,
) -> NamedTuple(
    "Outputs",
    [
        ("challenger_better", bool),
        ("champion_metric", float),
        ("challeger_metric", float),
    ],
):
    """_summary_

    Args:
        test_data (Input[Dataset]): _description_
        target_column (str): _description_
        challenger_model (Input[Model]): _description_
        champion_model (Input[Model]): _description_
        metric_to_optimise (str): _description_
        higher_is_better (bool, optional): _description_. Defaults to True.

    Returns:
        bool: Whether the challenger model is better than the current champion
        float: Value of the metric to optimise for the challenger model
        float: Value of the metric to optimise for the champion model
    """
    import joblib
    import pandas as pd
    from loguru import logger

    from src.base.model import evaluate_model

    champion = joblib.load(champion_model.path)
    logger.info("")
    challenger = joblib.load(challenger_model.path)
    logger.info("")

    df_test = pd.read_parquet(test_data.path)
    df_test = df_test.drop(columns=["transaction_id"])
    y_test = df_test.pop(target_column)
    logger.info(f"Loaded test data, shape {df_test.shape}.")

    champion_metrics, _, _ = evaluate_model(champion, df_test, y_test)
    challenger_metrics, _, _ = evaluate_model(challenger, df_test, y_test)
    logger.info("Evaluation completed.")

    champion_metric = champion_metrics[metric_to_optimise]
    challenger_metric = challenger_metrics[metric_to_optimise]

    if (
        (higher_is_better is True)
        and (challenger_metric > (champion_metric + absolute_threshold))
    ) or (
        (higher_is_better is False)
        and (challenger_metric < (champion_metric - absolute_threshold))
    ):
        return (True, champion_metric, challenger_metric)
    else:
        return (False, champion_metric, challenger_metric)
