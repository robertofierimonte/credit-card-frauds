from kfp.dsl import Artifact, Dataset, Input, Output, component

from src.components.dependencies import LOGURU, PANDAS, PYARROW, PYTHON, TFDV


@component(
    base_image=PYTHON,
    packages_to_install=[LOGURU, PYARROW, PANDAS, TFDV],
)
def generate_training_stats_schema(
    training_data: Input[Dataset],
    training_stats: Output[Artifact],
    training_schema: Output[Artifact],
    target_column: str,
    artifacts_gcs_folder_path: str = None,
) -> None:
    """_summary_

    Args:
        training_data (Input[Dataset]): _description_
        training_stats (Output[Artifact]): _description_
        training_schema (Output[Artifact]): _description_
        target_column (str): _description_
        artifacts_gcs_folder_path (str, optional): _description_. Defaults to None.
    """
    from pathlib import Path

    import pandas as pd
    import tensorflow_data_validation as tfdv
    from loguru import logger

    df_train = pd.read_parquet(training_data.path)
    df_train = df_train.drop(columns=["transaction_id"])
    logger.info(f"Loaded training data, shape {df_train.shape}.")

    stats_train = tfdv.generate_statistics_from_dataframe(df_train)
    schema_train = tfdv.infer_schema(stats_train)
    schema_train.default_environment.append("TRAINING")
    schema_train.default_environment.append("SERVING")
    logger.info("Create training stats and schema.")

    tfdv.get_feature(schema_train, target_column).not_in_environment.append("SERVING")
    logger.info("Excluded target column from serving environment.")

    if artifacts_gcs_folder_path is not None:
        artifacts_gcs_folder_path = artifacts_gcs_folder_path.replace("gs://", "/gcs/")
        training_stats.path = artifacts_gcs_folder_path
        training_schema.path = artifacts_gcs_folder_path

    training_stats.path = f"{training_stats.path}/training_stats.pbtxt"
    directory = Path(training_stats.path).parent.absolute()
    directory.mkdir(parents=True, exist_ok=True)

    training_schema.path = f"{training_schema.path}/training_schema.pbtxt"
    directory = Path(training_schema.path).parent.absolute()
    directory.mkdir(parents=True, exist_ok=True)

    tfdv.write_stats_text(stats_train, training_stats.path)
    tfdv.write_schema_text(schema_train, training_schema.path)
