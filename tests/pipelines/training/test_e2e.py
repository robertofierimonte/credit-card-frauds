import os

from tests.e2e.test_e2e import pipeline_e2e_test


def test_pipeline_run() -> None:
    """Tests if pipeline is run successfully.

    Triggers pipeline synchronously.
    Tests will fail if:
    - Any errors are thrown during execution
    - Any of the expected component outputs are empty (size == 0kb)
    """
    payload_file = os.environ["PAYLOAD"]
    payload_path = f"src/pipelines/training/payloads/{payload_file}"

    common_tasks = {
        "generate_bq_query": [],
        "execute_bq_query": [],
        "extract-bq-to-dataset": ["dataset"],
        "preprocess": [
            "output_validation_data",
            "output_train_data",
            "model_metadata",
            "model_directory",
            "output_test_data",
        ],
        "train-model": ["training_result", "output_model", "metrics"],
        "predict-evaluate-model": ["testing_result", "metrics"],
        "load-dataset-to-bq": [],
        "load-dataset-to-bq-2": [],
        "condition-model-deploy-decision-1": [],
        "upload-model": [],
    }

    pipeline_e2e_test(payload_path=payload_path, common_tasks=common_tasks)
