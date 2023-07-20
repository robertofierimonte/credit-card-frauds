import os

import joblib
import pandas as pd
import uvicorn
from fastapi import FastAPI, Response, status
from google.cloud import storage
from loguru import logger

from src.serving_api.models import Data, Prediction

app = FastAPI(title="Credit Card Frauds Prediction Model")

global_items = {}
model_file = "model.joblib"


@app.on_event("startup")
def startup() -> None:
    path = os.environ.get("AIP_STORAGE_URI", "/tmp/model")
    path = os.path.join(path, model_file)
    logger.info(f"Loading model file from {path}.")

    if path.startswith("gs://"):
        dest_file_name = f"/tmp/{model_file}"
        storage_client = storage.Client()
        with open(dest_file_name, "wb") as f:
            storage_client.download_blob_to_file(path, f)
        logger.info(f"Downloaded model file from GCS to {dest_file_name}.")
    else:
        dest_file_name = path

    global_items["model"] = joblib.load(dest_file_name)
    logger.info("Successfully loaded model.")


@app.get("/health")
async def health_check() -> Response:
    logger.info("Health check.")
    return Response("Healthy", status_code=status.HTTP_200_OK)


@app.post("/predict")
async def prediction(data: Data) -> Response:
    try:
        df = pd.DataFrame(data.instances)
        logger.info("Loaded data.")

        model = global_items["model"]
        proba = model.predict_proba(df)
        logger.info("Computed probabilities.")
        logger.debug(f"Probabilities: {proba}.")
        predictions = [Prediction(fraud_probability=p[1]).model_dump() for p in proba]
        response = {"predictions": predictions}
        return response

    except Exception as err:
        raise err


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080, reload=False, log_level="info")
