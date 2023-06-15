import base64
import json
import sys

import uvicorn
from fastapi import FastAPI, Response, status
from loguru import logger

from src.trigger.main import trigger_pipeline_from_payload
from src.trigger.models import Envelope

logger.remove()
logger.add(sys.stderr, level="WARNING")
logger.add(sys.stdout, filter=lambda record: record["level"].no < 40, level="INFO")

app = FastAPI(title="Pub/Sub Trigger", version="0.1.0")


@app.post("/")
async def index(envlope: Envelope, response: Response):

    # Read data from Pub/Sub event
    data_decoded = base64.b64decode(envlope.message.data).decode("utf-8").strip()
    data = json.loads(data_decoded)
    attributes = envlope.message.attributes

    # Log Info
    logger.info(f"Received message with data: {data}")
    logger.info(f"Received message with attributes: {attributes}")

    # Trigger Pipeline
    payload = dict(attributes=attributes, data=data)
    trigger_pipeline_from_payload(payload)

    # Log Info
    logger.info(f"Triggered pipeline with payload: {payload}")
    return Response(status_code=status.HTTP_204_NO_CONTENT)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080, reload=False)
