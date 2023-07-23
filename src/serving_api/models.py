from pydantic import BaseModel


class Data(BaseModel):
    """Base model representing the input data to the endpoint."""

    instances: list


class Prediction(BaseModel):
    """Base model representing the reponse of the endpoint."""

    fraud_probability: float
