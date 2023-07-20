from pydantic import BaseModel


class Data(BaseModel):
    instances: list


class Prediction(BaseModel):
    fraud_probability: float
