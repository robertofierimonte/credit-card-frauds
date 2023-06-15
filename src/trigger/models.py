from datetime import datetime

from pydantic import BaseModel


class Message(BaseModel):
    attributes: dict
    data: str
    message_id: float
    publish_time: datetime


class Envelope(BaseModel):
    message: Message
    subscription: str
