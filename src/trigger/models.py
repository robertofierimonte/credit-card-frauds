from datetime import datetime

from pydantic import BaseModel


class Message(BaseModel):
    """Base model representing the content of the Pub/Sub message to the trigger."""

    attributes: dict
    data: str
    message_id: float
    publish_time: datetime


class Envelope(BaseModel):
    """Base model representing a Pub/Sub message to the trigger."""

    message: Message
    subscription: str
