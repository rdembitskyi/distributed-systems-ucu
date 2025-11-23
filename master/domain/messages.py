from dataclasses import dataclass


@dataclass
class PostMessageResponse:
    status: str
    message: str
    total_messages: int  # debugging param
