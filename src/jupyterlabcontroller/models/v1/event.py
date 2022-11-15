"""Event model for jupyterlab-controller."""

from enum import auto
from typing import Deque, Dict, TypeAlias

from pydantic import BaseModel
from sse_starlette import ServerSentEvent

from ..enum import NubladoEnum

"""GET /nublado/spawner/v1/labs/username/events"""


class EventTypes(NubladoEnum):
    COMPLETE = auto()
    ERROR = auto()
    FAILED = auto()
    INFO = auto()
    PROGRESS = auto()


class Event(BaseModel):
    data: str
    event: EventTypes
    sent: bool = False

    def toSSE(self) -> ServerSentEvent:
        """The ServerSentEvent is the thing actually emitted to the client."""
        return ServerSentEvent(data=self.data, event=self.event)


EventQueue: TypeAlias = Deque[Event]
EventMap: TypeAlias = Dict[str, EventQueue]
