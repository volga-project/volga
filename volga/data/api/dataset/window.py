
from pydantic import BaseModel

from volga.common.time_utils import Duration


class Window(BaseModel):
    start: Duration
    end: Duration