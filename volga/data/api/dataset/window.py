
from pydantic import BaseModel

Duration = str

class Window(BaseModel):
    start: Duration
    end: Duration