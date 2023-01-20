from dataclasses import dataclass
from enum import Enum

from typing import Callable, Any


class Instruction(Enum):
    register_callable = 1
    process = 2
    process_registered = 3
    start = 4
    stop = 5
    quit = 6
    done = 7
    exception = 8


@dataclass
class Message:
    instruction: Instruction
    id: int
    data: Any = None

    def __str__(self):
        return f"Message: {self.instruction=}, {self.data=}, {self.id=}"

    def __repr__(self):
        return self.__str__()


@dataclass
class SavedCallable:
    """Simple dataclass for grouping registered callable information"""

    callable: Callable
    id: int

    def __str__(self):
        return str(f"Saved callable {self.callable}.")

    def __eq__(self, value: Callable):
        if callable(value):
            if value == self.callable:
                return True
            else:
                return False
        else:
            return False


@dataclass
class Process_Job:
    function: Callable
    args: Any
    kwargs: Any


@dataclass
class Registered_Job:
    id: int
    args: Any
    kwargs: Any
