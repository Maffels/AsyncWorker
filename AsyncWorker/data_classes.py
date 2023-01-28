from dataclasses import dataclass
from enum import Enum
import threading
import multiprocessing
import random

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
    data: Any

    def __str__(self):
        return f"Message: {self.instruction=}, {self.data=}, {self.id=}"

    def __repr__(self):
        return self.__str__()
    
@dataclass
class Worker_Message(Message):
    worker: int
    
    def __str__(self):
        return f"Message: {self.instruction=}, {self.data=}, {self.id=}, {self.worker=}"



class Worker_Future:
    """
    Dataclass for emulating a 'future equivalent' data object used in communicating between the worker_manager and the worker
    """
    def __init__(self, message: Message|None,id:int):
        self.status = threading.Event()
        self.message = message
        self.id = id
    
        
    def set(self, message:Message):
        self.message = message
        self.status.set()
        
    def wait(self,timeout:float):
        print(f'waiting on message for {timeout} seconds {self.status=}')
        if self.status.wait(timeout=timeout):
            print(f"Worker_future {self.message} wait complete {self.status=}")
            return self.message
        else:
            raise TimeoutError()
        
        
    
class Worker_Futures:
    def __init__(self, name: int):
        self.name = name
        self._futures:dict = {}
        self.msgnum = 0
        self.lock = threading.Lock()
        
    def get_msgnum(self):
        with self.lock:
            self.msgnum += 1
            return self.msgnum   
        
    def put(self,id:int, message:Message | None =None):
        self._futures[id] = Worker_Future(message=message, id=id)
        print(f'worker_futures{self.name} added {id}: {message} to futures: {self._futures}')
        
    def set(self,message:Message):
        print(f'worker_futures{self.name} set: {message} in  {self._futures}')
        self._futures[message.id].set(message)
        print(f'futures after set: {self._futures}')
        
    def wait(self,id:int, timeout:float):
        try:
            message = self._futures[id].wait(timeout)
            self._futures.pop(id)
            return message
        except TimeoutError:
            raise TimeoutError(f"Worker {self.name} did not return message {id} within {timeout} seconds.")

        
        
        
    
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
