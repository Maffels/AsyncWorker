""" Module for handling threaded CPU-bound work asynchronously within the asyncio framework. 
    Its aim is to function as an easy to use bridge between I/O-bound and CPU-bound code.

    Can be used in an asyncio contextmanager or by constructing an instance of it.
    When not using it in a context manager, needs .run() to start,
    and .quit() or it will keep itself alive waiting for work.
    

    Callables can currently be processed in two ways:
    - Callables can be processed directly by calling .process(Callable, *args, **kwargs), 
      which is an awaitable that returns as soon as the callable with it's args has been processed.
    - Callables can be registered with the class by calling .register(Callable). 
      This will return a coroutine that is an awaitable substitute for the original callable,
      which can be called with the same arguments and returns as soon as the result is ready. 
    
    Will by default use total-1 system processing threads for multiprocessing work,
    but can this be specified by using the parameter worker_amount=n at initialisation.
    
    Looks to be somewhat quicker than using a ProcessPoolExecutor with the standard executor.submit method, 
    but way slower than executor.map. 
    Ideally in the future AsyncWorker would be somewhat closer to the performance of executor.map, 
    but not losing the current flexibility in the process might be difficult.  
"""

import asyncio
import functools
import logging
import os
import queue
import concurrent.futures
from dataclasses import dataclass
from asyncworker._data_classes import (
    Instruction,
    Message,
    Saved_Callable,
    Registered_Job,
    Process_Job,
)
from typing import Any, Awaitable, Coroutine, Dict, Callable, Mapping, Union
from asyncworker._worker_manager import _WorkerManager

@dataclass
class Saved_Callable:
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


class Job:
    _id: int = 0

    def __init__(
        self,
        callable: Callable,
        args: Any = None,
        kwargs: Any = None,
        result: asyncio.Future = None,
    ):
        self.callable = (callable,)
        self.args = (args,)
        self.kwargs = (kwargs,)
        __class__._id += 1
        self.id = __class__._id
        self.result = result

    @property
    def get_callable(self):
        async def callable(func, *args, **kwargs):
            result = None
            try:
                result = func(*args, **kwargs)
            except Exception as e:
                result = e
            return {"id": self.id, "result": result}

        return callable

logger = logging.getLogger(__name__)

def wrapped_func(id,func,args,kwargs):
    try:
        result = func(*args,**kwargs)
    except Exception as e:
        result = e
    return (id,result)


class AsyncWorker:
    """
    Class for providing an interface to asynchronously process cpu-bound tasks in a configurable amount of processing threads.
    Currently provides two ways of processing work:
        -   Calling the coroutine .process(Callable,*args,**kwargs) will process the callable with the given
            arguments and returns the result when done.
        -   Register a callable with the class via .register which will
            register the callable with all workunits and return a coroutine. This can then be used
            as a proxy for asynchronously processing work on different processing threads in the future.
    """

    def __init__(self, worker_amount: int | None = None):
        if worker_amount:
            self.worker_amount = worker_amount
        else:
            cpus = os.cpu_count()
            if not cpus:
                raise ValueError("could not get cpu_count from system")
            else:
                self.worker_amount = cpus - 1 if cpus > 2 else 1
        self.jobnum = 0
        self.future_dict: dict[int, asyncio.Future] = {}
        self.loop = asyncio.get_running_loop()

    async def __aenter__(self):
        await self.initialize()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.quit()

    async def _update_resultdict(self, message):
        """Used by the external workermanager thread to reinsert results into the loop
        Uses the message id to find the relevant dict entry and sets its results to the return value.
        """
        try:
            self._internal_results[message.id].set_result(message)
        except KeyError:
            logger.error(f"got unregistered returnmessage: {message}.")

    async def _communicate(self, instruction: Instruction, data: Any = None) -> Message:
        """Abstracts away the communication to and from the workermanager thread.
        Builds an awaitable asyncio.Future object which will be set by the
        workermanager thread when the result is available,
        which in turn will be used to return the result to the calling coroutine
        """
        self._worknum += 1
        worknum = self._worknum
        self._internal_results[worknum] = self.loop.create_future()
        message = Message(instruction, worknum, data)
        self._workqueue.put(message)

        returnmessage = await self._internal_results[worknum]
        self._internal_results.pop(worknum)
        return returnmessage

    async def _submit_callable_job(self, callable_id: int, args, kwargs):
        # Functions as a "proxy" that gets returned when a new callable is registered
        job = Registered_Job(callable_id, args, kwargs)
        result = await self._communicate(Instruction.process_registered, job)
        if result.instruction == Instruction.exception:
            raise result.data
        else:
            return result.data

    async def set_future(self, id,result):
        future = self.future_dict.pop(id)
        
        if isinstance(result, Exception):
            future.set_exception(result)
        else:
            print('setting result')
            future.set_result(result)
            print('set result')

    def _submit_result(self, future:concurrent.futures.Future):
        print('in submit result')
        id, job_result = future.result()
        print('after submit result')
        print(id,job_result)
        #id = result.get('id')
        #job_result = result.get('result')

        asyncio.run_coroutine_threadsafe(self.set_future(id,job_result),self.loop)
    
    
    def asyncify(self,func:Callable):
        
        async def inner(*args,**kwargs):
            return await self.process(func,args,kwargs)
        
        return inner
       
        
    async def _register_job(self, func: Callable, args, kwargs) -> Awaitable:
        # job = Job(
        #     callable=func, args=args, kwargs=kwargs, result=self.loop.create_future()
        # )
        self.jobnum += 1
        id = self.jobnum
        result = self.loop.create_future()
        
        
        self.future_dict.update({id : result})
        print('before apply_async')
        pool_future = self.pool.submit(wrapped_func,id=id,func=func,args=args,kwargs=kwargs)
        pool_future.add_done_callback(self._submit_result)
        # self.pool.apply_async(
        #     job.get_callable, args=args, kwargs=kwargs, callback=self._submit_result
        # )
        print('returning job.result')
        return result

    async def process(self, func: Callable, *args, **kwargs) -> Awaitable:
        """Coroutine that processes the given callable with the provided arguments,
        returns the results when ready.
        """
        print('in process')
        result = await self._register_job(func,args,kwargs)
        return await result

    async def register_callable(self, newcallable: Callable) -> Callable:
        """Coroutine for registering a callable method, returns an awaitable.
        This awaitable can be called together with the original callable arguments and will return the results when ready.

        The goal is to simply transform the given CPU-bound callable into an awaitable coroutine
        which the event loop can await instead of the callable blocking the event loop.
        """

        # Build and store a new saved_callable instance,
        # and tell the workermanager to register it with all processing threads
        callable_id = len(self.saved_callables)
        savedcallable = Saved_Callable(id=callable_id, callable=newcallable)
        self.saved_callables[savedcallable.id] = savedcallable
        returnmessage = await self._communicate(
            Instruction.register_callable, savedcallable
        )
        if returnmessage.instruction != Instruction.done:
            raise returnmessage.data

        # Helper function wrapping the coroutine to be returned as the given callable.
        @functools.wraps(newcallable)
        async def get_callable(*args, **kwargs) -> Coroutine:
            return await self._submit_callable_job(callable_id, args, kwargs)

        return get_callable

    async def quit(self) -> None:
        # Tells and waits for the workermanager to quit.
        self.pool.shutdown(wait=True)
        print('after shutdown')
        while self.future_dict:
            await asyncio.sleep(0.01)

    async def initialize(self) -> None:
        # Builds the workermanager thread and tells it to start managing the workerthreads
        self.pool = concurrent.futures.ProcessPoolExecutor(max_workers= self.worker_amount)
