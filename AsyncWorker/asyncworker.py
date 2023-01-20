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
import multiprocessing
import os
import queue
import threading
import time

from typing import Any, Awaitable, Coroutine, Dict, Callable, Mapping, Union

from .data_classes import (
    Instruction,
    Message,
    SavedCallable,
    Process_Job,
    Registered_Job,
)

from .worker_manager import _WorkerManager

logger = logging.getLogger(__name__)


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

    def __init__(
        self,
        worker_amount: int | None = None,
        timeout: float = 0.5,
        sleeptime: float = 0.01,
        loop: asyncio.BaseEventLoop | None = None,
    ):
        if worker_amount:
            self.worker_amount = worker_amount
        else:
            cpus = os.cpu_count()
            if not cpus:
                raise ValueError("could not get cpu_count from system")
            else:
                self.worker_amount = cpus - 1 if cpus > 2 else 1

        self.timeout = timeout
        self.sleeptime = sleeptime
        self._worknum = 0
        self.saved_callables = {}
        self._workqueue = queue.SimpleQueue()
        self.loop = loop if loop else asyncio.get_event_loop()
        self._internal_results = {}

    async def __aenter__(self):
        await self.init()
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

    async def process(self, func: Callable, *args, **kwargs) -> Awaitable:
        """Coroutine that processes the given callable with the provided arguments,
        returns the results when ready.
        """
        job = Process_Job(func, args, kwargs)
        result = await self._communicate(Instruction.process, job)
        if result.instruction == Instruction.exception:
            raise result.data
        else:
            return result.data

    async def register_callable(self, newcallable: Callable) -> Callable:
        """Coroutine for registering a callable method, returns an awaitable.
        This awaitable can be called together with the original callable arguments and will return the results when ready.

        The goal is to simply transform the given CPU-bound callable into an awaitable coroutine
        which the event loop can await instead of the callable blocking the event loop.
        """

        # Build and store a new saved_callable instance,
        # and tell the workermanager to register it with all processing threads
        callable_id = len(self.saved_callables)
        savedcallable = SavedCallable(id=callable_id, callable=newcallable)
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
        await self._communicate(Instruction.quit)

    async def init(self) -> None:
        # Builds the workermanager thread and tells it to start managing the workerthreads
        self.workermanager = _WorkerManager(
            worker_amount=self.worker_amount,
            hostqueue=self._workqueue,
            loop=self.loop,
            sleeptime=self.sleeptime,
            timeout=self.timeout,
            return_callable=self._update_resultdict,
        )

        self.workermanagerthread = threading.Thread(
            target=self.workermanager.run, name="AsyncWorker WorkerManager"
        )
        self.workermanagerthread.start()

        # Tells and waits for the workermanager to start.
        await self._communicate(Instruction.start)
