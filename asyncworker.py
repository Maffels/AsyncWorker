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
import queue
import threading
import time
from types import MappingProxyType, coroutine
from typing import Any, Awaitable, Coroutine, Dict, Callable, Mapping, Union


logger = logging.getLogger(__name__)

INSTRUCTIONS = frozenset(
    {
        "register_callable",
        "process",
        "process_callable",
        "start",
        "stop",
        "quit",
        "confirm",
        "done",
        "instruction_error",
        "id_error",
        "error",
        "exception",
    }
)


def MESSAGE(
    instruction: INSTRUCTIONS, data: Any = None, id: int = None
) -> Dict[str, Any]:
    # Basic information structure used to pass information around the different parts of this module
    if instruction not in INSTRUCTIONS:
        raise KeyError(f"{instruction} not a valid instruction")
    else:
        return {"instruction": instruction, "data": data, "id": id}


class _WorkerProcess:
    """Class describing the worker that will run inside of its own multiprocessing.Process instance.
    Will check its hostconnection for instructions, and its workqueue for work when enabled.
    """

    def get_instructions(self) -> Mapping[str, Callable[[Any], None]]:
        """Returns a mapping of the instruction keywords to the WorkerProcess class function calls
        for ease of use when decoding messages.
        Using a MappingProxyType seemed interesting and potentially best fitting for this use.
        """
        return MappingProxyType(
            {
                "register_callable": self.register_callable,
                "process": self.process,
                "process_callable": self.process_callable,
                "start": self.start,
                "stop": self.stop,
                "quit": self.quit,
            }
        )

    def __init__(
        self,
        sendQueue: multiprocessing.Queue,
        receiveQueue: multiprocessing.Queue,
        workQueue: multiprocessing.Queue,
        resultQueue: multiprocessing.Queue,
        workername: str,
        sleeptime: float = 0.1,
    ):
        self.sendQueue = sendQueue
        self.receiveQueue = receiveQueue
        self.resultqueue = resultQueue
        self.workQueue = workQueue
        self.name = workername
        self.sleeptime = sleeptime
        self.instructions = self.get_instructions()
        self.registered_callables = {}
        self.do_work = False
        self.running = False

    def register_callable(self, message: MESSAGE):
        # Used to register the given callable with this instance.
        try:
            callable_id = message["data"]["id"]
            new_callable = message["data"]["callable"]
            self.registered_callables[callable_id] = new_callable
            self.sendQueue.put(MESSAGE("confirm", "registered", message["id"]))
        except Exception as e:
            self.sendQueue.put(MESSAGE("error", e, message["id"]))

    def process_callable(self, message: MESSAGE):
        # Will call the specified callable with the given parameters and send back the results.
        id = message["data"]["id"]
        try:
            saved_callable = self.registered_callables[id]
        except KeyError:
            returnmessage = MESSAGE("id_error", id, message["id"])
            self.resultqueue.put(returnmessage)
            return

        args = message["data"]["args"]
        kwargs = message["data"]["kwargs"]
        try:
            result = saved_callable(*args, **kwargs)
            returnmessage = MESSAGE("done", result, message["id"])
        except Exception as e:
            result = e
            returnmessage = MESSAGE("exception", result, message["id"])

        self.resultqueue.put(returnmessage)

    def process(self, message: MESSAGE):
        # Will process the given callable with the given parameters and send back the results.
        args = message["data"]["args"]
        kwargs = message["data"]["kwargs"]
        function = message["data"]["function"]
        try:
            result = function(*args, **kwargs)
            returnmessage = MESSAGE("done", result, message["id"])
        except Exception as e:
            result = e
            returnmessage = MESSAGE("exception", result, message["id"])

        self.resultqueue.put(returnmessage)

    def handle_message(self, message: MESSAGE):
        # Uses the included instruction to call the correct function for the enclosed data
        #  with the help of the instruction mapping
        try:
            self.instructions[message["instruction"]](message)
        except KeyError:
            self.sendQueue.put(
                MESSAGE("instruction_error", "unknown_instruction", message["id"])
            )

    def work(self):
        # Used as target for the workerthread, pulls work from the workqueue and hands it over for processing.
        while self.do_work:
            try:
                work = self.workQueue.get(block=True, timeout=self.sleeptime)
                self.handle_message(work)
                # return True
            except queue.Empty:
                pass

    def start(self, message: MESSAGE):
        # Will start a workerprocess that'll handle the work put in the workqueue
        self.do_work = True
        self.workthread = threading.Thread(
            target=self.work, name=f"workerprocess {self.name} workthread"
        )
        self.workthread.start()
        self.sendQueue.put(MESSAGE("confirm", id=message["id"]))
        # self.run()

    def stop(self, message: MESSAGE = None):
        # For telling this instance to stop checking the shared workqueue for work, but not quit.
        self.do_work = False
        self.workthread.join()
        if message:
            self.sendQueue.send(MESSAGE("confirm", id=message["id"]))

    def quit(self, message: MESSAGE = None):
        # Will shut down this worker process.
        self.stop()
        self.running = False
        if message:
            try:
                self.sendQueue.put(MESSAGE("confirm", id=message["id"]))
            except BrokenPipeError as e:
                pass

    def run(self):
        # Main work loop checking for messages from the workermanager thread.
        self.running = True
        while self.running:
            try:
                message = self.receiveQueue.get(block=True, timeout=self.sleeptime)
                self.handle_message(message)
            except queue.Empty:
                pass


class _WorkerManager:
    """Class used by AsyncWorker to do work outside of the event loop, but within the same namespace.
    Used with the goal of letting the event loop be stalled for the least amount of time possible
    when interacting with the objects needed for multiprocessing.

    """

    class _WorkerProxy:
        """Class functioning as the proxy for a workerclass in another process,
        handles direct communication through two queues and the thread
        """

        @staticmethod
        def _work(
            workerobj: _WorkerProcess,
            receiveQueue: multiprocessing.Queue,
            sendQueue: multiprocessing.Queue,
            workQueue: multiprocessing.Queue,
            resultQueue: multiprocessing.Queue,
            workername: str,
        ):
            # Helper function used to start the worker instance in a multiprocessing.Process
            worker = workerobj(
                receiveQueue, sendQueue, workQueue, resultQueue, workername
            )
            worker.run()

        def __init__(
            self,
            name: str,
            workqueue: multiprocessing.Queue,
            resultqueue: multiprocessing.Queue,
            sleeptime: Union[int, float] = 0.1,
            timeout: Union[int, float] = 0.5,
        ):
            self.name = name
            self.workqueue = workqueue
            self.resultqueue = resultqueue
            self.sleeptime = sleeptime
            self.timeout = timeout
            self.sendQueue = multiprocessing.Queue()
            self.receiveQueue = multiprocessing.Queue()
            self.workerprocess = None
            self.received = {}
            self.running = False
            self.registered_callables = {}
            self._msgnum = 0
            self.listenprocess = None

        def _listen_to_process(self):
            """Used as the target for a thread used for handling return messages from the worker process
            After receiving a message, look for the corresponding message id and set its event so the waiting thread
            gets notified and can continue.
            """
            while self.running:
                try:
                    returnmessage = self.receiveQueue.get(
                        block=True, timeout=self.timeout
                    )
                    if returnmessage["id"] in self.received.keys():
                        self.received[returnmessage["id"]]["message"] = returnmessage
                        self.received[returnmessage["id"]]["done"].set()
                    else:
                        logger.error(
                            f"Got unhandled message {returnmessage} from worker {self.name}."
                        )
                except queue.Empty:
                    pass

        def _communicate(self, instruction, data=None):
            """Used to abstract away the needs of communicating directlty with the worker process.

            Constructs an awaitable dict entry by using the message id as the identifier and adding a settable Event object.
            After putting a message in the communication queue to the worker of this workerproxy instance,
            this message id can then be used by the listening thread to return received results
            to the formerly built awaitable dict and then notify the thread waiting for it by setting the Event object.

            """
            self._msgnum += 1
            msgnum = self._msgnum
            message = MESSAGE(instruction, data, msgnum)
            event = multiprocessing.Event()
            self.received[self._msgnum] = {"done": event, "message": None}
            self.sendQueue.put(message)
            if self.received[msgnum]["done"].wait(timeout=self.timeout):
                return self.received[msgnum]["message"]
            else:
                logger.error(
                    f"Worker {self.name} did not return message within {self.timeout} seconds."
                )
                return False

        def register_callable(self, newcallable=None):
            """Used to register a callable with the worker process,
            which can then be used by later calls for processing this specific callable.
            """
            returnmessage = self._communicate("register_callable", newcallable)
            if returnmessage["instruction"] == "error":
                logger.error(
                    f"""Worker{self.name} during the registering of the callabe: {newcallable} 
                    encountered error: {returnmessage["data"]} """
                )
            else:
                self.registered_callables[newcallable["id"]] = newcallable

        def start(self) -> None:
            """Initialisation of the worker instance in another process and creating a thread
            for monitoring communications.
            """
            self.running = True
            self.listenprocess = threading.Thread(
                target=self._listen_to_process,
                name=f"listen to workerprocess{self.name}",
            )
            self.listenprocess.start()

            self.workerprocess = multiprocessing.Process(
                target=self._work,
                args=(
                    _WorkerProcess,
                    self.receiveQueue,
                    self.sendQueue,
                    self.workqueue,
                    self.resultqueue,
                    self.name,
                ),
            )
            self.workerprocess.start()

            while not self.workerprocess.is_alive():
                time.sleep(self.sleeptime)
            returnmessage = self._communicate("start")
            if returnmessage:
                return True
            else:
                return False

        def shutdown(self):
            # Used to get the process to shut down.
            if self.workerprocess.is_alive():
                returnmessage = self._communicate("quit")
                if returnmessage:
                    while self.workerprocess.is_alive():
                        # Wait until the process and its instance is shut down.
                        time.sleep(self.sleeptime)
            self.running = False

    def get_instructions(self) -> Mapping[str, Callable[[Any], None]]:
        """Returns a mapping of the instruction keywords to the function calls of this class,
        for ease of use when decoding messages.
        Using a MappingProxyType seemed interesting and potentially best fitting for this use.
        """
        return MappingProxyType(
            {
                "register_callable": self.register_callable,
                "process": self.process,
                "process_callable": self.process_callable,
                "start": self.start,
                "stop": self.stop,
                "quit": self.quit,
            }
        )

    def __init__(
        self,
        worker_amount: int,
        hostqueue: queue.SimpleQueue,
        loop: asyncio.AbstractEventLoop,
        sleeptime: float,
        timeout: float,
        return_callable: Coroutine,
    ):
        self.worker_amount = worker_amount
        self.threads = []
        self.hostqueue = hostqueue
        self.loop = loop
        self.sleeptime = sleeptime
        self.timeout = timeout
        self.return_callable = return_callable
        self.instructions = self.get_instructions()
        self.workqueue = multiprocessing.Queue()
        self.workreturnqueue = multiprocessing.Queue()
        self.workers = {}
        self.running = False

    def _handle_returnqueue(self):
        """Used as the target of a dedicated thread that handles the work returnqueue
        which will then send it back to the caller event loop.
        """
        while self.running:
            try:
                message = self.workreturnqueue.get(block=True, timeout=self.sleeptime)
                self.return_result(message)
            except queue.Empty:
                pass

    def return_result(self, message):
        # Used to reinsert responses back into the master asyncio eventloop.
        asyncio.run_coroutine_threadsafe(self.return_callable(message), self.loop)

    def register_callable(self, message):
        """Used to register calleables with all the worker processes.
        Builds a thread for each process which then communicates the callable,
        and returns when all threads are done.
        """

        def registertask():
            jhandler = message["data"]
            registertasks = [
                threading.Thread(
                    target=worker.register_callable,
                    args=(jhandler,),
                    name=f"registertask for worker{worker.name}",
                )
                for worker in self.workers.values()
            ]
            for task in registertasks:
                task.start()
            for task in registertasks:
                task.join()

            returnmessage = MESSAGE("done", None, message["id"])
            self.return_result(returnmessage)

        t = threading.Thread(target=registertask, name="registerTask")
        t.start()

    def process(self, message):
        """Simple passthrough method that puts work in the workqueue
        The return of the work is handled by _handle_returnqueue in its own thread.
        """
        self.workqueue.put(message)

    def process_callable(self, message):
        """Simple passthrough method that puts work for an already registered callable in the workqueue
        The return of the work is handled by _handle_returnqueue in its own thread.
        """
        self.workqueue.put(message)

    def start(self, message):
        """Used to start the worker processes which can then process given work on multiple threads
        Starts a dedicated startup thread for each worker so they can be started in parallel,
        returns when all workers report they're ready for work.
        Also starts the thread that will be handling the work returnqueue.
        """

        def dostart():
            starttreads = []
            for workernum in range(self.worker_amount):
                self.workers[workernum] = self._WorkerProxy(
                    name=workernum,
                    workqueue=self.workqueue,
                    resultqueue=self.workreturnqueue,
                    sleeptime=self.sleeptime,
                )
                starttreads.append(
                    threading.Thread(
                        target=self.workers[workernum].start,
                        name=f"Worker#{workernum} starttask",
                    )
                )

            for thread in starttreads:
                thread.start()
            for thread in starttreads:
                thread.join()
            returnmessage = MESSAGE("done", id=message["id"])
            self.return_result(returnmessage)

        # Building thread to handle the work return queue
        returnthread = threading.Thread(
            target=self._handle_returnqueue, name="handle work return thread"
        )
        returnthread.start()
        self.threads.append(returnthread)
        t = threading.Thread(target=dostart, name="start workers")
        t.start()

    def stop(self, message=None):
        """will shut down the worker processes by spawning a thread for each worker
        that then tries to stop the workers in parallel. Returns when all worker processes are stopped.
        """

        def stoptask():
            stoptasks = [
                threading.Thread(
                    target=worker.shutdown, name=f"Worker{worker.name} shutdown"
                )
                for worker in self.workers.values()
            ]
            for task in stoptasks:
                task.start()
            for task in stoptasks:
                task.join()

        stopthread = threading.Thread(target=stoptask, name="worker shutdown thread")
        stopthread.start()
        stopthread.join()
        if message:
            returnmessage = MESSAGE("done", None, message["id"])
            self.return_result(returnmessage)

    def quit(self, message=None):
        """Will shut down this workermanager thread and all it's subprocesses."""
        self.stop()
        if message:
            returnmessage = MESSAGE("done", None, message["id"])
            self.return_result(returnmessage)
        self.running = False

    def run(self):
        """Functions as the target of the newly built workermanagerthread,
        starts listening and handling further instructions from the host queue and its bound event loop.
        """
        self.running = True

        while self.running:
            try:
                message = self.hostqueue.get(block=True, timeout=self.sleeptime)
                try:
                    self.instructions[message["instruction"]](message)
                    # if "data" not in message.keys():
                    # message["data"] = None
                except KeyError:
                    pass
                    # self.hostconn.send(
                    # MESSAGE("instruction_error", "unknown_instruction", message["id"]))
            except queue.Empty:
                pass


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

    class _SavedCallable:
        """Simple dataclass for grouping "callable" information"""

        def __init__(self, id: int, newcallable: Callable):
            self.callable = newcallable
            self.id = id

        def __str__(self):
            return str(f"Saved callable {self.callable}.")

        def __eq__(self, value: Callable):
            return callable(value)
            # if isinstance(value, Call):
            #     if self.callable != value:
            #         return False
            #     else:
            #         return True
            # else:
            #     return False

    def __init__(
        self,
        worker_amount: int = None,
        timeout: Union[int, float] = 0.5,
        sleeptime: Union[int, float] = 0.01,
        loop: asyncio.BaseEventLoop = None,
    ):
        if worker_amount:
            self.worker_amount = worker_amount
        else:
            import os

            cpus = os.cpu_count()
            self.worker_amount = cpus - 1 if cpus > 2 else 1

        self.timeout = timeout
        self.sleeptime = sleeptime
        self._worknum = 0
        self.saved_callables = {}
        self._workqueue = queue.SimpleQueue()
        self.loop = loop if loop else asyncio.get_event_loop()
        self._internal_results = {}

    async def __aenter__(self):
        await self.run()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.quit()

    async def _update_resultdict(self, message):
        """Used by the external workermanager thread to reinsert results into the loop
        Uses the message id to find the relevant dict entry and sets its results to the return value.
        """
        try:
            self._internal_results[message["id"]].set_result(message)
        except KeyError:
            logger.error(f"got unregistered returnmessage: {message}.")

    async def _communicate(self, instruction, data=None):
        """Abstracts away the communication to and from the workermanager thread.
        Builds an awaitable asyncio.Future object which will be set by the
        workermanager thread when the result is available,
        which in turn will be used to return the result to the calling coroutine
        """
        self._worknum += 1
        worknum = self._worknum
        self._internal_results[worknum] = self.loop.create_future()
        message = MESSAGE(instruction, data, worknum)
        self._workqueue.put(message)

        returnmessage = await self._internal_results[worknum]
        self._internal_results.pop(worknum)
        return returnmessage

    async def _submit_callable_job(self, callableid: int, args, kwargs):
        # Functions as a "proxy" that gets returned when a new callable is registered
        job = {"id": callableid, "args": args, "kwargs": kwargs}
        result = await self._communicate("process_callable", job)
        if result["instruction"] == "exception":
            raise result["data"]
        else:
            return result["data"]

    async def process(self, func: Callable, *args, **kwargs) -> Awaitable:
        """Coroutine that processes the given callable with the provided arguments,
        returns the results when ready.
        """
        job = {"function": func, "args": args, "kwargs": kwargs}
        result = await self._communicate("process", job)
        if result["instruction"] == "exception":
            raise result["data"]
        else:
            return result["data"]

    async def register_callable(self, newcallable: Callable) -> Coroutine:
        """Coroutine for registering a callable method, returns an awaitable.
        This awaitable can be called together with the original callable arguments and will return the results when ready.

        The goal is to simply transform the given CPU-bound callable into an awaitable coroutine
        which the event loop can simply await instead of block on executing.
        """
        id = None

        # Build and store a new saved_callable instance
        #  and tell the workermanager to register it with all processing threads
        id = len(self.saved_callables)
        savedcallable = self._SavedCallable(id=id, newcallable=newcallable)
        self.saved_callables[savedcallable.id] = savedcallable
        callabledata = {"id": savedcallable.id, "callable": newcallable}
        returnmessage = await self._communicate("register_callable", callabledata)
        if returnmessage["instruction"] != "done":
            raise returnmessage["data"]

        # Helper function wrapping the coroutine to be return with the correct parameters
        @functools.wraps(newcallable)
        async def get_callable(*args, **kwargs):
            return await self._submit_callable_job(id, args, kwargs)

        return get_callable

    async def quit(self):
        # Tells and waits for the workermanager to quit.
        await self._communicate("quit")

    async def run(self) -> bool:
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
        returnmessage = await self._communicate("start")

        if returnmessage["instruction"] == "done":
            return True
        else:
            return False
