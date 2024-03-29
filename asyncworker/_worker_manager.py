import asyncio
import functools
import logging
import multiprocessing
import os
import queue
import threading
import time
from ._data_classes import Instruction, Message, Saved_Callable
from enum import Enum
from typing import Any, Awaitable, Coroutine, Dict, Callable, Mapping, Union


logger = logging.getLogger(__name__)


class _WorkerProcess:
    """Class describing the worker that will run inside of its own multiprocessing.Process instance.
    Will check its hostconnection for instructions, and its workqueue for work when enabled.
    """

    def get_instructions(self) -> dict[Instruction, Callable]:
        """Returns a mapping of the instruction keywords to the WorkerProcess class function calls
        for ease of use when decoding messages.
        Using a MappingProxyType seemed interesting and potentially best fitting for this use.
        """
        return {
            Instruction.register_callable: self.register_callable,
            Instruction.process: self.process,
            Instruction.process_registered: self.process_registered,
            Instruction.start: self.start,
            Instruction.stop: self.stop,
            Instruction.quit: self.quit,
        }

    def __init__(
        self,
        sendQueue: multiprocessing.Queue,
        receiveQueue: multiprocessing.Queue,
        workQueue: multiprocessing.Queue,
        resultQueue: multiprocessing.Queue,
        workername: str,
        sleeptime: float = 0.1,
    ):
        self.sendQueue: multiprocessing.Queue = sendQueue
        self.receiveQueue: multiprocessing.Queue = receiveQueue
        self.resultqueue: multiprocessing.Queue = resultQueue
        self.workQueue: multiprocessing.Queue = workQueue
        self.name = workername
        self.sleeptime = sleeptime
        self.instructions = self.get_instructions()
        self.registered_callables = {}
        self.do_work = False
        self.running = False

    def register_callable(self, message: Message):
        # Used to register the given callable with this instance.
        try:
            callable_id = message.data.id
            new_callable = message.data.callable
            self.registered_callables[callable_id] = new_callable
            self.sendQueue.put(
                Message(Instruction.done, message.id, message.instruction)
            )
        except Exception as e:
            self.sendQueue.put(Message(Instruction.exception, message.id, e))

    def process_registered(self, message: Message):
        # Will call the specified callable with the given parameters and send back the results.
        callable_id = message.data.id

        saved_callable = self.registered_callables[callable_id]

        args = message.data.args
        kwargs = message.data.kwargs
        try:
            result = saved_callable(*args, **kwargs)
            returnmessage = Message(Instruction.done, message.id, result)
        except Exception as e:
            result = e
            returnmessage = Message(Instruction.exception, message.id, result)

        self.resultqueue.put(returnmessage)

    def process(self, message: Message):
        # Will process the given callable with the given parameters and send back the results.
        args = message.data.args
        kwargs = message.data.kwargs
        function = message.data.function
        try:
            result = function(*args, **kwargs)
            returnmessage = Message(Instruction.done, message.id, result)
        except Exception as e:
            result = e
            returnmessage = Message(Instruction.exception, message.id, result)
        self.resultqueue.put(returnmessage)

    def handle_message(self, message: Message):
        # Uses the included instruction to call the correct function for the enclosed data
        #  with the help of the instruction mapping
        self.instructions[message.instruction](message)

    def work(self):
        # Used as target for the workerthread, pulls work from the workqueue and hands it over for processing.
        while self.do_work:
            try:
                work = self.workQueue.get(block=True, timeout=self.sleeptime)
                self.handle_message(work)
                # return True
            except queue.Empty:
                pass

    def start(self, message: Message):
        # Will start a workerprocess that'll handle the work put in the workqueue
        self.do_work = True
        self.workthread = threading.Thread(
            target=self.work, name=f"workerprocess {self.name} workthread"
        )
        self.workthread.start()
        self.sendQueue.put(
            Message(Instruction.done, id=message.id, data=f"started worker {self.name}")
        )
        # self.run()

    def stop(self, message: Message | None = None):
        # For telling this instance to stop checking the shared workqueue for work, but not quit.
        self.do_work = False
        self.workthread.join()
        if message:
            self.sendQueue.put(
                Message(
                    Instruction.done, id=message.id, data=f"stopped worker {self.name}"
                )
            )

    def quit(self, message: Message | None = None):
        # Will shut down this worker process.
        self.stop()
        self.running = False
        if message:
            try:
                self.sendQueue.put(
                    Message(
                        Instruction.done, id=message.id, data=f"quit worker {self.name}"
                    )
                )
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


class _WorkerProxy:
    """Class functioning as the proxy for a workerclass in another process,
    handles direct communication through two queues and the thread
    """

    @staticmethod
    def _work(
        workerobj,
        receiveQueue: multiprocessing.Queue,
        sendQueue: multiprocessing.Queue,
        workQueue: multiprocessing.Queue,
        resultQueue: multiprocessing.Queue,
        workername: str,
    ):
        # Helper function used to start the worker instance in a multiprocessing.Process
        worker = workerobj(receiveQueue, sendQueue, workQueue, resultQueue, workername)
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
        self.workerprocess: multiprocessing.Process
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
                returnmessage = self.receiveQueue.get(block=True, timeout=self.timeout)
                if returnmessage.id in self.received.keys():
                    self.received[returnmessage.id]["message"] = returnmessage
                    self.received[returnmessage.id]["done"].set()
                else:
                    logger.error(
                        f"Got unhandled message {returnmessage} from worker {self.name}."
                    )
            except queue.Empty:
                pass

    def _communicate(
        self, instruction: Instruction, data: Any | None = None
    ) -> Message:
        """Used to abstract away the needs of communicating directlty with the worker process.
        Constructs an awaitable dict entry by using the message id as the identifier and adding a settable Event object.
        After putting a message in the communication queue to the worker of this workerproxy instance,
        this message id can then be used by the listening thread to return received results
        to the formerly built awaitable dict and then notify the thread waiting for it by setting the Event object.
        """
        self._msgnum += 1
        msgnum = self._msgnum
        message = Message(instruction, msgnum, data)
        event = multiprocessing.Event()
        self.received[msgnum] = {"done": event, "message": None}
        self.sendQueue.put(message)
        if self.received[msgnum]["done"].wait(timeout=self.timeout):
            return self.received[msgnum]["message"]
        elif self.received[msgnum]["done"].wait(timeout=self.timeout):
            logger.warning(
                f"Worker {self.name} did not return message within {self.timeout} seconds."
            )
            return self.received[msgnum]["message"]
        else:
            raise TimeoutError(
                f"Worker {self.name} did not return message within {self.timeout} seconds."
            )

    def register_callable(self, newcallable: Saved_Callable):
        """Used to register a callable with the worker process,
        which can then be used by later calls for processing this specific callable.
        """
        returnmessage = self._communicate(Instruction.register_callable, newcallable)
        if returnmessage.instruction == Instruction.exception:
            logger.error(
                f"""Worker{self.name} during the registering of the callabe: {newcallable} 
                encountered error: {returnmessage.data} """
            )
        else:
            self.registered_callables[newcallable.id] = newcallable

    def start(self) -> bool:
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
        returnmessage = self._communicate(Instruction.start)
        if returnmessage:
            return True
        else:
            return False

    def shutdown(self):
        # Used to get the process to shut down.
        if self.workerprocess.is_alive():
            returnmessage = self._communicate(Instruction.quit)
            if returnmessage:
                while self.workerprocess.is_alive():
                    # Wait until the process and its instance is shut down.
                    time.sleep(self.sleeptime)
        self.running = False


class _WorkerManager:
    """Class used by AsyncWorker to do work outside of the event loop, but within the same namespace.
    Used with the goal of letting the event loop be stalled for the least amount of time possible
    when interacting with the objects needed for multiprocessing.

    """

    def get_instructions(self) -> dict[Instruction, Callable]:
        """Returns a mapping of the instruction keywords to the function calls of this class,
        for ease of use when decoding messages.
        Using a MappingProxyType seemed interesting and potentially best fitting for this use.
        """
        return {
            Instruction.register_callable: self.register_callable,
            Instruction.process: self.process,
            Instruction.process_registered: self.process_registered,
            Instruction.start: self.start,
            Instruction.stop: self.stop,
            Instruction.quit: self.quit,
        }

    def __init__(
        self,
        worker_amount: int,
        hostqueue: queue.SimpleQueue,
        loop: asyncio.AbstractEventLoop,
        sleeptime: float,
        timeout: float,
        return_callable: Callable,
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

    def return_result(self, message: Message):
        # Used to reinsert responses back into the event loop.
        # print(f"got returnmessage: {message}")
        asyncio.run_coroutine_threadsafe(self.return_callable(message), self.loop)

    def register_callable(self, message: Message):
        """Used to register callables with all the worker processes.
        Builds a thread for each process which then communicates the callable,
        and returns when all threads are done.
        """

        def registertask():
            # print('in registertask')
            jhandler = message.data
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

            returnmessage = Message(
                Instruction.done, message.id, f"registertask for workers"
            )
            self.return_result(returnmessage)

        t = threading.Thread(target=registertask, name="registerTask")
        t.start()

    def process(self, message: Message):
        """Simple passthrough method that puts work in the workqueue
        The return of the work is handled by _handle_returnqueue in its own thread.
        """
        self.workqueue.put(message)

    def process_registered(self, message: Message):
        """Simple passthrough method that puts work for an already registered callable in the workqueue
        The return of the work is handled by _handle_returnqueue in its own thread.
        """
        self.workqueue.put(message)

    def start(self, message: Message):
        """Used to start the worker processes which can then process given work on multiple threads
        Starts a dedicated startup thread for each worker so they can be started in parallel,
        returns when all workers report they're ready for work.
        Also starts the thread that will be handling the work returnqueue.
        """

        def dostart():
            # print('in dostart')
            starttreads = []
            for workernum in range(self.worker_amount):
                self.workers[workernum] = _WorkerProxy(
                    name=str(workernum),
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
            returnmessage = Message(
                Instruction.done, id=message.id, data=f"start for workers"
            )
            self.return_result(returnmessage)

        # Building a thread to handle the work return queue.
        returnthread = threading.Thread(
            target=self._handle_returnqueue, name="handle work return thread"
        )
        returnthread.start()
        self.threads.append(returnthread)

        # Building a thread that starts the worker processes.
        t = threading.Thread(target=dostart, name="start workers")
        t.start()

    def stop(self, message: Message | None = None):
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
            returnmessage = Message(
                Instruction.done, message.id, f"stoptask for workers"
            )
            self.return_result(returnmessage)

    def quit(self, message: Message | None = None):
        """Will shut down this workermanager thread and all it's subprocesses."""
        self.stop()
        if message:
            returnmessage = Message(Instruction.done, message.id, message.instruction)
            self.return_result(returnmessage)
        self.running = False

    def run(self):
        """Functions as the target of the newly built workermanagerthread,
        starts listening and handling further instructions from the event loop via the given queue.
        """
        self.running = True
        # print('here goes')
        while self.running:
            try:
                message = self.hostqueue.get(block=True, timeout=self.sleeptime)
                self.instructions[message.instruction](message)
            except queue.Empty:
                pass
