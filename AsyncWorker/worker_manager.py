import multiprocessing
import logging
import time
import asyncio
import threading
import queue

from typing import Any, Callable

from .data_classes import (
    Instruction,
    Message,
    SavedCallable,
    Process_Job,
    Registered_Job,
)
from .worker import _WorkerProcess


logger = logging.getLogger(__name__)


def inititialize_worker_manager(
    worker_amount: int,
    hostqueue: multiprocessing.Queue,
    loop: asyncio.AbstractEventLoop,
    sleeptime: float,
    timeout: float,
    return_queue: multiprocessing.Queue
):
    # print('in the worker_manager process')
    # print(worker_amount,hostqueue,loop,sleeptime,timeout,return_queue)
    workermanager = _WorkerManager(
            worker_amount=worker_amount,
            hostqueue=hostqueue,
            loop=loop,
            sleeptime=sleeptime,
            timeout=timeout,
            return_queue=return_queue,
        )

    workermanagerthread = threading.Thread(
            target=workermanager.run, name="AsyncWorker Worker Manager"
        )
    workermanagerthread.start()
    


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
        sleeptime: int | float = 0.1,
        timeout: int | float = 0.5,
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
        # print(f'worker{self.name} put message:{message} in the queue')
        self.sendQueue.put(message)
        if self.received[msgnum]["done"].wait(timeout=self.timeout):
            # print(f'worker{self.name} got message:{message} from the queue')
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

    def register_callable(self, newcallable: SavedCallable):
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
            name=f'workerprocess{self.name}'
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
        hostqueue: multiprocessing.Queue,
        loop: asyncio.AbstractEventLoop,
        sleeptime: float,
        timeout: float,
        return_queue: multiprocessing.Queue,
    ):
        self.worker_amount = worker_amount
        self.threads = []
        self.hostqueue = hostqueue
        self.loop = loop
        self.sleeptime = sleeptime
        self.timeout = timeout
        self.return_queue = return_queue
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
                message = self.workreturnqueue.get_nowait()
                self.return_result(message)
            except queue.Empty:
                time.sleep(self.sleeptime)

    def return_result(self, message: Message):
        # Used to reinsert responses back into the event loop.
        # print(f"got returnmessage: {message}")
        self.return_queue.put(message)

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
        # print(f'workermanager putting work in the workqueue:{message}')
        self.workqueue.put(message)

    def process_registered(self, message: Message):
        """Simple passthrough method that puts work for an already registered callable in the workqueue
        The return of the work is handled by _handle_returnqueue in its own thread.
        """
        # print(f'workermanager putting work in the workqueue:{message}')
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
                    timeout=self.timeout
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
        # print(f'{self.hostqueue} empty? {self.hostqueue.empty()}')
        while self.running:
            try:
                message = self.hostqueue.get_nowait()
                # print(f"manager_thread got message: {message}")
                self.instructions[message.instruction](message)
            except queue.Empty:
                time.sleep(self.sleeptime)


class ManagerProcess:
    def __init__(self):
        ...
