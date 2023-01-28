import multiprocessing
import logging
import time
import asyncio
import threading
import queue
import concurrent.futures

from typing import Any, Callable

from .data_classes import (
    Instruction,
    Message,
    Worker_Message,
    SavedCallable,
    Worker_Futures,
    Worker_Future,
    Process_Job,
    Registered_Job,
)
from .worker import _WorkerProcess


logger = logging.getLogger(__name__)


def initialize_worker_manager(
    worker_amount: int,
    host_command_queue: multiprocessing.Queue,
    loop: asyncio.AbstractEventLoop,
    sleeptime: float,
    timeout: float,
    host_return_queue: multiprocessing.Queue,
):
    workermanager = _WorkerManager(
        worker_amount=worker_amount,
        host_command_queue=host_command_queue,
        loop=loop,
        sleeptime=sleeptime,
        timeout=timeout,
        host_return_queue=host_return_queue,
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
        command_send_queue: multiprocessing.Queue,
        command_return_queue: multiprocessing.Queue,
        work_queue: multiprocessing.Queue,
        result_queue: multiprocessing.Queue,
        name: str,
    ):
        # Helper function used to start the worker instance in a multiprocessing.Process
        worker = workerobj(
            command_send_queue, command_return_queue, work_queue, result_queue, name
        )
        worker.run()

    def __init__(
        self,
        name: int,
        work_queue: multiprocessing.Queue,
        result_queue: multiprocessing.Queue,
        command_return_queue: multiprocessing.Queue,
        sleeptime: int | float = 0.1,
        timeout: int | float = 0.5,
    ):
        self.name = name
        self.work_queue = work_queue
        self.result_queue = result_queue
        self.sleeptime = sleeptime
        self.timeout = timeout
        self.command_send_queue = multiprocessing.Queue()
        self.command_return_queue = command_return_queue
        self.workerprocess: multiprocessing.Process
        self.futures = Worker_Futures(self.name)
        self.running = False
        self.registered_callables = {}

    def _communicate(
        self, instruction: Instruction, data: Any | None = None
    ) -> Message:
        """Used to abstract away the needs of communicating directlty with the worker process.

        Constructs an awaitable dict entry by using the message id as the identifier and adding a settable Event object.
        After putting a message in the communication queue to the worker of this workerproxy instance,
        this message id can then be used by the listening thread to return received results
        to the formerly built awaitable dict and then notify the thread waiting for it by setting the Event object.

        """
        msgnum = self.futures.get_msgnum()
        message = Worker_Message(
            instruction=instruction, id=msgnum, data=data, worker=self.name
        )
        self.futures.put(message.id)
        print(f"workerproxy{self.name} sending: {message}")
        self.command_send_queue.put(message)
        try:
            returnmessage = self.futures.wait(id=message.id, timeout=self.timeout)
            print(f"_communicate got returnmessage:{returnmessage}")
            return returnmessage
        except TimeoutError as e:
            # logger.error(e)
            raise TimeoutError(e)

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
            return True

    def start(self) -> bool:
        """Initialisation of the worker instance in another process and creating a thread
        for monitoring communications.
        """
        self.workerprocess = multiprocessing.Process(
            target=self._work,
            kwargs={
                "workerobj": _WorkerProcess,
                "command_send_queue": self.command_send_queue,
                "command_return_queue": self.command_return_queue,
                "work_queue": self.work_queue,
                "result_queue": self.result_queue,
                "name": self.name,
            },
            name=f"workerprocess{self.name}",
        )
        self.workerprocess.start()

        while not self.workerprocess.is_alive():
            time.sleep(self.sleeptime)
        returnmessage = self._communicate(Instruction.start)
        if returnmessage:
            return True
        else:
            return False

    def quit(self):
        # Used to get the process to shut down.
        if self.workerprocess.is_alive():
            returnmessage = self._communicate(Instruction.quit)
            if returnmessage:
                while self.workerprocess.is_alive():
                    # Wait until the process and its instance is shut down.
                    time.sleep(self.sleeptime)
        self.running = False
        self.workerprocess.join()


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
        host_command_queue: multiprocessing.Queue,
        loop: asyncio.AbstractEventLoop,
        sleeptime: float,
        timeout: float,
        host_return_queue: multiprocessing.Queue,
    ):
        self.worker_amount = worker_amount
        self.host_command_queue = host_command_queue
        self.loop = loop
        self.sleeptime = sleeptime
        self.timeout = timeout
        self.host_return_queue = host_return_queue
        self.instructions = self.get_instructions()
        self.work_queue = multiprocessing.Queue()
        self.result_queue = multiprocessing.Queue()
        self.command_return_queue = multiprocessing.Queue()
        self.workers: dict[int, _WorkerProxy] = {}
        self.running = False

    def _handle_result_queue(self):
        """Used as the target of a dedicated thread that handles the work returnqueue
        which will then send it back to the caller event loop.
        """
        """Used as the target for a thread used for handling return messages from the worker process
        After receiving a message, look for the corresponding message id and set its event so the waiting thread
        gets notified and can continue.
        """
        print("started listenprocess")
        while self.running:
            try:
                message = self.result_queue.get_nowait()
                self.return_result(message)
            except queue.Empty:
                time.sleep(self.sleeptime)
                
    def _handle_command_return_queue(self):
        while self.running:
            try:
                returnmessage: Worker_Message = self.command_return_queue.get_nowait()
                print(f"command listen thread got returnmessage: {returnmessage}")
                self.workers[returnmessage.worker].futures.set(returnmessage)
            except queue.Empty:
                time.sleep(self.sleeptime)
        

    def return_result(self, message: Message):
        # Used to reinsert responses back into the event loop.
        self.host_return_queue.put(message)

    def register_callable(self, message: Message):
        """Used to register callables with all the worker processes.
        Builds a thread for each process which then communicates the callable,
        and returns when all threads are done.
        """

        def registertask():
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
        t.join()

    def process(self, message: Message):
        """Simple passthrough method that puts work in the work_queue
        The return of the work is handled by _handle_returnqueue in its own thread.
        """
        self.work_queue.put(message)

    def process_registered(self, message: Message):
        """Simple passthrough method that puts work for an already registered callable in the work_queue
        The return of the work is handled by _handle_returnqueue in its own thread.
        """
        self.work_queue.put(message)

    def start(self, message: Message):
        """Used to start the worker processes which can then process given work on multiple threads
        Starts a dedicated startup thread for each worker so they can be started in parallel,
        returns when all workers report they're ready for work.
        Also starts the thread that will be handling the work returnqueue.
        """
        self._handle_result_queue_thread = threading.Thread(
            target=self._handle_result_queue, name="handle return queue"
        )
        self._handle_result_queue_thread.start()
        
        self._handle_return_command_queue_thread = threading.Thread(
            target=self._handle_command_return_queue, name="handle return queue"
        )
        self._handle_return_command_queue_thread.start()

        # def start_worker(worker_id: int) -> _WorkerProxy:
        #     proxy = _WorkerProxy(
        #         name=worker_id,
        #         work_queue=self.work_queue,
        #         result_queue=self.result_queue,
        #         command_return_queue=self.command_return_queue,
        #         sleeptime=self.sleeptime,
        #         timeout=self.timeout,
        #     )
        #     print(f'built proxy {worker_id}')
        #     proxy.start()
        #     return proxy
        # print('before threadpoolexecutor')
        # with concurrent.futures.ThreadPoolExecutor(
        #     max_workers=self.worker_amount,
        #     thread_name_prefix="start worker"
        # ) as executor:
        #     print('in executor')
        #     start_threads = {
        #         executor.submit(start_worker, worker_id): worker_id
        #         for worker_id in range(self.worker_amount)
        #     }
        #     print('submitted threads')
        #     for completed in concurrent.futures.as_completed(start_threads):
        #         worker_id = start_threads[completed]
        #         try:
        #             self.workers[worker_id] = completed.result()
        #         except Exception as e:
        #             print(e)

        def dostart():
            starttreads = []
            for workernum in range(self.worker_amount):
                self.workers[workernum] = _WorkerProxy(
                    name=workernum,
                    work_queue=self.work_queue,
                    result_queue=self.result_queue,
                    command_return_queue=self.command_return_queue,
                    sleeptime=self.sleeptime,
                    timeout=self.timeout,
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
                Instruction.done, id=message.id, data=Instruction.start
            )

            self.return_result(returnmessage)

        # Building a thread to handle the work return queue.

        # Building a thread that starts the worker processes.
        t = threading.Thread(target=dostart, name="start workers")
        t.start()
        t.join()
        print("done starting")

    def stop(self, message: Message | None = None):
        """will shut down the worker processes by spawning a thread for each worker
        that then tries to stop the workers in parallel. Returns when all worker processes are stopped.
        """

        # def stoptask():
        #     stoptasks = [
        #         threading.Thread(
        #             target=worker.quit, name=f"Workerproxy{worker.name} quit"
        #         )
        #         for worker in self.workers.values()
        #     ]
        #     for task in stoptasks:
        #         task.start()
        #         time.sleep(0.5)
        #     for task in stoptasks:
        #         task.join()
        
        def stop_task():
            for worker in self.workers.values():
                print(f'worker{worker.name} quitting')
                worker.quit()
                
        

        stopthread = threading.Thread(target=stop_task, name="worker shutdown thread")
        stopthread.start()
        print("stopthread started")
        # time.sleep(0.5)
        # self.command_return_queue.put_nowait('JeMeODeRRR')
        stopthread.join()
        print("stopthread joined")
        # if message:
        #     returnmessage = Message(
        #         Instruction.done, message.id, f"stoptask for workers"
        #     )
        #     self.return_result(returnmessage)

    def quit(self, message: Message | None = None):
        """Will shut down this workermanager thread and all it's subprocesses."""
        self.stop()

        self.running = False
        
        self._handle_result_queue_thread.join()
        self._handle_return_command_queue_thread.join()
        
        if message:
            returnmessage = Message(Instruction.done, message.id, message.instruction)
            self.return_result(returnmessage)
        
        

    def run(self):
        """Functions as the target of the newly built workermanagerthread,
        starts listening and handling further instructions from the event loop via the given queue.
        """
        self.running = True
        while self.running:
            try:
                message = self.host_command_queue.get_nowait()
                self.instructions[message.instruction](message)
            except queue.Empty:
                time.sleep(self.sleeptime)
