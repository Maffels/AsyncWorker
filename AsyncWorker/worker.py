import multiprocessing
import queue
import threading
import time

from typing import Any, Callable

from .data_classes import (
    Instruction,
    Message,
    Worker_Message,
    SavedCallable,
    Process_Job,
    Registered_Job,
)


class _WorkerProcess:
    """Class describing the worker that will run inside of its own multiprocessing.Process instance.
    Will check its hostconnection for instructions, and its work_queue for work when enabled.
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
        command_send_queue: multiprocessing.Queue,
        command_return_queue: multiprocessing.Queue,
        work_queue: multiprocessing.Queue,
        result_queue: multiprocessing.Queue,
        workername: int,
        sleeptime: float = 0.1,
    ):
        self.command_send_queue = command_send_queue
        self.command_return_queue = command_return_queue
        self.result_queue = result_queue
        self.work_queue = work_queue
        self.name = workername
        self.sleeptime = sleeptime
        self.instructions = self.get_instructions()
        self.registered_callables = {}
        self.do_work = False
        self.running = False

    def register_callable(self, message: Message):
        # Used to register the given callable with this instance.
        print(f"workerprocess{self.name} registering callable: {message}")
        try:
            callable_id = message.data.id
            new_callable = message.data.callable
            self.registered_callables[callable_id] = new_callable
            self.command_return_queue.put(
                Worker_Message(
                    Instruction.done,
                    id=message.id,
                    data=message.instruction,
                    worker=self.name,
                )
            )
            print(f"workerprocess returned message with id:{message.id}")
        except Exception as e:
            print(f"worker{self.name} encountered: {e}")
            self.command_return_queue.put(
                Worker_Message(Instruction.exception, message.id, e, self.name)
            )

    def process_registered(self, message: Message):
        # Will call the specified callable with the given parameters and send back the results.
        callable_id = message.data.id

        saved_callable = self.registered_callables[callable_id]

        args = message.data.args
        kwargs = message.data.kwargs
        try:
            result = saved_callable(*args, **kwargs)
            returnmessage = Worker_Message(
                Instruction.done, message.id, result, self.name
            )
        except Exception as e:
            result = e
            returnmessage = Worker_Message(
                Instruction.exception, message.id, result, self.name
            )

        self.result_queue.put(returnmessage)

    def process(self, message: Message):
        # Will process the given callable with the given parameters and send back the results.
        args = message.data.args
        kwargs = message.data.kwargs
        function = message.data.function
        try:
            result = function(*args, **kwargs)
            returnmessage = Worker_Message(
                Instruction.done, message.id, result, self.name
            )
        except Exception as e:
            result = e
            returnmessage = Worker_Message(
                Instruction.exception, message.id, result, self.name
            )
        self.result_queue.put(returnmessage)

    def handle_message(self, message: Message):
        # Uses the included instruction to call the correct function for the enclosed data
        #  with the help of the instruction mapping
        self.instructions[message.instruction](message)

    def work(self):
        # Used as target for the workerthread, pulls work from the work_queue and hands it over for processing.
        while self.do_work:
            try:
                work = self.work_queue.get_nowait()
                self.handle_message(work)
                # return True
            except queue.Empty:
                time.sleep(self.sleeptime)

    def start(self, message: Message):
        # Will start a workerprocess that'll handle the work put in the work_queue
        self.do_work = True
        self.workthread = threading.Thread(
            target=self.work, name=f"workerprocess {self.name} workthread"
        )
        self.workthread.start()
        self.command_return_queue.put_nowait(
            Worker_Message(
                Instruction.done,
                id=message.id,
                data=Instruction.start,
                worker=self.name,
            )
        )
        # self.run()

    def stop(self, message: Message | None = None):
        # For telling this instance to stop checking the shared work_queue for work, but not quit.
        self.do_work = False
        self.workthread.join()
        if message:
            self.command_return_queue.put_nowait(
                Worker_Message(
                    Instruction.done,
                    id=message.id,
                    data=Instruction.stop,
                    worker=self.name,
                )
            )

    def quit(self, message: Message | None = None):
        # Will shut down this worker process.
        print(f"workerprocess{self.name} got {message}")
        self.stop()
        print(f"workerprocess{self.name} stopped")
        self.running = False

        if message:
            return_message = Worker_Message(
                Instruction.done,
                id=message.id,
                data=f"quit worker {self.name},",
                worker=self.name,
            )
            try:

                self.command_return_queue.put_nowait(return_message)
                print(
                    f"workerprocess{self.name} put {return_message} in command_return_queue"
                )
            except BrokenPipeError as e:
                pass

    def run(self):
        # Main work loop checking for messages from the workermanager thread.
        self.running = True
        while self.running:
            try:
                message = self.command_send_queue.get_nowait()
                self.handle_message(message)
            except queue.Empty:
                time.sleep(self.sleeptime)
