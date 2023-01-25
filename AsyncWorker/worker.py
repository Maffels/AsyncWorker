import multiprocessing
import queue
import threading
import time

from typing import Any, Callable

from .data_classes import (
    Instruction,
    Message,
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
        workername: str,
        sleeptime: float = 0.1,
    ):
        self.command_send_queue = command_send_queue
        self.command_return_queue = command_return_queue
        self.result_queue= result_queue
        self.work_queue = work_queue
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
            self.command_return_queue.put(
                Message(Instruction.done, message.id, message.instruction)
            )
        except Exception as e:
            self.command_return_queue.put(Message(Instruction.exception, message.id, e))

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

        self.result_queue.put(returnmessage)

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
        self.command_return_queue.put(
            Message(Instruction.done, id=message.id, data=f"started worker {self.name}")
        )
        # self.run()

    def stop(self, message: Message | None = None):
        # For telling this instance to stop checking the shared work_queue for work, but not quit.
        self.do_work = False
        self.workthread.join()
        if message:
            self.command_return_queue.put(
                Message(
                    Instruction.done, id=message.id, data=f"stopped worker {self.name}"
                )
            )

    def quit(self, message: Message | None = None):
        # Will shut down this worker process.
        self.stop()
        self.running = False
        print(f'worker{self.name} quitting with message:{message}')
        if message:
            try:
                self.command_return_queue.put(
                    Message(
                        Instruction.done, id=message.id, data=f"quit worker {self.name}"
                    )
                )
                print(f'worker{self.name} sent quit confirmation')
                print(f'queue empty for worker? {self.command_return_queue.empty()}')
            except BrokenPipeError as e:
                print(e)

    def run(self):
        # Main work loop checking for messages from the workermanager thread.
        self.running = True
        while self.running:
            try:
                message = self.command_send_queue.get_nowait()
                self.handle_message(message)
            except queue.Empty:
                time.sleep(self.sleeptime)
