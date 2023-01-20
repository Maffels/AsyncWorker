import asyncio
import math
import time
import os

from AsyncWorker.asyncworker import AsyncWorker

""" Example which uses find_primes as a CPU-bound task scheduled from inside an event loop.

    Use MAX_NUM to specify the maximum integer for looking up prime numbers.
    Use NUM_JOBS to specify the amount of tasks the AsyncWorker should schedule.

    CPU_THREADS Optionally specifies the amount of compute threads AsyncWorker should use.
"""


MAX_NUM = 50000
NUM_JOBS = 100
CPU_THREADS = None


# CPU-bound task
def find_primes(max_num):
    def is_prime(n):
        if n < 2:
            return False
        if n == 2:
            return n
        if n % 2 == 0:
            return False
        sqrt_n = int(math.floor(math.sqrt(n)))
        for i in range(3, sqrt_n + 1, 2):
            if n % i == 0:
                return False
        return n

    primes = []
    for num in range(max_num + 1):
        outcome = is_prime(num)
        if outcome:
            primes.append(outcome)
    return primes


# default processing of cpu-bound tasks to compare against
def default_process_workload():

    starttime = time.time()
    for _ in range(NUM_JOBS):
        find_primes(MAX_NUM)
    time_elapsed = time.time() - starttime

    return time_elapsed


# Simplest example directly running a cpu-bound function within the asyncworker context
async def simple_example():

    # Starting the AsyncWorker Class via a context manager for ease of use
    async with AsyncWorker(CPU_THREADS) as worker:
        # Calling asyncworker.process(callable, *args, *kwargs) and awaiting the result
        result = await worker.process(find_primes, 1000)
        print(result)


# Simplest example of running a pre-registered cpu-bound function within the asyncworker context
async def simple_registered_example():

    # Starting the AsyncWorker class via a context manager for ease of use
    async with AsyncWorker(CPU_THREADS) as worker:
        # Registering a cpu-bound callable by calling asyncworker.register_callable(callable)
        awaitable_workload = await worker.register_callable(find_primes)
        # Awaiting the processing of the pre-registered callable with the given arguments
        # by calling registered_callabe(*args,*kwargs) and awaiting the result
        result = await awaitable_workload(1000)
        print(result)

    # Simplest example of using AsyncWorker without the use of a context manager


async def simple_no_context_manager():

    # getting an instance of AsyncWorker
    worker = AsyncWorker(CPU_THREADS)

    # awaiting the initialisation of the workers by calling asyncworker.init()
    await worker.init()

    # Calling asyncworker.process(callable, *args, *kwargs) and awaiting the result
    result = await worker.process(find_primes, 1000)
    print(result)

    # awaiting the teardown of the workers by calling asyncworker.quit()
    await worker.quit()


# Example showing the use of AsyncWorker.process with a higher number of tasks
async def process_multiple_callable_example():

    # Starting the AsyncWorker class via a context manager
    async with AsyncWorker(CPU_THREADS) as worker:
        starttime = time.time()
        # queueing up processing tasks by using asyncio.create_task(asyncworker.process(workload,*args,*kwargs))
        tasks = [
            asyncio.create_task(worker.process(find_primes, MAX_NUM))
            for _ in range(NUM_JOBS)
        ]
        # awaiting the work being done
        await asyncio.wait(tasks)
        time_elapsed = time.time() - starttime

    return time_elapsed


# Example showing the use of AsyncWorker.register_callable and its returned coroutine with a higher number of tasks
async def process_multiple_registered_callable_example():

    # Starting the AsyncWorker class via a context manager
    async with AsyncWorker(CPU_THREADS) as worker:

        # Registering a cpu-bound callable by calling asyncworker.register_callable(callable)
        async_find_primes = await worker.register_callable(find_primes)
        starttime = time.time()
        # queueing up processing tasks by using asyncio.create_task(asyncworker.process(workload,*args,*kwargs))
        tasks = [
            asyncio.create_task(async_find_primes(MAX_NUM)) for _ in range(NUM_JOBS)
        ]

        # awaiting the work being done
        await asyncio.wait(tasks)
        time_elapsed = time.time() - starttime

    return time_elapsed


async def main():
    cpu_threads = CPU_THREADS if CPU_THREADS else (os.cpu_count() - 1 if os.cpu_count() > 2 else 1)
    print(f"Running example of AsyncWorker using {cpu_threads} threads")
    print(
        f"Processing the finding of prime numbers up til {MAX_NUM}, {NUM_JOBS} times \n"
    )
    print(
        "starting with the default single threaded processing, this might take some time..."
    )
    default_time = default_process_workload()
    print(
        f"single threaded processing of find_primes done in {default_time} seconds) \n"
    )

    print("First async batch example:")
    callable_time = await process_multiple_callable_example()
    print(f"Async processing of find_primes done in {callable_time} seconds) \n")
    print("Second async batch example:")
    registered_callable_time = await process_multiple_registered_callable_example()
    print(
        f"Async processing of pre-registered async_find_primes done in {registered_callable_time} seconds."
    )

    print(
        f"Average speed-up for this example: {default_time/((callable_time + registered_callable_time)/2)}"
    )


if __name__ == "__main__":
    asyncio.run(main())
