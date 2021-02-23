import asyncio
from asyncworker import AsyncWorker
import math
import time

""" Simple example which uses find_primes as a CPU-bound task scheduled from inside an event loop.

    Use MAX_NUM to specify the maximum integer for looking up prime numbers
    Use NUB_JOBS to specify the amount of tasks the AsyncWorker should schedule.

    CPU_THREADS Optionally specifies the amount of compute threads AsyncWorker should use.
"""


MAX_NUM = 1000
NUM_JOBS = 1337
CPU_THREADS = None


# CPU-bound task
def find_primes(maxnum):
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
    for num in range(maxnum + 1):
        outcome = is_prime(num)
        if outcome:
            primes.append(outcome)
    return primes


async def _process_callable_example(asyncworker):
    # Example showing the use of AsyncWorker.process:

    print(f"queuing up {NUM_JOBS} find_primes processing tasks")
    
    starttime = time.time()
    tasks = [
        asyncio.create_task(asyncworker.process(func=find_primes, maxnum=MAX_NUM))
        for _ in range(NUM_JOBS)
    ]
    await asyncio.wait(tasks)
    
    print(f"processing find_primes done in {time.time() - starttime} seconds)")


async def _register_callable_example(asyncworker):
    # Example showing the use of AsyncWorker.register_callable and its returned coroutine:

    print("registering the find_primes function to the worker.")
    
    starttime = time.time()
    async_find_primes = await asyncworker.register_callable(find_primes)
    
    print(f"queuing up {NUM_JOBS} async_find_primes coroutine tasks")
    tasks = [asyncio.create_task(async_find_primes(MAX_NUM)) for _ in range(NUM_JOBS)]
    await asyncio.wait(tasks)
    
    print(f"processing async_find_primes done in {time.time() - starttime} seconds)")




async def main():

    async with AsyncWorker() as asyncworker:
        print(f"Started AsyncWorker with {asyncworker.worker_amount} worker processes")

        await _process_callable_example(asyncworker)

        await _register_callable_example(asyncworker)


if __name__ == "__main__":
    asyncio.run(main())
