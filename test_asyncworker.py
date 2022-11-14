import pytest
import asyncio
import math

import asyncworker

# All test coroutines will be treated as marked.
pytestmark = pytest.mark.asyncio

# Number of separate "jobs" to queue
NUM_JOBS = 420
# Number of CPU hardware threads to use, "None" uses the auto setting
HARDWARE_THREADS = [1, 2, 4, None]


# Maximum int size when looking for prime numbers starting at 1:
MAX_NUM = 4200
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


answer = find_primes(MAX_NUM)


@pytest.fixture
async def default_worker():
    worker = asyncworker.AsyncWorker(HARDWARE_THREADS)
    await worker.run()
    yield worker
    await worker.quit()


@pytest.fixture
async def context_worker():
    async with asyncworker.AsyncWorker(HARDWARE_THREADS) as worker:
        yield worker


@pytest.fixture
async def workers():
    for n in HARDWARE_THREADS:
        async with asyncworker.AsyncWorker(n) as worker:
            yield worker
        worker = asyncworker.AsyncWorker(n)
        await worker.run()
        yield worker
        await worker.quit()


async def test_test():
    assert True == True


async def test_process_default(workers: list[asyncworker.AsyncWorker]):
    async for worker in workers:
        jobs = [asyncio.create_task(worker.process(find_primes, MAX_NUM)) for _ in range(NUM_JOBS)]
        for completed in asyncio.as_completed(jobs):
            assert await completed == answer
            
async def test_registered_function(workers: list[asyncworker.AsyncWorker]):
    async for worker in workers:
        async_work = await worker.register_callable(find_primes)
        jobs = [asyncio.create_task(async_work(MAX_NUM)) for _ in range(NUM_JOBS)]
        for completed in asyncio.as_completed(jobs):
            assert await completed == answer
           
