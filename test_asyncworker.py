import pytest
import asyncio
import math
from typing import AsyncIterable

import asyncworker.asyncworker as asyncworker

# All test coroutines will be treated as marked.
pytestmark = pytest.mark.asyncio

# Number of separate "jobs" to queue
NUM_JOBS = 100
# Number of CPU hardware threads to use, "None" uses the auto setting
HARDWARE_THREADS = [1, 2, 4, None]


# Maximum int size when looking for prime numbers starting at 1:
MAX_NUM = 4200
# CPU-bound workload
def find_primes_workload(max_num):
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


# Exception raising workload
def excepting_workload():
    raise TypeError("test typeerror")


answer = find_primes_workload(MAX_NUM)


@pytest.fixture
async def default_worker() -> AsyncIterable[asyncworker.AsyncWorker]:
    for num_workers in HARDWARE_THREADS:
        worker = asyncworker.AsyncWorker(num_workers)
        await worker.initialize()
        yield worker
        await worker.quit()


@pytest.fixture
async def context_worker() -> AsyncIterable[asyncworker.AsyncWorker]:
    for num_workers in HARDWARE_THREADS:
        async with asyncworker.AsyncWorker(num_workers) as worker:
            yield worker


@pytest.fixture
async def worker_types(default_worker, context_worker) -> AsyncIterable[asyncworker.AsyncWorker]:
    async for worker in default_worker:
        yield worker
    async for worker in context_worker:
        yield worker


async def test_test():
    assert True == True




async def test_process_default(worker_types: AsyncIterable[asyncworker.AsyncWorker]):
    async for worker in worker_types:
        jobs = [
            asyncio.create_task(worker.process(find_primes_workload, MAX_NUM))
            for _ in range(NUM_JOBS)
        ]
        for completed in asyncio.as_completed(jobs):
            assert await completed == answer


# async def test_registered_function(worker_types: AsyncIterable[asyncworker.AsyncWorker]):
#     async for worker in worker_types:
#         async_work = await worker.register_callable(find_primes_workload)
#         jobs = [asyncio.create_task(async_work(MAX_NUM)) for _ in range(NUM_JOBS)]
#         for completed in asyncio.as_completed(jobs):
#             assert await completed == answer


async def test_process_exception(worker_types: AsyncIterable[asyncworker.AsyncWorker]):
    async for worker in worker_types:
        with pytest.raises(TypeError) as e:
            job = asyncio.create_task(worker.process(excepting_workload))
            await job


# async def test_registered_exception(worker_types: AsyncIterable[asyncworker.AsyncWorker]):
#     async for worker in worker_types:
#         async_work = await worker.register_callable(find_primes_workload)
#         with pytest.raises(TypeError) as e:
#             job = asyncio.create_task(async_work())
#             await job
