import pytest
import asyncio
import math

import asyncworker

# All test coroutines will be treated as marked.
pytestmark = pytest.mark.asyncio

# Number of separate "jobs" to queue 
NUM_JOBS = 1337
# Number of CPU hardware threads to use, "None" uses the auto setting
CPU_THREADS = None


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


@pytest.fixture
async def worker():
    return asyncworker.AsyncWorker(CPU_THREADS) 

async def test_test(worker):
    ...