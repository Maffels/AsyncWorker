from AsyncWorker.asyncworker import AsyncWorker
import asyncio

def f():
    return 1+1



async def dostuff():
    async with AsyncWorker(worker_amount=1) as worker:
        res = await worker.process(f)
        print(res)
        
if __name__ == "__main__":
    asyncio.run(dostuff())