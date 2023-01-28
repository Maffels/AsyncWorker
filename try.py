from AsyncWorker.asyncworker import AsyncWorker
import asyncio

def f():
    return 1+1



async def dostuff():
    async with AsyncWorker(worker_amount=2) as worker:
        res = await worker.process(f)
        print(res)
        
async def register_stuff():
    async with AsyncWorker(worker_amount=2) as worker:
        reg = await worker.register_callable(f)
        await reg()
        
if __name__ == "__main__":
    asyncio.run(register_stuff())