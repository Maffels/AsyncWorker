# AsyncWorker

Asyncio multithreading bridge module for Python.

A module for handling threaded CPU-bound work asynchronously within the asyncio framework, 
aimed at functioning as an easy-to-use bridge between I/O-bound and CPU-bound code.

Can also be used in an async context manager.   

CPU-bound callables can be processed by calling *initialized instance*.process(callable, args, kwargs), which returns an awaitable coroutine that will return when another process has calculated the result.

example:
      
      async with AsyncWorker() as asyncworker:
        result = await asyncworker.process(CPU_bound_function, *args, **kwargs)

Another way of using Asyncworker is by registring a cpu-bound callable beforehand, receiving the awaitable version in return for later use.      

an example for this:

      async with AsyncWorker() as asyncworker:
        CPU_bound_coroutine = await asyncworker.register_callable(CPU_bound_function)
        result = await CPU_bound_coroutine(*args, **kwargs)

Will use total-1 system processing threads by default, but can this be specified by including worker_amount=n.
    

