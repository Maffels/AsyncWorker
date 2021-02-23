# AsyncWorker
Asyncio multithreading bridge module for Python

Module for handling threaded CPU-bound work asynchronously within the asyncio framework. 
It is aimed to function as an easy to use bridge between I/O-bound and CPU-bound code.

Can be used in an async context manager or by constructing an instance and calling .run() on it.
When not using it in a context manager, needs a call to .run() to start, and .quit() to stop.
    

Callables can currently be processed in two ways:

    - Callables can be processed directly by calling .process(Callable, *args, **kwargs), 
      which is an awaitable that returns as soon as the callable with its args has been processed
      
      async with AsyncWorker() as asyncworker:
        result = await asyncworker.process(CPU_bound_function, *args, **kwargs)

      
    - Callables can be registered with the class by calling .register(Callable). 
      This will return a coroutine that functions as an awaitable substitute for the original callable,
      which can be called with the same arguments and returns as soon as the result is ready.
      
      async with AsyncWorker() as asyncworker:
        CPU_bound_coroutine = await asyncworker.register_callable(CPU_bound_function)
        result = await CPU_bound_coroutine(*args, **kwargs)


Will use total-1 system processing threads by default, but can this be specified by including worker_amount=n.
    
Looks like its somewhat quicker than using a ProcessPoolExecutor with the standard executor.submit method, 
but way slower than executor.map. 
