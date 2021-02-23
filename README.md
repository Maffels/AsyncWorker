# AsyncWorker
Asyncio multithreading bridge module for Python

    Module for handling threaded CPU-bound work asynchronously within the asyncio framework. 
    It is aimed to function as a easy to use bridge between I/O-bound and CPU-bound code.

    Can be used in a (async) context manager or by contructing an instance and calling .run() on it.
    When not using it in a context manager, needs .run() to start,
    and .quit() or it will keep itself alive waiting for work.
    

    Callables can currently be processed in two ways:
    - Callables can be processed directly by calling .process(Callable, *args, **kwargs), 
      which is an awaitable that returns as soon as the callable with it's args has been processed.
    - Callables can be registered with the class by calling .register(Callable). 
      This will return a coroutine that is an awaitable substitute for the original callable,
      which can be called with the same arguments and returns as soon as the result is ready. 
    
    Will by default use total-1 system processing threads, but can this be specified by including worker_amount=n.
    
    Looks to be somewhat quicker than using a ProcessPoolExecutor with the standard executor.submit method, 
    but way slower than executor.map. 
    Ideally in the future AsyncWorker would be somewhat closer to the performance of executor.map, 
    but not losing the current flexibility in the process might be difficult.  
