---
title: Considerations
parent: User Guide
layout: default
nav_order: 4
permalink: /docs/UserGuide/Considerations
---

# Considerations
{: .no_toc }

1. TOC
{:toc}

## How Many Workers?

Pyper is a framework for concurrent programming, so it is worth discussing in a little more detail what the [concurrency](TaskParameters#concurrency) parameter is doing. Under the hood, this is an integer which determines:

* How many [threads](https://docs.python.org/3/library/threading.html) to spin up (for a synchronous task)
* Or how many [asyncio Tasks](https://docs.python.org/3/library/asyncio-task.html) to create (for an asynchronous task) 

Therefore, a task will only benefit from a higher concurrency value if the function itself fundamentally benefits from concurrent execution.

When does a function benefit from concurrency? In short, when it can make progress off the CPU after releasing the [GIL](https://wiki.python.org/moin/GlobalInterpreterLock), (by doing something that doesn't require computation) for example by:

* Performing a sleep
* Sending a network request
* Reading from a database

A function with this property is referred as IO-bound, whereas a function that hogs the CPU intensely, without releasing the GIL, is called CPU-bound. This includes all 'heavy-computation' type operations like:

* Crunching numbers
* Parsing text data
* Sorting and searching

{: .warning}
Increasing the number of workers for a CPU-bound task does not improve performance

To experiment for yourself, try running the following script with a range of concurrency values. You'll notice that a higher concurrency will in fact decrease performance, due to the overhead of creating threads.

```python
import time
from pyper import task

def get_data(limit: int):
    for i in range(limit):
        yield i

def long_computation(data: int):
    for i in range(1_000_000):
        data += i
    return data

def print_sum(data):
    total = 0
    for i in data:
        total += i
    print(total)

def main(computation_concurrency: int = 1):
    run = task(get_data, daemon=True) \
        | task(long_computation, concurrency=computation_concurrency, daemon=True) \
        > print_sum
    run(1000)

if __name__ == "__main__":
    main()
```

## To daemon or not to daemon?

The advantage of using `daemon` threads is that they do not prevent the main program from exiting and therefore allow errors to be raised immediately. The danger of using `daemon` threads is that they end _abruptly_ when the program terminates, which can lead to memory leaks and improper resource cleanup.

Therefore, there is a simple consideration that determines whether to set `daemon=True` on a particular task:

{: .info}
Tasks can be created with `daemon=True` when they do NOT reach out to external resources

This includes all **pure functions** (functions which simply take an input and generate an output, without mutating external state).

Functions that should _not_ use `daemon` threads include:
* Writing to a database
* Processing a file
* Making a network request

Recall that only synchronous tasks can be created with `daemon=True`.