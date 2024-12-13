---
title: task
parent: API Reference
layout: default
nav_order: 1
permalink: /docs/ApiReference/task
---

# pyper.task
{: .no_toc }

* TOC
{:toc}

> For convenience, we will use the following terminology on this page:
> * **Producer**: The _first_ task within a pipeline
> * **Producer-consumer**: Any task after the first task within a pipeline

## task

```python
def __new__(
    cls,
    func: Optional[Callable] = None,
    /,
    *,
    branch: bool = False,
    join: bool = False,
    workers: int = 1,
    throttle: int = 0,
    multiprocess: bool = False,
    bind: Optional[Tuple[Tuple[Any], Dict[str, Any]]] = None):
```

Used to initialize a [Pipeline](Pipeline) object, consisting of one 'task' (one functional operation).

Pipelines created this way can be [composed](../UserGuide/ComposingPipelines) into new pipelines that contain multiple tasks.

---

{: .text-green-200 .text-gamma}
**Parameters**

{: .text-beta}
### `func`

* **type:** `Optional[Callable]`
* **default:** `None`

The function or callable object defining the logic of the task. Does not need to be passed explicitly if using `@task` as a decorator.

```python
from pyper import task

@task
def add_one(x: int):
    return x + 1
```

{: .text-beta}
### `branch`

* **type:** `bool`
* **default:** `False`

When `branch` is `False`, the output of the task is the value it returns.
Setting `branch` to `True` allows a task to generate multiple outputs. This requires the task to return an `Iterable` (or `AsyncIterable`).

```python
from pyper import task

def create_data(x: int):
    return [x + 1, x + 2, x + 3]

if __name__ == "__main__":
    pipeline1 = task(create_data)
    for output in pipeline1(0):
        print(output)
        # Prints:
        # [1, 2, 3]

    pipeline2 = task(create_data, branch=True)
    for output in pipeline2(0):
        print(output)
        # Prints:
        # 1
        # 2
        # 3
```

This can be applied to generator functions (or async generator functions) to submit outputs lazily:

```python
from pyper import task

def create_data(x: int):
    yield 1
    yield 2
    yield 3

if __name__ == "__main__":
    pipeline = task(create_data, branch=True)
    for output in pipeline(0):
        print(output)
        # Prints:
        # 1
        # 2
        # 3
```

{: .text-beta}
### `join`

* **type:** `bool`
* **default:** `False`

When `join` is `False`, a producer-consumer takes each individual output from the previous task as input. When `True`, a producer-consumer takes a stream of inputs from the previous task.

```python
from typing import Iterable
from pyper import task

@task(branch=True)
def create_data(x: int):
    return [x + 1, x + 2, x + 3]

@task(branch=True, join=True)
def running_total(data: Iterable[int]):
    total = 0
    for item in data:
        total += item
        yield total

if __name__ == "__main__":
    pipeline = create_data | running_total
    for output in pipeline(0):
        print(output)
        # Prints:
        # 1
        # 3
        # 6
```

{: .warning}
A producer _cannot_ have `join` set as `True`

A task with `join=True` can also be run with multiple workers, which will pull from the previous task in a thread-safe/process-safe way.
Note, however, that the order of outputs cannot be maintained consistently when a joined task is run with more than one worker.

{: .text-beta}
### `workers`

* **type:** `int`
* **default:** `1`

The parameter `workers` takes a `int` value which determines the number of workers executing the task concurrently or in parallel.

```python
import time
from pyper import task

def slow_func(data: int):
    time.sleep(2)
    return data

if __name__ == "__main__":
    pipeline = task(range, branch=True) | task(slow_func, workers=20)
    # Runs in ~2 seconds
    for output in pipeline(20):
        print(output)
```

{: .warning}
A producer _cannot_ have `workers` set greater than `1`

{: .text-beta}
### `throttle`

* **type:** `int`
* **default:** `0`

The parameter `throttle` determines the maximum size of a task's output queue. The purpose of this parameter is to give finer control over memory in situations where:

* A producer/producer-consumer generates data very quickly
* A producer-consumer/consumer processes that data very slowly

```python
import time
from pyper import task

@task(branch=True, throttle=5000)
def fast_producer():
    for i in range(1_000_000):
        yield i

@task
def slow_consumer(data: int):
    time.sleep(10)
    return data
```

In the example above, workers on `fast_producer` are paused after `5000` values have been generated, until workers for `slow_consumer` are ready to start processing again.
If no throttle were specified, workers for `fast_producer` would quickly flood its output queue with up to `1_000_000` values, which all have to be allocated in memory.

{: .text-beta}
### `multiprocess`

* **type:** `bool`
* **default:** `False`

By default, synchronous tasks are run in `threading.Thread` workers and asynchronous tasks are run in `asyncio.Task` workers.
The `multiprocess` parameter allows synchronous tasks be be run with `multiprocessing.Process` instead, benefitting heavily CPU-bound tasks. 

```python
from pyper import task

def slow_func(data: int):
    for i in range(1, 10_000_000):
        i *= i
    return data

if __name__ == "__main__":
    pipeline = (
        task(range, branch=True)
        | task(slow_func, workers=20, multiprocess=True)
    )
    for output in pipeline(20):
        print(output)
```

{: .warning}
An asynchronous task cannot set `multiprocessing` as `True`

See some [considerations](../UserGuide/AdvancedConcepts#cpu-bound-work) for when to set this parameter.

Note, also, that normal Python multiprocessing restrictions apply:

* Only [picklable](https://docs.python.org/3/library/pickle.html#module-pickle) functions can be multiprocessed, which excludes certain types of functions like lambdas and closures.
* Arguments and return values of multiprocessed tasks must also be picklable, which excludes objects like file handles, connections, and (on Windows) generators.

{: .text-beta}
### `bind`

* **type:** `Optional[Tuple[Tuple[Any], Dict[str, Any]]]`
* **default:** `None`

The parameter `bind` allows additional `args` and `kwargs` to be bound to a task when creating a pipeline.

```python
from pyper import task

def apply_multiplier(data: int, multiplier: int):
    return data * multiplier

if __name__ == "__main__":
    pipeline = (
        task(range, branch=True)
        | task(apply_multiplier, bind=task.bind(multiplier=10))
    )
    for output in pipeline(1, 4):
        print(output)
        # Prints:
        # 10
        # 20
        # 30
```

Given that each producer-consumer expects to be given one input argument, the purpose of the `bind` parameter is to allow functions to be defined flexibly in terms of the inputs they wish to take, as well as allowing tasks to access external states, like contexts.

## task.bind

```python
@staticmethod
def bind(*args, **kwargs):
```

`task.bind` is the utility method that can be used to supply arguments to the `bind` parameter, which uses `functools.partial` under the hood.

The method accepts normal valid `*args` and `**kwargs`.
