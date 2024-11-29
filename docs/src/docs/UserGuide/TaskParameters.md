---
title: Task Parameters
parent: User Guide
layout: default
nav_order: 2
permalink: /docs/UserGuide/TaskParameters
---

# Task Parameters
{: .no_toc }

1. TOC
{:toc}

> For convenience, we will use the following terminology on this page:
> * **Producer**: The _first_ task within a pipeline
> * **Producer-consumer**: Any task after the first task within a pipeline

## `join`

* **type:** `int`
* **default:** `False`

```python
from typing import Iterable
from pyper import task

@task
def step1(x: int):
    yield x + 1
    yield x + 2
    yield x + 3

@task(join=True)
def step2(data: Iterable[int]):
    for item in data:
        yield item * 2

pipeline = step1 | step2
for output in pipeline(0):
    print(output)
# Prints:
# 2
# 4
# 6
```

The parameter `join` takes a `boolean` value. When `False`, a producer-consumer takes each individual output from the previous task as input. When `True`, a producer-consumer takes a stream of inputs from the previous task.

{: .warning}
A producer _cannot_ have `join` set as `True`

A task with `join=True` can also be run concurrently, which means that multiple workers will pull from the previous task in a thread-safe way.

## `concurrency`

* **type:** `int`
* **default:** `1`

```python
import time
from pyper import task

@task
def step1(x: int):
    yield x + 1
    yield x + 2
    yield x + 3

@task(concurrency=3)
def step2(data: int):
    time.sleep(data)
    return "Done"

pipeline = step1 | step2
for output in pipeline(0):
    print(output)
# Runs in ~3 seconds
```

The parameter `concurrency` takes a `int` value which determines the number of workers executing the task concurrently. Note that only [IO-bound functions](https://stackoverflow.com/questions/868568) benefit from concurrent execution.

{: .warning}
A producer _cannot_ have `concurrency` set greater than `1`

## `throttle`

* **type:** `int`
* **default:** `0`

```python
import time
from pyper import task

@task(throttle=5000)
def step1():
    for i in range(1_000_000):
        yield i

@task
def step2(data: int):
    time.sleep(10)
    return data
```

The parameter `throttle` takes a `int` value which determines the maximum queue size between a given task and the next. The purpose of this parameter is to give finer control over memory in situations where:

* A producer/producer-consumer generates data very quickly
* A producer-consumer/consumer processes that data very slowly

In the example above, workers on `step1` are paused after `5000` values have been generated, until workers for `step2` are ready to start processing again. If no throttle were specified, workers for `step1` would quickly flood its output queue with up to `1_000_000` values.

## `daemon`

* **type:** `bool`
* **default:** `False`

```python
import time
from pyper import task

@task(daemon=True)
def step1():
    yield 1
    yield 2
    yield 3

@task(concurrency=3, daemon=True)
def step2(data: int):
    if data == 2:
        raise RuntimeError("An error has occurred")
    time.sleep(10)
    return data

pipeline = step1 | step2
for output in pipeline():
    print(output)
# Raises RuntimeError immediately
```

The parameter `daemon` takes a `boolean` value which determines whether thread workers for the task are [daemon threads](https://www.geeksforgeeks.org/python-daemon-threads/).

The purpose of this parameter is to allow programs to fail fast -- when an error is thrown somewhere, we don't always want to wait for all threads to finish executing, but instead we want the program to exit.

The example above allows the main thread to raise a `RuntimeError` immediately. Without specifying `daemon=True`, the program would take ~10 seconds to run, as it would wait for each worker in `step2` to finish its job.

See some [considerations](Considerations#to-daemon-or-not-to-daemon) for when to set this flag.

{: .warning}
An asynchronous task _cannot_ have `daemon` set as `True`

## `bind`

* **type:** `Optional[Tuple[Tuple, Dict]]`
* **default:** `None`

```python
from pyper import task

def step1():
    yield 1
    yield 2
    yield 3

def step2(data: int, multiplier: int):
    return data * multiplier

pipeline = task(step1) | task(step2, bind=task.bind(multiplier=10))
for output in pipeline():
    print(output)
# Prints:
# 10
# 20
# 30
```

The parameter `bind` allows additional `args` and `kwargs` to be bound to a task when creating a pipeline (via [functools.partial](https://www.learnpython.org/en/Partial_functions)).

Given that each producer-consumer expects to be given one input argument, the purpose of the `bind` parameter is to allow functions to be defined flexibly in terms of the inputs they wish to take, as well as allowing tasks to access external states, like contexts.

`task.bind(*args, **kwargs)` is a utility method that can be used to supply these additonal arguments.