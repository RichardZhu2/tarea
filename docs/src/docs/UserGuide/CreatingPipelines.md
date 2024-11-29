---
title: Creating Pipelines
parent: User Guide
layout: default
nav_order: 1
permalink: /docs/UserGuide/CreatingPipelines
---

# Creating Pipelines
{: .no_toc }

1. TOC
{:toc}

## The `task` Decorator

A `Pipeline` represents a flow of data. In Pyper, functions are the building blocks used to create pipelines; the simplest way to do so is with the `task` decorator:

```python
from pyper import task, Pipeline

@task
def func(x: int):
    return x + 1

assert isinstance(func, Pipeline)
```

This creates a `Pipeline` object consisting of one 'task' (one step of data transformation). Later, pipelines can be [combined](CombiningPipelines) to form composite pipelines that handle a series of tasks.

The `task` decorator can also be used more dynamically, which is slightly preferable in general as this separates execution logic from the functional definitions themselves:

```python
from pyper import task

def func(x: int):
    return x + 1

pipeline = task(func)
```

In addition to functions, anything `callable` in Python can be wrapped in `task` in the same way:

```python
from pyper import task

class Doubler:
    def __call__(self, x: int):
        return 2 * x

pipeline1 = task(Doubler())
pipeline2 = task(lambda x: x - 1)
pipeline3 = task(range)
```

## Pipeline Usage

In keeping with functional design, a `Pipeline` is itself essentially a function, returning a [Generator](https://wiki.python.org/moin/Generators) object (Python's mechanism for lazily iterating through data).

```python
from pyper import task

def func(x: int):
    return x + 1

pipeline = task(func)
for output in pipeline(x=0):
    print(output)
# Prints:
# 1
```

{: .info}
A Pipeline always takes the input of its first task, and yields each output from its last task

A pipeline that generates _multiple_ outputs can be created through functions that use `yield`:

```python
from pyper import task

def func(x: int):
    yield x + 1
    yield x + 2
    yield x + 3

pipeline = task(func)
for output in pipeline(x=0):
    print(output)
# Prints:
# 1
# 2
# 3
```

## Asynchronous Code

Asynchronous functions/callables are used to create `AsyncPipeline` objects, which behave in an intuitively analogous way to `Pipeline`:

```python
import asyncio
from pyper import task

async def func(x: int):
    return x + 1

async def main():
    pipeline = task(func)
    async for output in pipeline(x=0):
        print(output)

asyncio.run(main())
# Prints:
# 1
```