---
title: Creating Pipelines
parent: User Guide
layout: default
nav_order: 2
permalink: /docs/UserGuide/CreatingPipelines
---

# Creating Pipelines
{: .no_toc }

* TOC
{:toc}

## The `task` Decorator

Pyper's `task` decorator is the means by which we instantiate pipelines and control their behaviour:

```python
from pyper import task, Pipeline

@task
def func(x: int):
    return x + 1

assert isinstance(func, Pipeline)
```

This creates a `Pipeline` object consisting of one 'task' (one step of data transformation). 

The `task` decorator can also be used more dynamically, which is preferable in most cases as this separates execution logic from the functional definitions themselves:

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

{: .info}
The internal behaviour of a pipeline (e.g number of workers) is controlled by the different parameters for `task`. Refer to the [API Reference](../ApiReference/task)

## Pipeline Usage

Recall that a `Pipeline` is itself essentially a function. Pipelines return a [Generator](https://wiki.python.org/moin/Generators) object (Python's mechanism for lazily iterating through data).

```python
from pyper import task

def func(x: int):
    return x + 1

if __name__ == "__main__":
    pipeline = task(func)
    for output in pipeline(x=0):
        print(output)
        # Prints:
        # 1
```

{: .info}
A Pipeline always takes the input of its first task, and yields each output from its last task

A pipeline that generates _multiple_ outputs can be created using the `branch` parameter:

```python
from pyper import task

def func(x: int):
    yield x + 1
    yield x + 2
    yield x + 3

if __name__ == "__main__":
    pipeline = task(func, branch=True)
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
        # Prints:
        # 1

if __name__ == "__main__":
    asyncio.run(main())
```

Note that `AsyncPipeline` objects return an `AsyncGenerator` which is iterated over with `async for` syntax.