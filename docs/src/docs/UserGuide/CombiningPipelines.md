---
title: Combining Pipelines
parent: User Guide
layout: page
nav_order: 3
permalink: /docs/UserGuide/CombiningPipelines
---

# Combining Pipelines
{: .no_toc }

1. TOC
{:toc}

## Piping and the `|` Operator 

The `|` operator (inspired by UNIX syntax) is used to pipe one pipeline into another. This is syntactic sugar for the `Pipeline.pipe` method.

```python
from pyper import task, Pipeline

p1 = task(lambda x: x + 1)
p2 = task(lambda x: 2 * x)
p3 = task(lambda x: x - 1)

new_pipeline = p1 | p2 | p3
assert isinstance(new_pipeline, Pipeline)
# OR
new_pipeline = p1.pipe(p2).pipe(p3)
assert isinstance(new_pipeline, Pipeline)
```

Intuitively, this represents defining a new function that:

1. takes the inputs of the first task
2. takes the outputs of each task and passes them as the inputs of the next task
3. finally, generates each output from the last task

Composite pipelines can of course be combined further.

```python
p0 = task(lambda x: x)
p4 = task(lambda x: x ** 2)
new_new_pipeline = p0 | new_pipeline | p4
new_new_new_pipeline = new_pipeline | new_new_pipeline
```

## Consumer Functions and the `>` Operator

It is often useful to define resuable functions that process the results of a pipeline, which we'll call a 'consumer'. For example:

```python
import json
from typing import Dict, Iterable

from pyper import task

@task
def step1(limit: int):
    for i in range(limit):
        yield {"data": i}

@task
def step2(data: Dict):
    return data | {"hello": "world"}

class JsonFileWriter:
    def __init__(self, filepath):
        self.filepath = filepath
    
    def __call__(self, data: Iterable[Dict]):
        data_list = list(data)
        with open(self.filepath, 'w', encoding='utf-8') as f:
            json.dump(data_list, f, indent=4)

pipeline = step1 | step2  # The pipeline
writer = JsonFileWriter("data.json")  # A consumer
writer(pipeline(limit=10))  # Run
```

The `>` operator (again inspired by UNIX syntax) is used to pipe a `Pipeline` into a consumer function (any callable that takes a data stream) returning simply a function that handles the 'run' operation. This is syntactic sugar for the `Pipeline.consume` method.
```python
run = step1 | step2 > JsonFileWriter("data.json")
run(limit=10)
# OR
run = step1.pipe(step2).consume(JsonFileWriter("data.json"))
run(limit=10)
```

{: .info}
Pyper comes with fantastic IDE intellisense support which understands these operators, and will always show you which variables are `Pipeline` or `AsyncPipeline` objects; this also preserves type hints from your own functions, showing you the parameter and return type specs for each pipeline or consumer

## Asynchronous Code

Recall that an `AsyncPipeline` is created from an asynchronous function:

```python
from pyper import task, AsyncPipeline

async def func():
    yield 1
    yield 2

assert isinstance(task(func), AsyncPipeline)
```

When combining pipelines, the following rule applies:

* `Pipeline` + `Pipeline` = `Pipeline`
* `Pipeline` + `AsyncPipeline` = `AsyncPipeline`
* `AsyncPipeline` + `Pipeline` = `AsyncPipeline`
* `AsyncPipeline` + `AsyncPipeline` = `AsyncPipeline`

In other words:

{: .info}
A pipeline that contains _at least one_ asynchronous task becomes asynchronous

This reflects a (sometimes awkward) trait of Python, which is that `async` and `await` syntax bleeds everywhere -- as soon as one function is defined asynchronously, you often find that many other parts program need to become asynchronous. Hence, the sync vs async decision is usually one made at the start of designing an application.

The Pyper framework slightly assuages the developer experience by allowing synchronous functions to be used inside of an `AsyncPipeline`. Pyper will handle unifying synchronous and asynchronous execution under the hood.

Consumer functions will, however need to adapt to asynchronous output. For example:

```python
import asyncio
import json
from typing import AsyncIterable, Dict

from pyper import task

@task
async def step1(limit: int):
    for i in range(limit):
        yield {"data": i}

@task
def step2(data: Dict):
    return data | {"hello": "world"}

class JsonFileWriter:
    def __init__(self, filepath):
        self.filepath = filepath
    
    async def __call__(self, data: AsyncIterable[Dict]):
        data_list = [row async for row in data]
        with open(self.filepath, 'w', encoding='utf-8') as f:
            json.dump(data_list, f, indent=4)

async def main():
    run = step1 | step2 > JsonFileWriter("data.json")
    await run(limit=10)

if __name__ == "__main__":
    asyncio.run(main())
```