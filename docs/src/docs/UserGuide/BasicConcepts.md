---
title: Basic Concepts
parent: User Guide
layout: page
nav_order: 1
permalink: /docs/UserGuide/BasicConcepts
---

# Basic Concepts
{: .no_toc }

* TOC
{:toc}

## Pipeline Design

Pyper follows the [functional paradigm](https://docs.python.org/3/howto/functional.html), which by design maximizes the modularity and composability of data flows. This pattern takes effect in the usage API on two levels:

* Python functions are the building blocks used to create `Pipeline` objects
* `Pipeline` objects can themselves be thought of as functions

For example, to create a simple pipeline, we can wrap a function in the `task` decorator:

```python
from pyper import task

@task
def len_strings(x: str, y: str) -> int:
    return len(x) + len(y)
```

This defines `len_strings` as a pipeline consisting of a single task. It takes the parameters `(x: str, y: str)` and generates `int` outputs from an output queue:

<img src="../../assets/img/diagram1.png" alt="Diagram" style="height: 250px; width: auto;">

**Key Concepts**

* A <b style="color:#3399FF;">pipeline</b> is a functional reprentation of data-flow _(Pyper API)_
* A **task** represents a single functional operation within a pipeline _(user defined)_
* Under the hood, tasks pass data along via <b style="color:#FF8000;">workers</b> and <b style="color:#FF8000;">queues</b> _(Pyper internal)_

Pipelines are composable components; to create a pipeline which runs multiple tasks, we can 'pipe' pipelines together using the `|` operator:

```python
import time
from pyper import task

@task
def len_strings(x: str, y: str) -> int:
    return len(x) + len(y)

@task(workers=3)
def sleep(data: int) -> int:
    time.sleep(data)
    return data

@task(workers=2)
def calculate(data: int) -> bool:
    time.sleep(data)
    return data % 2 == 0

pipeline = len_strings | sleep | calculate
```

This defines `pipeline` as a series of tasks, taking the parameters `(x: str, y: str)` and generating `bool` outputs:

<img src="../../assets/img/diagram2.png" alt="Diagram" style="height: 250px; width: auto;">

We can think of this pipeline as one function.

The internal behaviour handles, intuitively, taking the outputs of each task and passing them as inputs to the next, where tasks communicate with each other via queue-based data structures. Running a task with multiple workers is the key mechanism underpinning how concurrency and parallelism are achieved.

## Next Steps

In the next few sections, we'll go over some more details on pipeline usage. Skip ahead to see:

* [More on creating pipelines](CreatingPipelines)
* [More on composing pipelines](ComposingPipelines)
* [Advanced concepts](AdvancedConcepts)