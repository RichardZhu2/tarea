---
title: Pipeline
parent: API Reference
layout: default
nav_order: 2
permalink: /docs/ApiReference/Pipeline
---

# pyper.Pipeline
{: .no_toc }

* TOC
{:toc}

## Pipeline

```python
def __new__(cls, tasks: List[Task]) -> Pipeline:
```

An object that represents a data flow consisting of a series of (at least one) tasks.

{: .warning}
It is not recommended to instantiate a `Pipeline` directly. Use the [task](task) class

## Pipeline.\__call__

```python
def __call__(self, *args, **kwargs) -> Generator[Any, None, None]:
```

A `Pipeline` is a callable object with the parameter specification of its first task which generates each output from its last task.

[Example](../UserGuide/CreatingPipelines#pipeline-usage)

## Pipeline.pipe

```python
def pipe(self, other: Pipeline) -> Pipeline:
```

Allows two `Pipeline` objects to be composed together, returning a new pipeline with a combined list of tasks.

[Example](../UserGuide/ComposingPipelines#piping-and-the--operator)

## Pipeline.\__or__

```python
def __or__(self, other: Pipeline) -> Pipeline:
```

Allows the use of the operator `|` as syntactic sugar for `Pipeline.pipe`.

## Pipeline.consume

```python
def consume(self, other: Callable) -> Callable:
```

Allows a consumer function to be attached to a `Pipeline`.

[Example](../UserGuide/ComposingPipelines#consumer-functions-and-the--operator)


## Pipeline.\__gt__

```python
def __gt__(self, other: Callable) -> Callable:
```

Allows the use of the operator `>` as syntactic sugar for `Pipeline.consume`.
