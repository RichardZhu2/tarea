<p align="center">
  <img src="https://raw.githubusercontent.com/pyper-dev/pyper/refs/heads/main/docs/src/assets/img/pyper.png" alt="Pyper" style="width: 500px;">
</p>
<p align="center" style="font-size: 1.5em;">
    <em>Concurrent Python made simple</em>
</p>

<p align="center">
<a href="https://github.com/pyper-dev/pyper/actions/workflows/test.yml" target="_blank">
    <img src="https://github.com/pyper-dev/pyper/actions/workflows/test.yml/badge.svg" alt="Test">
</a>
<a href="https://coveralls.io/github/pyper-dev/pyper" target="_blank">
    <img src="https://coveralls.io/repos/github/pyper-dev/pyper/badge.svg" alt="Coverage">
</a>
<a href="https://pypi.org/project/python-pyper" target="_blank">
    <img src="https://img.shields.io/pypi/v/python-pyper?color=%2334D058&label=pypi%20package" alt="Package version">
</a>
<a href="https://pypi.org/project/python-pyper" target="_blank">
    <img src="https://img.shields.io/pypi/pyversions/python-pyper.svg?color=%2334D058" alt="Supported Python versions">
</a>
</p>

---

Pyper is a flexible framework for concurrent and parallel data-processing, based on functional programming patterns. Used for 🔀 **ETL Systems**, ⚙️ **Data Microservices**, and 🌐 **Data Collection**

See the [Documentation](https://pyper-dev.github.io/pyper/)

**Key features:**

* 💡**Intuitive API**: Easy to learn, easy to think about. Implements clean abstractions to seamlessly unify threaded, multiprocessed, and asynchronous work.
* 🚀 **Functional Paradigm**: Python functions are the building blocks of data pipelines. Let's you write clean, reusable code naturally.
* 🛡️ **Safety**: Hides the heavy lifting of underlying task execution and resource clean-up. No more worrying about race conditions, memory leaks, or thread-level error handling.
* ⚡ **Efficiency**: Designed from the ground up for lazy execution, using queues, workers, and generators.
* ✨ **Pure Python**: Lightweight, with zero sub-dependencies.

## Installation

Install the latest version using `pip`:

```console
$ pip install python-pyper
```

Note that `python-pyper` is the [pypi](https://pypi.org/project/python-pyper) registered package.

## Usage

In Pyper, the `task` decorator is used to transform functions into composable pipelines.

Let's simulate a pipeline that performs a series of transformations on some data. 

```python
import asyncio
import time

from pyper import task


def get_data(limit: int):
    for i in range(limit):
        yield i


async def step1(data: int):
    await asyncio.sleep(1)
    print("Finished async wait", data)
    return data


def step2(data: int):
    time.sleep(1)
    print("Finished sync wait", data)
    return data


def step3(data: int):
    for i in range(10_000_000):
        _ = i*i
    print("Finished heavy computation", data)
    return data


async def main():
    # Define a pipeline of tasks using `pyper.task`
    pipeline = task(get_data, branch=True) \
        | task(step1, workers=20) \
        | task(step2, workers=20) \
        | task(step3, workers=20, multiprocess=True)

    # Call the pipeline
    total = 0
    async for output in pipeline(limit=20):
        total += output
    print("Total:", total)


if __name__ == "__main__":
    asyncio.run(main())
```

Pyper provides an elegant abstraction of the execution of each task, allowing you to focus on building out the **logical** functions of your program. In the `main` function:

* `pipeline` defines a function; this takes the parameters of its first task (`get_data`) and yields each output from its last task (`step3`)
* Tasks are piped together using the `|` operator (motivated by Unix's pipe operator) as a syntactic representation of passing inputs/outputs between tasks.

In the pipeline, we are executing three different types of work:

* `task(step1, workers=20)` spins up 20 `asyncio.Task`s to handle asynchronous IO-bound work

* `task(step2, workers=20)` spins up 20 `threads` to handle synchronous IO-bound work 

* `task(step3, workers=20, multiprocess=True)` spins up 20 `processes` to handle synchronous CPU-bound work

`task` acts as one intuitive API for unifying the execution of each different type of function.

Each task has workers that submit outputs to the next task within the pipeline via queue-based data structures; this is the mechanism underpinning how concurrency and parallelism are achieved. See the [docs](https://pyper-dev.github.io/pyper/docs/UserGuide/BasicConcepts) for a breakdown of what a pipeline looks like under the hood.

---

</details>

<details markdown="1">
<summary><u>See a non-async example</u></summary>

<br>

Pyper pipelines are by default non-async, as long as their tasks are defined as synchronous functions. For example:

```python
import time

from pyper import task


def get_data(limit: int):
    for i in range(limit):
        yield i

def step1(data: int):
    time.sleep(1)
    print("Finished sync wait", data)
    return data

def step2(data: int):
    for i in range(10_000_000):
        _ = i*i
    print("Finished heavy computation", data)
    return data


def main():
    pipeline = task(get_data, branch=True) \
        | task(step1, workers=20) \
        | task(step2, workers=20, multiprocess=True)
    total = 0
    for output in pipeline(limit=20):
        total += output
    print("Total:", total)


if __name__ == "__main__":
    main()
```

A pipeline consisting of _at least one asynchronous function_ becomes an `AsyncPipeline`, which exposes the same usage API, provided `async` and `await` syntax in the obvious places. This makes it effortless to combine synchronously defined and asynchronously defined functions where need be.

</details>

## Examples

To explore more of Pyper's features, see some further [examples](https://pyper-dev.github.io/pyper/docs/Examples)

## Dependencies

Pyper is implemented in pure Python, with no sub-dependencies. It is built on top of the well-established built-in Python modules:
* [threading](https://docs.python.org/3/library/threading.html) for thread-based concurrency
* [multiprocessing](https://docs.python.org/3/library/multiprocessing.html) for parallelism
* [asyncio](https://docs.python.org/3/library/asyncio.html) for async-based concurrency
* [concurrent.futures](https://docs.python.org/3/library/concurrent.futures.html) for unifying threads, processes, and async code

## License

This project is licensed under the terms of the MIT license.