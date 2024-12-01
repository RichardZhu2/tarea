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

Pyper is a generalized framework for concurrent data-processing, based on functional programming patterns. Used for 🌐 **Data Collection**, 🔀 **ETL systems**, and general-purpose 🛠️ **Python Scripting**

See the [Documentation](https://pyper-dev.github.io/pyper/)

Key features:

* 💡**Intuitive API**: Easy to learn, easy to think about. Implements clean abstractions to seamlessly unify threaded and asynchronous work.
* 🚀 **Functional Paradigm**: Python functions are the building blocks of data pipelines. Let's you write clean, reusable code naturally.
* 🛡️ **Safety**: Hides the heavy lifting of underlying task creation and execution. No more worrying about race conditions, memory leaks, and thread-level error handling.
* ⚡ **Efficiency**: Designed from the ground up for lazy execution, using queues, workers, and generators.
* ✨ **Pure Python**: Lightweight, with zero sub-dependencies.

## Installation

Install the latest version using `pip`:

```console
$ pip install python-pyper
```

Note that `python-pyper` is the [pypi](https://pypi.org/project/python-pyper) registered package.

## Usage

Let's simulate a pipeline that performs a series of transformations on some data. 

```python
import asyncio
import time
from typing import AsyncIterable

from pyper import task


def step1(limit: int):
    """Generate some data."""
    for i in range(limit):
        yield i


async def step2(data: int):
    """Simulate some asynchronous work."""
    await asyncio.sleep(1)
    print("Finished async sleep")
    return data + 1


def step3(data: int):
    """Simulate some IO-bound (non awaitable) work."""
    time.sleep(1)
    print("Finished sync sleep")
    return 2 * data - 1


async def print_sum(data: AsyncIterable[int]):
    """Print the sum of values from a data stream."""
    total = 0
    async for output in data:
        total += output
    print("Total ", total)


async def main():
    # Define a pipeline of tasks using `pyper.task`
    run = task(step1) | task(step2, concurrency=20) | task(step3, concurrency=20) > print_sum
    await run(limit=20)


if __name__ == "__main__":
    asyncio.run(main()) # takes ~2 seconds
```

Pyper provides an elegant abstraction of the concurrent execution of each function via `pyper.task`, allowing you to focus on building out the **logical** functions of your program.

In our pipeline:

* `task(step1)` generates 20 data values

* `task(step2, concurrency=20)` spins up 20 asynchronous workers, taking each value as input and returning an output

* `task(step3, concurrency=20)` spins up 20 threaded workers, taking each value as input and returning an output

The script therefore takes ~2 seconds to complete, as `step2` and `step3` in the pipeline only take the 1 second of sleep time, performed concurrently. If you'd like, experiment with tweaking the `limit` and `concurrency` values for yourself.

---

<details markdown="1">
<summary><u>What does the logic translate to in non-concurrent code?</u></summary>

<br>

Having defined the logical operations we want to perform on our data as functions, all we are doing is piping the output of one function to the input of another. In sequential code, this could look like:

```python
# Analogous to:
# pipeline = task(step1) | task(step2) | task(step3)
async def pipeline(limit: int):
    for data in step1(limit):
        data = await step2(data)
        data = step3(data)
        yield data


# Analogous to:
# run = pipeline > print_sum
async def run(limit: int):
    await print_sum(pipeline(limit))


async def main():
    await run(20) # takes ~40 seconds
```

Pyper uses the `|` (motivated by Unix's pipe operator) syntax as a representation of this input-output piping between tasks.

</details>

<details markdown="1">
<summary><u>What would the implementation look like without Pyper?</u></summary>

<br>

Concurrent programming in Python is notoriously difficult to get right. In a concurrent data pipeline, some challenges are:

* We want producers to concurrently execute tasks and send results to the next stage as soon as it's done processing
* We want consumers to lazily pick up output as soon as it's available from the previous stage
* We need to somehow unify the execution of threads and coroutines, without letting non-awaitable tasks clog up the event-loop

The basic approach to doing this is by using queues-- a simplified and very unabstracted implementation could be:

```python
async def pipeline(limit: int):
    q1 = asyncio.Queue()
    q2 = asyncio.Queue()
    q3 = asyncio.Queue()

    step2_concurrency=20
    step3_concurrency=20

    async def worker1():
        for data in step1(limit):
            await q1.put(data)
        for _ in range(step2_concurrency): 
            await q1.put(None)

    worker2s_finished = 0
    async def worker2():
        nonlocal worker2s_finished
        while True:
            data = await q1.get()
            if data is None:
                break
            output = await step2(data)
            await q2.put(output)
        worker2s_finished += 1
        if worker2s_finished == step2_concurrency:
            for _ in range(step3_concurrency): 
                await q2.put(None)

    worker3s_finished = 0
    async def worker3():
        nonlocal worker3s_finished
        loop = asyncio.get_running_loop()
        while True:
            data = await q2.get()
            if data is None:
                break
            # Pyper uses a custom thread group handler instead of run_in_executor
            output = await loop.run_in_executor(None, step3, data)
            await q3.put(output)
        worker3s_finished += 1
        if worker3s_finished == step3_concurrency:
            await q3.put(None)

    async with asyncio.TaskGroup() as tg:
        # Start all workers in the background
        tg.create_task(worker1())
        for _ in range(step2_concurrency):
            tg.create_task(worker2())
        for _ in range(step3_concurrency):
            tg.create_task(worker3())
        # Yield data until all workers have stopped
        while True:
            data = await q3.get()
            if data is None:
                break
            yield data


async def run(limit: int):
    await print_sum(pipeline(limit))


async def main():
    await run(20) # takes ~2 seconds
```

This implementation achieves the basic desired concurrent data flow, but still lacks some quality-of-life features that Pyper takes care of, like error handling within threads.

Pyper handles the complexities of managing queues and workers, so that this code can be reduced to the two-line main function in the example above.

</details>

<details markdown="1">
<summary><u>Do I have to use <code>async</code>?</u></summary>

<br>

No-- not every program is asynchronous, so Pyper pipelines are by default synchronous, as long as their tasks are defined as synchronous functions. For example:

```python
import time
from typing import Iterable

from pyper import task


def step1(limit: int):
    for i in range(limit):
        yield i


def step2(data: int):
    time.sleep(1)
    return data + 1


def step3(data: int):
    time.sleep(1)
    return 2 * data - 1


def print_sum(data: Iterable[int]):
    total = 0
    for output in data:
        total += output
    print("Total ", total)


def main():
    run = task(step1) \
        | task(step2, concurrency=20) \
        | task(step3, concurrency=20) \
        > print_sum
    # Run synchronously
    run(limit=20)


if __name__ == "__main__":
    main() # takes ~2 seconds
```

A pipeline consisting of _at least one asynchronous function_ becomes an `AsyncPipeline`, which exposes the same logical function, provided `async` and `await` syntax in all of the obvious places. This makes it effortless to unify synchronously defined and asynchronously defined functions where need be.

</details>

## Examples

To explore more of Pyper's features, see some further [examples](https://pyper-dev.github.io/pyper/docs/Examples)

## Dependencies

Pyper is implemented in pure Python, with no sub-dependencies. It relies heavily on the well-established built-in modules:
* [asyncio](https://docs.python.org/3/library/asyncio.html) for handling async-based concurrency
* [threading](https://docs.python.org/3/library/threading.html) for handling thread-based concurrency

## License

This project is licensed under the terms of the MIT license.