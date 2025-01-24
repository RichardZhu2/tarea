from pyper import task, AsyncFork
import pytest

class TestError(Exception): ...

async def af1(data):
    return data + 1

def f2(data):
    return data + 2

def f3(data):
    raise TestError

def test_async_fork_type():
    assert isinstance(task.fork(af1), AsyncFork)
    assert isinstance(task.fork(af1) & task.fork(f2), AsyncFork)
    assert isinstance(task.fork(f2) & task.fork(af1), AsyncFork)
    assert isinstance(task.fork(af1) & task.fork(af1), AsyncFork)

@pytest.mark.asyncio
async def test_basic_fork():
    fork = (
        task.fork(af1)
        & task.fork(f2)
    )
    res1, res2 = await fork(0)
    assert res1 == 1
    assert res2 == 2

@pytest.mark.asyncio
async def test_multiprocess_fork():
    fork = (
        task.fork(af1)
        & task.fork(f2, multiprocess=True)
    )
    res1, res2 = await fork(0)
    assert res1 == 1
    assert res2 == 2

@pytest.mark.asyncio
async def test_basic_fork_error_handling():
    fork = (
        task.fork(af1)
        & task.fork(f2)
        & task.fork(f3)
    )
    try:
        await fork(0)
    except Exception as e:
        assert isinstance(e, TestError)
    else:
        raise AssertionError

@pytest.mark.asyncio
async def test_multiprocess_fork_error_handling():
    fork = (
        task.fork(af1)
        & task.fork(f3, multiprocess=True)
    )
    try:
        await fork(0)
    except Exception as e:
        assert isinstance(e, TestError)
    else:
        raise AssertionError
