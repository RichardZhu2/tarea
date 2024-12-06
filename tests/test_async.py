from pyper import task
import pytest

class TestError(Exception): ...

def f1(data):
    return data

def f2(data):
    yield data

def f3(data):
    raise TestError

def f4(data):
    return [data]

async def af1(data):
    return data

async def af2(data):
    yield data

async def af3(data):
    raise TestError

async def af4(data):
    async for row in data:
        yield row

async def consumer(data):
    total = 0
    async for i in data:
        total += i
    return total

@pytest.mark.asyncio
async def test_aiterable_branched_pipeline():
    p = task(af1) | task(f2, branch=True)
    assert await p(1).__anext__() == 1

@pytest.mark.asyncio
async def test_iterable_branched_pipeline():
    p = task(af1) | task(f4, branch=True)
    assert await p(1).__anext__() == 1

@pytest.mark.asyncio
async def test_joined_pipeline():
    p = task(af1) | task(af2, branch=True) | task(af4, branch=True, join=True)
    assert await p(1).__anext__() == 1

@pytest.mark.asyncio
async def test_consumer():
    p = task(af1) | task(af2, branch=True) > consumer
    assert await p(1) == 1

@pytest.mark.asyncio
async def test_invalid_first_stage_workers():
    try:
        p = task(af1, workers=2) | task(af2, branch=True) > consumer
        await p(1)
    except Exception as e:
        assert isinstance(e, RuntimeError)
    else:
        raise AssertionError
    
@pytest.mark.asyncio
async def test_invalid_first_stage_join():
    try:
        p = task(af1, join=True) | task(af2, branch=True) > consumer
        await p(1)
    except Exception as e:
        assert isinstance(e, RuntimeError)
    else:
        raise AssertionError
    
@pytest.mark.asyncio
async def test_invalid_branch_result():
    try:
        p = task(af1, branch=True) > consumer
        await p(1)
    except Exception as e:
        assert isinstance(e, TypeError)
    else:
        raise AssertionError

async def _try_catch_error(pipeline):
    try:
        p = task(af1) | pipeline > consumer
        await p(1)
    except Exception as e:
        return isinstance(e, TestError)
    else:
        return False
    
@pytest.mark.asyncio
async def test_async_error_handling():
    p = task(af3)
    assert await _try_catch_error(p)
    
@pytest.mark.asyncio
async def test_threaded_error_handling():
    p = task(f3, workers=2)
    assert await _try_catch_error(p)
    
@pytest.mark.asyncio
async def test_multiprocessed_error_handling():
    p = task(f3, workers=2, multiprocess=True)
    assert await _try_catch_error(p)
    
@pytest.mark.asyncio
async def test_unified_pipeline():
    p = task(af1) | task(f1) | task(f2, branch=True, multiprocess=True) > consumer
    assert await p(1) == 1
