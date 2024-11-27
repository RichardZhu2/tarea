from pyper import task
import pytest


def f1(data):
    return data

def f2(data):
    yield data

def f3(data):
    raise RuntimeError

async def af1(data):
    return data

async def af2(data):
    yield data

async def af3(data):
    raise RuntimeError

async def af4(data):
    async for row in data:
        yield row

async def consumer(data):
    total = 0
    async for i in data:
        total += i
    return total

@pytest.mark.asyncio
async def test_pipeline():
    p = task(f1) | task(f2)
    assert p(1).__next__() == 1

@pytest.mark.asyncio
async def test_joined_pipeline():
    p = task(af1) | task(af2) | task(af4, join=True)
    assert await p(1).__anext__() == 1

@pytest.mark.asyncio
async def test_consumer():
    p = task(af1) | task(af2) > consumer
    assert await p(1) == 1

@pytest.mark.asyncio
async def test_invalid_first_stage_concurrency():
    try:
        p = task(af1, concurrency=2) | task(af2) > consumer
        await p(1)
    except Exception as e:
        assert isinstance(e, RuntimeError)
    else:
        raise AssertionError
    
@pytest.mark.asyncio
async def test_invalid_first_stage_join():
    try:
        p = task(af1, join=True) | task(af2) > consumer
        await p(1)
    except Exception as e:
        assert isinstance(e, RuntimeError)
    else:
        raise AssertionError

@pytest.mark.asyncio
async def test_error_handling():
    try:
        p = task(af1) | task(af2) | task(af3) > consumer
        await p(1)
    except Exception as e:
        assert isinstance(e, RuntimeError)
    else:
        raise AssertionError
    
@pytest.mark.asyncio
async def test_unified_pipeline():
    p = task(af1) | task(f1) | task(af2) | task(f2) > consumer
    assert await p(1) == 1

@pytest.mark.asyncio
async def test_error_handling_in_daemon():
    try:
        p = task(af1) | task(af2) | task(f3, daemon=True) > consumer
        await p(1)
    except Exception as e:
        assert isinstance(e, RuntimeError)
    else:
        raise AssertionError
    