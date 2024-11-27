from pyper import task, AsyncPipeline, Pipeline


def func(x):
    return x

def gen(x):
    yield x

async def afunc(x):
    return x

async def agen(x):
    yield x

class Func:
    def __call__(self, x):
        return x
    
class Gen:
    def __call__(self, x):
        yield x
    
class AFunc:
    async def __call__(self, x):
        return x
    
class AGen:
    async def __call__(self, x):
        yield x

def test_as_decorator():
    p = task(func)
    assert isinstance(p, Pipeline)

def test_as_decorator_with_params():
    p = task(join=True, concurrency=2, throttle=2)(func)
    assert isinstance(p, Pipeline)

def test_as_wrapper_with_params():
    p = task(func, join=True, concurrency=2, throttle=2)
    assert isinstance(p, Pipeline)

def _try_invalid_concurrency(value, exc_type):
    try:
        task(func, concurrency=value)
    except Exception as e:
        return isinstance(e, exc_type)
    return False

def test_raise_for_invalid_concurrency():
    assert _try_invalid_concurrency(0, ValueError)
    assert _try_invalid_concurrency(-1, ValueError)
    assert _try_invalid_concurrency("1",TypeError)
    assert _try_invalid_concurrency(1.5, TypeError)

def _try_invalid_throttle(value, exc_type):
    try:
        task(func, throttle=value)
    except Exception as e:
        return isinstance(e, exc_type)
    return False

def test_raise_for_invalid_throttle():
    assert _try_invalid_throttle(-1, ValueError)
    assert _try_invalid_throttle("1",TypeError)
    assert _try_invalid_throttle(1.5, TypeError)

def test_raise_for_invalid_daemon():
    try:
        task(afunc, daemon=True)
    except Exception as e:
        assert isinstance(e, ValueError)
    else:
        raise AssertionError

def test_raise_for_invalid_func():
    try:
        task(1)
    except Exception as e:
        assert isinstance(e, TypeError)
    else:
        raise AssertionError

def test_async_task():
    p = task(afunc)
    assert isinstance(p, AsyncPipeline)

def test_piped_async_task():
    p = task(afunc) | task(func)
    assert isinstance(p, AsyncPipeline)

def test_invalid_pipe():
    try:
        task(func) | 1
    except Exception as e:
        assert isinstance(e, TypeError)
    else:
        raise AssertionError

def test_invalid_async_pipe():
    try:
        task(afunc) | 1
    except Exception as e:
        assert isinstance(e, TypeError)
    else:
        raise AssertionError

def test_invalid_consumer():
    try:
        task(func) > 1
    except Exception as e:
        assert isinstance(e, TypeError)
    else:
        raise AssertionError

def test_invalid_async_consumer():
    try:
        task(afunc) > func
    except Exception as e:
        assert isinstance(e, TypeError)
    else:
        raise AssertionError

def test_gen_inspect():
    is_gen = lambda f: task(f).tasks[0].is_gen
    assert is_gen(gen)
    assert is_gen(agen)
    assert is_gen(Gen())
    assert is_gen(AGen())
    assert not is_gen(func)
    assert not is_gen(afunc)
    assert not is_gen(Func())
    assert not is_gen(AFunc())
    assert not is_gen(lambda x: x)

def test_async_inspect():
    is_async = lambda f: task(f).tasks[0].is_async
    assert is_async(afunc)
    assert is_async(agen)
    assert is_async(AFunc())
    assert is_async(AGen())
    assert not is_async(func)
    assert not is_async(Func())
    assert not is_async(gen)
    assert not is_async(Gen())
    assert not is_async(lambda x: x)

def test_repr():
    p = task(func)
    assert "Pipeline" in repr(p)
