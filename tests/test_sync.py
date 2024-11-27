import time
from pyper import task


def f1(data):
    return data

def f2(data):
    yield data

def f3(data):
    for row in data:
        yield row

def f4(a1, a2, a3, data, k1, k2):
    return data

def f5(data):
    # Make queue monitor timeout on main thread
    time.sleep(0.2)
    raise RuntimeError

def consumer(data):
    total = 0
    for i in data:
        total += i
    return total

def test_pipeline():
    p = task(f1) | task(f2)
    assert p(1).__next__() == 1

def test_joined_pipeline():
    p = task(f1) | task(f2) | task(f3, join=True)
    assert p(1).__next__() == 1

def test_bind():
    p = task(f1) | task(f4, bind=task.bind(1, 1, 1, k1=1, k2=2))
    assert p(1).__next__() == 1

def test_redundant_bind_ok():
    p = task(f1) | task(f2, bind=task.bind())
    assert p(1).__next__() == 1

def test_consumer():
    p = task(f1) | task(f2) > consumer
    assert p(1) == 1

def test_invalid_first_stage_concurrency():
    try:
        p = task(f1, concurrency=2) | task(f2) > consumer
        p(1)
    except Exception as e:
        assert isinstance(e, RuntimeError)
    else:
        raise AssertionError
    
def test_invalid_first_stage_join():
    try:
        p = task(f1, join=True) | task(f2) > consumer
        p(1)
    except Exception as e:
        assert isinstance(e, RuntimeError)
    else:
        raise AssertionError

def test_error_handling():
    try:
        p = task(f1) | task(f2) | task(f5) > consumer
        p(1)
    except Exception as e:
        print(e)
        assert isinstance(e, RuntimeError)
    else:
        raise AssertionError

def test_error_handling_in_daemon():
    try:
        p = task(f5, daemon=True) | task(f2) > consumer
        p(1)
    except Exception as e:
        print(e)
        assert isinstance(e, RuntimeError)
    else:
        raise AssertionError
