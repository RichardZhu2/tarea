import time
from pyper import task

class TestError(Exception): ...

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
    raise TestError

def consumer(data):
    total = 0
    for i in data:
        total += i
    return total

def test_branched_pipeline():
    p = task(f1) | task(f2, branch=True)
    assert p(1).__next__() == 1

def test_joined_pipeline():
    p = task(f1) | task(f2, branch=True) | task(f3, branch=True, join=True)
    assert p(1).__next__() == 1

def test_bind():
    p = task(f1) | task(f4, bind=task.bind(1, 1, 1, k1=1, k2=2))
    assert p(1).__next__() == 1

def test_redundant_bind_ok():
    p = task(f1) | task(f2, branch=True, bind=task.bind())
    assert p(1).__next__() == 1

def test_consumer():
    p = task(f1) | task(f2, branch=True) > consumer
    assert p(1) == 1

def test_invalid_first_stage_workers():
    try:
        p = task(f1, workers=2) | task(f2) > consumer
        p(1)
    except Exception as e:
        assert isinstance(e, RuntimeError)
    else:
        raise AssertionError
    
def test_invalid_first_stage_join():
    try:
        p = task(f1, join=True) | task(f2, branch=True) > consumer
        p(1)
    except Exception as e:
        assert isinstance(e, RuntimeError)
    else:
        raise AssertionError

def test_invalid_branch_result():
    try:
        p = task(f1, branch=True) > consumer
        p(1)
    except Exception as e:
        assert isinstance(e, TypeError)
    else:
        raise AssertionError

def test_threaded_error_handling():
    try:
        p = task(f1) | task(f5, workers=2) > consumer
        p(1)
    except Exception as e:
        assert isinstance(e, TestError)
    else:
        raise AssertionError

def test_multiprocessed_error_handling():
    try:
        p = task(f1) | task(f5, workers=2, multiprocess=True) > consumer
        p(1)
    except Exception as e:
        assert isinstance(e, TestError)
    else:
        raise AssertionError
