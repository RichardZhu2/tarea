from pyper import task, Fork

class TestError(Exception): ...

def f1(data):
    return data + 1

def f2(data):
    return data + 2

def f3(data):
    return data + 3

def f4(data):
    raise TestError

def test_fork_type():
    assert isinstance(task.fork(f1), Fork)
    assert isinstance(task.fork(f1) & task.fork(f1), Fork)

def test_raise_for_invalid_fork_():
    try:
        task.fork(f1) & task(f1)
    except Exception as e:
        assert isinstance(e, TypeError)
    else:
        raise AssertionError

def test_basic_fork():
    fork = (
        task.fork(f1)
        & task.fork(f2)
        & task.fork(f3)
    )
    res1, res2, res3 = fork(0)
    assert res1 == 1
    assert res2 == 2
    assert res3 == 3

def test_multiprocess_fork():
    fork = (
        task.fork(f1)
        & task.fork(f2, multiprocess=True)
    )
    res1, res2, = fork(0)
    assert res1 == 1
    assert res2 == 2

def test_fork_in_multiprocess_pipeline():
    p = (
        task(lambda x: x)
        | task(
            task.fork(f1)
            & task.fork(f2),
            workers=2,
            multiprocess=True
        )
    )
    res1, res2 = p(0).__next__()
    assert res1 == 1
    assert res2 == 2

def test_basic_fork_error_handling():
    fork = (
        task.fork(f1)
        & task.fork(f2)
        & task.fork(f4)
    )
    try:
        fork(0)
    except Exception as e:
        assert isinstance(e, TestError)
    else:
        raise AssertionError

def test_multiprocess_fork_error_handling():
    fork = (
        task.fork(f1)
        & task.fork(f4, multiprocess=True)
    )
    try:
        fork(0)
    except Exception as e:
        assert isinstance(e, TestError)
    else:
        raise AssertionError

def test_fork_repr():
    f = task.fork(f1)
    assert "Fork" in repr(f)
    