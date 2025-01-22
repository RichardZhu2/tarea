import functools


def unpack_args(func):
    """Decorator to modify how an input gets passed into a function.
    - If the input is a `dict`, unpack the arguments with **
    - If the input is a `tuple` or `list`, unpack the arguments with *
    - Otherwise, raise an error
    """
    @functools.wraps(func)
    def wrapper(data):
        if isinstance(data, dict):
            return func(**data)
        if isinstance(data, (tuple, list)):
            return func(*data)
        raise TypeError(f"data of type {type(data)} cannot be unpacked into {func} (must be 'dict', 'tuple', or 'list')")
    
    return wrapper
