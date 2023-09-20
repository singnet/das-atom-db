from functools import wraps
from typing import Callable


def set_is_toplevel(function: Callable) -> Callable:
    @wraps(function)
    def wrapper(*args, **kwargs):
        result = function(*args, **kwargs)
        result['is_toplevel'] = True
        return result

    return wrapper
