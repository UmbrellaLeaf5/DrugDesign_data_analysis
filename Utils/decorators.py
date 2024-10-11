import warnings
from typing import Callable


def IgnoreWarnings(func: Callable) -> Callable:
    """
    Декоратор для игнорирования предупреждений RuntimeWarning и UserWarning
    """

    def wrapper(*args, **kwargs):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", RuntimeWarning)
            warnings.simplefilter("ignore", UserWarning)

            return func(*args, **kwargs)

    return wrapper
