import warnings
from typing import Callable
import time
from functools import wraps

from Utils.file_and_logger_funcs import logger


def IgnoreWarnings(func: Callable) -> Callable:
    """
    Декоратор для игнорирования предупреждений RuntimeWarning и UserWarning

    Args:
        func (Callable): декорируемая функция

    Returns:
        Callable: декорируемая функция
    """

    @wraps(func)
    def Wrapper(*args, **kwargs):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", RuntimeWarning)
            warnings.simplefilter("ignore", UserWarning)

            return func(*args, **kwargs)

    return Wrapper


def Retry(attempts_amount=3, exception_to_check=Exception, sleep_time=1) -> Callable:
    """
    Декоратор для повторения попыток выполнения функции

    Args:
        attempts_amount (int, optional): кол-во попыток. Defaults to 1.
        exception_to_check (_type_, optional): улавливаемое исключение. Defaults to Exception.
        sleep_time (int, optional): время ожидания между попытками. Defaults to 3.

    Returns:
        Callable: декорируемая функция
    """

    def Decorate(func: Callable) -> Callable:
        @wraps(func)
        def Wrapper(*args, **kwargs):
            for attempt in range(1, attempts_amount+1):
                try:
                    return func(*args, **kwargs)

                except exception_to_check as exception:
                    logger.warning(f"{exception} | Attempt: {
                                   attempt}".ljust(77))
                    if attempt < attempts_amount:
                        time.sleep(sleep_time)

            logger.error("All attempts failed".ljust(77))
            return None
        return Wrapper
    return Decorate
