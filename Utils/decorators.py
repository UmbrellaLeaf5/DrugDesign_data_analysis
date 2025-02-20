from functools import wraps
import time
from typing import Callable
import warnings

from Utils.logger_funcs import logger, LogException


def IgnoreWarnings(func: Callable) -> Callable:
    """
    Декоратор для игнорирования предупреждений RuntimeWarning и UserWarning.

    Args:
        func (Callable): декорируемая функция.

    Returns:
        Callable: декорируемая функция.
    """

    @wraps(func)
    def Wrapper(*args, **kwargs):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", RuntimeWarning)
            warnings.simplefilter("ignore", UserWarning)

            return func(*args, **kwargs)

    return Wrapper


def ReTry(attempts_amount: int = 5,
          exception_to_check: type[Exception] = Exception,
          sleep_time: float = 1
          ) -> Callable:
    """
    Декоратор для повторения попыток выполнения функции.

    Args:
        attempts_amount (int, optional): кол-во попыток. Defaults to 5.
        exception_to_check (type[Exception], optional): улавливаемое исключение. Defaults to Exception.
        sleep_time (int, optional): время ожидания между попытками. Defaults to 1.

    Returns:
        Callable: декорируемая функция.
    """

    def Decorate(func: Callable) -> Callable:
        @wraps(func)
        def Wrapper(*args, **kwargs):
            for attempt in range(1, attempts_amount + 1):
                try:
                    return func(*args, **kwargs)

                except exception_to_check as exception:
                    LogException(exception)

                    if attempt < attempts_amount:
                        logger.warning(f"Attempt: {attempt}. Retrying.")
                        time.sleep(sleep_time)

            if attempts_amount != 1:
                logger.error("All attempts failed!")
            # else: означает, что в функции просто 1 раз отлавливается исключение

            return None

        return Wrapper
    return Decorate
