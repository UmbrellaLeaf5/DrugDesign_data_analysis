from functools import wraps
import time
from typing import Callable
import warnings

from Utils.verbose_logger import v_logger

from Configurations.config import config, Config


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


retry_config: Config = config["Utils"]["ReTry"]


def ReTry(attempts_amount: int = retry_config["attempts_amount"],
          exception_to_check: type[Exception] = Exception,
          sleep_time: float = retry_config["sleep_time"]
          ) -> Callable:
    """
    Декоратор для повторения попыток выполнения функции.

    В том случае, если `attempts_amount == 1`, можно сказать, просто оборачивает
    декорируемую функцию в `try-except`. 

    Args:
        attempts_amount (int, optional): кол-во попыток.
        exception_to_check (type[Exception], optional): тип улавливаемого исключения. 
                                                        Defaults to Exception.
        sleep_time (int, optional): время ожидания между попытками.

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
                    v_logger.LogException(exception)

                    if attempt < attempts_amount:
                        v_logger.warning(f"Attempt: {attempt}. Retrying.")
                        time.sleep(sleep_time)

            if attempts_amount != 1:
                v_logger.error("All attempts failed!")
            # else: означает, что в функции просто 1 раз отлавливается исключение

            return None

        return Wrapper
    return Decorate
