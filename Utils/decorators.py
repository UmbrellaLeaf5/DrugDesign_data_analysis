"""
Utils/decorators.py

Этот модуль содержит декораторы для обработки исключений и игнорирования
предупреждений.
"""

import time
import warnings
from collections.abc import Callable
from functools import wraps

from Configurations.config import Config, config
from Utils.verbose_logger import v_logger


def IgnoreWarnings(func: Callable) -> Callable:
  """
  Игнорирует предупреждения RuntimeWarning и UserWarning.

  Args:
      func (Callable): декорируемая функция.

  Returns:
      Callable: декорируемая функция.
  """

  @wraps(func)
  def Wrapper(*args, **kwargs):
    # перехватываем предупреждения.
    with warnings.catch_warnings():
      # игнорируем RuntimeWarning.
      warnings.simplefilter("ignore", RuntimeWarning)
      # игнорируем UserWarning.
      warnings.simplefilter("ignore", UserWarning)

      # вызываем декорируемую функцию.
      return func(*args, **kwargs)

  return Wrapper


# конфигурация для повторных попыток.
retry_config: Config = config["Utils"]["ReTry"]


def ReTry(
  attempts_amount: int = retry_config["attempts_amount"],
  exception_to_check: type[Exception] = Exception,
  sleep_time: float = retry_config["sleep_time"],
) -> Callable:
  """
  Повторяет попытки выполнения функции в случае возникновения исключения.

  Если `attempts_amount == 1`, просто оборачивает декорируемую функцию в
  `try-except`.

  Args:
      attempts_amount (int, optional): количество попыток.
                                       Defaults to [берется из конфигурации].
      exception_to_check (type[Exception], optional): тип исключения для
                                                      перехвата. Defaults to Exception.
      sleep_time (int, optional): время ожидания между попытками (в секундах).
                                  Defaults to [берется из конфигурации].

  Returns:
      Callable: декорируемая функция.
  """

  def Decorate(func: Callable) -> Callable:
    """Декорирует функцию."""

    @wraps(func)
    def Wrapper(*args, **kwargs):
      # итерируемся по количеству попыток.
      for attempt in range(1, attempts_amount + 1):
        try:
          # пытаемся выполнить функцию.
          return func(*args, **kwargs)

        # если возникло исключение.
        except exception_to_check as exception:
          # логируем исключение.
          v_logger.LogException(exception)

          # если это не последняя попытка.
          if attempt < attempts_amount:
            v_logger.warning(f"Attempt: {attempt}. Retrying.")
            # ждем перед следующей попыткой.
            time.sleep(sleep_time)

      # если все попытки не удались.
      if attempts_amount != 1:
        v_logger.error("All attempts failed!")
      # если количество попыток равно 1,
      # значит в функции просто отлавливается исключение.

      return None

    # возвращаем функцию-обертку.
    return Wrapper

  # возвращаем декоратор.
  return Decorate
