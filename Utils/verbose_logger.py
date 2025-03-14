"""
Utils/verbose_logger.py

Этот модуль реализует класс VerboseLogger для расширенного логирования с
настраиваемым форматированием и записью исключений в файл.
"""

from enum import Enum
from io import TextIOWrapper
import re
import sys
import traceback
from typing import Any, Callable, TextIO

from Configurations.config import config

import loguru
# аннотируем тип logger.
Logger = loguru._logger.Logger  # type: ignore


class LogMode(Enum):
    """
    Перечисление, определяющее режим логирования:
        - RETICENTLY: логируются только важные сообщения (по умолчанию).
        - VERBOSELY: логируются все сообщения.
    """

    RETICENTLY = 0
    VERBOSELY = 1


class VerboseLogger:
    """
    Реализует расширенное логирование с возможностью настройки уровня
    детализации, формата сообщений и записи исключений в файл.
    """

    __logger: Logger  # type: ignore
    __log_mode: LogMode

    __message_ljust: int
    __exceptions_file: str

    __format: Callable[[dict], str]

    __labels: list[str] = []
    __colors: list[str] = []

    __standard_output: TextIOWrapper | TextIO | Any

    def __init__(self,
                 logger: Logger,  # type: ignore
                 log_mode: LogMode,
                 message_ljust: int,
                 exceptions_file: str,
                 standard_output: TextIO | TextIOWrapper = sys.stdout):
        """
        Инициализирует класс VerboseLogger.

        Args:
            logger (Logger): объект логгера loguru.
            log_mode (LogMode): режим логирования (RETICENTLY или VERBOSELY).
            message_ljust (int): ширина поля для выравнивания сообщений.
            exceptions_file (str): путь к файлу для записи исключений.
            standard_output (TextIO | TextIOWrapper, optional): вывод.
        """

        self.__logger = logger
        self.__log_mode = log_mode

        self.__message_ljust = message_ljust
        self.__exceptions_file = exceptions_file

        self.__standard_output = standard_output

    @classmethod
    def FromConfig(cls):
        """
        Создает экземпляр VerboseLogger на основе конфигурации.

        Returns:
            VerboseLogger: экземпляр класса.
        """

        # получаем конфигурацию для verbose логгера.
        v_logger_config = config["Utils"]["VerboseLogger"]

        # создаем и возвращаем экземпляр класса.
        return cls(loguru.logger,
                   LogMode.VERBOSELY if v_logger_config["verbose_print"]
                   else LogMode.RETICENTLY,
                   v_logger_config["message_ljust"],
                   v_logger_config["exceptions_file"],
                   v_logger_config["exceptions_file"] if
                   v_logger_config["output_to_exceptions_file"]
                   else sys.stdout)

    def UpdateFormat(self,
                     logger_label: str,
                     logger_color: str) -> int:
        """
        Обновляет формат вывода логирования.

        Args:
            logger_label (str): текст заголовка для логирования.
            logger_color (str): цвет заголовка для логирования.

        Returns:
            int: индекс текущего формата.
        """

        # задаем формат вывода.
        self.__format = lambda record: "[{time:DD.MM.YYYY HH:mm:ss}] " +\
            f"<{logger_color}>{logger_label}:</{logger_color}> " +\
            f"{record['message']} ".ljust(self.__message_ljust) +\
            f"[<level>{record['level']}</level>]\n"

        # добавляем заголовок и цвет в списки.
        self.__labels.append(logger_label)
        self.__colors.append(logger_color)

        # настраиваем логгер.
        self.__Configure()

        return len(self.__labels) - 1

    def RestoreFormat(self, index: int):
        """
        Восстанавливает формат логгера по индексу.

        Args:
            index (int): индекс одного из предыдущих форматов.

        Raises:
            NotImplementedError: если не установлен формат логгера.
            IndexError: если index выходит за границы.
        """

        # проверяем, установлен ли формат логгера.
        if len(self.__labels) == 0:
            raise NotImplementedError(
                "VerboseLogger: logger format is not set. Call "
                "'logger.UpdateFormat' first.")

        # проверяем, не выходит ли index за границы.
        if index >= len(self.__labels):
            raise IndexError(
                f"VerboseLogger: RestoreFormat: index ({index}) is out of "
                f"range [0, {len(self.__labels) - 1}].")

        # если текущий формат уже установлен, предупреждаем и выходим.
        if index == len(self.__labels) - 1:
            self.__logger.warning("Current format is already set.")
            return

        # задаем формат вывода.
        self.__format = lambda record: "[{time:DD.MM.YYYY HH:mm:ss}] " +\
            f"<{self.__colors[index]}>{self.__labels[index]}:" \
            f"</{self.__colors[index]}> " +\
            f"{record['message']} ".ljust(self.__message_ljust) +\
            f"[<level>{record['level']}</level>]\n"

        # обрезаем списки заголовков и цветов.
        self.__labels = self.__labels[:index + 1]
        self.__colors = self.__colors[:index + 1]

        # настраиваем логгер.
        self.__Configure()

    def LogException(self,
                     exception: Exception):
        """
        Логирует исключение в консоль и записываем его в файл.

        Args:
            exception (Exception): объект исключения.

        Raises:
            NotImplementedError: если не установлен формат логгера.
        """

        # проверяем, установлен ли формат логгера.
        if len(self.__labels) == 0:
            raise NotImplementedError(
                "VerboseLogger: logger format is not set. Call "
                "'logger.UpdateFormat' first.")

        # логируем исключение в консоль.
        self.__logger.error(f"{exception}")

        try:
            # настраиваем логгер для записи в файл исключений.
            self.__Configure(self.__exceptions_file)

            # логируем traceback в файл исключений.
            self.__logger.error(
                f"{re.sub(r'"(.*?)\",\s+line\s+(\d+)',
                          r'\1:\2', traceback.format_exc())}")

        # если не удалось записать исключение в файл.
        except Exception as exception:
            self.__logger.error(
                "VerboseLogger: failed to write exception to file: "
                f"{exception}.")

        # в любом случае восстанавливаем исходную конфигурацию логгера.
        finally:
            self.__Configure()

    def Log(self,
            level: str,
            message: str,
            log_mode: LogMode = LogMode.RETICENTLY):
        """
        Логирует сообщение с указанным уровнем.

        Args:
            level (str): уровень логирования.
            message (str): сообщение для логирования.
            log_mode (LogMode, optional): режим логирования.

        Raises:
            NotImplementedError: если не установлен формат логгера.
        """

        # проверяем, установлен ли формат логгера.
        if len(self.__labels) == 0:
            raise NotImplementedError(
                "VerboseLogger: logger format is not set. Call "
                "'logger.UpdateFormat' first!")

        # логируем сообщение, если режим логирования соответствует.
        if self.__log_mode == LogMode.VERBOSELY or\
                log_mode == LogMode.RETICENTLY:
            self.__logger.log(level, message)

    def __Configure(self,
                    output: TextIO | Any | None = None):
        """
        Настраивает вывод логгера.

        Args:
            output (TextIO | Any | None, optional): поток вывода.
        """

        # если вывод не задан, используем стандартный.
        if output is None:
            output = self.__standard_output

        # удаляем предыдущие обработчики.
        self.__logger.remove()
        # добавляем новый обработчик с заданным форматом и выводом.
        self.__logger.add(sink=output,
                          format=self.__format)

    def __LogMethod(self,
                    level: str) -> Callable:
        """
        Создает функцию-обертку для логирования сообщений с определенным уровнем.

        Args:
            level (str): уровень логирования.

        Returns:
            Callable: функция-обертка для логирования.
        """

        # получаем имя уровня в верхнем регистре.
        level_name = level.upper()

        def Wrapper(message: str = f"{"-" * (self.__message_ljust - 1)}",
                    log_mode: LogMode = LogMode.RETICENTLY):
            # вызываем метод Log с заданным уровнем и сообщением.
            self.Log(level_name, message, log_mode)

        # возвращаем функцию-обертку.
        return Wrapper

    def __getattr__(self,
                    name: str) -> Any:
        """
        Перехватывает обращение к атрибутам класса.

        Если это методы логирования (debug, info и т.д.), возвращает функцию-
        обертку для логирования. В противном случае, возвращает атрибут из
        базового логгера loguru.

        Args:
            name (str): имя атрибута.

        Returns:
            Any: атрибут или функция-обертка для логирования.
        """

        # проверяем, является ли имя одним из уровней логирования.
        if name in [string.lower() for string in
                    loguru.logger._core.levels.keys()]:  # type: ignore
            # возвращаем обертку для метода логирования.
            return self.__LogMethod(name)

        # иначе возвращаем атрибут из базового логгера.
        else:  # другие методы loguru.logger
            return getattr(self.__logger, name)


# MARK: v_logger
v_logger = VerboseLogger.FromConfig()
