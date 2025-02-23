from enum import Enum
from io import TextIOWrapper
import os
import re
import sys
import traceback
from typing import Any, Callable, TextIO

from Configurations.config import Config, GetConfig

import loguru
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
    Класс, реализующий расширенное логирование с возможностью настройки уровня детализации,
    формата сообщений и записи исключений в файл.
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
        Конструктор класса VerboseLogger.

        Args:
            logger (Logger): объект логгера loguru.
            log_mode (LogMode): режим логирования (RETICENTLY или VERBOSELY).
            message_ljust (int): ширина поля для выравнивания сообщений по левому краю.
            exceptions_file (str): путь к файлу для записи исключений.
        """

        self.__logger = logger
        self.__log_mode = log_mode

        self.__message_ljust = message_ljust
        self.__exceptions_file = exceptions_file

        self.__standard_output = standard_output

    @classmethod
    def FromConfig(cls,
                   config: Config):
        """
        Создает экземпляр VerboseLogger на основе конфигурации.

        Args:
            config (Config): словарь конфигурации.

        Returns:
            VerboseLogger: экземпляр класса.
        """

        log_mode: LogMode = LogMode.RETICENTLY
        if config["verbose_print"]:
            log_mode = LogMode.VERBOSELY

        standard_output: TextIO | Any = sys.stdout
        if config["output_to_exceptions_file"]:
            standard_output = config["exceptions_file"]

        return cls(loguru.logger,
                   log_mode,
                   config["message_ljust"],
                   config["exceptions_file"],
                   standard_output)

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

        self.__format = lambda record: "[{time:DD.MM.YYYY HH:mm:ss}] " +\
            f"<{logger_color}>{logger_label}:</{logger_color}> " +\
            f"{record['message']} ".ljust(self.__message_ljust) +\
            f"[<level>{record['level']}</level>]\n"

        self.__labels.append(logger_label)
        self.__colors.append(logger_color)

        self.__Configure()

        return len(self.__labels) - 1

    def RestoreFormat(self, index: int):
        """
        Восстанавливает формат логгера по индексу.

        Args:
            index (int): индекс одного из предыдущих форматов.

        Raises:
            NotImplementedError: если не установлен формат логгера.
            IndexError: если index выходит за границы диапазона [0, {len(self.__labels) - 1}].
        """

        if len(self.__labels) == 0:
            raise NotImplementedError(
                "VerboseLogger: logger format is not set. Call 'logger.UpdateFormat' first.")

        if index >= len(self.__labels):
            raise IndexError(
                f"VerboseLogger: RestoreFormat: index ({index}) is out of range "
                f"[0, {len(self.__labels) - 1}].")

        if index == len(self.__labels) - 1:
            self.__logger.warning("Current format is already set.")
            return

        self.__format = lambda record: "[{time:DD.MM.YYYY HH:mm:ss}] " +\
            f"<{self.__colors[index]}>{self.__labels[index]}:</{self.__colors[index]}> " +\
            f"{record['message']} ".ljust(self.__message_ljust) +\
            f"[<level>{record['level']}</level>]\n"

        self.__labels = self.__labels[:index+1]
        self.__colors = self.__colors[:index+1]

        self.__Configure()

    def LogException(self,
                     exception: Exception):
        """
        Логирует исключение в консоль и записывает его в файл.

        Args:
            exception (Exception): объект исключения.

        Raises:
            NotImplementedError: если не установлен формат логгера.
        """

        if len(self.__labels) == 0:
            raise NotImplementedError(
                "VerboseLogger: logger format is not set. Call 'logger.UpdateFormat' first.")

        self.__logger.error(f"{exception}")

        os.makedirs(os.path.dirname(self.__exceptions_file), exist_ok=True)

        try:
            self.__Configure(self.__exceptions_file)

            self.__logger.error(
                f"{re.sub(r'"(.*?)\",\s+line\s+(\d+)', r'\1:\2', traceback.format_exc())}")

        except Exception as exception:
            self.__logger.error("VerboseLogger: "
                                f"failed to write exception to file: {exception}.")

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
            log_mode (LogMode, optional): режим логирования. Defaults to LogMode.RETICENTLY.

        Raises:
            NotImplementedError: если не установлен формат логгера.
        """

        if len(self.__labels) == 0:
            raise NotImplementedError(
                "VerboseLogger: logger format is not set. Call 'logger.UpdateFormat' first!")

        if self.__log_mode == LogMode.VERBOSELY or\
                log_mode == LogMode.RETICENTLY:
            self.__logger.log(level, message)

    def __Configure(self,
                    output: TextIO | Any | None = None):
        """
        Настраивает вывод логгера.

        Args:
            output (TextIO | Any | None, optional): поток вывода. 
                                                    Defaults to None: в таком случае 
                                                    устанавливает на стандартный.
        """
        if output is None:
            output = self.__standard_output

        self.__logger.remove()
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

        level_name = level.upper()

        def Wrapper(message: str,
                    log_mode: LogMode = LogMode.RETICENTLY):
            self.Log(level_name, message, log_mode)

        return Wrapper

    def __getattr__(self,
                    name: str) -> Any:
        """
        Перехватывает обращение к атрибутам класса и, 
        если это методы логирования (debug, info и т.д.), 
        возвращает функцию-обертку для логирования.  

        В противном случае, возвращает атрибут из базового логгера loguru.

        Args:
            name (str): имя атрибута.

        Returns:
            Any: атрибут или функция-обертка для логирования.
        """

        if name in [string.lower() for string in loguru.logger._core.levels.keys()]:  # type: ignore
            return self.__LogMethod(name)

        else:  # другие методы loguru.logger
            return getattr(self.__logger, name)


# MARK: v_logger
v_logger = VerboseLogger.FromConfig(GetConfig()["Utils"]["VerboseLogger"])  # type: ignore
