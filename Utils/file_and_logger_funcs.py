import sys
import os

from loguru import logger


def DeleteFilesInFolder(folder_path: str, except_files: list[str]) -> None:
    """
    Удаляет все файлы в указанной папке, кроме файлов в списке исключений.

    Args:
        folder_path (str): путь к папке.
        except_files (list[str]): список имен файлов, которые нужно исключить из удаления.
    """

    for file_name in os.listdir(folder_path):
        full_file_path = os.path.join(folder_path, file_name)

        if os.path.isfile(full_file_path) and file_name not in except_files:
            os.remove(full_file_path)


def IsFileInFolder(folder_path: str, file_name: str) -> bool:
    """
    Проверяет, существует ли файл в указанной папке.

    Args:
      file_name: путь к файлу, который нужно проверить.
      folder_path: путь к папке, в которой нужно проверить наличие файла.

    Returns:
      True, если файл существует в папке, в противном случае False.
    """

    full_file_path = os.path.join(folder_path, file_name)
    return os.path.exists(full_file_path)


def CreateFolder(folder_path: str, folder_name: str):
    """
    Создает папку, использует логирование
    (в случае исключения также выводит об этом в консоль)

    Args:
        folder_path (str): путь к папке.
        folder_name (str): имя папки (для логирования).
    """

    try:
        logger.info(f"Creating folder '{folder_name}'...".ljust(77))
        os.mkdir(folder_path)
        logger.success(f"Creating folder '{
                       folder_name}': SUCCESS".ljust(77))

    except Exception as exception:
        logger.warning(f"{exception}".ljust(77))


def UpdateLoggerFormat(logger_label: str, color: str) -> None:
    """
    Обновляет формат вывода логирования

    Args:
        logger_label (str): текст заголовка для логирования
        color (str): цвет заголовка для логирования
    """

    logger.remove()
    logger.add(sink=sys.stdout,
               format="[{time:DD.MM.YYYY HH:mm:ss}]" +
                      f" <{color}>{logger_label}:</{color}>" +
                      " <white>{message}</white>" +
                      " [<level>{level}</level>]")
