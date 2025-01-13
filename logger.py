__author__ = 'InfSub'
__contact__ = 'ADmin@TkYD.ru'
__copyright__ = 'Copyright (C) 2024, [LegioNTeaM] InfSub'
__date__ = '2024/12/26'
__deprecated__ = False
__email__ = 'ADmin@TkYD.ru'
__maintainer__ = 'InfSub'
__status__ = 'Production'  # 'Production / Development'
__version__ = '1.7.0'

import logging
# noinspection PyPackageRequirements
import colorlog
from os import makedirs
from os.path import join
from datetime import datetime

from config import get_log_config


# Получение параметров из ENV
env = get_log_config()


# Вспомогательная функция для форматирования текущей даты
def get_formatted_date(format_str):
    return datetime.now().strftime(format_str)


# Декоратор для настройки и получения логгера
def with_logger_setup(func):
    def wrapper(*args, **kwargs):
        logger = configure_logging()
        return func(logger, *args, **kwargs)
    return wrapper


# Настройка логирования
def configure_logging():
    # Получаем директорию для логов из переменной окружения
    log_dir = join(env['dir'], get_formatted_date('%Y'), get_formatted_date('%Y.%m'))

    # Убедитесь, что директория существует
    makedirs(log_dir, exist_ok=True)

    # Создаем имя файла лога с текущей датой
    log_file_name = f'{get_formatted_date("%Y.%m.%d")}.log'
    log_file_path = join(log_dir, log_file_name)

    # Создаем форматтер для логов с цветами
    color_formatter = colorlog.ColoredFormatter(
        '%(filename)s:%(lineno)-5d\t| %(log_color)s%(asctime)s| %(funcName)-27s| %(levelname)-8s| %(message)s',
        log_colors={
            'DEBUG': 'orange',
            'INFO': 'light_green',
            'WARNING': 'yellow',
            'ERROR': 'red',
            'CRITICAL': 'bold_red',
        }
    )

    # Настраиваем StreamHandler для вывода в консоль
    console_handler = colorlog.StreamHandler()
    console_handler.setFormatter(color_formatter)

    # Настраиваем FileHandler для вывода в файл
    file_handler = logging.FileHandler(log_file_path)
    file_formatter = logging.Formatter(
        '%(filename)s:%(lineno)-5d | %(asctime)s | %(funcName)-26s | %(levelname)-8s | %(message)s')
    file_handler.setFormatter(file_formatter)

    # Создаем логгер
    logger = logging.getLogger(__name__)

    # Проверяем, есть ли уже обработчики
    if logger.hasHandlers():
        # Если обработчики уже есть, выходим, ене добавляя новых
        return logger

    # Устанавливаем уровень логирования
    logger.setLevel(logging.INFO)

    # Добавляем обработчики в логгер
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger


# Пример использования декоратора
@with_logger_setup
def main(logger):
    logger.info('Программа запущена')


if __name__ == '__main__':
    # Вызов основного метода
    main()

