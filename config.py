__author__ = 'InfSub'
__contact__ = 'ADmin@TkYD.ru'
__copyright__ = 'Copyright (C) 2024, [LegioNTeaM] InfSub'
__date__ = '2024/12/26'
__deprecated__ = False
__email__ = 'ADmin@TkYD.ru'
__maintainer__ = 'InfSub'
__status__ = 'Production'
__version__ = '1.7.0'

from os import getenv
from dotenv import load_dotenv

# Загрузка переменных окружения из файла .env
load_dotenv()


def load_env() -> dict:
    """
    Загрузка переменных окружения из файла .env.
    """
    # Загрузка всех переменных, которые могут понадобиться в проекте
    return {
        'CSV_SEPARATOR': getenv('CSV_SEPARATOR'),
        'CSV_PATH_TEMPLATE_DIRECTORY': getenv('CSV_PATH_TEMPLATE_DIRECTORY'),
        'CSV_PATH_DIRECTORY': getenv('CSV_PATH_DIRECTORY'),
        'CSV_FILE_PATTERN': getenv('CSV_FILE_PATTERN'),
        'CSV_FILE_NAME': getenv('CSV_FILE_NAME', ''),
        'CSV_FILE_NAME_FOR_DTA': getenv('CSV_FILE_NAME_FOR_DTA', ''),
        'CSV_FILE_NAME_FOR_CHECKER': getenv('CSV_FILE_NAME_FOR_CHECKER', ''),
        
        'MAX_WIDTH': int(getenv('MAX_WIDTH', 200)),
        'DECIMAL_PLACES': int(getenv('DECIMAL_PLACES', 2)),
        'NAME_OF_PRODUCT_TYPE': getenv('NAME_OF_PRODUCT_TYPE'),
        'INACTIVITY_LIMIT_HOURS': int(getenv('INACTIVITY_LIMIT_HOURS', 24)),
        
        'TELEGRAM_TOKEN': getenv('TELEGRAM_TOKEN'),
        'TELEGRAM_CHAT_ID': getenv('TELEGRAM_CHAT_ID'),

        'LOG_DIR': getenv('LOG_DIRECTORY', 'logs'),
        'LOG_LEVEL_CONSOLE': getenv('LOG_LEVEL_CONSOLE', 'WARNING'),
        'LOG_LEVEL_FILE': getenv('LOG_LEVEL_FILE', 'WARNING'),

    }


def get_csv_config() -> dict:
    """
    Получение парамметров для CSV.
    """
    env = load_env()
    return {
        'csv_separator': env['CSV_SEPARATOR'],
        'csv_path_template_directory': env['CSV_PATH_TEMPLATE_DIRECTORY'],
        'csv_path_directory': env['CSV_PATH_DIRECTORY'],
        'csv_file_pattern': env['CSV_FILE_PATTERN'],
        'csv_file_name': env['CSV_FILE_NAME'],
        'csv_file_name_for_dta': env['CSV_FILE_NAME_FOR_DTA'],
        'csv_file_name_for_checker': env['CSV_FILE_NAME_FOR_CHECKER'],
        
        'max_width': env['MAX_WIDTH'],
        'decimal_places': env['DECIMAL_PLACES'],
        'name_of_product_type': env['NAME_OF_PRODUCT_TYPE'],
        'inactivity_limit_hours': env['INACTIVITY_LIMIT_HOURS']
    }


def get_log_config() -> dict:
    """
    Получение конфигурации для логгера.
    """
    env = load_env()
    return {
        'dir': env['LOG_DIR'],
        'level_console': env['LOG_LEVEL_CONSOLE'],
        'level_file': env['LOG_LEVEL_FILE'],
    }


def get_telegram_config() -> dict:
    """
    Получение конфигурации для Telegram.
    """
    env = load_env()
    return {
        'telegram_token': env['TELEGRAM_TOKEN'],
        'telegram_chat_id': env['TELEGRAM_CHAT_ID'],
    }


if __name__ == '__main__':
    # print('Database Config:', get_db_config())
    # print('SMB Config:', get_smb_config())
    # print('Schedule Config:', get_schedule_config())
    pass
