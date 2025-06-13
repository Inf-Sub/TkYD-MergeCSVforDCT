# __author__ = 'InfSub'
# __contact__ = 'https:/t.me/InfSub'
# __copyright__ = 'Copyright (C) 2025, [LegioNTeaM] InfSub'
# __date__ = '2025/06/01'
# __deprecated__ = False
# __maintainer__ = 'InfSub'
# __status__ = 'Development'  # 'Production / Development'
# __version__ = '1.7.2.2'

from os import getenv
from typing import Dict, Any

from dotenv import load_dotenv
from datetime import datetime as dt
import logging


class Config:
    """Синглтон для загрузки и хранения конфигурации приложения."""
    _instance = None
    
    def __new__(cls, *args, **kwargs) -> 'Config':
        """Создает новый экземпляр класса Config, если он еще не создан.

        :param args: Позиционные аргументы.
        :param kwargs: Именованные аргументы.
        :return: Экземпляр класса Config.
        """
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        """Инициализация экземпляра Config.

        Загружает переменные окружения из файла ._env и инициализирует параметры.
        """
        # Проверяем, инициализирован ли уже экземпляр
        if not hasattr(self, '_initialized'):
            self._initialized = True  # Устанавливаем флаг инициализации
            logging.info('Загрузка переменных окружения из файла ._env')
            load_dotenv()
            self._current_date = dt.now()
            self._env = self._load_env()
    
    def _load_env(self) -> Dict[str, Any]:
        """
        Загрузка переменных окружения из файла ._env.

        :return: Возвращает словарь с параметрами из файла ._env.
        """
        current_date = self._current_date
        try:
            return {
                'CSV_SEPARATOR': getenv('CSV_SEPARATOR'),
                'CSV_PATH_TEMPLATE_DIRECTORY': getenv('CSV_PATH_TEMPLATE_DIRECTORY'),
                'CSV_PATH_DIRECTORY': getenv('CSV_PATH_DIRECTORY'),
                'CSV_FILE_PATTERN': getenv('CSV_FILE_PATTERN'),
                # 'CSV_FILE_NAME': getenv('CSV_FILE_NAME', ''),
                'CSV_FILE_NAME_FOR_DTA': getenv('CSV_FILE_NAME_FOR_DTA', ''),
                'CSV_FILE_NAME_FOR_CHECKER': getenv('CSV_FILE_NAME_FOR_CHECKER', ''),

                'DATAS_MAX_WIDTH': int(getenv('DATAS_MAX_WIDTH', 200)),
                'DATAS_DECIMAL_PLACES': int(getenv('DATAS_DECIMAL_PLACES', 2)),
                'DATAS_NAME_OF_PRODUCT_TYPE': getenv('DATAS_NAME_OF_PRODUCT_TYPE'),

                'INACTIVITY_LIMIT_HOURS': int(getenv('INACTIVITY_LIMIT_HOURS', 24)),
                
                'TELEGRAM_TOKEN': getenv('TELEGRAM_TOKEN'),
                'TELEGRAM_CHAT_ID': getenv('TELEGRAM_CHAT_ID'),

                'MSG_LANGUAGE': getenv('MSG_LANGUAGE', 'en').lower(),

                'LOG_DIR': current_date.strftime(getenv('LOG_DIR', r'logs\%Y\%Y.%m')),
                'LOG_FILE': current_date.strftime(getenv('LOG_FILE', 'backup_log_%Y.%m.%d.log')),
                'LOG_LEVEL_ROOT': getenv('LOG_LEVEL_ROOT', 'INFO').upper(),
                'LOG_LEVEL_CONSOLE': getenv('LOG_LEVEL_CONSOLE', 'INFO').upper(),
                'LOG_LEVEL_FILE': getenv('LOG_LEVEL_FILE', 'WARNING').upper(),
                'LOG_IGNORE_LIST': getenv('LOG_IGNORE_LIST', ''),
                'LOG_FORMAT_CONSOLE': getenv('LOG_FORMAT_CONSOLE').replace(r'\t', '\t').replace(r'\n', '\n'),
                'LOG_FORMAT_FILE': getenv('LOG_FORMAT_FILE').replace(r'\t', '\t').replace(r'\n', '\n'),
                'LOG_DATE_FORMAT': getenv('LOG_DATE_FORMAT', '%Y.%m.%d %H:%M:%S'),  # Default: None
                'LOG_CONSOLE_LANGUAGE': getenv('MSG_LANGUAGE', 'en').lower(),  # temp
            }
        except (TypeError, ValueError) as e:
            logging.error(e)
            exit()
    
    def get_config(self, *config_types: str) -> Dict[str, Any]:
        """
        Получение конфигурации по указанным типам.

        :param config_types: Префиксы для поиска переменных окружения.
        :return: Возвращает словарь с параметрами, соответствующими указанным префиксам.
        """
        result = {}
        for config_type in config_types:
            result.update(
                {key.lower(): self._env[key] for key in self._env.keys() if key.startswith(config_type.upper() + '_')})
        return result


if __name__ == "__main__":
    from pprint import pprint
    
    config = Config()
    server_config = config.get_config('SERVER')
    backup_files_config = config.get_config('FILES')
    log_config = config.get_config('LOG')
    
    print("Server Config:")
    pprint(server_config)
    print()
    print("Backup Files Config:")
    pprint(backup_files_config)
    print()
    print("Log Config:")
    pprint(log_config)
