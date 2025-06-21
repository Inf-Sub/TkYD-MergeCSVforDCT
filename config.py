# __author__ = 'InfSub'
# __contact__ = 'https:/t.me/InfSub'
# __copyright__ = 'Copyright (C) 2025, [LegioNTeaM] InfSub'
# __date__ = '2025/06/19'
# __deprecated__ = False
# __maintainer__ = 'InfSub'
# __status__ = 'Development'  # 'Production / Development'
# __version__ = '1.8.0.0'

from enum import Enum
from os import getenv
from typing import Dict, Any

from dotenv import load_dotenv
from datetime import datetime as dt
import logging


class ConfigNames(Enum):
    CSV = 'csv'
    DATAS = 'datas'
    INACTIVITY = 'inactivity'
    TELEGRAM = 'telegram'
    MSG = 'msg'
    LOG = 'log'

    def __str__(self):
        return f"ConfigNames.{self.name} = {self.value}"
    
    def __repr__(self):
        return f"<ConfigNames.{self.name} (value={self.value})>"


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
                'CSV_SEPARATOR': getenv('CSV_SEPARATOR', ';'),
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
                'TELEGRAM_MAX_MSG_LENGTH': int(getenv('TELEGRAM_MAX_MSG_LENGTH', 4096)),
                'TELEGRAM_LINE_HEIGHT': int(getenv('TELEGRAM_LINE_HEIGHT', 25)),
                'TELEGRAM_PARSE_MODE': getenv('TELEGRAM_PARSE_MODE', None),

                'MSG_LANGUAGE': getenv('MSG_LANGUAGE', 'en').lower(),

                'LOG_DIR': current_date.strftime(getenv('LOG_DIR', r'logs\%Y\%Y.%m')),
                'LOG_FILE': current_date.strftime(getenv('LOG_FILE', 'backup_log_%Y.%m.%d.log')),
                'LOG_LEVEL_ROOT': getenv('LOG_LEVEL_ROOT', 'INFO').upper(),
                'LOG_LEVEL_CONSOLE': getenv('LOG_LEVEL_CONSOLE', 'INFO').upper(),
                'LOG_LEVEL_FILE': getenv('LOG_LEVEL_FILE', 'WARNING').upper(),
                'LOG_IGNORE_LIST': [item.strip() for item in getenv('LOG_IGNORE_LIST', '').split(',') if item.strip()],
                'LOG_FORMAT_CONSOLE': getenv('LOG_FORMAT_CONSOLE').replace(r'\t', '\t').replace(r'\n', '\n'),
                'LOG_FORMAT_FILE': getenv('LOG_FORMAT_FILE').replace(r'\t', '\t').replace(r'\n', '\n'),
                'LOG_DATE_FORMAT': getenv('LOG_DATE_FORMAT', '%Y.%m.%d %H:%M:%S'),  # Default: None
                'LOG_CONSOLE_LANGUAGE': getenv('MSG_LANGUAGE', 'en').lower(),  # temp
            }
        except (TypeError, ValueError) as e:
            logging.error(e)
            exit()
    
    def get_config(self, *config_types: str | ConfigNames) -> Dict[str, Any]:
        """
        Получение конфигурационных параметров по указанным типам. Можно передавать как строки, так и члены Enum
        ConfigNames.

        Метод ищет все переменные окружения в self._env, название которых начинается с указанного префикса
        (в верхнем регистре, с добавленным подчеркиванием). Например, для префикса 'CSV' он ищет переменные,
        начинающиеся с 'CSV_'.

        :param config_types: Один или несколько префиксов, или членов Enum ConfigNames, по которым осуществляется поиск
            переменных окружения.

        :return: Словарь с параметрами, соответствующими указанным префиксам. Ключи в результате приводятся к нижнему
            регистру.
        """
        result = {}
        for config_type in config_types:
            prefix = config_type.value if isinstance(config_type, ConfigNames) else config_type
            result.update({key.lower(): self._env[key] for key in self._env if key.startswith(prefix.upper() + '_')})
        return result
    
    # ver 1.7.2.2  # def get_config(self, *config_types: str) -> Dict[str, Any]:
    #     """
    #     Получение конфигурации по указанным типам.
    #
    #     :param config_types: Префиксы для поиска переменных окружения.
    #     :return: Возвращает словарь с параметрами, соответствующими указанным префиксам.
    #     """
    #     result = {}
    #     for config_type in config_types:
    #         result.update(
    #             {key.lower(): self._env[key] for key in self._env.keys() if key.startswith(config_type.upper() + '_')})
    #     return result
    
    # ver 1.8.0.0 alternative
    # def get_config(self, *config_types: str | ConfigNames) -> Dict[str, Any]:
    #     result = {}
    #     for config_type in config_types:
    #         # Обработка Enum
    #         if isinstance(config_type, ConfigNames):
    #             prefix = config_type.value
    #         # Проверка, что строка соответствует существующему Enum
    #         elif isinstance(config_type, str):
    #             prefix = config_type
    #         else:
    #             raise ValueError(f"Unsupported config type: {config_type} ({type(config_type)})")
    #
    #         # Проверка, что для строки есть соответствующий Enum (опционально)
    #         # Например, если нужно убедиться, что строка соответствует Enum, можно сделать так:
    #         # if isinstance(config_type, str):
    #         #     if not any(config_type == enum_member.value for enum_member in ConfigNames):
    #         #         raise ValueError(f"String '{config_type}' не соответствует ни одному значению ConfigNames")
    #
    #         # Получение переменных окружения, начинающихся с префикса
    #         prefix_upper = prefix.upper()
    #         result.update({key.lower(): self._env[key] for key in self._env if key.startswith(prefix_upper + '_')})
    #     return result
    



if __name__ == '__main__':
    from pprint import pprint
    
    config = Config()
    server_config = config.get_config('SERVER')
    files_config = config.get_config('files')
    csv_config = config.get_config(ConfigNames.CSV)
    datas_config = config.get_config(ConfigNames.DATAS)
    log_config = config.get_config(ConfigNames.LOG)
    
    print('Server Config:')
    pprint(server_config)
    print()
    print('Files Config:')
    pprint(files_config)
    print()
    print('CSV Config:')
    pprint(csv_config)
    print()
    print('Datas Config:')
    pprint(datas_config)
    print()
    print('Log Config:')
    pprint(log_config)
    print()

    print('ConfigNames')
    print(ConfigNames)
    print(repr(ConfigNames))
    print()
    print('ConfigNames.CSV')
    print(ConfigNames.CSV)
    print(repr(ConfigNames.CSV))
    print()
    print('ConfigNames.DATAS')
    print(ConfigNames.DATAS)
    print(repr(ConfigNames.DATAS))
    print()
