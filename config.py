# __author__ = 'InfSub'
# __contact__ = 'https:/t.me/InfSub'
# __copyright__ = 'Copyright (C) 2025, [LegioNTeaM] InfSub'
# __date__ = '2025/06/26'
# __deprecated__ = False
# __maintainer__ = 'InfSub'
# __status__ = 'Development'  # 'Production / Development'
# __version__ = '1.9.0.1'

from enum import Enum
from os import getenv
from typing import Dict, Any
from datetime import datetime as dt
import logging
from configparser import ConfigParser
from pathlib import Path
from dotenv import load_dotenv


class ConfigNames(Enum):
    """
    Enum для определения доступных секций конфигурации.
    
    Каждое значение соответствует секции в config.ini файле.
    Используется для типизированного доступа к конфигурационным параметрам.
    """
    # CONFIG_FILE = 'config.ini'
    CSV = 'csv'
    DATAS = 'datas'
    INACTIVITY = 'inactivity'
    TELEGRAM = 'telegram'
    MSG = 'msg'
    LOG = 'log'
    LOGFORMAT = 'logformat'
    RUN = 'run'

    def __str__(self):
        """Возвращает строковое представление enum'а."""
        return f'ConfigNames.{self.name} = {self.value}'
    
    def __repr__(self):
        """Возвращает техническое представление enum'а."""
        return f'<ConfigNames.{self.name} (value={self.value})>'


class Config:
    """
    Синглтон для загрузки и хранения конфигурации приложения.
    
    Загружает параметры из двух источников:
    - config.ini (приоритет) - для не приватных настроек
    - .env файл (fallback) - для приватных настроек (токены, ID)
    
    Реализует паттерн Singleton для обеспечения единственного экземпляра
    конфигурации во всем приложении.
    """
    _instance = None
    
    def __new__(cls, *args, **kwargs) -> 'Config':
        """
        Создает новый экземпляр класса Config, если он еще не создан.

        :param args: Позиционные аргументы.
        :param kwargs: Именованные аргументы.
        :return: Экземпляр класса Config.
        """
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        """
        Инициализация экземпляра Config.

        Загружает переменные окружения из файла .env и config.ini и инициализирует параметры.
        Инициализация происходит только один раз благодаря паттерну Singleton.
        """
        # Проверяем, инициализирован ли уже экземпляр
        if not hasattr(self, '_initialized'):
            self._initialized = True  # Устанавливаем флаг инициализации
            # Убираем логирование отсюда, чтобы не логировать до настройки логирования
            # logging.info('Загрузка переменных окружения из файла .env и config.ini')
            load_dotenv()
            self._current_date = dt.now()
            self._config_ini = self._load_ini()
            self._env = self._load_env()
    
    @staticmethod
    def _load_ini():
        """
        Загружает и парсит config.ini файл.
        
        :return: ConfigParser объект с загруженными данными из config.ini.
        """
        config = ConfigParser(interpolation=None)
        ini_path = Path(Path(__file__).parent, 'config.ini')
        config.read(ini_path, encoding='utf-8')
        return config

    def _get_ini_section(self, section: ConfigNames | str) -> dict:
        """
        Получает секцию из config.ini файла.
        
        :param section: ConfigNames enum или строка с именем секции.
        :return: Словарь с параметрами секции (ключи в верхнем регистре).
        """
        section_name = section.value.upper() if isinstance(section, ConfigNames) else section.upper()
        if section_name in self._config_ini:
            return {k.upper(): v for k, v in self._config_ini[section_name].items()}
        return {}

    def _load_env(self) -> Dict[str, Any]:
        """
        Загружает и объединяет параметры из config.ini и .env файлов.
        
        Приоритет: config.ini > .env файл
        Приватные параметры (токены, ID) загружаются только из .env
        
        :return: Словарь с объединенными параметрами конфигурации.
        """
        current_date = self._current_date
        # INI > ENV
        ini_csv = self._get_ini_section(ConfigNames.CSV)
        ini_datas = self._get_ini_section(ConfigNames.DATAS)
        ini_log = self._get_ini_section(ConfigNames.LOG)
        ini_logformat = self._get_ini_section(ConfigNames.LOGFORMAT)
        ini_telegram = self._get_ini_section(ConfigNames.TELEGRAM)
        ini_msg = self._get_ini_section(ConfigNames.MSG)
        ini_inactivity = self._get_ini_section(ConfigNames.INACTIVITY)
        ini_run = self._get_ini_section(ConfigNames.RUN)
        
        # Подстановка переменных LOGFORMAT в форматы
        def substitute_logformat(format_str: str) -> str:
            """
            Подставляет значения из LOGFORMAT секции в строки форматов логов.
            
            :param format_str: Строка формата с плейсхолдерами ${LOGFORMAT_XXX}.
            :return: Строка с подставленными значениями.
            """
            if not format_str:
                return format_str
            for key, value in ini_logformat.items():
                format_str = format_str.replace(f'${{LOGFORMAT_{key}}}', value)
            return format_str.replace(r'\t', '\t').replace(r'\n', '\n')

        return {
            # CSV
            'CSV_SEPARATOR': ini_csv.get('SEPARATOR', getenv('CSV_SEPARATOR', ';')),
            'CSV_PATH_TEMPLATE_DIRECTORY': ini_csv.get('PATH_TEMPLATE_DIRECTORY', getenv('CSV_PATH_TEMPLATE_DIRECTORY')),
            'CSV_PATH_DIRECTORY': ini_csv.get('PATH_DIRECTORY', getenv('CSV_PATH_DIRECTORY')),
            'CSV_FILE_PATTERN': ini_csv.get('FILE_PATTERN', getenv('CSV_FILE_PATTERN')),
            'CSV_FILE_NAME_FOR_DTA': ini_csv.get('FILE_NAME_FOR_DTA', getenv('CSV_FILE_NAME_FOR_DTA', '')),
            'CSV_FILE_NAME_FOR_CHECKER': ini_csv.get('FILE_NAME_FOR_CHECKER', getenv('CSV_FILE_NAME_FOR_CHECKER', '')),
            
            # DATAS
            'DATAS_MAX_WIDTH': int(ini_datas.get('MAX_WIDTH', getenv('DATAS_MAX_WIDTH', 200))),
            'DATAS_DECIMAL_PLACES': int(ini_datas.get('DECIMAL_PLACES', getenv('DATAS_DECIMAL_PLACES', 2))),
            'DATAS_NAME_OF_PRODUCT_TYPE': ini_datas.get('NAME_OF_PRODUCT_TYPE', getenv('DATAS_NAME_OF_PRODUCT_TYPE')),
            
            # INACTIVITY
            'INACTIVITY_LIMIT_HOURS': int(ini_inactivity.get('LIMIT_HOURS', getenv('INACTIVITY_LIMIT_HOURS', 24))),
            
            # TELEGRAM (только приватные из env)
            'TELEGRAM_TOKEN': getenv('TELEGRAM_TOKEN'),
            'TELEGRAM_CHAT_ID': getenv('TELEGRAM_CHAT_ID'),
            'TELEGRAM_MAX_MSG_LENGTH': int(ini_telegram.get('MAX_MSG_LENGTH', getenv('TELEGRAM_MAX_MSG_LENGTH', 4096))),
            'TELEGRAM_LINE_HEIGHT': int(ini_telegram.get('LINE_HEIGHT', getenv('TELEGRAM_LINE_HEIGHT', 25))),
            'TELEGRAM_PARSE_MODE': ini_telegram.get('PARSE_MODE', getenv('TELEGRAM_PARSE_MODE', None)),
            
            # MSG
            'MSG_LANGUAGE': ini_msg.get('LANGUAGE', getenv('MSG_LANGUAGE', 'en')).lower(),
            
            # LOG
            'LOG_DIR': current_date.strftime(ini_log.get('DIR', getenv('LOG_DIR', r'logs\%Y\%Y.%m'))),
            'LOG_FILE': current_date.strftime(ini_log.get('FILE', getenv('LOG_FILE', 'backup_log_%Y.%m.%d.log'))),
            'LOG_LEVEL_ROOT': ini_log.get('LEVEL_ROOT', getenv('LOG_LEVEL_ROOT', 'INFO')).upper(),
            'LOG_LEVEL_CONSOLE': ini_log.get('LEVEL_CONSOLE', getenv('LOG_LEVEL_CONSOLE', 'INFO')).upper(),
            'LOG_LEVEL_FILE': ini_log.get('LEVEL_FILE', getenv('LOG_LEVEL_FILE', 'WARNING')).upper(),
            'LOG_IGNORE_LIST': [item.strip() for item in ini_log.get(
                'IGNORE_LIST', getenv('LOG_IGNORE_LIST', '')).split(',') if item.strip()],
            'LOG_FORMAT_CONSOLE': substitute_logformat(ini_log.get('FORMAT_CONSOLE', getenv('LOG_FORMAT_CONSOLE', ''))),
            'LOG_FORMAT_FILE': substitute_logformat(ini_log.get('FORMAT_FILE', getenv('LOG_FORMAT_FILE', ''))),
            'LOG_DATE_FORMAT': ini_log.get('DATE_FORMAT', getenv('LOG_DATE_FORMAT', '%Y.%m.%d %H:%M:%S')),
            'LOG_CONSOLE_LANGUAGE': ini_log.get('CONSOLE_LANGUAGE', getenv('LOG_CONSOLE_LANGUAGE', 'en')).lower(),
            
            # RUN
            'RUN_MAIN_SCRIPT': ini_run.get('MAIN_SCRIPT', getenv('RUN_MAIN_SCRIPT', 'merge_csv')),
            'RUN_REQUIREMENTS_FILE': ini_run.get(
                'REQUIREMENTS_FILE', getenv('RUN_REQUIREMENTS_FILE', 'requirements.txt')),
            'RUN_VENV_PATH': ini_run.get('VENV_PATH', getenv('RUN_VENV_PATH', '.venv')),
            'RUN_VENV_INDIVIDUAL': ini_run.get(
                'VENV_INDIVIDUAL', getenv('RUN_VENV_INDIVIDUAL', 'True')).lower() in ('true', '1'),
            'RUN_GIT_PULL_ENABLED': ini_run.get('GIT_PULL_ENABLED', getenv('RUN_GIT_PULL_ENABLED', 'True')).lower() in ('true', '1'),
        }

    def get_config(self, *config_types: str | ConfigNames) -> Dict[str, Any]:
        """
        Получение конфигурационных параметров по указанным типам.
        
        Можно передавать как строки, так и члены Enum ConfigNames.
        Метод ищет все переменные окружения в self._env, название которых 
        начинается с указанного префикса (в верхнем регистре, с добавленным подчеркиванием). 
        Например, для префикса 'CSV' он ищет переменные, начинающиеся с 'CSV_'.

        :param config_types: Один или несколько префиксов, или членов Enum ConfigNames, 
            по которым осуществляется поиск переменных окружения.
        :return: Словарь с параметрами, соответствующими указанным префиксам. 
            Ключи в результате приводятся к нижнему регистру.
        """
        result = {}
        for config_type in config_types:
            prefix = config_type.value if isinstance(config_type, ConfigNames) else config_type
            result.update({key.lower(): self._env[key] for key in self._env if key.startswith(prefix.upper() + '_')})
        return result


if __name__ == '__main__':
    cfg = Config()
    
    print('=== CONFIG TEST ===')
    print()
    
    # Цикл по всем ConfigNames
    for config_name in ConfigNames:
        config_data = cfg.get_config(config_name)
        print(f'{config_name.name} Config:')
        if config_data:
            for cfg_key, cfg_value in config_data.items():
                print(f'\t{cfg_key}: {cfg_value}')
        else:
            print('\t(empty)')
        print()
    
    print()
    print(f'{'=' * 40}')
    print()

    print('=== ConfigNames INFO ===')
    print(f'All ConfigNames: {list(ConfigNames)}')
    print(f'Total ConfigNames: {len(ConfigNames)}')
    print()

    for config_name in ConfigNames:
        print(f'{config_name.name}: {config_name.value} ({repr(config_name)})')

    print()
    print(f'{ConfigNames.DATAS} => value: {ConfigNames.DATAS.value} (repr: {repr(ConfigNames.DATAS)})')
