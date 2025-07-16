# __author__ = 'InfSub'
# __contact__ = 'https:/t.me/InfSub'
# __copyright__ = 'Copyright (C) 2025, [LegioNTeaM] InfSub'
# __date__ = '2025/06/27'
# __deprecated__ = False
# __maintainer__ = 'InfSub'
# __status__ = 'Development'  # 'Production / Development'
# __version__ = '2.1.0.0'

from enum import Enum
from os import getenv
from typing import Dict, Any
from datetime import datetime as dt
from configparser import ConfigParser
from pathlib import Path
# from dotenv import load_dotenv  # Убираю из base, будет только в наследнике


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


class BaseConfig:
    """
    Базовый класс для работы только с config.ini (без .env и dotenv).
    Не использует сторонние зависимости.
    """
    def __init__(self):
        self._current_date = dt.now()
        self._config_ini = self._load_ini()

    @staticmethod
    def _load_ini():
        config = ConfigParser(interpolation=None)
        ini_path = Path(Path(__file__).parent, 'config.ini')
        config.read(ini_path, encoding='utf-8')
        return config

    def _get_ini_section(self, section: ConfigNames | str) -> dict:
        section_name = section.value.upper() if isinstance(section, ConfigNames) else section.upper()
        if section_name in self._config_ini:
            return {k.upper(): v for k, v in self._config_ini[section_name].items()}
        return {}

    def get_config(self, *config_types: str | ConfigNames) -> Dict[str, Any]:
        result = {}
        for config_type in config_types:
            prefix = config_type.value if isinstance(config_type, ConfigNames) else config_type
            # Только из ini, без env
            for key, value in self._get_all_ini_vars().items():
                if key.startswith(prefix.upper() + '_'):
                    result[key.lower()] = value
        return result

    def _get_all_ini_vars(self) -> Dict[str, Any]:
        # Собирает все переменные из всех секций ini (ключи в верхнем регистре)
        all_vars = {}
        for section in self._config_ini.sections():
            for k, v in self._config_ini[section].items():
                all_vars[k.upper()] = v
        return all_vars

    def get_normalized_config(self) -> Dict[str, Any]:
        """
        Возвращает параметры с "человеческими" именами, аналогично Config, но только из ini.
        """
        current_date = self._current_date
        ini_csv = self._get_ini_section(ConfigNames.CSV)
        ini_datas = self._get_ini_section(ConfigNames.DATAS)
        ini_log = self._get_ini_section(ConfigNames.LOG)
        ini_logformat = self._get_ini_section(ConfigNames.LOGFORMAT)
        ini_telegram = self._get_ini_section(ConfigNames.TELEGRAM)
        ini_msg = self._get_ini_section(ConfigNames.MSG)
        ini_inactivity = self._get_ini_section(ConfigNames.INACTIVITY)
        ini_run = self._get_ini_section(ConfigNames.RUN)

        def substitute_logformat(format_str: str) -> str:
            if not format_str:
                return format_str
            for key, value in ini_logformat.items():
                format_str = format_str.replace(f'${{LOGFORMAT_{key}}}', value)
            return format_str.replace(r'\t', '\t').replace(r'\n', '\n')

        return {
            # CSV
            'CSV_SEPARATOR': ini_csv.get('SEPARATOR', ';'),
            'CSV_PATH_TEMPLATE_DIRECTORY': ini_csv.get('PATH_TEMPLATE_DIRECTORY'),
            'CSV_PATH_DIRECTORY': ini_csv.get('PATH_DIRECTORY'),
            'CSV_FILE_PATTERN': ini_csv.get('FILE_PATTERN'),
            'CSV_FILE_NAME_FOR_DTA': ini_csv.get('FILE_NAME_FOR_DTA', ''),
            'CSV_FILE_NAME_FOR_CHECKER': ini_csv.get('FILE_NAME_FOR_CHECKER', ''),
            # DATAS
            'DATAS_MAX_WIDTH': int(ini_datas.get('MAX_WIDTH', 200)) if ini_datas.get('MAX_WIDTH') else 200,
            'DATAS_DECIMAL_PLACES': int(ini_datas.get('DECIMAL_PLACES', 2)) if ini_datas.get('DECIMAL_PLACES') else 2,
            'DATAS_NAME_OF_PRODUCT_TYPE': ini_datas.get('NAME_OF_PRODUCT_TYPE'),
            # INACTIVITY
            'INACTIVITY_LIMIT_HOURS': int(ini_inactivity.get('LIMIT_HOURS', 24)) if ini_inactivity.get('LIMIT_HOURS') else 24,
            # TELEGRAM
            'TELEGRAM_MAX_MSG_LENGTH': int(ini_telegram.get('MAX_MSG_LENGTH', 4096)) if ini_telegram.get('MAX_MSG_LENGTH') else 4096,
            'TELEGRAM_LINE_HEIGHT': int(ini_telegram.get('LINE_HEIGHT', 25)) if ini_telegram.get('LINE_HEIGHT') else 25,
            'TELEGRAM_PARSE_MODE': ini_telegram.get('PARSE_MODE'),
            # MSG
            'MSG_LANGUAGE': ini_msg.get('LANGUAGE', 'en').lower() if ini_msg.get('LANGUAGE') else 'en',
            # LOG
            'LOG_DIR': current_date.strftime(ini_log.get('DIR', r'logs\%Y\%Y.%m')) if ini_log.get('DIR') else current_date.strftime(r'logs\%Y\%Y.%m'),
            'LOG_FILE': current_date.strftime(ini_log.get('FILE', 'backup_log_%Y.%m.%d.log')) if ini_log.get('FILE') else current_date.strftime('backup_log_%Y.%m.%d.log'),
            'LOG_LEVEL_ROOT': ini_log.get('LEVEL_ROOT', 'INFO').upper() if ini_log.get('LEVEL_ROOT') else 'INFO',
            'LOG_LEVEL_CONSOLE': ini_log.get('LEVEL_CONSOLE', 'INFO').upper() if ini_log.get('LEVEL_CONSOLE') else 'INFO',
            'LOG_LEVEL_FILE': ini_log.get('LEVEL_FILE', 'WARNING').upper() if ini_log.get('LEVEL_FILE') else 'WARNING',
            'LOG_IGNORE_LIST': [item.strip() for item in ini_log.get('IGNORE_LIST', '').split(',') if item.strip()],
            'LOG_FORMAT_CONSOLE': substitute_logformat(ini_log.get('FORMAT_CONSOLE', '')),
            'LOG_FORMAT_FILE': substitute_logformat(ini_log.get('FORMAT_FILE', '')),
            'LOG_DATE_FORMAT': ini_log.get('DATE_FORMAT', '%Y.%m.%d %H:%M:%S') if ini_log.get('DATE_FORMAT') else '%Y.%m.%d %H:%M:%S',
            'LOG_CONSOLE_LANGUAGE': ini_log.get('CONSOLE_LANGUAGE', 'en').lower() if ini_log.get('CONSOLE_LANGUAGE') else 'en',
            # RUN
            'RUN_MAIN_SCRIPT': ini_run.get('MAIN_SCRIPT', 'merge_csv') if ini_run.get('MAIN_SCRIPT') else 'merge_csv',
            'RUN_REQUIREMENTS_FILE': ini_run.get('REQUIREMENTS_FILE', 'requirements.txt') if ini_run.get('REQUIREMENTS_FILE') else 'requirements.txt',
            'RUN_VENV_PATH': ini_run.get('VENV_PATH', '.venv') if ini_run.get('VENV_PATH') else '.venv',
            'RUN_VENV_INDIVIDUAL': ini_run.get('VENV_INDIVIDUAL', 'True').lower() in ('true', '1') if ini_run.get('VENV_INDIVIDUAL') else True,
            'RUN_GIT_PULL_ENABLED': ini_run.get('GIT_PULL_ENABLED', 'True').lower() in ('true', '1') if ini_run.get('GIT_PULL_ENABLED') else True,
            'RUN_LOG_OUTPUT_ENABLED': ini_run.get('LOG_OUTPUT_ENABLED', 'True').lower() in ('true', '1') if ini_run.get('LOG_OUTPUT_ENABLED') else True,
        }


class Config(BaseConfig):
    """
    Расширенный класс: добавляет .env и переменные окружения к ini.
    Singleton.
    """
    _instance = None

    def __new__(cls, *args, **kwargs) -> 'Config':
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if not hasattr(self, '_initialized'):
            self._initialized = True
            from dotenv import load_dotenv
            load_dotenv()
            super().__init__()
            self._env = self._load_env()

    def _load_env(self) -> Dict[str, Any]:
        config = super().get_normalized_config()
        # Переопределяем только те ключи, которые реально могут быть в .env
        # CSV
        config['CSV_SEPARATOR'] = getenv('CSV_SEPARATOR', config['CSV_SEPARATOR'])
        config['CSV_PATH_TEMPLATE_DIRECTORY'] = getenv('CSV_PATH_TEMPLATE_DIRECTORY', config['CSV_PATH_TEMPLATE_DIRECTORY'])
        config['CSV_PATH_DIRECTORY'] = getenv('CSV_PATH_DIRECTORY', config['CSV_PATH_DIRECTORY'])
        config['CSV_FILE_PATTERN'] = getenv('CSV_FILE_PATTERN', config['CSV_FILE_PATTERN'])
        config['CSV_FILE_NAME_FOR_DTA'] = getenv('CSV_FILE_NAME_FOR_DTA', config['CSV_FILE_NAME_FOR_DTA'])
        config['CSV_FILE_NAME_FOR_CHECKER'] = getenv('CSV_FILE_NAME_FOR_CHECKER', config['CSV_FILE_NAME_FOR_CHECKER'])
        # DATAS
        config['DATAS_MAX_WIDTH'] = int(getenv('DATAS_MAX_WIDTH', config['DATAS_MAX_WIDTH']))
        config['DATAS_DECIMAL_PLACES'] = int(getenv('DATAS_DECIMAL_PLACES', config['DATAS_DECIMAL_PLACES']))
        config['DATAS_NAME_OF_PRODUCT_TYPE'] = getenv('DATAS_NAME_OF_PRODUCT_TYPE', config['DATAS_NAME_OF_PRODUCT_TYPE'])
        # INACTIVITY
        config['INACTIVITY_LIMIT_HOURS'] = int(getenv('INACTIVITY_LIMIT_HOURS', config['INACTIVITY_LIMIT_HOURS']))
        # TELEGRAM
        config['TELEGRAM_TOKEN'] = getenv('TELEGRAM_TOKEN', None)
        config['TELEGRAM_CHAT_ID'] = getenv('TELEGRAM_CHAT_ID', None)
        config['TELEGRAM_MAX_MSG_LENGTH'] = int(getenv('TELEGRAM_MAX_MSG_LENGTH', config['TELEGRAM_MAX_MSG_LENGTH']))
        config['TELEGRAM_LINE_HEIGHT'] = int(getenv('TELEGRAM_LINE_HEIGHT', config['TELEGRAM_LINE_HEIGHT']))
        config['TELEGRAM_PARSE_MODE'] = getenv('TELEGRAM_PARSE_MODE', config['TELEGRAM_PARSE_MODE'])
        # MSG
        config['MSG_LANGUAGE'] = getenv('MSG_LANGUAGE', config['MSG_LANGUAGE']).lower()
        # LOG
        config['LOG_DIR'] = getenv('LOG_DIR', config['LOG_DIR'])
        config['LOG_FILE'] = getenv('LOG_FILE', config['LOG_FILE'])
        config['LOG_LEVEL_ROOT'] = getenv('LOG_LEVEL_ROOT', config['LOG_LEVEL_ROOT']).upper()
        config['LOG_LEVEL_CONSOLE'] = getenv('LOG_LEVEL_CONSOLE', config['LOG_LEVEL_CONSOLE']).upper()
        config['LOG_LEVEL_FILE'] = getenv('LOG_LEVEL_FILE', config['LOG_LEVEL_FILE']).upper()
        config['LOG_IGNORE_LIST'] = [item.strip() for item in getenv('LOG_IGNORE_LIST', ','.join(config['LOG_IGNORE_LIST'])).split(',') if item.strip()]
        config['LOG_FORMAT_CONSOLE'] = getenv('LOG_FORMAT_CONSOLE', config['LOG_FORMAT_CONSOLE'])
        config['LOG_FORMAT_FILE'] = getenv('LOG_FORMAT_FILE', config['LOG_FORMAT_FILE'])
        config['LOG_DATE_FORMAT'] = getenv('LOG_DATE_FORMAT', config['LOG_DATE_FORMAT'])
        config['LOG_CONSOLE_LANGUAGE'] = getenv('LOG_CONSOLE_LANGUAGE', config['LOG_CONSOLE_LANGUAGE']).lower()
        # RUN
        config['RUN_MAIN_SCRIPT'] = getenv('RUN_MAIN_SCRIPT', config['RUN_MAIN_SCRIPT'])
        config['RUN_REQUIREMENTS_FILE'] = getenv('RUN_REQUIREMENTS_FILE', config['RUN_REQUIREMENTS_FILE'])
        config['RUN_VENV_PATH'] = getenv('RUN_VENV_PATH', config['RUN_VENV_PATH'])
        config['RUN_VENV_INDIVIDUAL'] = getenv('RUN_VENV_INDIVIDUAL', str(config['RUN_VENV_INDIVIDUAL'])).lower() in ('true', '1')
        config['RUN_GIT_PULL_ENABLED'] = getenv('RUN_GIT_PULL_ENABLED', str(config['RUN_GIT_PULL_ENABLED'])).lower() in ('true', '1')
        config['RUN_LOG_OUTPUT_ENABLED'] = getenv('RUN_LOG_OUTPUT_ENABLED', str(config['RUN_LOG_OUTPUT_ENABLED'])).lower() in ('true', '1')
        return config

    def get_config(self, *config_types: str | ConfigNames) -> Dict[str, Any]:
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
