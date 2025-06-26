# __author__ = 'InfSub'
# __contact__ = 'https:/t.me/InfSub'
# __copyright__ = 'Copyright (C) 2025, [LegioNTeaM] InfSub'
# __date__ = '2025/06/26'
# __deprecated__ = False
__maintainer__ = 'InfSub'
# __status__ = 'Production'  # 'Production / Development'
# __version__ = '2.0.0.0'

import logging
from logging import getLogger
from os import getlogin
from sys import platform
from subprocess import check_call, run as sub_run, CalledProcessError
from pathlib import Path
from typing import Dict, Any
from venv import create as venv_create
from configparser import ConfigParser

from config import Config, ConfigNames

# Константы
CONFIG_FILE = 'config.ini'
DEFAULT_LOG_FORMAT = (
    '%(filename)s:%(lineno)d\n%(asctime)-20s| %(levelname)-8s| %(name)-14| %(funcName)-27s| %(message)s')
DEFAULT_LOG_DATE_FORMAT = '%Y.%m.%d %H:%M:%S'
DEFAULT_LOG_LEVEL = 'INFO'

# Сообщения логирования
LOG_MESSAGES = {
    'en': {
        'venv_create': 'Creating virtual environment in "{path}"...',
        'venv_exists': 'Virtual environment already exists in "{path}".',
        'requirements': 'Installing dependencies...',
        'pip_output': 'Pip output: {output}',
        'run_script': 'Running script "{file}"...',
        'task_cancelled': 'Task was cancelled.',
        'file_not_found': 'File "{file}" not found: {error}.',
        'unknown_error': 'Unknown error: {error}.',
        'git_pull_start': 'Git pull enabled, updating repository...',
        'git_pull_success': 'Git pull completed successfully.',
        'git_pull_not_found': 'Git not found in PATH.',
        'git_pull_failed': 'Git pull failed: "{error}".',
        'setup_complete': 'Setup completed successfully.',
        'setup_failed': 'Setup failed: {error}.'
    },
    'ru': {
        'venv_create': 'Создаем виртуальное окружение в "{path}"...',
        'venv_exists': 'Виртуальное окружение уже существует в "{path}".',
        'requirements': 'Устанавливаем зависимости...',
        'pip_output': 'Результат выполнения pip: {output}',
        'run_script': 'Запускаем скрипт "{file}"...',
        'task_cancelled': 'Задание отменено.',
        'file_not_found': 'Файл "{file}" не найден: {error}.',
        'unknown_error': 'Неизвестная ошибка: {error}.',
        'git_pull_start': 'Git pull включен, обновляем репозиторий...',
        'git_pull_success': 'Git pull успешно завершен.',
        'git_pull_not_found': 'Git не найден в PATH.',
        'git_pull_failed': 'Ошибка git pull: "{error}".',
        'setup_complete': 'Настройка завершена успешно.',
        'setup_failed': 'Ошибка настройки: {error}.'
    }
}


class VirtualEnvironmentManager:
    """Менеджер виртуального окружения и запуска проекта"""
    
    def __init__(self) -> None:
        # Не создаём Config() здесь, чтобы не логировать до настройки логирования
        self._config = None
        self._log_language = 'en'  # значение по умолчанию
        self._main_script = 'merge_csv_oop'  # значение по умолчанию
        self._requirements_file = 'requirements.txt'  # значение по умолчанию
        self.venv_dir = Path('.venv')  # значение по умолчанию
        self.git_pull_enabled = True  # значение по умолчанию
        
        # Определяем пути к исполняемым файлам
        self._bin_dir = 'Scripts' if platform == 'win32' else 'bin'
        self._exe_ext = '.exe' if platform == 'win32' else ''
        self.python_executable = Path(self.venv_dir, self._bin_dir, f'python{self._exe_ext}')
        self.pip_executable = Path(self.venv_dir, self._bin_dir,  f'pip{self._exe_ext}')

    def _load_config(self) -> None:
        """Загружает конфигурацию (вызывается после настройки логирования)"""
        if self._config is None:
            self._config = {**Config().get_config(ConfigNames.RUN), **Config().get_config(ConfigNames.MSG)}
            
            # Определяем настройки
            is_maintainer = getlogin().lower() == __maintainer__.lower()
            individual = not is_maintainer and self._config.get('run_venv_individual', True)
            git_pull_enabled = not is_maintainer and self._config.get('run_git_pull_enabled', True)
            
            self._log_language = self._config.get('msg_language', 'en')
            self._main_script = self._config.get('run_main_script', 'merge_csv_oop')
            self._requirements_file = self._config.get('run_requirements_file', 'requirements.txt')
            
            # Настройка пути к виртуальному окружению
            venv_path = Path(self._config.get('run_venv_path', '.venv'))
            self.venv_dir = venv_path.with_name(f'{venv_path.name}_{getlogin()}') if individual else venv_path
            self.git_pull_enabled = git_pull_enabled
            
            # Обновляем пути к исполняемым файлам
            self.python_executable = Path(self.venv_dir, self._bin_dir, f'python{self._exe_ext}')
            self.pip_executable = Path(self.venv_dir, self._bin_dir,  f'pip{self._exe_ext}')

    def _log(self, message_key: str, **kwargs) -> None:
        """Логирование с поддержкой локализации"""
        message = LOG_MESSAGES[self._log_language][message_key].format(**kwargs)
        logging.info(message)

    def create_virtual_environment(self) -> None:
        """Создает виртуальное окружение"""
        if not self.venv_dir.exists():
            self._log('venv_create', path=str(self.venv_dir))
            venv_create(str(self.venv_dir), with_pip=True)
        else:
            self._log('venv_exists', path=str(self.venv_dir))
    
    def install_dependencies(self) -> None:
        """Устанавливает зависимости"""
        if not self.pip_executable.exists():
            raise FileNotFoundError(f"Pip executable not found: {self.pip_executable}")
            
        self._log('requirements')
        try:
            result = sub_run(
                [str(self.pip_executable), 'install', '-r', self._requirements_file],
                capture_output=True, text=True, check=True
            )
            self._log('pip_output', output=f'\n{result.stdout.strip()}')
        except CalledProcessError as e:
            raise RuntimeError(f"Failed to install dependencies: {e.stderr}")
    
    def run_main_script(self) -> None:
        """Запускает основной скрипт"""
        if not self.python_executable.exists():
            raise FileNotFoundError(f'Python executable not found: {self.python_executable}')
            
        self._log('run_script', file=self._main_script)
        try:
            # Обновляем pip
            sub_run([str(self.python_executable), '-m', 'pip', 'install', '--upgrade', 'pip'],
                   capture_output=True, check=True)
            
            # Запускаем основной скрипт
            check_call([str(self.python_executable), f'{self._main_script}.py'])
        except KeyboardInterrupt:
            self._log('task_cancelled')
        except CalledProcessError as e:
            raise RuntimeError(f"Failed to run script: {e}")

    def git_pull(self) -> None:
        """Выполняет git pull"""
        if not self.git_pull_enabled:
            return
            
        self._log('git_pull_start')
        try:
            result = sub_run(['git', 'pull'], capture_output=True, text=True, check=True)
            self._log('git_pull_success')
            logging.debug(f"Git output: {result.stdout.strip()}")
        except FileNotFoundError:
            self._log('git_pull_not_found')
        except CalledProcessError as e:
            self._log('git_pull_failed', error=e.stderr)
        except Exception as e:
            self._log('git_pull_failed', error=str(e))

    def setup(self) -> None:
        """Полная настройка и запуск проекта"""
        try:
            # Загружаем конфигурацию (после настройки логирования)
            self._load_config()
            
            # Обновление репозитория
            self.git_pull()
            
            # Создание виртуального окружения
            self.create_virtual_environment()
            
            # Проверка существования виртуального окружения
            if not (self.venv_dir / self._bin_dir).exists():
                raise FileNotFoundError(f"Virtual environment not properly created: {self.venv_dir}")
            
            # Установка зависимостей и запуск
            self.install_dependencies()
            self._log('setup_complete')
            self.run_main_script()
            
            
        except Exception as e:
            self._log('setup_failed', error=str(e))
            raise


def setup_logging():
    """Настраивает логирование на основе config.ini"""
    try:
        # Используем Config() для основных секций
        config_data = Config().get_config(ConfigNames.LOG)
        
        # Читаем LOGFORMAT напрямую из config.ini (Config().get_config() не возвращает эту секцию)
        config_parser = ConfigParser(interpolation=None)
        ini_path = Path(Path(__file__).parent, CONFIG_FILE)
        config_parser.read(ini_path, encoding='utf-8')
        logformat_data = dict(config_parser['LOGFORMAT']) if 'LOGFORMAT' in config_parser else {}
        
        # Уровни логирования
        level_root = config_data.get('log_level_root', DEFAULT_LOG_LEVEL).upper()
        level_console = config_data.get('log_level_console', DEFAULT_LOG_LEVEL).upper()
        level_file = config_data.get('log_level_file', 'WARNING').upper()
        
        # Форматы
        date_format = config_data.get('log_date_format', DEFAULT_LOG_DATE_FORMAT)
        format_console = config_data.get('log_format_console', DEFAULT_LOG_FORMAT)
        format_file = config_data.get('log_format_file', DEFAULT_LOG_FORMAT)
        
        # Удаляем colorlog (еще не установлен)
        format_console = format_console.replace('%(log_color)s', '')
        format_file = format_file.replace('%(log_color)s', '')
        
        # Подставляем значения из LOGFORMAT
        for key, value in logformat_data.items():
            format_console = format_console.replace(f'${{LOGFORMAT_{key.upper()}}}', value)
            format_file = format_file.replace(f'${{LOGFORMAT_{key.upper()}}}', value)
        
        # Обрабатываем escape-последовательности
        format_console = format_console.replace(r'\t', '\t').replace(r'\n', '\n')
        format_file = format_file.replace(r'\t', '\t').replace(r'\n', '\n')
        
        # Создаем директорию для логов
        from datetime import datetime
        current_date = datetime.now()
        
        log_dir = current_date.strftime(config_data.get('log_dir', 'logs'))
        log_file = current_date.strftime(config_data.get('log_file', 'app.log'))
        
        log_path = Path(log_dir)
        log_path.mkdir(parents=True, exist_ok=True)
        
        full_log_path = log_path / log_file
        
        # Настраиваем логирование
        logging.basicConfig(
            level=getattr(logging, level_root, logging.INFO),
            format=format_console,
            datefmt=date_format,
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler(full_log_path, encoding='utf-8')
            ]
        )
        
        # Устанавливаем разные уровни для хендлеров
        root_logger = logging.getLogger()
        for handler in root_logger.handlers:
            if isinstance(handler, logging.StreamHandler) and not isinstance(handler, logging.FileHandler):
                handler.setLevel(getattr(logging, level_console, logging.INFO))
            elif isinstance(handler, logging.FileHandler):
                handler.setLevel(getattr(logging, level_file, logging.WARNING))
                handler.setFormatter(logging.Formatter(format_file, date_format))
                
    except Exception as e:
        # Fallback к дефолтным значениям
        logging.basicConfig(
            level=logging.INFO,
            format=DEFAULT_LOG_FORMAT,
            datefmt=DEFAULT_LOG_DATE_FORMAT
        )
        logging.warning(f'Failed to load logging config: "{e}". Using defaults.')


if __name__ == '__main__':
    # Сначала настраиваем логирование
    setup_logging()
    logger = getLogger(__name__)
    
    try:
        # Теперь создаём менеджер (после настройки логирования)
        manager = VirtualEnvironmentManager()
        manager.setup()
    except Exception as e:
        logger.error(f'Application failed: "{e}".')
        exit(1)
