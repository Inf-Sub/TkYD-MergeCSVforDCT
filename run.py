# __author__ = 'InfSub'
# __contact__ = 'https:/t.me/InfSub'
# __copyright__ = 'Copyright (C) 2025, [LegioNTeaM] InfSub'
# __date__ = '2025/06/25'
# __deprecated__ = False
__maintainer__ = 'InfSub'
# __status__ = 'Production'  # 'Production / Development'
# __version__ = '1.9.0.0'

import logging
from os import getlogin
from sys import platform
from subprocess import check_call, run as sub_run, CalledProcessError
from pathlib import Path
from typing import Dict, Any
from venv import create as venv_create

from config import Config, ConfigNames



LOG_MESSAGE = {
    'venv_create': {
        'en': 'Creating a virtual environment in directory "{path}"...',
        'ru': 'Создаем виртуальное окружение в каталоге "{path}"...',
    },
    'venv_exists': {
        'en': 'Virtual environment already exists in directory "{path}".',
        'ru': 'Виртуальное окружение уже существует в каталоге "{path}".',
    },
    'requirements': {
        'en': 'Installing dependencies (requirements)...',
        'ru': 'Устанавливаем зависимости...',
    },
    'command_output_result': {
        'en': 'Command output result: {output}',
        'ru': 'Результат выполнения команды:{output}',
    },
    'run_script': {
        'en': 'Running script "{file}"...',
        'ru': 'Запускаем скрипт "{file}"...',
    },
    'task_cancelled': {
        'en': 'Task was cancelled.',
        'ru': 'Задание отменено',
    },
    'dir_not_found': {
        'en': 'Directory "{path}" not found!',
        'ru': 'Каталог "{path}" не найден!',
    },
    'file_not_found': {
        'en': 'File "{file}" not found: {error}.',
        'ru': 'Файл "{file}" не найден: {error}.',
    },
    'unknown_error': {
        'en': 'Unknown error: {error}.',
        'ru': 'Неизвестная ошибка: {error}.',
    },
    'git_pull_start': {
        'en': 'Git pull is enabled, updating repository...',
        'ru': 'Git pull включён, обновляем репозиторий...'
    },
    'git_pull_begin': {
        'en': 'Starting git pull operation...',
        'ru': 'Запуск операции git pull...'
    },
    'git_pull_success': {
        'en': 'Git pull completed successfully.',
        'ru': 'Git pull успешно завершён.'
    },
    # 'git_pull_output': {
    #     'en': 'Git pull output: {output}.',
    #     'ru': 'Результат git pull: {output}.'
    # },
    'git_pull_not_found': {
        'en': 'Git is not installed or not found in PATH.',
        'ru': 'Git не установлен или не найден в PATH.'
    },
    'git_pull_failed': {
        'en': 'Git pull failed: "{error}".',
        'ru': 'Ошибка git pull: "{error}".'
    },
    'git_pull_unexpected': {
        'en': 'Unexpected error during git pull: "{error}".',
        'ru': 'Неожиданная ошибка при git pull: "{error}".'
    },
}


class VirtualEnvironmentManager:
    def __init__(self) -> None:
        config: Dict[str: Any] = Config().get_config(ConfigNames.RUN)

        individual: bool = False if getlogin().lower() == __maintainer__.lower() else config.get(
            'VENV_INDIVIDUAL', True)
        git_pull_enabled: bool = False if getlogin().lower() == __maintainer__.lower() else config.get(
            'GIT_PULL_ENABLED', True)
        
        self._log_language = config.get('LOG_LANGUAGE', 'en')
        self._main_script = config.get('MAIN_SCRIPT')
        self._requirements_file = config.get('REQUIREMENTS_FILE', 'requirements.txt')

        venv_path: Path = Path(config.get('VENV_PATH', '.venv'))
        if individual:
            self.venv_dir = venv_path.with_name(f'{venv_path.name}_{getlogin()}')
        else:
            self.venv_dir = venv_path
        self.git_pull_enabled = git_pull_enabled

    def create_virtual_environment(self) -> None:
        """Создает виртуальное окружение в указанной директории."""
        lng = self._log_language
        if not self.venv_dir.exists():
            logging.info(LOG_MESSAGE['venv_create'][lng].format(path=str(self.venv_dir)))
            venv_create(str(self.venv_dir), with_pip=True)
        else:
            logging.warning(LOG_MESSAGE['venv_exists'][lng].format(path=str(self.venv_dir)))
    
    def install_dependencies(self) -> None:
        """Устанавливает зависимости из файла requirements.txt."""
        lng = self._log_language
        pip_executable = Path(
            self.venv_dir, 'Scripts', 'pip') if platform == 'win32' else Path(self.venv_dir, 'bin', 'pip')
        logging.info(LOG_MESSAGE['requirements'][lng])
        try:
            result = sub_run(
                [str(pip_executable), 'install', '-r', self._requirements_file],
                capture_output=True, text=True, check=True)
            output = result.stdout.strip()  # результат выполнения команды
 
            logging.debug(LOG_MESSAGE['command_output_result'][lng].format(output=f'\n{output}'))
        except CalledProcessError as e:
            logging.error(LOG_MESSAGE['file_not_found'][lng].format(file=str(pip_executable), error=e))
    
    def run_main_script(self) -> None:
        """Запускает основной скрипт проекта в виртуальном окружении."""
        lng = self._log_language
        python_executable = Path(
            self.venv_dir, 'Scripts', 'python') if platform == 'win32' else Path(self.venv_dir, 'bin', 'python')
        try:
            result_install = sub_run([str(python_executable), '-m', 'pip', 'install', '--upgrade', 'pip'],
                capture_output=True, text=True, check=True)
            output_install = result_install.stdout.strip()
            logging.debug(LOG_MESSAGE['command_output_result'][lng].format(output=f' {output_install}'))

            logging.info(LOG_MESSAGE['run_script'][lng].format(file=self._main_script))
            check_call([str(python_executable), f'{self._main_script}.py'])
        except KeyboardInterrupt:
            logging.error(LOG_MESSAGE['task_cancelled'][lng])
        except CalledProcessError as e:
            logging.error(LOG_MESSAGE['file_not_found'][lng].format(file=str(python_executable), error=e))
        except Exception as e:
            logging.error(LOG_MESSAGE['unknown_error'][lng].format(error=e))

    def git_pull(self) -> None:
        """Выполняет git pull для обновления репозитория."""
        lng = self._log_language
        logging.info(LOG_MESSAGE['git_pull_begin'][lng])
        try:
            result = sub_run(['git', 'pull'], capture_output=True, text=True, check=True)
            output = result.stdout.strip()
            logging.info(LOG_MESSAGE['git_pull_success'][lng])
            logging.info(LOG_MESSAGE['command_output_result'][lng].format(output=f'\n{output}'))
        except FileNotFoundError:
            logging.error(LOG_MESSAGE['git_pull_not_found'][lng])
        except CalledProcessError as e:
            logging.error(LOG_MESSAGE['git_pull_failed'][lng].format(error=e.stderr))
        except Exception as e:
            logging.error(LOG_MESSAGE['git_pull_unexpected'][lng].format(error=e))

    def setup(self) -> None:
        """
        Выполняет обновление репозитория (git pull), затем запускает процесс создания виртуального окружения,
        установки зависимостей и запуска основного скрипта проекта. Все этапы пишутся в логи.
        """
        lng = self._log_language
        if self.git_pull_enabled:
            logging.info(LOG_MESSAGE['git_pull_start'][lng])
            logging.info(LOG_MESSAGE['git_pull_begin'][lng])
            self.git_pull()
        self.create_virtual_environment()
        # Проверка существования каталога venv
        venv_subdir = Path(self.venv_dir, ('Scripts' if platform == 'win32' else 'bin'))
        if venv_subdir.exists():
            self.install_dependencies()
            self.run_main_script()
        else:
            logging.error(
                LOG_MESSAGE['file_not_found'][lng].format(
                    file=str(self.venv_dir), error='Directory not found.'))


if __name__ == '__main__':
    manager = VirtualEnvironmentManager()
    manager.setup()
