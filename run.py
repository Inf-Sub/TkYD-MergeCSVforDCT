# __author__ = 'InfSub'
# __contact__ = 'https:/t.me/InfSub'
# __copyright__ = 'Copyright (C) 2025, [LegioNTeaM] InfSub'
# __date__ = '2025/06/25'
# __deprecated__ = False
__maintainer__ = 'InfSub'
# __status__ = 'Production'  # 'Production / Development'
# __version__ = '1.8.0.0'

import logging
from os import getlogin
from sys import platform
from subprocess import check_call, run as sub_run, CalledProcessError
from pathlib import Path
from venv import create as venv_create

# Константы
MAIN_SCRIPT = 'merge_csv'
REQUIREMENTS_FILE = 'requirements.txt'
VENV_INDIVIDUAL = False if getlogin().lower() == __maintainer__.lower() else True
VENV_PATH = '.venv'
GIT_PULL_ENABLED = False if getlogin().lower() == __maintainer__.lower() else True
LOG_FORMAT = '%(filename)s:%(lineno)-10d\n%(asctime)-20s| %(levelname)-8s| %(name)-10s| %(funcName)-27s| %(message)s'
LOG_DATE_FORMAT = '%Y.%m.%d %H:%M:%S'
LOG_LANGUAGE = 'en'  # en / ru
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

# Настройка логирования
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, datefmt=LOG_DATE_FORMAT)

class VirtualEnvironmentManager:
    def __init__(self, venv_path: str, individual: bool = True, git_pull_enabled: bool = True) -> None:
        base_path = Path(venv_path)
        if individual:
            self.venv_dir = base_path.with_name(f'{base_path.name}_{getlogin()}')
        else:
            self.venv_dir = base_path
        self.git_pull_enabled = git_pull_enabled

    def create_virtual_environment(self) -> None:
        """Создает виртуальное окружение в указанной директории."""
        if not self.venv_dir.exists():
            logging.info(LOG_MESSAGE['venv_create'][LOG_LANGUAGE].format(path=str(self.venv_dir)))
            venv_create(str(self.venv_dir), with_pip=True)
        else:
            logging.warning(LOG_MESSAGE['venv_exists'][LOG_LANGUAGE].format(path=str(self.venv_dir)))
    
    def install_dependencies(self) -> None:
        """Устанавливает зависимости из файла requirements.txt."""
        pip_executable = Path(
            self.venv_dir, 'Scripts', 'pip') if platform == 'win32' else Path(self.venv_path, 'bin', 'pip')
        logging.info(LOG_MESSAGE['requirements'][LOG_LANGUAGE])
        try:
            result = sub_run(
                [str(pip_executable), 'install', '-r', REQUIREMENTS_FILE],
                capture_output=True, text=True, check=True)
            output = result.stdout.strip()  # результат выполнения команды
 
            logging.debug(LOG_MESSAGE['command_output_result'][LOG_LANGUAGE].format(output=f'\n{output}'))
        except CalledProcessError as e:
            logging.error(LOG_MESSAGE['file_not_found'][LOG_LANGUAGE].format(file=str(pip_executable), error=e))
    
    def run_main_script(self) -> None:
        """Запускает основной скрипт проекта в виртуальном окружении."""
        python_executable = Path(
            self.venv_dir, 'Scripts', 'python') if platform == 'win32' else Path(self.venv_path, 'bin', 'python')
        try:
            result_install = sub_run([str(python_executable), '-m', 'pip', 'install', '--upgrade', 'pip'],
                capture_output=True, text=True, check=True)
            output_install = result_install.stdout.strip()
            logging.debug(LOG_MESSAGE['command_output_result'][LOG_LANGUAGE].format(output=f' {output_install}'))

            logging.info(LOG_MESSAGE['run_script'][LOG_LANGUAGE].format(file=MAIN_SCRIPT))
            check_call([str(python_executable), f'{MAIN_SCRIPT}.py'])
        except KeyboardInterrupt:
            logging.error(LOG_MESSAGE['task_cancelled'][LOG_LANGUAGE])
        except CalledProcessError as e:
            logging.error(LOG_MESSAGE['file_not_found'][LOG_LANGUAGE].format(file=str(python_executable), error=e))
        except Exception as e:
            logging.error(LOG_MESSAGE['unknown_error'][LOG_LANGUAGE].format(error=e))

    @staticmethod
    def git_pull() -> None:
        """Выполняет git pull для обновления репозитория."""
        logging.info(LOG_MESSAGE['git_pull_begin'][LOG_LANGUAGE])
        try:
            result = sub_run(['git', 'pull'], capture_output=True, text=True, check=True)
            output = result.stdout.strip()
            logging.info(LOG_MESSAGE['git_pull_success'][LOG_LANGUAGE])
            logging.info(LOG_MESSAGE['command_output_result'][LOG_LANGUAGE].format(output=f'\n{output}'))
        except FileNotFoundError:
            logging.error(LOG_MESSAGE['git_pull_not_found'][LOG_LANGUAGE])
        except CalledProcessError as e:
            logging.error(LOG_MESSAGE['git_pull_failed'][LOG_LANGUAGE].format(error=e.stderr))
        except Exception as e:
            logging.error(LOG_MESSAGE['git_pull_unexpected'][LOG_LANGUAGE].format(error=e))

    def setup(self) -> None:
        """
        Выполняет обновление репозитория (git pull), затем запускает процесс создания виртуального окружения,
        установки зависимостей и запуска основного скрипта проекта. Все этапы пишутся в логи.
        """
        if self.git_pull_enabled:
            logging.info(LOG_MESSAGE['git_pull_start'][LOG_LANGUAGE])
            logging.info(LOG_MESSAGE['git_pull_begin'][LOG_LANGUAGE])
            self.git_pull()
        self.create_virtual_environment()
        # Проверка существования каталога venv
        venv_subdir = Path(self.venv_dir, ('Scripts' if platform == 'win32' else 'bin'))
        if venv_subdir.exists():
            self.install_dependencies()
            self.run_main_script()
        else:
            logging.error(
                LOG_MESSAGE['file_not_found'][LOG_LANGUAGE].format(file=str(self.venv_dir), error='Directory not found'))


if __name__ == "__main__":
    manager = VirtualEnvironmentManager(VENV_PATH, VENV_INDIVIDUAL, git_pull_enabled=GIT_PULL_ENABLED)
    manager.setup()
