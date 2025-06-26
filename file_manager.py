# __author__ = 'InfSub'
# __contact__ = 'ADmin@TkYD.ru'
# __copyright__ = 'Copyright (C) 2024-2025, [LegioNTeaM] InfSub'
# __date__ = '2025/06/27'
# __deprecated__ = False
# __email__ = 'ADmin@TkYD.ru'
# __maintainer__ = 'InfSub'
# __status__ = 'Production'  # 'Production / Development'
# __version__ = '2.0.0.1'

from typing import Dict, List, Optional
from aiofiles import open as aio_open
from os.path import dirname, getmtime, join as os_join
from os import walk as os_walk
from re import match
from datetime import datetime, timedelta
from shutil import copy as shutil_copy


class FileManager:
    """Класс для управления файлами"""
    
    def __init__(self, logger):
        self.logger = logger
    
    async def read_file_lines(self, file_path: str) -> Optional[List[str]]:
        """Чтение строк из файла"""
        try:
            async with aio_open(file_path, mode='r', encoding='utf-8') as file:
                lines = await file.readlines()
                return lines if lines else None
        except FileNotFoundError:
            self.logger.error(f'File not found: "{file_path}"')
        except PermissionError:
            self.logger.error(f'Access denied for file: "{file_path}"')
        except Exception as e:
            self.logger.error(f'An error occurred while reading "{file_path}": {str(e)}')
        return None
    
    async def check_file_modification(self, file_path: str, inactivity_limit_hours: int, telegram_messenger) -> None:
        """Проверка времени последней модификации файла"""
        file_mod_time = datetime.fromtimestamp(getmtime(file_path))
        current_time = datetime.now()
        file_mod_delta = current_time - file_mod_time
        
        days = file_mod_delta.days
        hours, remainder = divmod(file_mod_delta.seconds, 3600)
        minutes, _ = divmod(remainder, 60)
        
        time_components = []
        if days > 0:
            time_components.append(f'*{days}* days')
        if hours > 0:
            time_components.append(f'*{hours}* hours')
        time_components.append(f'*{minutes}* minutes')
        
        time_description = ', '.join(time_components)
        file_mod_date = file_mod_time.strftime('%y.%d.%m %H:%M:%S')
        message = f'*The file was modified at:*\n{file_mod_date}\n{time_description} ago.'
        
        if file_mod_delta <= timedelta(hours=inactivity_limit_hours):
            self.logger.info(message.replace('\n', ' ').replace('*', '').replace('`', ''))
        else:
            message = (
                f'*File:*```\n{file_path} ```has not been modified for more than *{inactivity_limit_hours}* '
                f'hours.\n\n{message}')
            self.logger.warning(message.replace('\n', ' ').replace('*', '').replace('`', ''))
            await telegram_messenger.add_message(f'🟥️ {message}')
    
    @staticmethod
    async def find_matching_files(directory: str, pattern: str) -> Dict[str, str]:
        """Поиск файлов по шаблону"""
        files_dict = {}
        for root, dirs, files in os_walk(directory):
            for file in files:
                if match(pattern, file):
                    file_path = os_join(root, file)
                    csv_id = match(pattern, file)
                    if csv_id:
                        file_name = csv_id.group(1)
                        files_dict[file_name] = file_path
        return files_dict
    
    async def copy_file(self, src: str, dst: str) -> None:
        """Копирование файла"""
        try:
            shutil_copy(src, dst)
            self.logger.info(f'File copied from "{src}" to "{dst}".')
        except Exception as e:
            self.logger.error(f'Failed to copy file from "{src}" to "{dst}": {e}.')
    
    @staticmethod
    def get_output_path(file_path: str, csv_file_name: str) -> str:
        """Получение пути для выходного файла"""
        return os_join(dirname(file_path), csv_file_name)
    
    @staticmethod
    def get_checker_path(file_path: str, csv_file_name_for_checker: str) -> str:
        """Получение пути для файла проверки"""
        return os_join(dirname(file_path), csv_file_name_for_checker) 