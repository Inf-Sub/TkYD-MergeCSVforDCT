# __author__ = 'InfSub'
# __contact__ = 'ADmin@TkYD.ru'
# __copyright__ = 'Copyright (C) 2024-2025, [LegioNTeaM] InfSub'
# __date__ = '2025/06/27'
# __deprecated__ = False
# __email__ = 'ADmin@TkYD.ru'
# __maintainer__ = 'InfSub'
# __status__ = 'Production'  # 'Production / Development'
# __version__ = '2.0.0.1'

from asyncio import run as aio_run
from csv_processor import CSVProcessor


async def main():
    """Основная функция для запуска процесса объединения CSV файлов"""
    processor = CSVProcessor()
    await processor.run_merge()


if __name__ == '__main__':
    aio_run(main()) 