# __author__ = 'InfSub'
# __contact__ = 'ADmin@TkYD.ru'
# __copyright__ = 'Copyright (C) 2024-2025, [LegioNTeaM] InfSub'
# __date__ = '2025/06/26'
# __deprecated__ = False
# __email__ = 'ADmin@TkYD.ru'
# __maintainer__ = 'InfSub'
# __status__ = 'Production'  # 'Production / Development'
# __version__ = '1.7.5.0'

"""
Примеры использования новой ООП архитектуры CSV Merge Tool
"""

import asyncio
from typing import Dict, List
from pandas import DataFrame, Series

from csv_processor import CSVProcessor
from column_enums import PackingColumns, DescriptionColumns, StorageColumns, AggregationColumns, ColumnGroups
from data_extractors import WidthExtractor, CompoundExtractor
from file_manager import FileManager


async def example_basic_usage():
    """Пример базового использования"""
    print("=== Базовое использование ===")
    
    # Создание процессора
    processor = CSVProcessor()
    
    # Запуск процесса объединения
    await processor.run_merge()


async def example_custom_processing():
    """Пример кастомной обработки"""
    print("=== Кастомная обработка ===")
    
    processor = CSVProcessor()
    
    # Загрузка шаблона заголовков
    template_path = "path/to/template.csv"
    header_template = await processor.load_header_template(template_path)
    print(f"Загружен шаблон: {header_template}")
    
    # Поиск файлов
    files_dict = await processor.file_manager.find_matching_files(
        processor.csv_config['csv_path_directory'],
        processor.csv_config['csv_file_pattern']
    )
    print(f"Найдено файлов: {len(files_dict)}")
    
    # Объединение файлов
    if files_dict:
        merged_df = await processor.merge_csv_files(files_dict)
        if merged_df is not None:
            print(f"Объединено строк: {len(merged_df)}")
            print(f"Столбцы: {list(merged_df.columns)}")


def example_column_usage():
    """Пример использования enums для столбцов"""
    print("=== Использование enums для столбцов ===")
    
    # Получение имен столбцов
    barcode_column = PackingColumns.BARCODE.value
    width_column = PackingColumns.WIDTH.value
    quantity_column = PackingColumns.QUANTITY.value
    
    print(f"Столбец штрих-кода: {barcode_column}")
    print(f"Столбец ширины: {width_column}")
    print(f"Столбец количества: {quantity_column}")
    
    # Генерация столбца хранения
    storage_column = StorageColumns.get_storage_column("MSK-001")
    print(f"Столбец хранения для MSK-001: {storage_column}")
    
    # Проверка типа столбца
    is_storage = ColumnGroups.is_storage_column("Storage_MSK-001")
    print(f"Storage_MSK-001 является столбцом хранения: {is_storage}")
    
    # Получение групп столбцов
    packing_columns = ColumnGroups.get_all_packing_columns()
    description_columns = ColumnGroups.get_all_description_columns()
    
    print(f"Столбцы упаковки: {packing_columns}")
    print(f"Столбцы описаний: {description_columns}")


def example_data_extraction():
    """Пример использования экстракторов данных"""
    print("=== Использование экстракторов данных ===")
    
    # Создание экстракторов
    from logger import logging
    from send_msg import TelegramMessenger
    
    telegram_messenger = TelegramMessenger()
    logger = logging.getLogger(__name__)
    
    width_extractor = WidthExtractor(telegram_messenger, logger, max_width=220)
    compound_extractor = CompoundExtractor(telegram_messenger, logger)
    
    # Пример данных (в реальном коде это будет Series из DataFrame)
    sample_data = {
        PackingColumns.WIDTH.value: 150.5,
        PackingColumns.BARCODE.value: "123456789",
        DescriptionColumns.DESCRIPTION.value: "Ткань 180см",
        PackingColumns.COMPOUND.value: "100% хлопок",
        StorageColumns.SOURCE_FILE.value: "MSK-001"
    }
    
    # Создание Series для демонстрации
    row = Series(sample_data)
    
    # Извлечение ширины
    width = width_extractor.extract(row, [])
    print(f"Извлеченная ширина: {width}")
    
    # Извлечение состава
    compound = compound_extractor.extract(row)
    print(f"Извлеченный состав: {compound}")


async def example_file_operations():
    """Пример работы с файлами"""
    print("=== Работа с файлами ===")
    
    from logger import logging
    
    logger = logging.getLogger(__name__)
    file_manager = FileManager(logger)
    
    # Чтение файла
    file_path = "example.csv"
    lines = await file_manager.read_file_lines(file_path)
    if lines:
        print(f"Прочитано строк: {len(lines)}")
    
    # Поиск файлов
    directory = "."
    pattern = r"^(MSK-[A-Za-z0-9]+)-Nomenclature\.csv$"
    files_dict = await file_manager.find_matching_files(directory, pattern)
    print(f"Найдено файлов по шаблону: {len(files_dict)}")
    
    # Копирование файла
    src = "source.csv"
    dst = "destination.csv"
    await file_manager.copy_file(src, dst)


def example_aggregation_columns():
    """Пример работы с агрегационными столбцами"""
    print("=== Агрегационные столбцы ===")
    
    # Получение столбцов для суммирования
    sum_columns = AggregationColumns.get_sum_columns()
    print(f"Столбцы для суммирования: {sum_columns}")
    
    # Получение столбцов для взятия первого значения
    first_columns = AggregationColumns.get_first_columns()
    print(f"Столбцы для первого значения: {first_columns}")
    
    # Создание словаря агрегации для pandas
    aggregation_dict = {
        PackingColumns.QUANTITY.value: 'sum',
        PackingColumns.FREE_BALANCE.value: 'sum',
        **{col: 'first' for col in first_columns}
    }
    print(f"Словарь агрегации: {aggregation_dict}")


async def example_complete_workflow():
    """Пример полного рабочего процесса"""
    print("=== Полный рабочий процесс ===")
    
    processor = CSVProcessor()
    
    try:
        # 1. Загрузка конфигурации
        print("1. Загрузка конфигурации...")
        csv_config = processor.csv_config
        print(f"   Директория: {csv_config['csv_path_directory']}")
        print(f"   Шаблон: {csv_config['csv_file_pattern']}")
        
        # 2. Поиск файлов
        print("2. Поиск файлов...")
        files_dict = await processor.file_manager.find_matching_files(
            csv_config['csv_path_directory'],
            csv_config['csv_file_pattern']
        )
        print(f"   Найдено файлов: {len(files_dict)}")
        
        if not files_dict:
            print("   Файлы не найдены")
            return
        
        # 3. Объединение файлов
        print("3. Объединение файлов...")
        merged_df = await processor.merge_csv_files(files_dict)
        
        if merged_df is None:
            print("   Ошибка объединения")
            return
        
        print(f"   Объединено строк: {len(merged_df)}")
        print(f"   Столбцов: {len(merged_df.columns)}")
        
        # 4. Обработка каждого файла
        print("4. Обработка файлов...")
        for file_name, file_path in files_dict.items():
            print(f"   Обработка: {file_name}")
            
            # Проверка модификации
            await processor.file_manager.check_file_modification(
                file_path,
                processor.inactivity_config['inactivity_limit_hours'],
                processor.telegram_messenger
            )
            
            # Создание копии для обработки
            current_df = merged_df.copy()
            
            # Переименование столбца хранения
            place_column = StorageColumns.get_storage_column(file_name)
            if place_column in current_df.columns:
                current_df.rename(columns={place_column: PackingColumns.STORAGE_PLACE.value}, inplace=True)
                
                # Удаление столбцов хранения
                place_columns = [col for col in current_df.columns if ColumnGroups.is_storage_column(col)]
                current_df.drop(columns=place_columns, inplace=True)
                
                print(f"     Обработано столбцов: {len(current_df.columns)}")
            else:
                print(f"     Предупреждение: отсутствует столбец {place_column}")
        
        print("5. Процесс завершен успешно")
        
    except Exception as e:
        print(f"Ошибка: {e}")


if __name__ == "__main__":
    # Запуск примеров
    print("Примеры использования ООП архитектуры CSV Merge Tool\n")
    
    # Синхронные примеры
    example_column_usage()
    print()
    
    example_data_extraction()
    print()
    
    example_aggregation_columns()
    print()
    
    # Асинхронные примеры
    async def run_async_examples():
        await example_file_operations()
        print()
        await example_complete_workflow()
    
    asyncio.run(run_async_examples()) 