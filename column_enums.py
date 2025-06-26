# __author__ = 'InfSub'
# __contact__ = 'ADmin@TkYD.ru'
# __copyright__ = 'Copyright (C) 2024-2025, [LegioNTeaM] InfSub'
# __date__ = '2025/06/27'
# __deprecated__ = False
# __email__ = 'ADmin@TkYD.ru'
# __maintainer__ = 'InfSub'
# __status__ = 'Production'  # 'Production / Development'
# __version__ = '2.0.0.1'

from enum import Enum
from typing import List


class PackingColumns(Enum):
    """Enum для столбцов, связанных с упаковкой товаров"""
    BARCODE = 'Packing.Barcode'
    WIDTH = 'Packing.Ширина'
    QUANTITY = 'Packing.Колво'
    FREE_BALANCE = 'Packing.СвободныйОстаток'
    COMPOUND = 'Packing.Состав'
    STORAGE_PLACE = 'Packing.МестоХранения'


class DescriptionColumns(Enum):
    """Enum для столбцов с описаниями"""
    DESCRIPTION = 'Description'
    ADDITIONAL_DESCRIPTION = 'AdditionalDescription'
    NAME = 'Наименование'


class StorageColumns(Enum):
    """Enum для столбцов хранения"""
    SOURCE_FILE = 'Source_File'
    
    @classmethod
    def get_storage_column(cls, file_name: str) -> str:
        """Генерирует имя столбца хранения для конкретного файла"""
        return f'Storage_{file_name}'


class AggregationColumns(Enum):
    """Enum для столбцов, которые агрегируются при группировке"""
    SUM_COLUMNS = 'SUM_COLUMNS'
    FIRST_COLUMNS = 'FIRST_COLUMNS'
    
    @classmethod
    def get_sum_columns(cls) -> List[str]:
        """Возвращает список столбцов для суммирования"""
        return [PackingColumns.QUANTITY.value, PackingColumns.FREE_BALANCE.value]
    
    @classmethod
    def get_first_columns(cls) -> List[str]:
        """Возвращает список столбцов для взятия первого значения"""
        return [
            PackingColumns.BARCODE.value,
            PackingColumns.WIDTH.value,
            PackingColumns.COMPOUND.value,
            DescriptionColumns.NAME.value,
            DescriptionColumns.DESCRIPTION.value,
            DescriptionColumns.ADDITIONAL_DESCRIPTION.value,
            StorageColumns.SOURCE_FILE.value
        ]


class ColumnGroups:
    """Группировка столбцов по функциональности"""
    
    @staticmethod
    def get_all_packing_columns() -> List[str]:
        """Возвращает все столбцы упаковки"""
        return [col.value for col in PackingColumns]
    
    @staticmethod
    def get_all_description_columns() -> List[str]:
        """Возвращает все столбцы описаний"""
        return [col.value for col in DescriptionColumns]
    
    @staticmethod
    def get_storage_columns_pattern() -> str:
        """Возвращает паттерн для поиска столбцов хранения"""
        return 'Storage_'
    
    @staticmethod
    def is_storage_column(column_name: str) -> bool:
        """Проверяет, является ли столбец столбцом хранения"""
        return column_name.startswith('Storage_') 