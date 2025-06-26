# __author__ = 'InfSub'
# __contact__ = 'ADmin@TkYD.ru'
# __copyright__ = 'Copyright (C) 2024-2025, [LegioNTeaM] InfSub'
# __date__ = '2025/06/26'
# __deprecated__ = False
# __email__ = 'ADmin@TkYD.ru'
# __maintainer__ = 'InfSub'
# __status__ = 'Production'  # 'Production / Development'
# __version__ = '1.7.5.0'

from typing import Optional, List
from pandas import Series, notna
from re import search
from asyncio import Task as aio_Task, create_task as aio_create_task

from column_enums import PackingColumns, DescriptionColumns, StorageColumns
from send_msg_optimized import TelegramMessenger


class DataExtractor:
    """Базовый класс для извлечения данных"""
    
    def __init__(self, telegram_messenger: TelegramMessenger, logger):
        self.telegram_messenger = telegram_messenger
        self.logger = logger


class WidthExtractor(DataExtractor):
    """Класс для извлечения значения ширины"""
    
    def __init__(self, telegram_messenger: TelegramMessenger, logger, max_width: int):
        super().__init__(telegram_messenger, logger)
        self.max_width = max_width
    
    def extract(self, row: Series, tasks: List[aio_Task]) -> Optional[float]:
        """Извлечение значения ширины из строки"""
        value = None
        
        # Проверяем основное поле ширины
        width_value = row[PackingColumns.WIDTH.value]
        if notna(width_value) and isinstance(width_value, (int, float)):
            value = float(width_value)
        # Если нет, ищем в описании
        else:
            desc_value = row[DescriptionColumns.DESCRIPTION.value]
            if isinstance(desc_value, str) and desc_value:
                found_value = search(r'\d+', desc_value)
                value = float(found_value.group()) if found_value else None
        
        # Проверяем валидность
        if value is not None:
            if not 0 < value <= self.max_width:
                barcode_value = row[PackingColumns.BARCODE.value]
                source_value = row[StorageColumns.SOURCE_FILE.value]
                message = (
                    f'*For product:* `{barcode_value}` the width value `{value}` was outside the acceptable range.\n\n*Source:* `{source_value}`'
                )
                self.logger.warning(message.replace('\n', ' ').replace('*', '').replace('`', ''))
                tasks.append(aio_create_task(self.telegram_messenger.add_message(f'️🟥 {message}')))
                return None
        
        return value


class CompoundExtractor(DataExtractor):
    """Класс для извлечения информации о составе"""
    
    def extract(self, row: Series) -> Optional[str]:
        """Извлечение информации о составе"""
        value = None
        
        # Проверяем основное поле состава
        compound_value = row[PackingColumns.COMPOUND.value]
        if isinstance(compound_value, str) and compound_value:
            value = compound_value
        # Если нет, проверяем дополнительное описание
        else:
            desc_value = row[DescriptionColumns.ADDITIONAL_DESCRIPTION.value]
            if isinstance(desc_value, str) and desc_value:
                value = desc_value
        
        return value.upper() if value else None 