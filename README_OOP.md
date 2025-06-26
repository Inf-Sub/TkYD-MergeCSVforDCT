# CSV Merge Tool - ООП Архитектура

## Обзор

Этот проект представляет собой переписанную версию CSV Merge Tool с использованием объектно-ориентированного подхода. Код был реорганизован для улучшения читаемости, поддерживаемости и расширяемости.

## Структура проекта

### Основные файлы

- `merge_csv_oop.py` - Главный файл для запуска приложения
- `csv_processor.py` - Основной класс для обработки CSV файлов
- `column_enums.py` - Enums для всех столбцов таблиц
- `data_extractors.py` - Классы для извлечения данных
- `file_manager.py` - Класс для управления файлами

### Вспомогательные файлы

- `config.py` - Конфигурация приложения
- `logger.py` - Система логирования
- `send_msg.py` - Telegram мессенджер

## Архитектура

### 1. Enums для столбцов (`column_enums.py`)

```python
class PackingColumns(Enum):
    BARCODE = 'Packing.Barcode'
    WIDTH = 'Packing.Ширина'
    QUANTITY = 'Packing.Колво'
    FREE_BALANCE = 'Packing.СвободныйОстаток'
    COMPOUND = 'Packing.Состав'
    STORAGE_PLACE = 'Packing.МестоХранения'

class DescriptionColumns(Enum):
    DESCRIPTION = 'Description'
    ADDITIONAL_DESCRIPTION = 'AdditionalDescription'
    NAME = 'Наименование'

class StorageColumns(Enum):
    SOURCE_FILE = 'Source_File'
    
    @classmethod
    def get_storage_column(cls, file_name: str) -> str:
        return f'Storage_{file_name}'
```

**Преимущества:**
- Типобезопасность при работе со столбцами
- Централизованное управление именами столбцов
- Легкое рефакторинг при изменении имен столбцов
- Автодополнение в IDE

### 2. Основной класс (`csv_processor.py`)

```python
class CSVProcessor:
    def __init__(self):
        self.config = Config()
        self.csv_config = self.config.get_config(ConfigNames.CSV)
        self.datas_config = self.config.get_config(ConfigNames.DATAS)
        self.inactivity_config = self.config.get_config(ConfigNames.INACTIVITY)
        self.telegram_messenger = TelegramMessenger()
        self.logger = logging.getLogger(__name__)
        
        # Инициализация вспомогательных классов
        self.file_manager = FileManager(self.logger)
        self.width_extractor = WidthExtractor(...)
        self.compound_extractor = CompoundExtractor(...)
```

**Преимущества:**
- Инкапсуляция всей логики в одном классе
- Четкое разделение ответственности
- Легкое тестирование
- Простое расширение функциональности

### 3. Классы для извлечения данных (`data_extractors.py`)

```python
class WidthExtractor(DataExtractor):
    def extract(self, row: Series, tasks: List[aio_Task]) -> Optional[float]:
        # Логика извлечения ширины

class CompoundExtractor(DataExtractor):
    def extract(self, row: Series) -> Optional[str]:
        # Логика извлечения состава
```

**Преимущества:**
- Специализация для конкретных типов данных
- Легкое добавление новых экстракторов
- Переиспользование кода
- Изолированное тестирование

### 4. Менеджер файлов (`file_manager.py`)

```python
class FileManager:
    async def read_file_lines(self, file_path: str) -> Optional[List[str]]
    async def check_file_modification(self, file_path: str, ...) -> None
    async def find_matching_files(self, directory: str, pattern: str) -> Dict[str, str]
    async def copy_file(self, src: str, dst: str) -> None
```

**Преимущества:**
- Централизованное управление файловыми операциями
- Единообразная обработка ошибок
- Легкое изменение логики работы с файлами

## Использование

### Запуск приложения

```python
from merge_csv_oop import main
import asyncio

asyncio.run(main())
```

### Создание экземпляра процессора

```python
processor = CSVProcessor()
await processor.run_merge()
```

## Преимущества новой архитектуры

### 1. Читаемость кода
- Четкое разделение ответственности
- Понятные имена классов и методов
- Логическая группировка функциональности

### 2. Поддерживаемость
- Легкое добавление новых функций
- Простое исправление багов
- Изолированные изменения

### 3. Тестируемость
- Каждый класс можно тестировать отдельно
- Легкое создание моков
- Изолированные unit-тесты

### 4. Расширяемость
- Легкое добавление новых типов данных
- Простое расширение функциональности
- Гибкая архитектура

### 5. Типобезопасность
- Использование enums для столбцов
- Строгая типизация
- Автодополнение в IDE

## Миграция с старой версии

### Основные изменения

1. **Столбцы**: Все строковые константы заменены на enums
2. **Функции**: Преобразованы в методы классов
3. **Конфигурация**: Инкапсулирована в классе
4. **Логирование**: Централизовано в каждом классе

### Пример миграции

**Старый код:**
```python
def extract_width(row: Series, tasks: List[aio_Task]) -> Optional[float]:
    if notna(row['Packing.Ширина']) and isinstance(row['Packing.Ширина'], (int, float)):
        value = row['Packing.Ширина']
```

**Новый код:**
```python
def extract(self, row: Series, tasks: List[aio_Task]) -> Optional[float]:
    if notna(row[PackingColumns.WIDTH.value]) and isinstance(row[PackingColumns.WIDTH.value], (int, float)):
        value = row[PackingColumns.WIDTH.value]
```

## Заключение

Новая ООП архитектура значительно улучшает качество кода, делая его более читаемым, поддерживаемым и расширяемым. Использование enums для столбцов обеспечивает типобезопасность, а разделение на классы упрощает тестирование и развитие проекта. 