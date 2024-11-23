__author__ = 'InfSub'
__contact__ = 'ADmin@TkYD.ru'
__copyright__ = 'Copyright (C) 2024, [LegioNTeaM] InfSub'
__date__ = '2024/11/23'
__deprecated__ = False
__email__ = 'ADmin@TkYD.ru'
__maintainer__ = 'InfSub'
__status__ = 'Production'  # 'Production / Development'
__version__ = '1.6.3'

from io import StringIO
from asyncio import gather as aio_gather, run as aio_run, sleep as aio_sleep
from typing import Dict, List, Optional, Union
from pandas import concat, read_csv, Series, DataFrame, notna
from decimal import Decimal, ROUND_HALF_UP
from aiofiles import open as aio_open
from os.path import dirname, getmtime, join
from os import getenv, walk
from re import match, search
from dotenv import load_dotenv
from datetime import datetime, timedelta
# import aiohttp
# from pprint import pprint

from logger import configure_logging
from send_msg import send_telegram_message


# Загрузка логгера с настройками
logging = configure_logging()

# Загрузка переменных из .env файла
load_dotenv()

CSV_PATH_TEMPLATE_DIRECTORY = getenv('CSV_PATH_TEMPLATE_DIRECTORY')
CSV_PATH_DIRECTORY = getenv('CSV_PATH_DIRECTORY', 'CSV')
CSV_FILE_PATTERN = getenv('CSV_FILE_PATTERN')
CSV_FILE_NAME = getenv('CSV_FILE_NAME')
CSV_SEPARATOR = getenv('CSV_SEPARATOR', ';')
DECIMAL_PLACES = int(getenv('DECIMAL_PLACES'))

MAX_WIDTH = float(getenv("MAX_WIDTH", 200.0))

NAME_OF_PRODUCT_TYPE = getenv('NAME_OF_PRODUCT_TYPE', '')

INACTIVITY_LIMIT_HOURS = int(getenv('INACTIVITY_LIMIT_HOURS'))


async def check_file_modification(file_path: str) -> None:
    # Получаем время последней модификации файла
    file_mod_time = datetime.fromtimestamp(getmtime(file_path))
    current_time = datetime.now()
    file_mod_delta = current_time - file_mod_time
    file_mod_hours = file_mod_delta.total_seconds() / 3600  # Преобразование разницы во времени в часы
    message = f'The file was modified at {file_mod_time}, {file_mod_hours:.2f} hours ago.'

    # Проверяем разницу во времени
    if file_mod_delta <= timedelta(hours=INACTIVITY_LIMIT_HOURS):
        logging.info(message)
    else:
        message = f'File {file_path} has not been modified for more than {INACTIVITY_LIMIT_HOURS} hours. {message}'
        logging.warning(message)

        # Отправляем уведомление в Telegram
        await send_telegram_message(message)


async def read_file_lines(file_path: str) -> Optional[List[str]]:
    """
    Асинхронно читает строки из файла и возвращает их в виде списка.

    :param file_path: Путь до файла.
    :return: Список строк из файла или None в случае ошибки.
    """
    try:
        async with aio_open(file_path, mode='r', encoding='utf-8') as file:
            lines = await file.readlines()
            return lines if lines else None
    except FileNotFoundError:
        logging.error(f'File not found: {file_path}')
    except PermissionError:
        logging.error(f'Access denied for file: {file_path}')
    except Exception as e:
        logging.error(f'An error occurred while reading {file_path}: {str(e)}')
    return None


async def process_headers(header_line: str) -> List[str]:
    """
    Обрабатывает строку заголовков и возвращает список непустых заголовков.

    :param header_line: Строка заголовков.
    :return: Список валидных заголовков.
    """
    await aio_sleep(0)
    headers = header_line.strip().split(CSV_SEPARATOR)
    return [header for header in headers if header.strip()]


async def load_header_template(template_path: str) -> List[str]:
    """
    Функция для загрузки шаблона заголовков столбцов из файла.

    :param template_path: Путь до файла с шаблоном заголовков.
    :return: Список заголовков.
    """
    lines = await read_file_lines(template_path)
    if lines:
        return await process_headers(lines[0])
    return []


async def read_csv_async(file_path: str) -> Optional[DataFrame]:
    """
    Читает CSV-файл асинхронно и возвращает DataFrame, содержащий только столбцы с корректными заголовками.
    """
    logging.info(f'Reading file: {file_path}')
    lines = await read_file_lines(file_path)
    if not lines:
        logging.warning(f'File is empty: {file_path}')
        return None

    # Обрабатываем первую строку (заголовки)
    valid_headers = await process_headers(lines[0])

    # Обрабатываем остальные строки
    data_lines = lines[1:]

    cleaned_data = []
    for line in data_lines:
        trimmed_line = CSV_SEPARATOR.join(
            [item.strip() for item in line.rstrip(f'{CSV_SEPARATOR}\n').split(CSV_SEPARATOR)])
        cleaned_data.append(trimmed_line)
    # Объединяем строки обратно в одну строку
    content = '\n'.join(cleaned_data)

    # Читаем данные в DataFrame, используя только непустые заголовки
    df = read_csv(
        StringIO(content), sep=CSV_SEPARATOR, names=valid_headers, usecols=valid_headers, header=None, skiprows=1)

    return df


async def sort_columns_by_template(df: DataFrame, header_template: list) -> DataFrame:
    """
    Асинхронная функция для сортировки столбцов DataFrame по заданному шаблону заголовков.

    :param df: Исходный DataFrame для сортировки.
    :param header_template: Список заголовков в нужном порядке.
    :return: Отсортированный DataFrame.
    """
    await aio_sleep(0)
    # Фильтруем и сортируем столбцы DataFrame по шаблону
    sorted_df = df.reindex(columns=header_template)
    return sorted_df


def safe_sum(series: Series, decimal_places: Optional[int] = None) -> float:
    """
    Функция для безопасного суммирования элементов Series, игнорируя NaN значения.

    :param series: Серия для суммирования.
    :param decimal_places: Количество знаков после запятой для округления результата.
                          Если None, возвращается результат в первоначальном виде.
    :return: Сумма элементов серии в виде float.
    """
    total = Decimal(0)
    for item in series.dropna():
        total += Decimal(str(item))

    if decimal_places is not None:
        # Округление до указанного количества знаков после запятой
        total = total.quantize(Decimal(10) ** -decimal_places, rounding=ROUND_HALF_UP)

    return float(total)


async def merge_csv_files(files_dict: Dict[str, str]) -> Optional[DataFrame]:
    dataframes = await aio_gather(*[read_csv_async(file_path) for file_path in files_dict.values()])
    dataframes = [df for df in dataframes if df is not None]

    if not dataframes:
        logging.warning('No valid dataframes to merge.')
        return None

    # Создаем комбинированный DataFrame
    combined_data = []
    for df, file_name in zip(dataframes, files_dict.keys()):
        df[f'Storage_{file_name}'] = df['Packing.МестоХранения'].fillna('').astype(str).apply(lambda x: f'{x}')
        df.drop(columns=['Packing.МестоХранения'], inplace=True)
        combined_data.append(df)

    # Объединение всех DataFrame
    combined_df = concat(combined_data, ignore_index=True)
    logging.info('Successfully merged dataframes.')

    # Обновление столбца "Наименование" только в случае, если new_name_value определено и не пусто
    if NAME_OF_PRODUCT_TYPE:
        combined_df['Наименование'] = NAME_OF_PRODUCT_TYPE
        logging.warning(
            f'The value of the cells in the "Наименование" column has been replaced with "{NAME_OF_PRODUCT_TYPE}"')
    else:
        logging.warning(
            'The value of the cells in the "Наименование" column has not been changed, the "CSV_NEW_NAME_VALUE" '
            'constant is empty.')

    # Обработка столбца 'Packing.МестоХранения'
    for column in combined_df.columns:
        if column.startswith('Storage_'):
            combined_df[column] = combined_df[column].fillna('').astype(str)

    # Обновление столбца 'Packing.Ширина' с учетом 'Description'
    def extract_width(row: Series) -> Union[float, None]:
        value = None
        # Проверяем, что значение не NaN и является числом
        if notna(row['Packing.Ширина']) and isinstance(row['Packing.Ширина'], (int, float)):
            value = row['Packing.Ширина']
        elif isinstance(row['Description'], str) and row['Description']:
            found_value = search(r'\d+', row['Description'])
            value = float(found_value.group()) if found_value else None

        # Проверяем, что значение в допустимом диапазоне
        if value is not None:
            if not 0 < value <= MAX_WIDTH:
                logging.warning(
                    f'For product "{row['Packing.Barcode']}", the width value "{value}" was outside '
                    f'the acceptable range.')

        return value

    combined_df['Packing.Ширина'] = combined_df.apply(extract_width, axis=1)

    # Обновление столбца 'Packing.Состав' с учетом 'AdditionalDescription'
    def extract_compound(row: Series) -> Union[str, None]:
        value = None
        if isinstance(row['Packing.Состав'], str) and row['Packing.Состав']:
            value = row['Packing.Состав'] if row['Packing.Состав'] else None
        elif isinstance(row['AdditionalDescription'], str) and row['AdditionalDescription']:
            value = row['AdditionalDescription'] if row['AdditionalDescription'] else None

        return value

    combined_df['Packing.Состав'] = combined_df.apply(extract_compound, axis=1)

    # Получаем список всех столбцов для агрегации
    all_columns = combined_df.columns.tolist()

    # Определяем столбцы для агрегации с помощью first
    first_columns = [col for col in all_columns if col not in ['Packing.Колво', 'Packing.СвободныйОстаток']]

    # Группировка и агрегация
    grouped_df = combined_df.groupby('Packing.Barcode', as_index=False).agg(
        {
            'Packing.Колво': lambda x: safe_sum(x, DECIMAL_PLACES),
            'Packing.СвободныйОстаток': lambda x: safe_sum(x, DECIMAL_PLACES),
            **{col: 'first' for col in first_columns},
            **{col: lambda x: ', '.join(filter(None, x)) for col in combined_df.columns if col.startswith('Storage_')}
        }
    )

    return grouped_df


# async def load_and_process_csv(input_path: str, header_template: list) -> 'DataFrame':
#     """Загружает и сортирует столбцы CSV файла по заданному шаблону заголовков."""
#     df = read_csv(input_path)
#     sorted_df = await sort_columns_by_template(df, header_template)
#     return sorted_df


async def save_dataframe_to_csv(df: 'DataFrame', output_path: str) -> None:
    """Сохраняет DataFrame в CSV файл."""
    df.to_csv(output_path, index=False, sep=CSV_SEPARATOR)


async def find_matching_files(directory: str, pattern: str) -> dict:
    """Ищет файлы в указанной директории, соответствующие заданному шаблону."""
    files_dict = {}
    for root, dirs, files in walk(directory):
        for file in files:
            if match(pattern, file):
                file_path = join(root, file)
                csv_id = match(pattern, file)
                if csv_id:
                    file_name = csv_id.group(1)
                    files_dict[file_name] = file_path
    return files_dict


async def process_and_save_all_csv(header_template_path: str) -> None:
    """Обрабатывает и сохраняет все CSV файлы согласно заданному шаблону заголовка."""
    header_template = await load_header_template(header_template_path)

    files_dict = await find_matching_files(CSV_PATH_DIRECTORY, CSV_FILE_PATTERN)
    logging.info(f'Found {len(files_dict)} files matching the pattern.')

    if files_dict:
        merged_df = await merge_csv_files(files_dict=files_dict)

        if merged_df is not None:
            for file_name, file_path in files_dict.items():
                await check_file_modification(file_path=file_path)

                current_df = merged_df.copy()
                place_column = f'Storage_{file_name}'
                if place_column in current_df.columns:
                    current_df.rename(columns={place_column: 'Packing.МестоХранения'}, inplace=True)
                    place_columns = [col for col in current_df.columns if col.startswith('Storage_')]
                    current_df.drop(columns=place_columns, inplace=True)

                    current_df = await sort_columns_by_template(current_df, header_template)

                    output_path = join(dirname(file_path), f'{CSV_FILE_NAME}')
                    await save_dataframe_to_csv(current_df, output_path)
                    logging.info(f"Saved merged file to {output_path}")
                else:
                    logging.warning(f'Missing expected column {place_column} for file {file_name}.')
        else:
            logging.warning('No data to save after merging.')
    else:
        logging.warning('No files found matching the pattern.')


if __name__ == '__main__':
    aio_run(process_and_save_all_csv(join(CSV_PATH_TEMPLATE_DIRECTORY, CSV_FILE_NAME)))
