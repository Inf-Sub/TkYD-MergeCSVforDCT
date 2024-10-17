__author__ = 'InfSub'
__contact__ = 'ADmin@TkYD.ru'
__copyright__ = 'Copyright (C) 2024, [LegioNTeaM] InfSub'
__date__ = '2024/10/16'
__deprecated__ = False
__email__ = 'ADmin@TkYD.ru'
__maintainer__ = 'InfSub'
__status__ = 'Production'  # 'Production / Development'
__version__ = '1.2.6'

from io import StringIO
from asyncio import gather as aio_gather, run as aio_run
from typing import Dict, Optional
from pandas import concat, read_csv, Series, DataFrame
from decimal import Decimal
from aiofiles import open as aio_open
from os.path import getmtime, join
from os import getenv, walk
from re import match
from dotenv import load_dotenv
from datetime import datetime, timedelta

from logging_setup import configure_logging


# Загрузка логгера с настройками
logging = configure_logging()


# Загрузка переменных из .env файла
load_dotenv()

CSV_PATH = getenv('CSV_PATH', 'CSV')
CSV_FILE_PATTERN = getenv('CSV_FILE_PATTERN')
CSV_FILE_MERGED = getenv('CSV_FILE_MERGED')
CSV_SEPARATOR = getenv('CSV_SEPARATOR', ';')

INACTIVITY_LIMIT_HOURS = int(getenv('INACTIVITY_LIMIT_HOURS'))

TELEGRAM_TOKEN = getenv('TELEGRAM_TOKEN')
TELEGRAM_CHAT_ID = getenv('TELEGRAM_CHAT_ID')


async def check_file_modification(file_path: str) -> None:
    # Получаем время последней модификации файла
    file_mod_time = datetime.fromtimestamp(getmtime(file_path))
    current_time = datetime.now()

    # Проверяем разницу во времени
    if (current_time - file_mod_time) > timedelta(hours=INACTIVITY_LIMIT_HOURS):
        message = f"File {file_path} has not been modified for more than {INACTIVITY_LIMIT_HOURS} hours."
        logging.warning(message)

        # Отправляем уведомление в Telegram
        send_telegram_message(message)


def send_telegram_message(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    pass
    # payload = {
    #     'chat_id': TELEGRAM_CHAT_ID,
    #     'text': message
    # }
    # response = requests.post(url, data=payload)
    #
    # if response.status_code != 200:
    #     logging.error(f"Failed to send message to Telegram. Status code: {response.status_code}")


# async def send_telegram_notification(message: str) -> None:
#     """Отправляет уведомление в Telegram."""
#     bot = Bot(token=TELEGRAM_TOKEN)
#     await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message)


async def read_csv_async(file_path: str) -> Optional[DataFrame]:
    """
    Читает CSV-файл асинхронно и возвращает DataFrame, содержащий только столбцы с корректными заголовками.
    """
    logging.info(f"Reading file: {file_path}")
    try:
        async with (aio_open(file_path, mode='r', encoding='utf-8') as f):
            # Читаем все строки из файла
            lines = await f.readlines()

            if not lines:
                logging.warning(f"File is empty: {file_path}")
                return None

            # Обрабатываем первую строку (заголовки)
            header = lines[0].rstrip(CSV_SEPARATOR)
            headers = header.split(CSV_SEPARATOR)
            # Фильтруем пустые заголовки
            valid_headers = [h.strip() for h in headers if h.strip()]

            # Обрабатываем остальные строки
            data_lines = lines[1:]
            # Удаляем лишний разделитель в конце каждой строки данных
            # print(f'data_lines:\t{data_lines}')
            # cleaned_data = [line.rstrip(CSV_SEPARATOR) for line in data_lines]

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

    except FileNotFoundError:
        logging.error(f"File not found: {file_path}")
    except Exception as e:
        logging.error(f"An error occurred while reading {file_path}: {e}")
    return None


def safe_sum(series: Series) -> float:
    total = Decimal(0)
    for item in series.dropna():
        total += Decimal(str(item))
    return float(total)


# def log_summarized_data(grouped_df):
#     for _, row in grouped_df.iterrows():
#         original_qty = row['Packing.Колво']
#         free_qty = row['Packing.СвободныйОстаток']
#         barcode = row['Packing.Barcode']
#         logging.info(f"Barcode: {barcode}, Original Quantity: {original_qty}, Free Quantity: {free_qty}")


async def merge_csv_files(files_dict: Dict[str, str]) -> Optional[DataFrame]:
    dataframes = await aio_gather(*[read_csv_async(file_path) for file_path in files_dict.values()])

    dataframes = [df for df in dataframes if df is not None]

    if not dataframes:
        logging.warning("No valid dataframes to merge.")
        return None

    # Получаем порядок столбцов из первого валидного DataFrame
    columns_order = dataframes[0].columns.tolist()

    # Создаем комбинированный DataFrame
    combined_data = []
    for df, file_name in zip(dataframes, files_dict.keys()):
        df['Packing.МестоХранения'] = df['Packing.МестоХранения'].fillna('').astype(str).apply(
            lambda x: f"{file_name}: ({x})")
        combined_data.append(df)

    combined_df = concat(combined_data, ignore_index=True)
    logging.info("Successfully merged dataframes.")

    combined_df['Packing.МестоХранения'] = combined_df['Packing.МестоХранения'].fillna('').astype(str)

    grouped_df = combined_df.groupby('Packing.Barcode', as_index=False).agg({
        'Packing.Колво': lambda x: safe_sum(x),
        'Packing.СвободныйОстаток': lambda x: safe_sum(x),
        'Packing.МестоХранения': lambda x: ', '.join(filter(None, x)),
        'Артикул': 'first',
        'Packing.Name': 'first',
        'Packing.Цена': 'first',
        'Packing.НоваяЦена': 'first',
        'Packing.Скидка': 'first',
        'Packing.Производитель': 'first',
        'Packing.СтранаПроизводства': 'first',
        'Наименование': 'first',
        'Packing.Организация': 'first',
        'Packing.АдресПроизводителя': 'first',
        'Packing.Ширина': 'first',
        'Packing.Состав': 'first',
        'Код': 'first',
        'Packing.Date': 'first'
    })

    # Код для группировки
    # log_summarized_data(grouped_df)
    # Применяем порядок колонок из исходного файла
    return grouped_df[columns_order]


async def run_merge_csv() -> None:
    directories = set()
    files_dict = {}

    logging.info(f"Starting file search in directory: {CSV_PATH}")

    for root, dirs, files in walk(CSV_PATH):
        for file in files:
            if match(CSV_FILE_PATTERN, file):
                file_path = join(root, file)
                directories.add(root)

                csv_id = match(CSV_FILE_PATTERN, file)
                if csv_id:
                    file_name = csv_id.group(1)
                    files_dict[file_name] = file_path

    logging.info(f"Found {len(files_dict)} files matching the pattern.")

    if files_dict:
        merged_df = await merge_csv_files(files_dict=files_dict)
        if merged_df is not None:
            for directory in directories:
                output_path = join(directory, CSV_FILE_MERGED)
                merged_df.to_csv(output_path, index=False, sep=CSV_SEPARATOR)
                logging.info(f"Saved merged file to {output_path}")
        else:
            logging.warning("No data to save after merging.")
    else:
        logging.warning("No files found matching the pattern.")


if __name__ == "__main__":
    aio_run(run_merge_csv())
