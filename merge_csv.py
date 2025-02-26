__author__ = 'InfSub'
__contact__ = 'ADmin@TkYD.ru'
__copyright__ = 'Copyright (C) 2024-2025, [LegioNTeaM] InfSub'
__date__ = '2025/02/26'
__deprecated__ = False
__email__ = 'ADmin@TkYD.ru'
__maintainer__ = 'InfSub'
__status__ = 'Production'  # 'Production / Development'
__version__ = '1.7.2.1'

from io import StringIO
from asyncio import gather as aio_gather, run as aio_run, sleep as aio_sleep, create_task as aio_create_task
from typing import Dict, List, Optional, Union
from pandas import concat, read_csv, Series, DataFrame, notna
from decimal import Decimal, ROUND_HALF_UP
from aiofiles import open as aio_open
from os.path import dirname, getmtime, join as os_join
from os import walk
from re import match, search
from datetime import datetime, timedelta
from shutil import copy as shutil_copy
# import aiohttp
# from pprint import pprint

from config import get_csv_config
from logger import configure_logging
from send_msg import send_telegram_message


# Загрузка логгера с настройками
logging = configure_logging()

# Получение параметров из ENV
env = get_csv_config()


async def check_file_modification(file_path: str) -> None:
    """
    Асинхронная функция для проверки времени последней модификации файла и отправки уведомления в случае
    превышения установленного лимита времени неактивности.

    :param file_path: Путь к файлу, который необходимо проверить на предмет последней модификации.
    :type file_path: str

    Функция выполняет следующие действия:
        1. Получает время последней модификации файла.
        2. Вычисляет разницу времени между текущим моментом и временем последней модификации файла.
        3. Преобразует разницу во времени в часы.
        4. Логирует информацию о времени последней модификации файла.
        5. Если файл не изменялся дольше, чем установлено в лимите времени неактивности, отправляет предупреждение в лог и уведомление в Telegram.
    """
    # Получаем время последней модификации файла
    file_mod_time = datetime.fromtimestamp(getmtime(file_path))
    current_time = datetime.now()
    file_mod_delta = current_time - file_mod_time
    # file_mod_hours = file_mod_delta.total_seconds() / 3600  # Преобразование разницы во времени в часы
    # message = f'The file was modified at {file_mod_time}, {file_mod_hours:.2f} hours ago.'

    # Определение времени, прошедшего с момента модификации
    days = file_mod_delta.days
    hours, remainder = divmod(file_mod_delta.seconds, 3600)
    minutes, _ = divmod(remainder, 60)
    
    time_components = []
    
    if days > 0:
        time_components.append(f'{days} days')
    if hours > 0:
        time_components.append(f'{hours} hours')
    time_components.append(f'{minutes} minutes')
    
    time_description = ', '.join(time_components)
    message = f'The file was modified at {file_mod_time}, {time_description} ago.'

    # Проверяем разницу во времени
    if file_mod_delta <= timedelta(hours=env['inactivity_limit_hours']):
        logging.info(message)
    else:
        message = f'File {file_path} has not been modified for more than {env['inactivity_limit_hours']} hours. {message}'
        logging.warning(message)

        # Отправляем уведомление в Telegram
        await send_telegram_message(message)


async def read_file_lines(file_path: str) -> Optional[List[str]]:
    """
    Асинхронная функция для чтения строк из файла.

    :param file_path: Путь к файлу, строки которого необходимо прочитать.
    :type file_path: str

    Функция выполняет следующие действия:
        1. Открывает файл в асинхронном режиме для чтения с кодировкой 'utf-8'.
        2. Читает все строки из файла.
        3. Возвращает список строк, если файл не пустой. Если файл пуст, возвращает None.
    
    Обработка исключений:
        - Logирует ошибку, если файл не найден.
        - Logирует ошибку, если доступ к файлу запрещен.
        - Logирует общую ошибку, если происходит другая неисправность во время чтения.

    :return: Список строк из файла или None, если файл пуст или произошла ошибка.
    :rtype: Optional[List[str]]
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
    Асинхронная функция для обработки строки заголовков CSV и фильтрации пустых заголовков.

    :param header_line: Строка, содержащая заголовки, разделенные заданным разделителем.
    :type header_line: str

    Функция выполняет следующие действия:
        1. Удаляет начальные и конечные пробелы в строке заголовков.
        2. Разделяет строку заголовков на отдельные заголовки, используя разделитель, указанный в переменной окружения 'csv_separator'.
        3. Фильтрует и возвращает список заголовков, удаляя те, которые содержат только пробелы.

    :return: Список непустых заголовков.
    :rtype: List[str]
    """
    await aio_sleep(0)
    headers = header_line.strip().split(env['csv_separator'])
    return [header for header in headers if header.strip()]


async def load_header_template(template_path: str) -> List[str]:
    """
    Асинхронная функция для загрузки и обработки шаблона заголовка из файла.

    :param template_path: Путь к файлу шаблона, который необходимо загрузить.
    :type template_path: str

    Функция выполняет следующие действия:
        1. Асинхронно считывает строки из указанного файла шаблона.
        2. Проверяет, есть ли считанные строки.
        3. Если строки имеются, обрабатывает первую строку как заголовок.
        4. Возвращает обработанный заголовок в виде списка строк.
        5. Если файл пуст или не существует, возвращает пустой список.
    """
    lines = await read_file_lines(template_path)
    if lines:
        return await process_headers(lines[0])
    return []


async def read_csv_async(file_path: str) -> Optional[DataFrame]:
    """
    Асинхронная функция для чтения CSV-файла и возвращения DataFrame, содержащего только столбцы с корректными заголовками.

    :param file_path: Путь к CSV-файлу, который необходимо прочитать.
    :type file_path: str
    :return: DataFrame, содержащий данные из файла с корректными заголовками, или None, если файл пуст.
    :rtype: Optional[DataFrame]

    Функция выполняет следующие действия:
        1. Логирует информацию о начале чтения файла.
        2. Асинхронно считывает строки из файла.
        3. Проверяет, пуст ли файл, и логирует предупреждение, если это так.
        4. Обрабатывает первую строку для извлечения корректных заголовков.
        5. Обрабатывает оставшиеся строки данных, удаляя лишние пробелы.
        6. Объединяет очищенные строки данных.
        7. Создает DataFrame из очищенных данных, используя только непустые заголовки для чтения.
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
        trimmed_line = env['csv_separator'].join(
            [item.strip() for item in line.rstrip(f'{env['csv_separator']}\n').split(env['csv_separator'])])
        cleaned_data.append(trimmed_line)
    # Объединяем строки обратно в одну строку
    content = '\n'.join(cleaned_data)

    # Читаем данные в DataFrame, используя только непустые заголовки
    df = read_csv(
        StringIO(content), sep=env['csv_separator'], names=valid_headers, usecols=valid_headers, header=None, skiprows=1)

    return df


async def sort_columns_by_template(df: DataFrame, header_template: list) -> DataFrame:
    """
    Асинхронная функция для фильтрации и сортировки столбцов DataFrame на основе заданного шаблона заголовков.

    :param df: DataFrame, столбцы которого необходимо отсортировать и отфильтровать.
    :type df: DataFrame
    :param header_template: Список с именами столбцов, определяющий порядок и фильтрацию столбцов DataFrame.
    :type header_template: list

    Функция выполняет следующие действия:
        1. Использует шаблон заголовков для фильтрации столбцов DataFrame.
        2. Сортирует столбцы в порядке, указанном в шаблоне заголовков.
        3. Возвращает новый DataFrame, столбцы которого соответствуют заданному шаблону.

    Важно:
        - Столбцы, отсутствующие в DataFrame, но указанные в шаблоне, будут добавлены как пустые.
        - Столбцы, отсутствующие в шаблоне, будут удалены из итогового DataFrame.
    """
    await aio_sleep(0)
    # Фильтруем и сортируем столбцы DataFrame по шаблону
    sorted_df = df.reindex(columns=header_template)
    return sorted_df


def safe_sum(series: Series, decimal_places: Optional[int] = None) -> float:
    """
    Функция для безопасного вычисления суммы значений в серии данных с возможностью округления результата до
    указанного количества десятичных знаков.

    :param series: Серия данных, содержащая числовые значения, которые необходимо суммировать.
    :type series: Series
    :param decimal_places: Количество знаков после запятой, до которого нужно округлить итоговую сумму.
                           Если None, то округление не выполняется.
    :type decimal_places: Optional[int]

    Функция выполняет следующие действия:
        1. Инициализирует переменную total, представляющую собой сумму, с нулевым значением типа Decimal.
        2. Перебирает значения в серии, исключая 'NaN' значения, и добавляет их к общей сумме, преобразуя каждое значение в тип Decimal.
        3. Если указано количество десятичных знаков, округляет итоговую сумму до заданной точности с использованием метода округления ROUND_HALF_UP.
        4. Преобразует итоговую сумму из типа Decimal в тип float и возвращает результат.
    """
    total = Decimal(0)
    for item in series.dropna():
        total += Decimal(str(item))

    if decimal_places is not None:
        # Округление до указанного количества знаков после запятой
        total = total.quantize(Decimal(10) ** -decimal_places, rounding=ROUND_HALF_UP)

    return float(total)


def extract_width(row: Series, tasks: list) -> Union[float, None]:
    """
    Функция для извлечения значения ширины из строки данных и проверки его валидности.

    :param row: Строка данных, содержащая информацию о продукте, включая ширину и описание.
    :type row: Series
    :param tasks: Список задач, который будет дополнен задачами отправки уведомлений при обнаружении ошибки.
    :type tasks: list

    Функция выполняет следующие действия:
        1. Проверяет наличие и тип данных в поле ширины ('Packing.Ширина'). Если значение является числом (int, float), оно извлекается.
        2. Если значение ширины отсутствует, пытается извлечь числовое значение из поля описания ('Description'), используя регулярное выражение для поиска чисел.
        3. Проверяет, находится ли извлеченное значение ширины в допустимом диапазоне (0 < value <= env['max_width']).
        4. Если значение не соответствует допустимому диапазону, генерирует предупреждающее сообщение и создает задачу для отправки уведомления в Telegram.
        5. Возвращает извлеченное значение ширины или None, если значение не удалось извлечь или оно некорректно.
    """
    value = None
    key_width = 'Packing.Ширина'
    key_description = 'Description'
    
    if notna(row[key_width]) and isinstance(row[key_width], (int, float)):
        value = row[key_width]
    elif isinstance(row[key_description], str) and row[key_description]:
        found_value = search(r'\d+', row[key_description])
        value = float(found_value.group()) if found_value else None

    if value is not None:
        if not 0 < value <= env['max_width']:
            message = f'For product "{row["Packing.Barcode"]}", the width value "{value}" was outside the acceptable range.'
            logging.warning(message)
            tasks.append(aio_create_task(send_telegram_message(message)))

    return value


def extract_compound(row: Series) -> Union[str, None]:
    """
    Функция для извлечения информации о составе упаковки из столбца 'Packing.Состав' или 'AdditionalDescription'.

    :param row: Строка данных, содержащая столбцы 'Packing.Состав' и 'AdditionalDescription'.
    :type row: Series

    Функция выполняет следующие действия:
        1. Проверяет, является ли значение в столбце 'Packing.Состав' строкой и не пусто ли оно.
        2. Если условие из пункта 1 выполняется, возвращает значение из 'Packing.Состав'.
        3. В противном случае проверяет, является ли значение в 'AdditionalDescription' строкой и не пусто ли оно.
        4. Если условие из пункта 3 выполняется, возвращает значение из 'AdditionalDescription'.
        5. Если ни одно из условий не выполняется, возвращает None.
    """
    value = None
    key_compound = 'Packing.Состав'
    key_description = 'AdditionalDescription'
    
    if isinstance(row[key_compound], str) and row[key_compound]:
        value = row[key_compound] if row[key_compound] else None
    elif isinstance(row[key_description], str) and row[key_description]:
        value = row[key_description] if row[key_description] else None

    return value


async def merge_csv_files(files_dict: Dict[str, str]) -> Optional[DataFrame]:
    """
    Асинхронная функция для объединения нескольких CSV-файлов в один DataFrame с обработкой и фильтрацией данных.

    :param files_dict: Словарь, где ключи - это названия файлов, а значения - пути к CSV-файлам.
    :type files_dict: Dict[str, str]

    Функция выполняет следующие действия:
        1. Асинхронно читает данные из указанных CSV-файлов и создает список DataFrame.
        2. Фильтрует невалидные DataFrame и выводит предупреждение в лог, если ни один из них не был валидным.
        3. Обрабатывает каждый DataFrame, добавляя новую колонку с префиксом 'Storage_' и удаляя колонку 'Packing.МестоХранения'.
        4. Объединяет все обработанные DataFrame в один общий DataFrame и выводит сообщение об успешном объединении.
        5. Заменяет значения в колонке 'Наименование' на заданное в переменной окружения 'name_of_product_type', если она указана.
        6. Заполняет пропуски в столбцах, начинающихся с 'Storage_' пустыми строками.
        7. Извлекает и проверяет значения ширины для каждого товара. Отправляет уведомление в Telegram, если значение выходит за допустимые пределы.
        8. Извлекает состав из колонок 'Packing.Состав' или 'AdditionalDescription'.
        9. Группирует данные по штрих-коду ('Packing.Barcode'), суммируя значения в колонках 'Packing.Колво' и 'Packing.СвободныйОстаток' и объединяя значения в других колонках.

    :return: Возвращает объединенный и обработанный DataFrame или None, если не удалось объединить данные.
    :rtype: Optional[DataFrame]
    """
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
    message = 'The value of the cells in the "Наименование" column has'
    if env['name_of_product_type']:
        combined_df['Наименование'] = env['name_of_product_type']
        logging.warning(
            f'{message} been replaced with "{env['name_of_product_type']}"')
    else:
        logging.warning(
            f'{message} not been changed, the "CSV_NEW_NAME_VALUE" constant is empty.')

    tasks = []  # Список для задач

    # Обработка столбца 'Packing.МестоХранения'
    for column in combined_df.columns:
        if column.startswith('Storage_'):
            combined_df[column] = combined_df[column].fillna('').astype(str)
    
    combined_df['Packing.Ширина'] = combined_df.apply(lambda row: extract_width(row, tasks), axis=1)

    # Дожидаемся выполнения всех задач
    await aio_gather(*tasks)

    combined_df['Packing.Состав'] = combined_df.apply(extract_compound, axis=1)

    # Получаем список всех столбцов для агрегации
    all_columns = combined_df.columns.tolist()

    # Определяем столбцы для агрегации с помощью first
    first_columns = [col for col in all_columns if col not in ['Packing.Колво', 'Packing.СвободныйОстаток']]

    # Группировка и агрегация
    grouped_df = combined_df.groupby('Packing.Barcode', as_index=False).agg(
        {
            'Packing.Колво': lambda x: safe_sum(x, env['decimal_places']),
            'Packing.СвободныйОстаток': lambda x: safe_sum(x, env['decimal_places']),
            **{col: 'first' for col in first_columns},
            **{col: lambda x: ', '.join(filter(None, x)) for col in combined_df.columns if col.startswith('Storage_')}
        }
    )

    return grouped_df


async def save_dataframe_to_csv(df: 'DataFrame', output_path: str) -> None:
    """
    Асинхронная функция для сохранения объекта DataFrame в CSV файл.

    :param df: DataFrame, который необходимо сохранить.
    :type df: DataFrame
    :param output_path: Путь, по которому будет сохранен CSV файл.
    :type output_path: str

    Функция выполняет следующее действие:
        - Сохраняет переданный DataFrame в файл формата CSV по указанному пути, используя разделитель, заданный в переменной окружения 'csv_separator'.
    """
    df.to_csv(output_path, index=False, sep=env['csv_separator'])


async def find_matching_files(directory: str, pattern: str) -> dict:
    """
    Асинхронная функция для поиска файлов в указанной директории, соответствующих заданному шаблону.

    :param directory: Директория, в которой необходимо искать файлы.
    :type directory: str
    :param pattern: Шаблон, по которому производится поиск файлов.
    :type pattern: str

    :return: Словарь, где ключи — это имена файлов, соответствующих шаблону, а значения — их полные пути.
    :rtype: dict

    Функция выполняет следующие действия:
        1. Обходит все файлы в указанной директории и поддиректориях.
        2. Проверяет каждый файл на соответствие заданному шаблону.
        3. Если файл соответствует шаблону, добавляет его имя и полный путь в словарь результатов.
    """
    files_dict = {}
    for root, dirs, files in walk(directory):
        for file in files:
            if match(pattern, file):
                file_path = os_join(root, file)
                csv_id = match(pattern, file)
                if csv_id:
                    file_name = csv_id.group(1)
                    files_dict[file_name] = file_path
    return files_dict


def get_valid_file_name() -> Optional[str]:
    """
    Функция для получения корректного имени файла из переменных окружения.

    Функция сначала пытается получить имя файла из переменной окружения 'csv_file_name'.
    Если переменная пуста, она пытается получить имя файла из 'csv_file_name_for_dta'.
    Если обе переменные пусты, функция возвращает None.

    :return: Имя файла в виде строки или None, если ни одна из переменных окружения не содержит имени файла.
    :rtype: Optional[str]
    """
    csv_file_name = env.get('csv_file_name', '')
    csv_file_name_for_dta = env.get('csv_file_name_for_dta', '')

    if csv_file_name:
        return csv_file_name
    elif csv_file_name_for_dta:
        return csv_file_name_for_dta
    else:
        return None


async def copy_file(src: str, dst: str) -> None:
    """
    Асинхронная функция для копирования файла из одного места в другое.

    Функция пытается скопировать файл из указанного источника в указанное место назначения.
    В случае успешного копирования, логируется информационное сообщение.
    Если копирование не удалось, логируется сообщение об ошибке с описанием исключения.

    :param src: Путь к исходному файлу, который необходимо скопировать.
    :type src: str
    :param dst: Путь, куда должен быть скопирован файл.
    :type dst: str
    """
    try:
        shutil_copy(src, dst)
        logging.info(f'File copied from {src} to {dst}')
    except Exception as e:
        logging.error(f'Failed to copy file from {src} to {dst}: {e}')


async def process_and_save_all_csv(header_template_path: str) -> None:
    """
    Асинхронная функция для обработки и сохранения CSV файлов. Она выполняет поиск файлов, соответствующих
    заданному шаблону, объединяет их, сортирует столбцы в соответствии с заданным шаблоном заголовков и
    сохраняет результат в новый CSV файл.

    :param header_template_path: Путь к файлу с шаблоном заголовка, который используется для сортировки столбцов.
    :type header_template_path: str

    Функция выполняет следующие шаги:
        1. Загружает шаблон заголовка для сортировки столбцов.
        2. Ищет файлы, соответствующие заданному шаблону имени файла в указанной директории.
        3. Логирует количество найденных файлов.
        4. Объединяет найденные файлы в один DataFrame, если они найдены.
        5. Проверяет время последней модификации каждого файла.
        6. Для каждого файла:
            - Копирует объединенные данные.
            - Переименовывает столбец, представляющий место хранения, если он существует.
            - Удаляет все столбцы, начинающиеся с 'Storage_'.
            - Сортирует столбцы по загруженному шаблону заголовка.
            - Генерирует корректное имя для нового CSV файла и сохраняет его.
            - Логирует путь сохраненного файла.
            - Если указано, копирует файл в другое место для проверки.
        7. Логирует предупреждения в случае отсутствия данных для сохранения или если не удалось найти файлы по шаблону.
        8. Логирует предупреждения в случае отсутствия ожидаемого столбца в данных.
    """
    header_template = await load_header_template(header_template_path)

    files_dict = await find_matching_files(env['csv_path_directory'], env['csv_file_pattern'])
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
                    
                    csv_file_name = get_valid_file_name()
                    if csv_file_name:
                        output_path = os_join(dirname(file_path), csv_file_name)
                        await save_dataframe_to_csv(current_df, output_path)
                        logging.info(f"Saved merged file to {output_path}")

                        csv_file_name_for_checker = env.get('csv_file_name_for_checker', '')
                        if csv_file_name_for_checker:
                            checker_path = os_join(dirname(file_path), csv_file_name_for_checker)
                            await copy_file(output_path, checker_path)
                    else:
                        logging.warning(f'Both CSV_FILE_NAME and CSV_FILE_NAME_FOR_DTA are empty for file {file_name}.')
                else:
                    logging.warning(f'Missing expected column {place_column} for file {file_name}.')
        else:
            logging.warning('No data to save after merging.')
    else:
        logging.warning('No files found matching the pattern.')


if __name__ == '__main__':
    env = get_csv_config()
    path = str(os_join(env['csv_path_template_directory'], env['csv_file_name_for_dta']))
    aio_run(process_and_save_all_csv(path))
