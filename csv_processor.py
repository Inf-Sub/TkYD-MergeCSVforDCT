# __author__ = 'InfSub'
# __contact__ = 'ADmin@TkYD.ru'
# __copyright__ = 'Copyright (C) 2024-2025, [LegioNTeaM] InfSub'
# __date__ = '2025/06/26'
# __deprecated__ = False
# __email__ = 'ADmin@TkYD.ru'
# __maintainer__ = 'InfSub'
# __status__ = 'Production'  # 'Production / Development'
# __version__ = '1.7.5.0'

from io import StringIO
from asyncio import gather as aio_gather, create_task as aio_create_task, Task as aio_Task
from typing import Dict, List, Optional
from pandas import concat, read_csv, Series, DataFrame, notna
from decimal import Decimal, ROUND_HALF_UP
from os.path import join as os_join

from config import Config, ConfigNames
from logger import logging
from send_msg import TelegramMessenger, MessageState
from column_enums import PackingColumns, DescriptionColumns, StorageColumns, AggregationColumns, ColumnGroups
from file_manager import FileManager
from data_extractors import WidthExtractor, CompoundExtractor


class CSVProcessor:
    """Основной класс для обработки CSV файлов"""
    
    def __init__(self):
        self.config = Config()
        self.csv_config = self.config.get_config(ConfigNames.CSV)
        self.datas_config = self.config.get_config(ConfigNames.DATAS)
        self.inactivity_config = self.config.get_config(ConfigNames.INACTIVITY)
        self.telegram_messenger = TelegramMessenger()
        self.logger = logging.getLogger(__name__)
        
        # Инициализация вспомогательных классов
        self.file_manager = FileManager(self.logger)
        self.width_extractor = WidthExtractor(
            self.telegram_messenger, 
            self.logger, 
            self.datas_config['datas_max_width']
        )
        self.compound_extractor = CompoundExtractor(self.telegram_messenger, self.logger)
    
    async def process_headers(self, header_line: str) -> List[str]:
        """Обработка строки заголовков CSV"""
        csv_sep = self.csv_config.get('csv_separator', ';')
        headers = header_line.strip().split(csv_sep)
        return [header for header in headers if header.strip()]
    
    async def load_header_template(self, template_path: str) -> List[str]:
        """Загрузка шаблона заголовка из файла"""
        lines = await self.file_manager.read_file_lines(template_path)
        if lines:
            return await self.process_headers(lines[0])
        return []
    
    async def read_csv_async(self, file_path: str) -> Optional[DataFrame]:
        """Чтение CSV файла в DataFrame"""
        csv_sep = self.csv_config.get('csv_separator', ';')
        self.logger.info(f'Reading file: {file_path}')
        
        lines = await self.file_manager.read_file_lines(file_path)
        if not lines:
            self.logger.warning(f'File is empty: {file_path}')
            return None
        
        valid_headers = await self.process_headers(lines[0])
        data_lines = lines[1:]
        
        cleaned_data = []
        for line in data_lines:
            trimmed_line = csv_sep.join([item.strip() for item in line.rstrip(f'{csv_sep}\n').split(csv_sep)])
            cleaned_data.append(trimmed_line)
        
        content = '\n'.join(cleaned_data)
        df = read_csv(StringIO(content), sep=csv_sep, names=valid_headers, header=None)
        
        return df
    
    async def sort_columns_by_template(self, df: DataFrame, header_template: List[str]) -> DataFrame:
        """Сортировка столбцов по шаблону"""
        return df.reindex(columns=header_template)
    
    def safe_sum(self, series: Series, decimal_places: Optional[int] = None) -> float:
        """Безопасное суммирование с округлением"""
        total = Decimal(0)
        for item in series.dropna():
            total += Decimal(str(item))
        
        if decimal_places is not None:
            total = total.quantize(Decimal(10) ** -decimal_places, rounding=ROUND_HALF_UP)
        
        return float(total)
    
    async def merge_csv_files(self, files_dict: Dict[str, str]) -> Optional[DataFrame]:
        """Объединение CSV файлов"""
        dataframes = await aio_gather(*[self.read_csv_async(file_path) for file_path in files_dict.values()])
        dataframes = [df for df in dataframes if df is not None]
        
        if not dataframes:
            self.logger.warning('No valid dataframes to merge.')
            return None
        
        combined_data = []
        for df, file_name in zip(dataframes, files_dict.keys()):
            df[StorageColumns.SOURCE_FILE.value] = file_name
            storage_column = StorageColumns.get_storage_column(file_name)
            df[storage_column] = df[PackingColumns.STORAGE_PLACE.value].fillna('').astype(str).apply(lambda x: f'{x}')
            df.drop(columns=[PackingColumns.STORAGE_PLACE.value], inplace=True)
            combined_data.append(df)
        
        combined_df = concat(combined_data, ignore_index=True)
        self.logger.info('Successfully merged dataframes.')
        
        # Обновление столбца "Наименование"
        message = 'The value of the cells in the "Наименование" column has'
        if self.datas_config['datas_name_of_product_type']:
            combined_df[DescriptionColumns.NAME.value] = self.datas_config['datas_name_of_product_type']
            self.logger.warning(f'{message} been replaced with "{self.datas_config["datas_name_of_product_type"]}"')
        else:
            self.logger.warning(f'{message} not been changed, the "CSV_NEW_NAME_VALUE" constant is empty.')
        
        tasks = []
        
        # Обработка столбцов хранения
        for column in combined_df.columns:
            if ColumnGroups.is_storage_column(column):
                combined_df[column] = combined_df[column].fillna('').astype(str)
        
        # Извлечение данных с использованием специализированных классов
        combined_df[PackingColumns.WIDTH.value] = combined_df.apply(
            lambda row: self.width_extractor.extract(row, tasks), axis=1
        )
        await aio_gather(*tasks)
        
        combined_df[PackingColumns.COMPOUND.value] = combined_df.apply(
            lambda row: self.compound_extractor.extract(row), axis=1
        )
        
        # Группировка и агрегация
        all_columns = combined_df.columns.tolist()
        first_columns = [col for col in all_columns if col not in AggregationColumns.get_sum_columns()]
        
        grouped_df = combined_df.groupby(PackingColumns.BARCODE.value, as_index=False).agg(
            {
                PackingColumns.QUANTITY.value: lambda x: self.safe_sum(x, self.datas_config['datas_decimal_places']),
                PackingColumns.FREE_BALANCE.value: lambda x: self.safe_sum(x, self.datas_config['datas_decimal_places']),
                **{col: 'first' for col in first_columns},
                **{col: lambda x: ', '.join(filter(None, x)) for col in combined_df.columns if ColumnGroups.is_storage_column(col)}
            }
        )
        
        return grouped_df
    
    async def save_dataframe_to_csv(self, df: DataFrame, output_path: str, sep: str) -> None:
        """Сохранение DataFrame в CSV файл"""
        df.to_csv(output_path, index=False, sep=sep)
    
    def get_valid_file_name(self) -> Optional[str]:
        """Получение корректного имени файла"""
        csv_file_name = self.csv_config.get('csv_file_name', '')
        csv_file_name_for_dta = self.csv_config.get('csv_file_name_for_dta', '')
        
        if csv_file_name:
            return csv_file_name
        elif csv_file_name_for_dta:
            return csv_file_name_for_dta
        else:
            return None
    
    async def process_and_save_all_csv(self, header_template_path: str) -> Dict[str, str]:
        """Обработка и сохранение всех CSV файлов"""
        header_template = await self.load_header_template(header_template_path)
        
        files_dict = await self.file_manager.find_matching_files(
            self.csv_config['csv_path_directory'], 
            self.csv_config['csv_file_pattern']
        )
        self.logger.info(f'Found {len(files_dict)} files matching the pattern.')
        
        if files_dict:
            merged_df = await self.merge_csv_files(files_dict=files_dict)
            await self.telegram_messenger.flush()
            
            if merged_df is not None:
                for file_name, file_path in files_dict.items():
                    await self.file_manager.check_file_modification(
                        file_path, 
                        self.inactivity_config['inactivity_limit_hours'],
                        self.telegram_messenger
                    )
                    
                    current_df = merged_df.copy()
                    place_column = StorageColumns.get_storage_column(file_name)
                    
                    if place_column in current_df.columns:
                        current_df.rename(columns={place_column: PackingColumns.STORAGE_PLACE.value}, inplace=True)
                        place_columns = [col for col in current_df.columns if ColumnGroups.is_storage_column(col)]
                        current_df.drop(columns=place_columns, inplace=True)
                        
                        current_df = await self.sort_columns_by_template(current_df, header_template)
                        
                        csv_file_name = self.get_valid_file_name()
                        if csv_file_name:
                            output_path = self.file_manager.get_output_path(file_path, csv_file_name)
                            await self.save_dataframe_to_csv(current_df, output_path, self.csv_config.get('csv_separator', ';'))
                            self.logger.info(f'Saved merged file to {output_path}')
                            
                            csv_file_name_for_checker = self.csv_config.get('csv_file_name_for_checker', '')
                            if csv_file_name_for_checker:
                                checker_path = self.file_manager.get_checker_path(file_path, csv_file_name_for_checker)
                                await self.file_manager.copy_file(output_path, checker_path)
                        else:
                            self.logger.warning(f'Both "CSV_FILE_NAME" and "CSV_FILE_NAME_FOR_DTA" are empty for file {file_name}.')
                    else:
                        self.logger.warning(f'Missing expected column {place_column} for file {file_name}.')
            else:
                self.logger.warning('No data to save after merging.')
        else:
            self.logger.warning('No files found matching the pattern.')
        
        return files_dict
    
    async def run_merge(self) -> None:
        """Основной метод запуска процесса объединения"""
        self.logger.info('Run Script!')
        path = str(os_join(
            self.csv_config['csv_path_template_directory'], 
            self.csv_config['csv_file_name_for_dta']
        ))
        
        files_dict = await self.process_and_save_all_csv(path)
        files_list_str = '\n'.join([f'{key}: {value}' for key, value in files_dict.items()])
        
        await self.telegram_messenger.flush()
        message = f'*CSV files merged completed successfully.*\n\nFiles:```\n{files_list_str}```'
        await self.telegram_messenger.add_message(message)
        self.logger.info('Finished Script!') 