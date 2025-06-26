# __author__ = 'InfSub'
# __contact__ = 'ADmin@TkYD.ru'
# __copyright__ = 'Copyright (C) 2024, [LegioNTeaM] InfSub'
# __date__ = '2025/06/26'
# __deprecated__ = False
# __email__ = 'ADmin@TkYD.ru'
# __maintainer__ = 'InfSub'
# __status__ = 'Production'  # 'Production / Development'
# __version__ = '2.0.0.0'

from typing import List, Optional, Dict, Literal, Tuple
from aiohttp import ClientSession as aio_ClientSession, ClientTimeout as aio_ClientTimeout
from asyncio import sleep as aio_sleep, Lock as aio_Lock, TimeoutError as aio_TimeoutError
from enum import Enum
import re

from config import Config, ConfigNames
from logger import logging


logging = logging.getLogger(__name__)


class MessageState(Enum):
    SEND = 'send'


class ParseMode(Enum):
    MARKDOWN = 'Markdown'
    MARKDOWN_V2 = 'MarkdownV2'
    HTML = 'HTML'
    NONE = None


class MessageFormatter:
    """Класс для форматирования сообщений в разных режимах"""
    
    @staticmethod
    def escape_markdown_v2(text: str) -> str:
        """Экранирование специальных символов для MarkdownV2"""
        special_chars = ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']
        for char in special_chars:
            text = text.replace(char, f'\\{char}')
        return text
    
    @staticmethod
    def escape_html(text: str) -> str:
        """Экранирование специальных символов для HTML"""
        html_escapes = {
            '&': '&amp;',
            '<': '&lt;',
            '>': '&gt;',
            '"': '&quot;',
            "'": '&#39;'
        }
        for char, escape in html_escapes.items():
            text = text.replace(char, escape)
        return text
    
    @staticmethod
    def format_message(text: str, parse_mode: ParseMode) -> str:
        """Форматирует сообщение в соответствии с выбранным режимом"""
        if parse_mode == ParseMode.NONE:
            return text
        
        # Заменяем Markdown разметку на соответствующий формат
        if parse_mode == ParseMode.MARKDOWN:
            return MessageFormatter._format_markdown(text)
        elif parse_mode == ParseMode.MARKDOWN_V2:
            return MessageFormatter._format_markdown_v2(text)
        elif parse_mode == ParseMode.HTML:
            return MessageFormatter._format_html(text)
        
        return text
    
    @staticmethod
    def _format_markdown(text: str) -> str:
        """Форматирование для Markdown"""
        # В режиме Markdown оставляем Markdown-синтаксис как есть
        # Не заменяем ` на <code>, так как это HTML-теги
        # Markdown поддерживает `код` и ```блок кода``` нативно
        return text
    
    @staticmethod
    def _format_markdown_v2(text: str) -> str:
        """Форматирование для MarkdownV2"""
        # В режиме MarkdownV2 оставляем Markdown-синтаксис как есть
        # Не заменяем ` на <code>, так как это HTML-теги
        # MarkdownV2 поддерживает `код` и ```блок кода``` нативно
        
        # Экранируем специальные символы для MarkdownV2
        special_chars = ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']
        for char in special_chars:
            text = text.replace(char, f'\\{char}')
        
        return text
    
    @staticmethod
    def _format_html(text: str) -> str:
        """Форматирование для HTML"""
        # Экранируем HTML символы
        text = MessageFormatter.escape_html(text)
        
        # Заменяем ``` на <pre><code>
        text = re.sub(r'```(.*?)```', r'<pre><code>\1</code></pre>', text, flags=re.DOTALL)
        # Заменяем ` на <code>
        text = re.sub(r'`([^`]+)`', r'<code>\1</code>', text)
        # Заменяем * на <b>
        text = re.sub(r'\*([^*]+)\*', r'<b>\1</b>', text)
        
        return text


class TelegramMessenger:
    """Оптимизированный класс для отправки сообщений в Telegram"""
    
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(
            self,
            telegram_token: Optional[str] = None,
            telegram_chat_id: Optional[str] = None,
            max_message_length: Optional[int] = None,
            parse_mode: Optional[Literal['Markdown', 'MarkdownV2', 'HTML']] = None
    ):
        # Инициализация, если еще не инициализирован
        if not hasattr(self, '_initialized'):
            self._initialized = True
    
            config: Dict[str, int | str] = Config().get_config(ConfigNames.TELEGRAM)
            telegram_token: str = config['telegram_token'] if telegram_token is None else telegram_token
            telegram_chat_id: str = config['telegram_chat_id'] if telegram_chat_id is None else telegram_chat_id
            telegram_parse_mode: Optional[Literal['Markdown', 'MarkdownV2', 'HTML']] = (
                config['telegram_parse_mode'] if parse_mode is None else parse_mode)
            telegram_line_height: int = config['telegram_line_height']
            max_message_length: int = config['telegram_max_msg_length'] if max_message_length is None else (
                max_message_length)
        
            self._telegram_token: Optional[str] = telegram_token
            self._chat_id, self._message_thread_id = self._parse_chat_id(telegram_chat_id)
            self._telegram_parse_mode: ParseMode = ParseMode(telegram_parse_mode) if telegram_parse_mode else ParseMode.NONE
            self.max_message_length: int = max_message_length
            self._messages: List[str] = []
            self.buffer: str = ''
            self.lock = aio_Lock()
            self.selector: str = f'\n\n{'─' * telegram_line_height}\n\n'
    
    async def __call__(self, message: Optional[str] = None, action: Optional[MessageState] = None):
        """Обработка вызова с обоими или одним аргументом"""
        if message is not None and action == MessageState.SEND:
            # Одновременный вызов: добавляем сообщение и отправляем
            await self.add_message(message)
            return await self.flush()
        elif message is not None:
            # Просто добавляем сообщение
            return await self.add_message(message)
        elif action == MessageState.SEND:
            # Только отправка
            return await self.flush()
        else:
            raise ValueError('Specify either "message" and/or "action=MessageState.SEND".')

    @staticmethod
    def _parse_chat_id(chat_id_str: str) -> Tuple[int, Optional[int]]:
        """Парсинг chat_id и message_thread_id"""
        parts = chat_id_str.split('/')
        chat_id = int(parts[0])
        message_thread_id = int(parts[1]) if len(parts) > 1 else None
        return chat_id, message_thread_id

    async def add_message(self, new_message: str) -> None:
        """Добавляет сообщение в буфер с форматированием"""
        async with self.lock:
            # Форматируем сообщение в соответствии с выбранным режимом
            formatted_message = MessageFormatter.format_message(new_message, self._telegram_parse_mode)
            
            logging.debug(f'Adding new message of length: "{len(formatted_message)}".')
            self._messages.append(formatted_message)

            # Объединяем все сообщения в одну строку
            combined_message = self.selector.join(self._messages)

            if len(combined_message) <= self.max_message_length:
                # Всё помещается — ничего не отправляем
                self.buffer = combined_message
                logging.debug('Messages combined within limit, buffer updated.')
            else:
                # Не помещается — отправляем текущие сообщения
                await self._send_buffer()
                # После отправки очищаем список сообщений
                self._messages = []

    async def flush(self) -> None:
        """Отправляет все накопленные сообщения"""
        logging.debug('Flushing buffer.')
        async with self.lock:
            # Отправляем все накопленные сообщения
            if self._messages:
                combined_message = self.selector.join(self._messages)
                self.buffer = combined_message
                await self._send_buffer()
                self._messages.clear()
        
    async def _send_buffer(self) -> bool:
        """Отправляет сообщение из буфера"""
        if not self.buffer:
            logging.info('Buffer is empty, nothing to send.')
            return True

        success = False
        max_retries = 3
        retries = 0
        message = self.buffer

        while retries < max_retries:
            response = await self._send_telegram_message(message)
            if response.get('ok', False):
                success = True
                break
            else:
                error_code = response.get('error_code')
                if error_code == 429:
                    logging.warning(f'Message failed with error code: {error_code}. Retrying after delay.')
                    retry_after = response.get('parameters', {}).get('retry_after', 5)
                    await aio_sleep(retry_after)
                else:
                    logging.error(f'Failed to send message: {response}')
                    break
            retries += 1

        if success:
            self.buffer = ''
        else:
            logging.error('Failed to send buffer after retries.')
        return success

    async def _send_telegram_message(self, message: str, parse_mode: Optional[str] = None) -> Dict:
        """
        Отправляет сообщение в Telegram с поддержкой выбранного типа разметки и таймаутом.

        :param message: Текст сообщения.
        :param parse_mode: Тип разметки: 'Markdown', 'MarkdownV2', 'HTML' или None (без разметки).
        :return: Ответ API Telegram.
        """
        url = f'https://api.telegram.org/bot{self._telegram_token}/sendMessage'
        payload = {'chat_id': self._chat_id, 'text': message}

        # Используем переданный parse_mode или настройку по умолчанию
        if parse_mode is not None:
            payload['parse_mode'] = parse_mode
        elif self._telegram_parse_mode != ParseMode.NONE:
            payload['parse_mode'] = self._telegram_parse_mode.value

        if self._message_thread_id is not None:
            payload['message_thread_id'] = self._message_thread_id

        # Указываем таймаут ожидания (10 секунд)
        timeout = aio_ClientTimeout(total=10)
        async with aio_ClientSession(timeout=timeout) as session:
            try:
                async with session.post(url, data=payload) as response:
                    resp_json = await response.json()
                    if response.status == 200:
                        return resp_json
                    else:
                        logging.error(f'HTTP {response.status} response: {resp_json}')
                        return resp_json
            except aio_TimeoutError:
                logging.error('Timeout occurred while sending message to Telegram API.')
                return {'ok': False, 'error': 'Timeout while sending message'}
            except Exception as e:
                logging.exception(f'Exception occurred while sending message: {e}')
                return {}

    def get_parse_mode(self) -> ParseMode:
        """Возвращает текущий режим парсинга"""
        return self._telegram_parse_mode
    
    def set_parse_mode(self, parse_mode: ParseMode) -> None:
        """Устанавливает новый режим парсинга"""
        self._telegram_parse_mode = parse_mode


if __name__ == '__main__':
    from asyncio import run as aio_run

    async def main():
        """Пример использования оптимизированного TelegramMessenger"""
        messenger = TelegramMessenger()
        
        # Тестируем разные форматы сообщений
        test_messages = [
            "*Bold text* with `code` and ```block code```",
            "Regular message without formatting",
            "*File:*```\n/path/to/file.txt``` has been modified",
            "Product *12345* has width `150` cm"
        ]
        
        for i, msg in enumerate(test_messages, 1):
            await messenger.add_message(f"Test message {i}: {msg}")
        
        await messenger(message="Final message", action=MessageState.SEND)
    
    aio_run(main()) 