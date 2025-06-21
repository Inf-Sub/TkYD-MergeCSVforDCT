# __author__ = 'InfSub'
# __contact__ = 'ADmin@TkYD.ru'
# __copyright__ = 'Copyright (C) 2024, [LegioNTeaM] InfSub'
# __date__ = '2025/06/19'
# __deprecated__ = False
# __email__ = 'ADmin@TkYD.ru'
# __maintainer__ = 'InfSub'
# __status__ = 'Production'  # 'Production / Development'
# __version__ = '1.7.4.3'

from typing import List, Optional, Dict, Literal, Tuple
from aiohttp import ClientSession as aio_ClientSession, ClientTimeout as aio_ClientTimeout
from asyncio import sleep as aio_sleep, Lock as aio_Lock, TimeoutError as aio_TimeoutError
from enum import Enum

from config import Config, ConfigNames
from logger import logging


logging = logging.getLogger(__name__)

class MessageState(Enum):
    SEND = 'send'


class TelegramMessenger:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(
            self, telegram_token: Optional[str] = None, telegram_chat_id: Optional[str] = None,
            max_message_length: Optional[int] = None, parse_mode: Optional[Literal['Markdown']] = None
    ):
        # Инициализация, если еще не инициализирован
        if not hasattr(self, '_initialized'):
            self._initialized = True
    
            _env: Dict[str: str] = Config().get_config(ConfigNames.TELEGRAM)
            telegram_token = _env['telegram_token'] if telegram_token is None else telegram_token
            telegram_chat_id = _env['telegram_chat_id'] if telegram_chat_id is None else telegram_chat_id
            telegram_parse_mode = _env['telegram_parse_mode'] if parse_mode is None else parse_mode
            telegram_line_height = _env['telegram_line_height']
            max_message_length = _env['telegram_max_msg_length'] if max_message_length is None else max_message_length
        
            self._telegram_token: Optional[str] = telegram_token
            self._chat_id, self._message_thread_id = self._parse_chat_id(telegram_chat_id)
            self._telegram_parse_mode: Optional[Literal['Markdown']] = telegram_parse_mode
            self.max_message_length: int = max_message_length
            self._messages: List[str] = []
            self.buffer: str = ''
            self.lock = aio_Lock()
            self.selector: str = f'\n\n{'─' * telegram_line_height}\n\n'
    
    async def __call__(self, message: Optional[str] = None, action: Optional[MessageState] = None):
        # Обработка вызова с обоими или одним аргументом
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
        parts = chat_id_str.split('/')
        chat_id = int(parts[0])
        message_thread_id = int(parts[1]) if len(parts) > 1 else None
        return chat_id, message_thread_id

    async def add_message(self, new_message: str) -> None:
        async with self.lock:
            logging.debug(f'Adding new message of length: "{len(new_message)}".')
            self._messages.append(new_message)

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
        logging.debug('Flushing buffer.')
        async with self.lock:
            # Отправляем все накопленные сообщения
            if self._messages:
                combined_message = self.selector.join(self._messages)
                self.buffer = combined_message
                await self._send_buffer()
                self._messages.clear()
        
    async def _send_buffer(self) -> bool:
        """
        Отправляет сообщение из буфера.
        """
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
        :param parse_mode: Тип разметки: 'Markdown', 'HTML' или None (без разметки).
        :return: Ответ API Telegram.
        """
        url = f'https://api.telegram.org/bot{self._telegram_token}/sendMessage'
        payload = {'chat_id': self._chat_id, 'text': message}

        parse_mode = self._telegram_parse_mode if parse_mode is None else parse_mode

        if self._message_thread_id is not None:
            payload['message_thread_id'] = self._message_thread_id

        if parse_mode is not None:
            payload['parse_mode'] = parse_mode

        # Указываем таймаут ожидания (например, 10 секунд)
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


# __version__ = '1.7.4.0'
# from typing import Dict
# from aiohttp import ClientSession as aio_ClientSession, ClientTimeout as aio_ClientTimeout
# from asyncio import sleep as aio_sleep, Lock as aio_Lock, TimeoutError as aio_TimeoutError
#
# from config import Config, ConfigNames
# from logger import logging
#
#
# logging = logging.getLogger(__name__)
#
# class TelegramMessenger:
#     _instance = None
#
#     def __new__(cls, *args, **kwargs):
#         if cls._instance is None:
#             cls._instance = super(TelegramMessenger, cls).__new__(cls)
#         return cls._instance
#
#     def __init__(
#             self, telegram_token: str | None = None, telegram_chat_id: str | None = None, max_message_length: int = 4096):
#         """
#         :param telegram_token: токен бота Telegram
#         :param telegram_chat_id: id чата (может содержать разделитель '/' для message_thread_id)
#         :param max_message_length: максимально допустимая длина сообщения
#         """
#         if hasattr(self, '_initialized') and self._initialized:
#             return
#         self._initialized = True
#
#         _env: Dict[str: str] = Config().get_config(ConfigNames.TELEGRAM)
#         telegram_token = _env['telegram_token'] if telegram_token is None else telegram_token
#         telegram_chat_id = _env['telegram_chat_id'] if telegram_chat_id is None else telegram_chat_id
#
#         self._telegram_token = telegram_token
#         self._chat_id, self._message_thread_id = self._parse_chat_id(telegram_chat_id)
#         self._telegram_parse_mode = 'Markdown'
#         self.max_message_length = max_message_length
#         self.buffer = ''
#         self.lock = aio_Lock()
#         self.selector = f'\n{"─" * 24}\n'
#
#     @staticmethod
#     def _parse_chat_id(chat_id_str: str):
#         parts = chat_id_str.split('/')
#         chat_id = parts[0]
#         message_thread_id = int(parts[1]) if len(parts) > 1 else None
#         return chat_id, message_thread_id
#
#     async def _send_message_in_parts(self, message: str) -> None:
#         """
#         Разбивает сообщение на части и отправляет каждую часть по отдельности.
#         """
#         for i in range(0, len(message), self.max_message_length):
#             part = message[i:i + self.max_message_length]
#             self.buffer = part
#             logging.warning(f'Sending message chunk: "{self.buffer[0:25]}...{self.buffer[-25:]}"')
#             await self._send_buffer()
#
#     async def add_message(self, new_message: str) -> None:
#         """
#         Добавляет сообщение в буфер. Если превышает лимит, разбивает на блоки и отправляет по частям.
#         """
#         async with self.lock:
#             logging.info(f'Adding new message of length: "{len(new_message)}".')
#             # Формируем кандидат в сообщение
#             if not self.buffer:
#                 candidate_message = new_message
#                 # logging.debug(f'The buffer is empty. Message added to the buffer. Message: "{new_message[0:25]}"...')
#             else:
#                 candidate_message = f'{self.buffer}{self.selector}{new_message}'
#                 # logging.debug(
#                 #     f'The buffer is not empty. The message is added to the message available in the buffer. '
#                 #     f'Buffer: "{self.buffer[0:25]}...{self.buffer[-25:]}"... Message: "{new_message[0:25]}"...'
#                 # )
#
#             if len(candidate_message) <= self.max_message_length:
#                 # Вписываем всё в буфер
#                 self.buffer = candidate_message
#                 logging.debug('Message added to buffer without sending.')
#             else:
#                 # Перед тем как отправлять, пытаемся отправить текущий буфер
#                 sent_success = await self._send_buffer()
#
#                 if not sent_success:
#                     # Если отправка не удалась, разбиваем и отправляем по частям
#                     await self._send_message_in_parts(candidate_message)
#                 else:
#                     # После успешной отправки, разбиваем и отправляем новые сообщения, если они большие
#                     if len(new_message) > self.max_message_length:
#                         await self._send_message_in_parts(new_message)
#                     else:
#                         # Иначе просто обновляем буфер новым сообщением
#                         self.buffer = new_message
#
#     async def flush(self) -> None:
#         """
#         Отправляет оставшийся в буфере текст.
#         """
#         async with self.lock:
#             if self.buffer:
#                 logging.info('Flushing buffer.')
#             await self._send_buffer()
#
#     async def _send_buffer(self) -> bool:
#         """
#         Отправляет сообщение из буфера.
#         """
#         if not self.buffer:
#             logging.info('Buffer is empty, nothing to send.')
#             return True
#
#         success = False
#         max_retries = 3
#         retries = 0
#         message = self.buffer
#
#         while retries < max_retries:
#             response = await self._send_telegram_message(message)
#             if response.get('ok', False):
#                 logging.info('Message sent successfully.')
#                 # logging.debug(f'Message sent: "{message[0:25]}"...')
#                 success = True
#                 break
#             else:
#                 error_code = response.get('error_code')
#                 if error_code == 429:
#                     logging.warning(f'Message sent failed with error code: {error_code}.')
#                     retry_after = response.get('parameters', {}).get('retry_after', 5)
#                     logging.warning(f'Too many requests. Retry after {retry_after} seconds.')
#                     await aio_sleep(retry_after)
#                 else:
#                     logging.error(f'Failed to send message: {response}')
#                     break
#             retries += 1
#
#         if success:
#             self.buffer = ''
#         else:
#             logging.error('Failed to send buffer after retries.')
#         return success
#
#     async def _send_telegram_message(self, message: str, parse_mode: str | None = None) -> Dict:
#         """
#         Отправляет сообщение в Telegram с поддержкой выбранного типа разметки и таймаутом.
#
#         :param message: Текст сообщения.
#         :param parse_mode: Тип разметки: 'Markdown', 'HTML' или None (без разметки).
#         :return: Ответ API Telegram.
#         """
#         url = f'https://api.telegram.org/bot{self._telegram_token}/sendMessage'
#         payload = {'chat_id': self._chat_id, 'text': message}
#
#         parse_mode = self._telegram_parse_mode if parse_mode is None else parse_mode
#
#         if self._message_thread_id is not None:
#             payload['message_thread_id'] = self._message_thread_id
#
#         if parse_mode is not None:
#             payload['parse_mode'] = parse_mode
#
#         # Указываем таймаут ожидания (например, 10 секунд)
#         timeout = aio_ClientTimeout(total=10)
#         async with aio_ClientSession(timeout=timeout) as session:
#             try:
#                 async with session.post(url, data=payload) as response:
#                     resp_json = await response.json()
#                     if response.status == 200:
#                         return resp_json
#                     else:
#                         logging.error(f'HTTP {response.status} response: {resp_json}')
#                         return resp_json
#             except aio_TimeoutError:
#                 logging.error('Timeout occurred while sending message to Telegram API.')
#                 return {'ok': False, 'error': 'Timeout while sending message'}
#             except Exception as e:
#                 logging.exception(f'Exception occurred while sending message: {e}')
#                 return {}



# class TelegramMessenger:
#     def __init__(self, _telegram_token: str, telegram_chat_id: str, max_message_length=4096):
#         """
#         :param _telegram_token: токен бота Telegram
#         :param telegram_chat_id: id чата (может содержать разделитель '/' для message_thread_id)
#         :param max_message_length: максимально допустимая длина сообщения
#         """
#         self._telegram_token = _telegram_token
#         self._chat_id, self._message_thread_id = self._parse_chat_id(telegram_chat_id)
#         self.max_message_length = max_message_length
#         self.buffer = ""
#         self.lock = aio_Lock()
#
#     @staticmethod
#     def _parse_chat_id(chat_id_str: str):
#         parts = chat_id_str.split('/')
#         chat_id = parts[0]
#         message_thread_id = int(parts[1]) if len(parts) > 1 else None
#         return chat_id, message_thread_id
#
#     async def add_message(self, new_message: str) -> None:
#         """
#         Добавляет сообщение в буфер. Если превышает лимит, отправляет текущий буфер.
#         """
#         async with self.lock:
#             logging.info(f"Adding message: {new_message}")
#             if not self.buffer:
#                 candidate_message = new_message
#             else:
#                 candidate_message = self.buffer + "\n---\n" + new_message
#
#             if len(candidate_message) <= self.max_message_length:
#                 self.buffer = candidate_message
#                 logging.info(f"Message added to buffer. Buffer length: {len(self.buffer)}")
#             else:
#                 logging.info("Buffer exceeds max length, attempting to send current buffer.")
#                 sent_success = await self._send_buffer()
#                 if not sent_success:
#                     # Если не удалось отправить, добавляем сообщение к буферу с разделителем
#                     combined_message = self.buffer + "\n---\n" + new_message
#                     if len(combined_message) <= self.max_message_length:
#                         self.buffer = combined_message
#                         logging.warning("Buffer was too large, but after combining, fits within limit.")
#                     else:
#                         # Слишком длинное сообщение, обрезаем
#                         truncated = combined_message[:self.max_message_length]
#                         self.buffer = truncated
#                         logging.warning("Buffer truncated due to size limit.")
#                         await self._send_buffer()
#
#     async def flush(self) -> None:
#         """
#         Отправляет оставшийся в буфере текст.
#         """
#         async with self.lock:
#             logging.info("Flushing buffer.")
#             await self._send_buffer()
#
#     async def _send_buffer(self) -> bool:
#         """
#         Отправляет сообщение из буфера. Возвращает True, если успешно.
#         """
#         if not self.buffer:
#             logging.info("Buffer is empty, nothing to send.")
#             return True
#
#         success = False
#         retry_after = None
#         message = self.buffer
#         logging.info(f"Attempting to send message of length {len(message)}")
#         while True:
#             response = await self._send_telegram_message(message)
#             if response.get("ok", False):
#                 logging.info("Message sent successfully.")
#                 success = True
#                 break
#             else:
#                 error_code = response.get("error_code")
#                 if error_code == 429:
#                     retry_after = response.get("parameters", {}).get("retry_after", 5)
#                     logging.warning(f"Rate limited. Retrying after {retry_after} seconds.")
#                     await aio_sleep(retry_after)
#                 else:
#                     logging.error(f"Failed to send message: {response}")
#                     break
#         if success:
#             self.buffer = ""
#         return success
#
#     async def _send_telegram_message(self, message: str) -> Dict:
#         """
#         Отправляет сообщение в Telegram API.
#         """
#         url = f'https://api.telegram.org/bot{self._telegram_token}/sendMessage'
#         payload = {
#             'chat_id': self._chat_id,
#             'text': message
#         }
#         if self._message_thread_id is not None:
#             payload['message_thread_id'] = self._message_thread_id
#
#         async with aio_ClientSession() as session:
#             try:
#                 async with session.post(url, data=payload) as response:
#                     if response.status == 200:
#                         result = await response.json()
#                         logging.info("API response received successfully.")
#                         return result
#                     else:
#                         text = await response.text()
#                         logging.error(f"HTTP {response.status}: {text}")
#                         return {}
#             except Exception as e:
#                 logging.exception(f"Exception occurred while sending message: {e}")
#                 return {}


# class TelegramMessenger:
#     def __init__(self, _telegram_token: str, telegram_chat_id: str, max_message_length=4096):
#         """
#         :param _telegram_token: токен бота Telegram
#         :param telegram_chat_id: id чата (может содержать разделитель '/' для message_thread_id)
#         :param max_message_length: максимально допустимая длина сообщения
#         """
#         self._telegram_token = _telegram_token
#         self._chat_id, self._message_thread_id = self._parse_chat_id(telegram_chat_id)
#         self.max_message_length = max_message_length
#         self.buffer = ""
#         self.lock = aio_Lock()
#
#     @staticmethod
#     def _parse_chat_id(chat_id_str: str):
#         parts = chat_id_str.split('/')
#          = parts[0]
#         message_thread_id = int(parts[1]) if len(parts) > 1 else None
#         return chat_id, message_thread_id
#
#     async def add_message(self, new_message: str) -> None:
#         """
#         Добавляет сообщение в буфер. Если превышает лимит, отправляет текущий буфер.
#         """
#         async with self.lock:
#             if not self.buffer:
#                 candidate_message = new_message
#             else:
#                 candidate_message = self.buffer + "\n---\n" + new_message
#
#             if len(candidate_message) <= self.max_message_length:
#                 self.buffer = candidate_message
#             else:
#                 # Отправляем текущий буфер
#                 sent_success = await self._send_buffer()
#                 if not sent_success:
#                     # Если не удалось отправить, добавляем через разделитель
#                     combined_message = self.buffer + "\n---\n" + new_message
#                     if len(combined_message) <= self.max_message_length:
#                         self.buffer = combined_message
#                     else:
#                         # Если всё равно слишком большое, обрезаем
#                         truncated = combined_message[:self.max_message_length]
#                         self.buffer = truncated
#                         await self._send_buffer()
#
#     async def flush(self) -> None:
#         """
#         Отправляет оставшийся в буфере текст.
#         """
#         async with self.lock:
#             await self._send_buffer()
#
#     async def _send_buffer(self) -> bool:
#         """
#         Sends a message from the buffer.
#
#         :return: Returns True, if successful.
#         """
#         if not self.buffer:
#             return True  # Нечего отправлять
#
#         success = False
#         retry_after = None
#         message = self.buffer
#         while True:
#             response = await self._send_telegram_message(message)
#             if response.get("ok", False):
#                 success = True
#                 break
#             else:
#                 error_code = response.get("error_code")
#                 if error_code == 429:
#                     retry_after = response.get("parameters", {}).get("retry_after", 5)
#                     logging.warning(f"Too many requests. Retry after {retry_after} seconds.")
#                     await aio_sleep(retry_after)
#                 else:
#                     logging.error(f"Failed to send message: {response}")
#                     break
#         if success:
#             self.buffer = ""  # Очищаем буфер при успехе
#         return success
#
#     async def _send_telegram_message(self, message: str) -> Dict:
#         """
#         Sends a message to the specified Telegram chat using the API.
#
#         :param message: The text of the message to be sent.
#         :return: The response from the Telegram API as a dictionary (JSON) or an empty dictionary in case of an error.
#         """
#         url = f'https://api.telegram.org/bot{self._telegram_token}/sendMessage'
#         payload = {
#             'chat_id': self._chat_id,
#             'text': message
#         }
#         if self._message_thread_id is not None:
#             payload['message_thread_id'] = self._message_thread_id
#
#         async with aio_ClientSession() as session:
#             try:
#                 async with session.post(url, data=payload) as response:
#                     if response.status == 200:
#                         return await response.json()
#                     else:
#                         text = await response.text()
#                         logging.error(f"HTTP {response.status}: {text}")
#                         return {}
#             except Exception as e:
#                 logging.exception(f'Exception occurred while sending message: "{e}"')
#                 return {}


# async def send_telegram_message(message: str, max_retries: int = 5) -> dict:
#     """
#     Sends a message to the specified Telegram chat using the API with automatic 429 error handling.
#     Retries when receiving the "Too Many Requests" error, taking into account retry_after.
#
#     :param message: The text of the message to be sent.
#     :param max_retries: Maximum number of retries.
#     :return: The response from the Telegram API as a dictionary (JSON) or an empty dictionary in case of an error.
#     --
#     Отправляет сообщение в указанный чат Telegram с помощью API с автоматической обработкой ошибок 429.
#     Повторяет при получении ошибки "Слишком много запросов" с учетом retry_after.
#
#     :param message: Текст сообщения для отправки.
#     :param max_retries: Максимальное количество повторных попыток.
#     :return: Ответ от API Telegram в виде словаря (JSON) или пустой словарь в случае ошибки.
#     """
#     _env: Dict[str: str] = Config().get_config('telegram')
#
#     logging.info('Preparing to send a message to Telegram.')
#
#     # URL for sending a message via the Telegram Bot API
#     url = f'https://api.telegram.org/bot{_env['_telegram_token']}/sendMessage'
#
#     # Split TELEGRAM_CHAT_ID into chat_id and message_thread_id if necessary
#     parts = _env['telegram_chat_id'].split('/')
#     chat_id = parts[0]
#     message_thread_id = parts[1] if len(parts) > 1 else None
#
#     # Prepare the payload for the POST request
#     payload = {
#         'chat_id': chat_id,  # The ID of the chat to which the message will be sent
#         'text': message  # The text of the message
#     }
#
#     # Add message_thread_id to the payload if specified
#     if message_thread_id:
#         payload['message_thread_id'] = message_thread_id
#
#     logging.debug(f'Payload for Telegram: {payload}')
#
#     # Create an HTTP client session
#     async with aio_ClientSession() as session:
#         retries = 0
#         while retries <= max_retries:
#             try:
#                 # Send the POST request
#                 async with session.post(url, data=payload) as response:
#                     resp_json = await response.json()
#                     # Check the response status
#                     if response.status == 200:
#                         logging.info('Message successfully sent to Telegram.')
#                         # Return the response in JSON format
#                         return resp_json
#                     elif resp_json.get('error_code') == 429:
#                         # Обработка ошибки "Too Many Requests"
#                         retry_after = resp_json.get('parameters', {}).get('retry_after', 5)
#                         logging.warning(f'429 Too Many Requests. Повтор через {retry_after} секунд.')
#                         await aio_sleep(retry_after)
#                         retries += 1
#                         continue
#                     else:
#                         # Handle the error in case of an unsuccessful request
#                         logging.error(f'Error sending message: {resp_json}')
#                         return {}  # Return an empty dictionary in case of an error
#             except Exception as e:
#                 # Conditional logging of stack trace
#                 # Условное логирование трассировки стека
#                 if logging.getLogger().isEnabledFor(logging.DEBUG):
#                     logging.exception('Exception occurred while trying to send a message to Telegram.')
#                 else:
#                     logging.error(f'Error sending message: {e}')
#
#         return {}  # Return an empty dictionary in case of an error


if __name__ == '__main__':
    from asyncio import run as aio_run

    # пример её использования:
    async def main():
        _env: Dict[str: str] = Config().get_config(ConfigNames.TELEGRAM)

        messenger = TelegramMessenger()
        # messenger_2 = TelegramMessenger()
        # messenger_3 = TelegramMessenger()
        # messenger_4 = TelegramMessenger()
        # messenger_5 = TelegramMessenger()
        _msg_copy_1 = 100
        _msg_copy_2 = 100
        _msg_copy_3 = 100
        _msg_copy_4 = 100
        _msg_copy_5 = 100

        # for idx in range(5):
            # await TelegramMessenger().add_message(f'add_message Сообщение 1:\n{f"{idx + 1}, " * _msg_copy_1}')
            # await TelegramMessenger().add_message(f'add_message Сообщение 2:\n{f"{idx + 1}, " * _msg_copy_2}')
            #
            # await messenger(message=f'message= Сообщение 1:\n{f"{idx + 1}, " * _msg_copy_1}')
            # await messenger(message=f'message= Сообщение 2:\n{f"{idx + 1}, " * _msg_copy_2}')

        # ...
        await messenger(message=f'Finish', action=MessageState.SEND)
        # await TelegramMessenger().flush()
        # await messenger_2.flush()
        # await messenger_3.flush()
        # await messenger_4.flush()
        # await messenger_5.flush()
    
    aio_run(main())
