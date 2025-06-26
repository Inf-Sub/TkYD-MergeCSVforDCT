# Форматирование сообщений Telegram

## Обзор

Оптимизированный модуль `send_msg_optimized.py` поддерживает три режима форматирования сообщений Telegram:

- **Markdown** - классический Markdown
- **MarkdownV2** - улучшенный Markdown с экранированием
- **HTML** - HTML разметка
- **None** - без форматирования

## Конфигурация

### Настройка в config.ini

```ini
[TELEGRAM]
MAX_MSG_LENGTH = 3700
LINE_HEIGHT = 26
PARSE_MODE = MarkdownV2  # Markdown, MarkdownV2, HTML или пусто для отключения
```

### Поддерживаемые значения PARSE_MODE:

- `Markdown` - классический Markdown
- `MarkdownV2` - улучшенный Markdown (рекомендуется)
- `HTML` - HTML разметка
- Пустое значение - без форматирования

## Использование

### Базовое использование

```python
from send_msg_optimized import TelegramMessenger, MessageState, ParseMode

# Создание мессенджера с настройками из config.ini
messenger = TelegramMessenger()

# Отправка сообщения
await messenger.add_message("*Жирный текст* с `кодом`")
await messenger.flush()

# Или в одну команду
await messenger(message="Сообщение", action=MessageState.SEND)
```

### Переключение режимов форматирования

```python
# Получение текущего режима
current_mode = messenger.get_parse_mode()

# Установка нового режима
messenger.set_parse_mode(ParseMode.HTML)
messenger.set_parse_mode(ParseMode.MARKDOWN_V2)
messenger.set_parse_mode(ParseMode.NONE)
```

## Поддерживаемые форматы

### Markdown (классический)

```python
# Жирный текст
"*Жирный текст*"

# Курсив
"_Курсивный текст_"

# Код
"`код`"

# Блок кода
"```\nблок кода\n```"

# Ссылки
"[Текст ссылки](https://example.com)"
```

### MarkdownV2 (рекомендуется)

```python
# Жирный текст
"*Жирный текст*"

# Курсив
"_Курсивный текст_"

# Код
"`код`"

# Блок кода
"```\nблок кода\n```"

# Ссылки
"[Текст ссылки](https://example.com)"
```

**Особенности MarkdownV2:**
- Автоматическое экранирование специальных символов
- Лучшая совместимость с Telegram API
- Более стабильная работа

### HTML

```python
# Жирный текст
"<b>Жирный текст</b>"

# Курсив
"<i>Курсивный текст</i>"

# Код
"<code>код</code>"

# Блок кода
"<pre><code>блок кода</code></pre>"

# Ссылки
"<a href='https://example.com'>Текст ссылки</a>"
```

## Автоматическое форматирование

Модуль автоматически преобразует Markdown разметку в соответствующий формат:

### Преобразования

| Исходный формат | Markdown | MarkdownV2 | HTML |
|----------------|----------|------------|------|
| `*текст*` | `*текст*` | `*текст*` | `<b>текст</b>` |
| `` `код` `` | `` `код` `` | `` `код` `` | `<code>код</code>` |
| ````код```` | `<pre><code>код</code></pre>` | `<pre><code>код</code></pre>` | `<pre><code>код</code></pre>` |

### Экранирование

**MarkdownV2** автоматически экранирует специальные символы:
```
_ * [ ] ( ) ~ ` > # + - = | { } . !
```

**HTML** экранирует HTML символы:
```
& < > " '
```

## Примеры использования в проекте

### Сообщения об ошибках

```python
# file_manager.py
message = f"*File:* `{file_path}` has not been modified for more than *{hours}* hours."

# data_extractors.py
message = f"*For product:* `{barcode}` the width value `{value}` was outside the acceptable range."

# csv_processor.py
message = f"*CSV files merged completed successfully.*\n\nFiles:\n`{files_list}`"
```

### Тестирование

```python
# Запуск тестов форматирования
python test_telegram_formats.py

# Тестирование конкретного режима
messenger = TelegramMessenger()
messenger.set_parse_mode(ParseMode.MARKDOWN_V2)
await messenger.add_message("Тестовое *сообщение* с `кодом`")
await messenger.flush()
```

## Рекомендации

### Выбор режима

1. **MarkdownV2** (рекомендуется)
   - Лучшая совместимость
   - Автоматическое экранирование
   - Стабильная работа

2. **HTML**
   - Больше возможностей форматирования
   - Прямой контроль над разметкой

3. **Markdown**
   - Простота использования
   - Может иметь проблемы с экранированием

4. **None**
   - Для простых текстовых сообщений
   - Максимальная совместимость

### Лучшие практики

1. **Используйте MarkdownV2** для большинства случаев
2. **Избегайте сложного форматирования** в критических сообщениях
3. **Тестируйте форматирование** перед продакшеном
4. **Используйте простые конструкции** для лучшей совместимости

### Обработка ошибок

```python
try:
    await messenger.add_message("*Важное сообщение*")
    await messenger.flush()
except Exception as e:
    # Fallback к простому тексту
    await messenger.set_parse_mode(ParseMode.NONE)
    await messenger.add_message("Важное сообщение")
    await messenger.flush()
```

## Миграция с старой версии

### Изменения в коде

1. **Импорт:**
   ```python
   # Старый
   from send_msg import TelegramMessenger, MessageState
   
   # Новый
   from send_msg_optimized import TelegramMessenger, MessageState, ParseMode
   ```

2. **Форматирование сообщений:**
   ```python
   # Старый - может не работать в MarkdownV2
   message = "*File:*```\n{path}```"
   
   # Новый - работает во всех режимах
   message = "*File:* `{path}`"
   ```

3. **Конфигурация:**
   ```ini
   # Старый
   PARSE_MODE = Markdown
   
   # Новый (рекомендуется)
   PARSE_MODE = MarkdownV2
   ```

### Обратная совместимость

Старый код продолжает работать, но рекомендуется:
1. Обновить импорты
2. Исправить форматирование сообщений
3. Переключиться на MarkdownV2

## Заключение

Новый модуль `send_msg_optimized.py` обеспечивает:
- ✅ Поддержку всех форматов Telegram
- ✅ Автоматическое форматирование
- ✅ Обратную совместимость
- ✅ Улучшенную стабильность
- ✅ Лучшую обработку ошибок

Рекомендуется использовать **MarkdownV2** как основной режим форматирования. 