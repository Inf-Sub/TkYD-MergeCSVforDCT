# Быстрый запуск

## 1. Настройка (5 минут)

```bash
# Клонирование
git clone <repository-url>
cd TkYD-MergeCSVforDCT

# Настройка конфигурации
cp example_config.ini config.ini
# Отредактируйте config.ini - укажите токен Telegram и пути к файлам
```

## 2. Запуск

```bash
python run.py
```

Готово! Система автоматически:
- Создаст виртуальное окружение
- Установит зависимости  
- Запустит обработку CSV файлов
- Отправит уведомления в Telegram

## 3. Что настроить в config.ini

```ini
[TELEGRAM]
telegram_token = 1234567890:ABCdefGHIjklMNOpqrsTUVwxyz
telegram_chat_id = -1001234567890

[CSV]
PATH_DIRECTORY = C:\Your\CSV\Files\
FILE_PATTERN = ^(MSK-[A-Za-z0-9]+)-Nomenclature\.csv$
```

## 4. Проверка работы

```bash
# Тест форматирования сообщений
python test_telegram_formats.py

# Просмотр логов
tail -f Logs/2025/2025.06/merge_log_2025.06.26.log
```

## Проблемы?

- **Ошибка токена**: Проверьте правильность токена бота
- **Файлы не найдены**: Проверьте PATH_DIRECTORY в config.ini
- **Git ошибка**: Установите Git или отключите GIT_PULL_ENABLED

Подробности в [README.md](README.md) 