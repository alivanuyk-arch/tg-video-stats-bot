import sys
import os

# Добавляем текущую директорию в путь для импорта src
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import sys
import os

# Добавляем src в путь Python
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

import asyncio
import logging

# Теперь импортируем из src (после добавления в путь)
from bot import SimpleBot

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Запуск бота"""
    try:
        bot = SimpleBot()
        bot.run()
    except KeyboardInterrupt:
        logger.info("Бот остановлен")
    except Exception as e:
        logger.error(f"Ошибка запуска бота: {e}")
        raise

if __name__ == "__main__":
    main()