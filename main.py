import logging
import sys
from src.bot import VideoAnalyticsBot
from src.config import config

# Настройка логирования
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

def main():
    """Точка входа"""
    
    # Проверка токена
    if not config.TELEGRAM_TOKEN:
        logger.error("TELEGRAM_TOKEN не указан в .env файле")
        print("\n❌ Ошибка: TELEGRAM_TOKEN не указан")
        print("Добавьте в .env файл:")
        print("TELEGRAM_TOKEN=ваш_токен_бота")
        print("\nПолучить токен: @BotFather в Telegram")
        sys.exit(1)
    
    # Проверка БД
    print("Проверка конфигурации...")
    print(f"База данных: {config.DB_HOST}:{config.DB_PORT}/{config.DB_NAME}")
    print(f"LLM: {'включен' if config.ENABLE_LLM else 'выключен'}")
    
    logger.info("Запуск бота...")
    
    try:
        bot = VideoAnalyticsBot()
        bot.run()
    except KeyboardInterrupt:
        logger.info("Бот остановлен пользователем")
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()