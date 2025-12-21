from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
import asyncio
import logging
import sys
import os

# Импорты из ТЕКУЩЕЙ директории src/

print("💡 Попробуй абсолютные импорты...")
# Пробуем абсолютные импорты
from config import config
from database import DatabaseManager
from llm_fallback import LLMTeacher

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SimpleBot:
    """Простой бот только с LLM"""
    
    def __init__(self):
        self.db = DatabaseManager(config.DATABASE_URL)
        self.llm = self._create_llm()
        self.application = None
    
    def _create_llm(self):
        """Создаёт LLM клиент"""
        try:
            llm = LLMTeacher(
                model=config.OLLAMA_MODEL,
                base_url=config.OLLAMA_BASE_URL
            )
            logger.info(f"✅ LLM инициализирован: {config.OLLAMA_MODEL}")
            return llm
        except Exception as e:
            logger.error(f"❌ КРИТИЧЕСКО: Не удалось инициализировать LLM: {e}")
            raise
    
    def setup_handlers(self):
        """Настройка обработчиков"""
        self.application.add_handler(CommandHandler("start", self._start_handler))
        self.application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self._message_handler))
    
    async def _start_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик /start"""
        await update.message.reply_text(
            "🤖 Бот для аналитики видео (LLM режим)\n\n"
            "Задавайте вопросы на естественном языке.\n"
            "Примеры:\n"
            "• Сколько всего видео?\n"
            "• Видео с просмотрами > 100000\n"
            "• Прирост просмотров 28 ноября 2025\n"
            "• Сумма лайков всех видео"
        )
    
    async def _message_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик текстовых сообщений"""
        user_query = update.message.text.strip()
        user_id = update.effective_user.id
        
        logger.info(f"Запрос от {user_id}: {user_query}")
        
        await update.message.reply_chat_action(action="typing")
        
        try:
            # Запрашиваем SQL у LLM
            result = await self.llm.ask(user_query)
            
            if not result or not result.sql:
                await update.message.reply_text("❌ LLM не смог сгенерировать SQL")
                return
            
            logger.info(f"Сгенерирован SQL: {result.sql}")
            
            # Выполняем SQL
            await self.db.connect()
            db_result = await self.db.execute_scalar(result.sql)
            
            # Ответ только числом
            answer = str(db_result) if db_result is not None else "0"
            await update.message.reply_text(answer)
            
            logger.info(f"Ответ: {answer}")
            
        except Exception as e:
            logger.error(f"Ошибка обработки запроса: {e}", exc_info=True)
            await update.message.reply_text(f"❌ Ошибка: {str(e)[:100]}")
    
    async def run_async(self):
        """Асинхронный запуск"""
        if not config.TELEGRAM_TOKEN:
            raise ValueError("❌ TELEGRAM_TOKEN не указан в конфиге")
        
        self.application = Application.builder().token(config.TELEGRAM_TOKEN).build()
        self.setup_handlers()
        
        logger.info("🚀 Бот запускается (LLM режим)...")
        
        # Подключаем БД
        await self.db.connect()
        
        # Запуск
        await self.application.initialize()
        await self.application.start()
        await self.application.updater.start_polling()
        
        logger.info("✅ Бот запущен и слушает сообщения...")
        
        # Ожидание
        try:
            await asyncio.Future()
        except (KeyboardInterrupt, SystemExit):
            logger.info("⏹️  Бот останавливается...")
        finally:
            if self.application:
                await self.application.stop()
            await self.db.disconnect()
    
    def run(self):
        """Синхронный запуск"""
        try:
            asyncio.run(self.run_async())
        except KeyboardInterrupt:
            logger.info("⏹️  Бот остановлен")
        except Exception as e:
            logger.error(f"❌ Фатальная ошибка: {e}")
            raise

def main():
    """Запуск бота"""
    bot = SimpleBot()
    bot.run()

if __name__ == "__main__":
    main()