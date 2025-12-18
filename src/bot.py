# src/bot.py
import asyncio
import logging
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes

from .config import config
from .database import DatabaseManager
from .query_constructor import QueryConstructor
from .llm_fallback import LLMTeacher

logger = logging.getLogger(__name__)

class VideoAnalyticsBot:
    """Telegram бот для аналитики видео"""
    
    def __init__(self):
        self.db = DatabaseManager(config.DATABASE_URL)
        self.query_constructor = self._create_constructor()
        self.application = None
    
    def _create_constructor(self):
        """Создаёт конструктор с LLM если нужно"""
        llm_client = None
        
        if config.ENABLE_LLM:
            try:
                from .llm_fallback import LLMTeacher
                llm_client = LLMTeacher(
                    model=config.OLLAMA_MODEL,
                    base_url=config.OLLAMA_BASE_URL
                )
                logger.info(f"LLM инициализирован: {config.OLLAMA_MODEL}")
            except Exception as e:
                logger.warning(f"Не удалось инициализировать LLM: {e}")
        
        return QueryConstructor(llm_client=llm_client)
    
    def setup_handlers(self):
        """Настройка обработчиков"""
        self.application.add_handler(CommandHandler("start", self._start_handler))
        self.application.add_handler(CommandHandler("learn", self._learn_handler))
        self.application.add_handler(CommandHandler("stats", self._stats_handler))
        self.application.add_handler(CommandHandler("clear", self._clear_handler))
        self.application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self._message_handler))
    
    async def _start_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик /start"""
        await update.message.reply_text(
            "🤖 Бот для аналитики видео\n\n"
            "Задавайте вопросы на естественном языке.\n"
            "Примеры:\n"
            "• Сколько всего видео?\n"
            "• Видео с просмотрами > 100000\n"
            "• Прирост просмотров 28 ноября 2025\n"
            "• Сумма лайков всех видео\n\n"
            "Команды:\n"
            "/learn вопрос | SQL - добавить пример\n"
            "/stats - статистика конструктора\n"
            "/clear - очистить кэш"
        )
    
    async def _learn_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик /learn - ручное обучение"""
        try:
            # /learn Сколько видео? | SELECT COUNT(*) FROM videos
            text = update.message.text.replace('/learn', '').strip()
            parts = [p.strip() for p in text.split('|', 1)]
            
            if len(parts) != 2:
                await update.message.reply("Формат: /learn вопрос | SQL")
                return
            
            question = parts[0].strip()
            sql = parts[1].strip()
            
            self.query_constructor.add_manual_pattern(question, sql)
            
            await update.message.reply_text(
                f"✅ Выучено:\n"
                f"Вопрос: {question}\n"
                f"SQL: {sql}"
            )
            
        except Exception as e:
            await update.message.reply_text(f"❌ Ошибка: {str(e)}")
    
    async def _stats_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик /stats - статистика"""
        stats = self.query_constructor.get_stats()
        
        response = (
            "📊 Статистика конструктора:\n"
            f"• Всего паттернов: {stats.total_patterns}\n"
            f"• Точных совпадений: {stats.exact_hits}\n"
            f"• Совпадений по паттернам: {stats.pattern_hits}\n"
            f"• Вызовов LLM: {stats.llm_calls}"
        )
        
        await update.message.reply_text(response)
    
    async def _clear_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик /clear - очистка кэша"""
        self.query_constructor.clear_cache()
        await update.message.reply_text("✅ Кэш очищен")
    
    async def _message_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик текстовых сообщений"""
        user_query = update.message.text.strip()
        user_id = update.effective_user.id
        
        logger.info(f"Запрос от {user_id}: {user_query}")
        
        await update.message.reply_chat_action(action="typing")
        
        try:
            # Строим SQL через асинхронный метод
            sql = await self.query_constructor.build_sql_async(
                user_query, 
                use_llm=config.ENABLE_LLM
            )
            
            # Выполняем запрос
            await self.db.connect()
            result = await self.db.execute_scalar(sql)
            
            # Ответ ТОЛЬКО числом
            answer = str(result) if result is not None else "0"
            await update.message.reply_text(answer)
            
            logger.info(f"Ответ пользователю {user_id}: {answer}")
            
        except Exception as e:
            logger.error(f"Ошибка обработки: {e}", exc_info=True)
            await update.message.reply_text("Ошибка обработки запроса")
    
    async def run_async(self):
        """Асинхронный запуск"""
        if not config.TELEGRAM_TOKEN:
            raise ValueError("TELEGRAM_TOKEN не указан в .env")
        
        self.application = Application.builder().token(config.TELEGRAM_TOKEN).build()
        self.setup_handlers()
        
        logger.info("Бот запускается...")
        
        # Проверка БД
        await self.db.connect()
        if await self.db.check_connection():
            logger.info("Соединение с БД установлено")
        else:
            logger.error("Не удалось подключиться к БД")
        
        # Запуск
        await self.application.initialize()
        await self.application.start()
        await self.application.updater.start_polling()
        
        # Бесконечное ожидание
        try:
            await asyncio.Future()
        except (KeyboardInterrupt, SystemExit):
            logger.info("Бот останавливается...")
        finally:
            await self.application.stop()
            await self.db.disconnect()
    
    def run(self):
        """Синхронный запуск"""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            loop.run_until_complete(self.run_async())
        except KeyboardInterrupt:
            pass
        finally:
            loop.close()