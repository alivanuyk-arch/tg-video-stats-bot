import os
from pathlib import Path
from dotenv import load_dotenv

# Ищем .env в правильных местах
env_paths = [
    Path(__file__).parent.parent / '.env',          # tg-video-stats-bot/.env
    Path(__file__).parent.parent.parent / '.env',   # родительская директория
    Path('.env'),                                    # текущая директория
]

for env_path in env_paths:
    if env_path.exists():
        load_dotenv(env_path)
        print(f"Загружен .env из: {env_path}")
        break
else:
    print("⚠️  Файл .env не найден. Используются значения по умолчанию.")

class Config:
    # Database
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = os.getenv("DB_PORT", "5432")
    DB_NAME = os.getenv("DB_NAME", "video_analytics")
    DB_USER = os.getenv("DB_USER", "postgres")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")
    
    # Telegram
    TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
    
    # LLM Settings
    ENABLE_LLM = os.getenv("ENABLE_LLM", "false").lower() == "true"
    OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.2:3b")
    OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
    
    # App
    DEBUG = os.getenv("DEBUG", "false").lower() == "true"
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    
    # Пути
    PROJECT_ROOT = Path(__file__).parent.parent
    DATA_DIR = PROJECT_ROOT / "data"
    JSON_FILE = DATA_DIR / "videos.json"
    
    # Файлы конструктора
    CACHE_FILE = PROJECT_ROOT / "query_cache.json"
    PATTERNS_FILE = PROJECT_ROOT / "learned_patterns.json"
    
    @property
    def DATABASE_URL(self):
        """Генерируем URL из параметров"""
        return f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"

config = Config()

# Проверка обязательных параметров
if not config.TELEGRAM_TOKEN:
    print("⚠️  ВНИМАНИЕ: TELEGRAM_TOKEN не установлен. Бот не запустится.")