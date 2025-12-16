import aiohttp
import re
import logging
from typing import Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class LLMResult:
    sql: str
    confidence: float
    is_safe: bool

class LLMTeacher:
    """OLLAMA клиент для конструктора"""
    
    def __init__(self, model: str = "llama3.2:3b", base_url: str = "http://localhost:11434"):
        self.model = model
        self.base_url = base_url
        self.schema_info = self._get_schema_info()
    
    def _get_schema_info(self) -> str:
        return """Ты SQL генератор для видео-аналитики. ВОПРОС → SQL.

    ТАБЛИЦЫ:
    1. videos - финальная статистика по видео:
    - id, creator_id, video_created_at
    - views_count, likes_count, comments_count, reports_count

    2. video_snapshots - почасовые замеры (для прироста):
    - video_id, created_at
    - views_count, likes_count, comments_count, reports_count (текущие)
    - delta_views_count, delta_likes_count, delta_comments_count, delta_reports_count (прирост)

    ВАЖНО:
    - "сколько всего X" → COUNT(*)
    - "сколько всего КРЕАТОРОВ" → COUNT(DISTINCT creator_id)
    - "сумма X за дату" → SUM(delta_X_count) FROM video_snapshots WHERE DATE(created_at) = 'дата'
    - "видео с X более N" → COUNT(*) FROM videos WHERE X_count > N
    - Даты в формате 'YYYY-MM-DD'

    Примеры:
    В: "Сколько всего видео?" → SELECT COUNT(*) FROM videos
    В: "Сколько всего креаторов?" → SELECT COUNT(DISTINCT creator_id) FROM videos
    В: "Сумма просмотров за 29 ноября 2025" → SELECT SUM(delta_views_count) FROM video_snapshots WHERE DATE(created_at) = '2025-11-29'
    В: "Видео с лайками более 5000" → SELECT COUNT(*) FROM videos WHERE likes_count > 5000
    В: "На сколько комментариев выросли видео 28 ноября?" → SELECT SUM(delta_comments_count) FROM video_snapshots WHERE DATE(created_at) = '2025-11-28'

    Теперь ответь:"""
    
    async def ask(self, user_query: str) -> Optional[LLMResult]:
        print(f"DEBUG_LLM: Получен запрос: {user_query}")
        prompt = self._build_prompt(user_query)
        print(f"DEBUG_LLM: Промпт (первые 300 символов): {prompt[:300]}...")
        
        try:
            response = await self._call_ollama(prompt)
            print(f"DEBUG_LLM: Ответ OLLAMA (полный): {response}")
            
            sql = self._extract_sql(response)
            print(f"DEBUG_LLM: Извлеченный SQL: '{sql}'")
            
            if not sql:
                print("DEBUG_LLM: _extract_sql вернул None!")
                return None
            
            is_safe = self._validate_sql(sql)
            print(f"DEBUG_LLM: SQL безопасен: {is_safe}")
            
            if not is_safe:
                print("DEBUG_LLM: SQL не прошел валидацию безопасности")
                return None
            
            return LLMResult(
                sql=sql,
                confidence=0.8,
                is_safe=is_safe
            )
            
        except Exception as e:
            print(f"DEBUG_LLM: Исключение в ask: {type(e).__name__}: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def _build_prompt(self, query: str) -> str:
        return f"""<s>[INST] You are an SQL generator. Return ONLY SQL code.

    TABLES:
    1. videos - video data: id, creator_id, video_created_at, views_count, likes_count, comments_count, reports_count
    2. video_snapshots - metric changes: video_id, created_at, delta_views_count, delta_likes_count, delta_comments_count, delta_reports_count

    RULES:
    - Use videos table for video counts and creator queries
    - Use video_snapshots for metric growth (delta_ fields)
    - Dates must be in 2025 year format: '2025-11-29', '2025-11-28', etc.
    - NO explanations, ONLY SQL

    EXAMPLES:
    Question: "How many videos?" → SELECT COUNT(*) FROM videos
    Question: "Videos with likes > 5000" → SELECT COUNT(*) FROM videos WHERE likes_count > 5000
    Question: "Sum of comments for November 29" → SELECT SUM(delta_comments_count) FROM video_snapshots WHERE DATE(created_at) = '2025-11-29'
    Question: "Videos from creator X between dates" → SELECT COUNT(*) FROM videos WHERE creator_id = 'X' AND DATE(video_created_at) BETWEEN '2025-11-01' AND '2025-11-28'

    IMPORTANT: All dates should be in 2025!

    Question: "{query}" [/INST]
    SELECT"""
       
    
    async def _call_ollama(self, prompt: str) -> str:
        print(f"DEBUG: вызываю {self.base_url}/api/generate")
        """Вызов OLLAMA"""
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{self.base_url}/api/generate",
                json={
                    "model": self.model,
                    "prompt": prompt,
                    "stream": False,
                    "options": {"temperature": 0.1},
                    "num_predict": 500
                },
                timeout=30
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get('response', '')
                else:
                    raise Exception(f"API error: {resp.status}")
    
    def _extract_sql(self, response: str) -> Optional[str]:
        """Извлечение SQL с очисткой форматирования"""
        # Убираем кодные блоки
        response = response.replace('```sql', '').replace('```', '').strip()
        
        # Убираем лишние переносы строк
        lines = []
        for line in response.split('\n'):
            line = line.strip()
            if line and not line.startswith('--'):  # Пропускаем пустые строки и комментарии
                lines.append(line)
        
        sql = ' '.join(lines)
        
        # Если SQL не начинается с SELECT, пробуем найти его
        if 'SELECT' in sql.upper() and 'FROM' in sql.upper():
            # Нормализуем пробелы
            sql = ' '.join(sql.split())
            return sql
        elif sql.upper().startswith('COUNT') or 'FROM' in sql.upper():
            # Если ответ вроде "COUNT(*) FROM videos" - добавляем SELECT
            return f"SELECT {sql}"
        
        return None
    
    def _validate_sql(self, sql: str) -> bool:
        """Проверка безопасности SQL"""
        # Приводим к верхнему регистру для проверки
        sql_upper = sql.upper()
        
        # Список опасных команд
        dangerous = ["DROP", "DELETE", "UPDATE", "INSERT", 
                    "ALTER", "TRUNCATE",  "--", "/*"]
        
        # Проверяем наличие опасных команд
        for word in dangerous:
            if word in sql_upper:
                print(f"DEBUG_VALIDATE: Обнаружена опасная команда '{word}' в SQL: {sql}")
                return False
        
        # Проверяем, что это SELECT-запрос
        # Ищем 'SELECT' и 'FROM' в любом регистре и в любой позиции
        has_select = "SELECT" in sql_upper
        has_from = "FROM" in sql_upper
        
        if not has_select or not has_from:
            print(f"DEBUG_VALIDATE: Нет SELECT или FROM. SELECT: {has_select}, FROM: {has_from}, SQL: {sql}")
            return False
        
        # Дополнительная проверка: не должно быть подозрительных конструкций
        suspicious = ["INFORMATION_SCHEMA", "PG_", "SYSTEM", "EXEC", "XP_"]
        for word in suspicious:
            if word in sql_upper:
                print(f"DEBUG_VALIDATE: Подозрительное слово '{word}' в SQL: {sql}")
                return False
        
        print(f"DEBUG_VALIDATE: SQL прошел проверку: {sql}")
        return True