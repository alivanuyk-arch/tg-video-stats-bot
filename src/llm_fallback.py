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
    
    async def ask(self, user_query: str) -> Optional[LLMResult]:
        print(f"\n{'='*60}")
        print(f"DEBUG_LLM: Получен запрос: '{user_query}'")
        prompt = self._build_prompt(user_query)
        print(f"DEBUG_LLM: Промпт (первые 300 символов): {prompt[:300]}...")
        
        try:
            response = await self._call_ollama(prompt)
            print(f"DEBUG_LLM: Ответ OLLAMA (первые 500 символов):\n{response[:500]}...")
            
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

        CRITICAL RULES:
        - NEVER nest aggregate functions (NO SUM(SUM(...)), NO COUNT(COUNT(...)))
        - Use simple aggregates: SUM(column), COUNT(*), AVG(column)
        - NO YEAR() function in PostgreSQL, use EXTRACT(YEAR FROM column)
        - Dates: '2025-11-28' format
        - NO placeholders like {{NUMBER}} or {{DATE}} - use actual values or omit
        - If no specific date/year mentioned, don't filter by date
         - Negative delta means: delta_views_count < 0
        - Positive delta means: delta_views_count > 0
        - NEVER use {{NUMBER}} placeholder
        - For negative growth: use delta_views_count < 0
        - For positive growth: use delta_views_count > 0

        TABLES:
        1. videos: id, creator_id, video_created_at, views_count, likes_count, comments_count, reports_count
        2. video_snapshots: video_id, created_at, delta_views_count, delta_likes_count, delta_comments_count, delta_reports_count

        SIMPLE EXAMPLES:
        Question: "How many videos?" → SELECT COUNT(*) FROM videos
        Question: "Total views" → SELECT SUM(views_count) FROM videos
        Question: "Total views for all creators" → SELECT SUM(views_count) FROM videos
        Question: "How many creators?" → SELECT COUNT(DISTINCT creator_id) FROM videos
        Question: "Views for creator X" → SELECT SUM(views_count) FROM videos WHERE creator_id = 'X'
        Question: "Growth of views on November 28" → SELECT SUM(delta_views_count) FROM video_snapshots WHERE DATE(created_at) = '2025-11-28'

        Question: "{query}" [/INST]
        SELECT"""
    
    async def _call_ollama(self, prompt: str) -> str:
        print(f"\nDEBUG_LLM: Вызываю {self.base_url}/api/generate")
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
                    response_text = data.get('response', '')
                    print(f"DEBUG_LLM: Статус ответа: {resp.status}")
                    print(f"DEBUG_LLM: Длина ответа: {len(response_text)} символов")
                    return response_text
                else:
                    error_text = await resp.text()
                    print(f"DEBUG_LLM: Ошибка API: {resp.status} - {error_text}")
                    raise Exception(f"API error: {resp.status} - {error_text}")
    
    def _extract_sql(self, response: str) -> Optional[str]:
        """Извлечение SQL с очисткой форматирования"""
        print(f"DEBUG_LLM _extract_sql: Начало обработки ответа LLM")
        print(f"Исходный ответ (первые 200 символов): {response[:200]}")
        
        # Убираем кодные блоки
        response = response.replace('```sql', '').replace('```', '').strip()
        
        # Убираем лишние переносы строк
        lines = []
        for line in response.split('\n'):
            line = line.strip()
            if line and not line.startswith('--'):  # Пропускаем пустые строки и комментарии
                lines.append(line)
        
        sql = ' '.join(lines)
        print(f"DEBUG_LLM _extract_sql: После очистки: '{sql}'")
        
        # Удаляем оставшиеся плейсхолдеры
        import re
        sql = re.sub(r'\{[A-Z_]+\}', '', sql)
        print(f"DEBUG_LLM _extract_sql: После удаления плейсхолдеров: '{sql}'")
        
        # Проверка на вложенные агрегаты
        if re.search(r'SUM\(.*SUM\(', sql, re.IGNORECASE) or \
           re.search(r'COUNT\(.*COUNT\(', sql, re.IGNORECASE) or \
           re.search(r'AVG\(.*AVG\(', sql, re.IGNORECASE):
            print(f"DEBUG_LLM _extract_sql: Ошибка: вложенные агрегатные функции")
            return None
        
        # Если SQL не начинается с SELECT, пробуем найти его
        if 'SELECT' in sql.upper() and 'FROM' in sql.upper():
            # Нормализуем пробелы
            sql = ' '.join(sql.split())
            print(f"DEBUG_LLM _extract_sql: Найден SELECT, возвращаем: '{sql}'")
            return sql
        elif sql.upper().startswith('COUNT') or 'FROM' in sql.upper():
            # Если ответ вроде "COUNT(*) FROM videos" - добавляем SELECT
            result = f"SELECT {sql}"
            print(f"DEBUG_LLM _extract_sql: Добавлен SELECT: '{result}'")
            return result
        
        print(f"DEBUG_LLM _extract_sql: Не удалось извлечь SQL")
        return None
    
    def _validate_sql(self, sql: str) -> bool:
        """Проверка безопасности SQL"""
        print(f"\nDEBUG_VALIDATE: Начало проверки безопасности")
        print(f"SQL для проверки: '{sql}'")
        
        # Приводим к верхнему регистру для проверки
        sql_upper = sql.upper()
        
        # Список опасных команд
        dangerous = ["DROP", "DELETE", "UPDATE", "INSERT", 
                    "ALTER", "TRUNCATE",  "--", "/*"]
        
        # Проверяем наличие опасных команд
        for word in dangerous:
            if word in sql_upper:
                print(f"DEBUG_VALIDATE: Обнаружена опасная команда '{word}' в SQL")
                return False
        
        # Проверяем, что это SELECT-запрос
        has_select = "SELECT" in sql_upper
        has_from = "FROM" in sql_upper
        
        print(f"DEBUG_VALIDATE: SELECT: {has_select}, FROM: {has_from}")
        
        if not has_select or not has_from:
            print(f"DEBUG_VALIDATE: Нет SELECT или FROM")
            return False
        
        # Дополнительная проверка: не должно быть подозрительных конструкций
        suspicious = ["INFORMATION_SCHEMA", "PG_", "SYSTEM", "EXEC", "XP_"]
        for word in suspicious:
            if word in sql_upper:
                print(f"DEBUG_VALIDATE: Подозрительное слово '{word}'")
                return False
        
        # Проверка на вложенные агрегаты
        import re
        if re.search(r'SUM\(.*SUM\(', sql_upper) or \
           re.search(r'COUNT\(.*COUNT\(', sql_upper) or \
           re.search(r'AVG\(.*AVG\(', sql_upper):
            print(f"DEBUG_VALIDATE: Ошибка: вложенные агрегатные функции")
            return False
        
        print(f"DEBUG_VALIDATE: SQL прошел проверку безопасности")
        return True