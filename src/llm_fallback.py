"""
LLM интеграция для конструктора
"""

import aiohttp
import re
import logging
from typing import Optional, List, Tuple
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class LLMResult:
    """Результат работы LLM"""
    sql: str
    confidence: float
    extracted_keywords: List[str]  # Изменено!
    is_safe: bool

class LLMTeacher:
    """OLLAMA клиент для конструктора"""
    
    def __init__(self, model: str = "llama3.2:3b", base_url: str = "http://localhost:11434"):
        self.model = model
        self.base_url = base_url
        self.schema_info = self._get_schema_info()
    
    def _get_schema_info(self) -> str:
        """Описание схемы"""
        return """
        База данных видео-аналитики:
        
        ТАБЛИЦА videos:
        - id (UUID), creator_id (VARCHAR), video_created_at (TIMESTAMPTZ)
        - views_count, likes_count, comments_count, reports_count (INTEGER)
        
        ТАБЛИЦА video_snapshots:
        - id, video_id, created_at (TIMESTAMPTZ)
        - views_count, likes_count, comments_count, reports_count
        - delta_views_count, delta_likes_count, delta_comments_count, delta_reports_count
        
        ПРАВИЛА:
        1. COUNT(*) для подсчёта
        2. SUM() для суммирования  
        3. WHERE для фильтров
        4. DATE() для дат
        5. Только SELECT запросы
        """
    
    async def ask(self, user_query: str) -> Optional[LLMResult]:
        """Запрос к LLM"""
        prompt = self._build_prompt(user_query)
        
        try:
            response = await self._call_ollama(prompt)
            sql = self._extract_sql(response)
            
            if not sql:
                return None
            
            is_safe = self._validate_sql(sql)
            if not is_safe:
                return None
            
            # Извлекаем ключевые слова для обучения конструктора
            keywords = self._extract_keywords(user_query)
            
            return LLMResult(
                sql=sql,
                confidence=0.8,
                extracted_keywords=keywords,  # Теперь ключевые слова!
                is_safe=is_safe
            )
            
        except Exception as e:
            logger.error(f"LLM error: {e}")
            return None
    
    def _build_prompt(self, query: str) -> str:
        """Строим промпт"""
        return f"""{self.schema_info}

Запрос: "{query}"
SQL запрос:"""
    
    async def _call_ollama(self, prompt: str) -> str:
        """Вызов OLLAMA"""
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{self.base_url}/api/generate",
                json={
                    "model": self.model,
                    "prompt": prompt,
                    "stream": False,
                    "options": {"temperature": 0.1}
                },
                timeout=30
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get('response', '')
                else:
                    raise Exception(f"API error: {resp.status}")
    
    def _extract_sql(self, response: str) -> Optional[str]:
        """Извлечение SQL"""
        response = response.replace('```sql', '').replace('```', '').strip()
        
        lines = []
        in_sql = False
        
        for line in response.split('\n'):
            line = line.strip()
            if line.upper().startswith('SELECT'):
                in_sql = True
            if in_sql:
                lines.append(line)
            if line.endswith(';') and in_sql:
                break
        
        sql = ' '.join(lines)
        if sql and 'SELECT' in sql.upper() and 'FROM' in sql.upper():
            return sql
        return None
    
    def _validate_sql(self, sql: str) -> bool:
        """Проверка безопасности"""
        dangerous = ["DROP", "DELETE", "UPDATE", "INSERT", 
                    "ALTER", "TRUNCATE", "CREATE", "--", "/*"]
        
        sql_upper = sql.upper()
        for word in dangerous:
            if word in sql_upper:
                return False
        
        return "SELECT" in sql_upper and "FROM" in sql_upper
    
    def _extract_keywords(self, query: str) -> List[str]:
        """
        Извлекаем ключевые слова из запроса
        для обучения конструктора
        """
        query_lower = query.lower()
        keywords = []
        
        # Слова, которые интересны конструктору
        interesting_words = [
            'сколько', 'сумма', 'среднее', 'максимум', 'минимум',
            'видео', 'просмотр', 'лайк', 'комментарий', 'жалоба',
            'креатор', 'больше', 'меньше', 'ноябрь', 'декабрь',
            '2025', 'дата', 'всего', 'разных', 'новые'
        ]
        
        for word in interesting_words:
            if word in query_lower:
                keywords.append(word)
        
        # Также добавляем числа и даты
        if re.search(r'\b\d+\b', query_lower):
            keywords.append('[HAS_NUMBER]')
        
        if re.search(r'\d{4}-\d{2}-\d{2}', query_lower):
            keywords.append('[HAS_DATE]')
        
        return keywords
