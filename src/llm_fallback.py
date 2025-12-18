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
        return f"""<s>[INST] You are an SQL generator for PostgreSQL. Return ONLY PostgreSQL SQL code.

    CRITICAL POSTGRESQL RULES:
    1. Always use EXTRACT(YEAR FROM column) NOT YEAR(column)
    2. Always use EXTRACT(MONTH FROM column) NOT MONTH(column)
    3. Always use EXTRACT(DAY FROM column) NOT DAY(column)
    4. For current date: use CURRENT_DATE NOT NOW() for dates
    5. Use SIMPLE queries only, no complex subqueries
    6. For negative deltas: WHERE delta_views_count < 0 (no extra conditions)
    7. Never use MySQL functions like YEAR(), MONTH(), DAY(), DATE_FORMAT()
    8. Use DATE(column) for date conversion
    9. For time intervals (hours): EXTRACT(HOUR FROM column) BETWEEN start_hour AND end_hour
    10. For specific date: DATE(column) = 'YYYY-MM-DD'
    11. For joining videos and video_snapshots: JOIN videos ON video_snapshots.video_id = videos.id
    12. For total growth (positive+negative): SUM(delta_views_count) WITHOUT delta_views_count < 0 condition
    13. For negative growth only: WHERE delta_views_count < 0

    TABLES:
    - videos: id, creator_id, video_created_at, views_count, likes_count, comments_count, reports_count
    - video_snapshots: id, video_id, created_at, delta_views_count, delta_likes_count, delta_comments_count, delta_reports_count

    EXAMPLES:
    Question: "Отрицательные дельты просмотров" → SELECT COUNT(*) FROM video_snapshots WHERE delta_views_count < 0
    Question: "Сколько замеров с отрицательным приростом" → SELECT COUNT(*) FROM video_snapshots WHERE delta_views_count < 0
    Question: "Сколько видео" → SELECT COUNT(*) FROM videos
    Question: "Сумма просмотров" → SELECT SUM(views_count) FROM videos
    Question: "Видео за ноябрь 2025" → SELECT COUNT(*) FROM videos WHERE EXTRACT(YEAR FROM video_created_at) = 2025 AND EXTRACT(MONTH FROM video_created_at) = 11
    Question: "Суммарный прирост просмотров креатора X с 10:00 до 15:00 28 ноября 2025" → 
    SELECT SUM(vs.delta_views_count) FROM video_snapshots vs JOIN videos v ON vs.video_id = v.id  WHERE v.creator_id = 'X' AND DATE(vs.created_at) = '2025-11-28' 
    AND EXTRACT(HOUR FROM vs.created_at) BETWEEN 10 AND 14

    IMPORTANT:
      For "negative delta" questions, use ONLY: SELECT COUNT(*) FROM video_snapshots WHERE delta_views_count < 0
    - For total growth/sum: use SUM(delta_views_count) WITHOUT "delta_views_count < 0" condition
    - For negative growth only: add WHERE delta_views_count < 0
    - Never use two WHERE clauses, use AND for additional conditions
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
        """Извлечение SQL с очисткой форматирования и исправлением для PostgreSQL"""
        print(f"DEBUG_LLM _extract_sql: Начало обработки ответа LLM")
        print(f"Исходный ответ (первые 200 символов): {response[:200]}")
        
        # Убираем кодные блоки
        response = response.replace('```sql', '').replace('```', '').strip()
        
        # ВАЖНО: Обрезаем все после первой точки с запятой
        if ';' in response:
            response = response.split(';')[0] + ';'
            print(f"DEBUG_LLM _extract_sql: Обрезано после ';'")
        
        # Убираем лишние переносы строк, но сохраняем структуру
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
        
        # ВАЖНОЕ ИСПРАВЛЕНИЕ 1: Исправляем MySQL функции на PostgreSQL
        # YEAR(date) -> EXTRACT(YEAR FROM date)
        # MONTH(date) -> EXTRACT(MONTH FROM date)
        # DAY(date) -> EXTRACT(DAY FROM date)
        
        # Заменяем YEAR(date) на EXTRACT(YEAR FROM date)
        while 'YEAR(' in sql.upper():
            match = re.search(r'(?i)YEAR\(([^)]+)\)', sql)
            if match:
                inner = match.group(1)
                sql = sql.replace(match.group(0), f'EXTRACT(YEAR FROM {inner})')
        
        # Заменяем MONTH(date) на EXTRACT(MONTH FROM date)
        while 'MONTH(' in sql.upper():
            match = re.search(r'(?i)MONTH\(([^)]+)\)', sql)
            if match:
                inner = match.group(1)
                sql = sql.replace(match.group(0), f'EXTRACT(MONTH FROM {inner})')
        
        # Заменяем DAY(date) на EXTRACT(DAY FROM date)
        while 'DAY(' in sql.upper():
            match = re.search(r'(?i)DAY\(([^)]+)\)', sql)
            if match:
                inner = match.group(1)
                sql = sql.replace(match.group(0), f'EXTRACT(DAY FROM {inner})')
        
        # Заменяем NOW() на CURRENT_TIMESTAMP для дат
        sql = re.sub(r'\bNOW\(\)', 'CURRENT_TIMESTAMP', sql, flags=re.IGNORECASE)
        
        print(f"DEBUG_LLM _extract_sql: После исправления MySQL функций: '{sql}'")
        
        # ВАЖНОЕ ИСПРАВЛЕНИЕ 2: Исправляем двойные WHERE на WHERE...AND
        # Считаем количество WHERE (case-insensitive)
        where_count = len(re.findall(r'\bWHERE\b', sql, re.IGNORECASE))
        if where_count > 1:
            print(f"DEBUG_LLM _extract_sql: Найдено {where_count} WHERE, исправляем...")
            # Находим все позиции WHERE
            where_positions = []
            for match in re.finditer(r'\bWHERE\b', sql, re.IGNORECASE):
                where_positions.append(match.start())
            
            # Оставляем первое WHERE, остальные заменяем на AND
            # Идем с конца чтобы позиции не сдвигались
            for i in range(len(where_positions)-1, 0, -1):
                pos = where_positions[i]
                sql = sql[:pos] + 'AND' + sql[pos+5:]  # WHERE = 5 символов
            
            print(f"DEBUG_LLM _extract_sql: Исправлено {where_count-1} WHERE на AND")
        
        # ВАЖНОЕ ИСПРАВЛЕНИЕ 3: Удаляем лишнее условие delta_views_count < 0 для запросов про суммарный прирост
        lower_response = response.lower()
        if ('суммарн' in lower_response or 'выросл' in lower_response or 
            'прирост' in lower_response or 'на сколько' in lower_response) \
            and ('delta_views_count < 0' in sql or 'delta_views_count<0' in sql):
            
            print(f"DEBUG_LLM _extract_sql: Запрос про суммарный прирост, удаляю условие delta_views_count < 0")
            
            # Удаляем условие delta_views_count < 0 разными способами
            patterns = [
                r'\s+AND\s+[a-zA-Z_\.]*delta_views_count\s*<\s*0',
                r'\s+AND\s+[a-zA-Z_\.]*delta_views_count\s*<=\s*0',
                r'\s+WHERE\s+[a-zA-Z_\.]*delta_views_count\s*<\s*0',
                r',\s*[a-zA-Z_\.]*delta_views_count\s*<\s*0',
                r'[a-zA-Z_\.]*delta_views_count\s*<\s*0\s+AND',
                r'[a-zA-Z_\.]*delta_views_count\s*<\s*0\s+WHERE',
            ]
            
            for pattern in patterns:
                sql_old = sql
                sql = re.sub(pattern, '', sql, flags=re.IGNORECASE)
                if sql != sql_old:
                    print(f"DEBUG_LLM _extract_sql: Удалено условие по паттерну: {pattern}")
                    break
            
            # Если осталось "AND AND" - исправляем
            sql = re.sub(r'AND\s+AND', 'AND', sql)
            sql = re.sub(r'WHERE\s+AND', 'WHERE', sql)
            sql = re.sub(r'\s+AND\s*$', '', sql)  # Удаляем AND в конце
            sql = re.sub(r'\s+WHERE\s*$', '', sql)  # Удаляем WHERE в конце
        
        # ВАЖНОЕ ИСПРАВЛЕНИЕ 4: Упрощаем сложные запросы про отрицательные дельты
        # Если запрос про отрицательные дельты и содержит сложную логику - упрощаем
        if ('отрицательн' in lower_response or 'меньше' in lower_response) and \
        ('IN (' in sql or 'EXISTS' in sql or 'ROW_NUMBER' in sql or 'PARTITION' in sql or 'JOIN LATERAL' in sql):
            print(f"DEBUG_LLM _extract_sql: Обнаружен сложный запрос про отрицательные дельты, упрощаем...")
            # Просто считаем все отрицательные дельты
            sql = 'SELECT COUNT(*) FROM video_snapshots WHERE delta_views_count < 0;'
            print(f"DEBUG_LLM _extract_sql: Упрощен до: '{sql}'")
        
        # ВАЖНОЕ ИСПРАВЛЕНИЕ 5: Если SQL не начинается с SELECT - добавляем
        sql = sql.strip()
        if not sql.upper().startswith('SELECT'):
            if sql.upper().startswith('COUNT') or sql.upper().startswith('SUM') or sql.upper().startswith('AVG') or 'FROM' in sql.upper():
                sql = f"SELECT {sql}"
                print(f"DEBUG_LLM _extract_sql: Добавлен SELECT: '{sql}'")
        
        # ВАЖНОЕ ИСПРАВЛЕНИЕ 6: Убираем лишние пробелы и переносы
        sql = ' '.join(sql.split())  # Заменяем все пробелы/переносы на одинарные пробелы
        
        # Проверка на вложенные агрегаты
        if re.search(r'SUM\(.*SUM\(', sql, re.IGNORECASE) or \
        re.search(r'COUNT\(.*COUNT\(', sql, re.IGNORECASE) or \
        re.search(r'AVG\(.*AVG\(', sql, re.IGNORECASE):
            print(f"DEBUG_LLM _extract_sql: Ошибка: вложенные агрегатные функции")
            return None
        
        # Проверяем что это похоже на SQL запрос
        if 'FROM' in sql.upper() and ('SELECT' in sql.upper() or 'COUNT' in sql.upper() or 'SUM' in sql.upper() or 'AVG' in sql.upper()):
            # Убедимся что есть точка с запятой в конце
            if not sql.endswith(';'):
                sql = sql + ';'
            print(f"DEBUG_LLM _extract_sql: Итоговый SQL: '{sql}'")
            return sql
        
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