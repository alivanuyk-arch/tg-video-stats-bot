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
    source: str  # "prompt" или "rules"

class LLMTeacher:
    
    def __init__(self, model: str = "mistral:7b", base_url: str = "http://localhost:11434"):
        self.model = model
        self.base_url = base_url
        self._init_month_map()
        
    def _init_month_map(self):
        """Словарь русских месяцев"""
        self.month_map = {
            'января': 1, 'февраля': 2, 'марта': 3, 'апреля': 4,
            'мая': 5, 'июня': 6, 'июля': 7, 'августа': 8,
            'сентября': 9, 'октября': 10, 'ноября': 11, 'декабря': 12,
        }
    
    def _preprocess_query(self, user_query: str) -> str:
        """Минимальный препроцессинг - только убираем 'то есть'"""
        # Убираем "то есть" и аналогичные фразы
        stop_phrases = ['то есть', 'другими словами', 'иными словами']
        
        clean_query = user_query
        for phrase in stop_phrases:
            if phrase in user_query.lower():
                clean_query = user_query.lower().split(phrase)[0].strip()
                if clean_query:
                    clean_query = clean_query[0].upper() + clean_query[1:]
                break
        
        # Нормализуем пробелы и пунктуацию
        clean_query = re.sub(r'\s+', ' ', clean_query).strip()
        clean_query = clean_query.rstrip(' .,;—:-')
        
        logger.info(f"Cleaned query: '{clean_query}'")
        return clean_query
    
    def _apply_minimal_rules(self, query: str) -> Optional[LLMResult]:
        """МИНИМАЛЬНЫЕ правила - только самые частые и точные"""
        query_lower = query.lower().strip(' ?')
        
        # ТОЧНЫЕ матчи (полное совпадение)
        exact_matches = {
            "сколько всего видео": "SELECT COUNT(*) FROM videos;",
            "сумма лайков": "SELECT SUM(likes_count) FROM videos;",
            "сколько всего замеров": "SELECT COUNT(*) FROM video_snapshots;",
            "отрицательные изменения": "SELECT COUNT(*) FROM video_snapshots WHERE delta_views_count < 0;",
            "сумма просмотров": "SELECT SUM(views_count) FROM videos;",
        }
        
        if query_lower in exact_matches:
            logger.info(f"Exact rule match: '{query_lower}'")
            return LLMResult(
                sql=exact_matches[query_lower],
                confidence=1.0,
                is_safe=True,
                source="rules"
            )
        
        # ТОЛЬКО базовые частичные матчи (очень строгие)
        if query_lower.startswith("сколько видео") and "креатор" not in query_lower and len(query_lower) < 25:
            # Только "сколько видео" без других условий
            return LLMResult(
                sql="SELECT COUNT(*) FROM videos;",
                confidence=0.9,
                is_safe=True,
                source="rules"
            )
        
        # ВСЁ ОСТАЛЬНОЕ → идём в промпт
        return None
    
    def _build_main_prompt(self, query: str) -> str:
        """МОЩНЫЙ промпт на английском с русскими примерами"""
        return f"""<s>[INST] You are a SQL translator for PostgreSQL. Translate Russian questions into SQL queries.

## DATABASE SCHEMA:

1. TABLE `videos` (final statistics for each video):
   - id (UUID, primary key) - unique video ID
   - creator_id (TEXT, 32 hex characters) - creator/author ID
   - video_created_at (TIMESTAMP) - video publication date and time
   - views_count (INTEGER) - FINAL total number of video views
   - likes_count (INTEGER) - final number of likes
   - comments_count (INTEGER) - final number of comments

2. TABLE `video_snapshots` (hourly statistics snapshots):
   - id (INTEGER, primary key) - unique snapshot ID
   - video_id (UUID, foreign key → videos.id) - reference to video
   - created_at (TIMESTAMP) - snapshot time (every hour: 00:00, 01:00, ..., 23:00)
   - delta_views_count (INTEGER) - VIEW GROWTH over the past hour
        • Positive = views increased
        • Negative = views decreased
        • Zero = no change
   - delta_likes_count (INTEGER) - likes growth over hour
   - delta_comments_count (INTEGER) - comments growth over hour

## HOW TO TRANSLATE RUSSIAN:

### FOR COUNTING:
- "Сколько всего видео?" → SELECT COUNT(*) FROM videos;
- "Сколько видео у креатора [id]?" → SELECT COUNT(*) FROM videos WHERE creator_id = '[id]';
- "Сколько разных видео?" → SELECT COUNT(DISTINCT video_id) FROM video_snapshots;

### FOR GROWTH/CHANGES (use delta_* in video_snapshots):
- "Прирост просмотров", "Выросли просмотры", "На сколько выросли" → SUM(delta_views_count)
- "Суммарный прирост" → SUM(delta_*)
- "Изменения просмотров" → delta_views_count

### FOR FINAL STATS (use *count in videos):
- "Набрало просмотров", "Больше N просмотров", "Итоговые просмотры" → views_count in videos

### FOR DATES:
- "28 ноября 2025" → '2025-11-28'
- "с [date] по [date]" → BETWEEN '[date1]' AND '[date2]'
- "в [month] [year]" → EXTRACT(MONTH) = N AND EXTRACT(YEAR) = YYYY

### FOR TIME INTERVALS:
- "с X:00 до Y:00" → EXTRACT(HOUR FROM created_at) BETWEEN X AND Y-1

### FOR CREATOR FILTERS IN VIDEO_SNAPSHOTS (IMPORTANT!):
- When filtering by creator_id for video_snapshots data, you MUST JOIN with videos table
- CORRECT: SELECT SUM(vs.delta_views_count) FROM video_snapshots vs JOIN videos v ON vs.video_id = v.id WHERE v.creator_id = '[id]'
- WRONG: SELECT FROM video_snapshots WHERE creator_id = '[id]' (creator_id doesn't exist there!)

### RUSSIAN MONTHS TO NUMBERS:
- январь/января=1, февраль/февраля=2, март/марта=3, апрель/апреля=4
- май/мая=5, июнь/июня=6, июль/июля=7, август/августа=8
- сентябрь/сентября=9, октябрь/октября=10, ноябрь/ноября=11, декабрь/декабря=12

## EXAMPLES:

Question: Сколько всего видео есть в системе?
SQL: SELECT COUNT(*) FROM videos;

Question: На сколько просмотров в сумме выросли все видео 28 ноября 2025?
SQL: SELECT SUM(delta_views_count) FROM video_snapshots WHERE DATE(created_at) = '2025-11-28';

Question: Сколько видео у креатора с id abc123 вышло с 1 по 5 ноября 2025 включительно?
SQL: SELECT COUNT(*) FROM videos WHERE creator_id = 'abc123' AND DATE(video_created_at) BETWEEN '2025-11-01' AND '2025-11-05';

Question: Сколько видео набрало больше 100000 просмотров за всё время?
SQL: SELECT COUNT(*) FROM videos WHERE views_count > 100000;

Question: Сколько разных видео получали новые просмотры 27 ноября 2025?
SQL: SELECT COUNT(DISTINCT video_id) FROM video_snapshots WHERE DATE(created_at) = '2025-11-27' AND delta_views_count > 0;

Question: На сколько просмотров выросли видео креатора def456 28 ноября 2025?
SQL: SELECT SUM(vs.delta_views_count) FROM video_snapshots vs JOIN videos v ON vs.video_id = v.id WHERE v.creator_id = 'def456' AND DATE(vs.created_at) = '2025-11-28';

Question: Прирост просмотров у креатора xyz789 с 10:00 до 15:00 28 ноября 2025?
SQL: SELECT SUM(vs.delta_views_count) FROM video_snapshots vs JOIN videos v ON vs.video_id = v.id WHERE v.creator_id = 'xyz789' AND DATE(vs.created_at) = '2025-11-28' AND EXTRACT(HOUR FROM vs.created_at) BETWEEN 10 AND 14;

Question: Суммарный прирост лайков у креатора xyz789 за ноябрь 2025?
SQL: SELECT SUM(vs.delta_likes_count) FROM video_snapshots vs JOIN videos v ON vs.video_id = v.id WHERE v.creator_id = 'xyz789' AND EXTRACT(YEAR FROM vs.created_at) = 2025 AND EXTRACT(MONTH FROM vs.created_at) = 11;

## YOUR TASK:
Translate the following Russian question into SQL. Return ONLY SQL code, no explanations.

Question: {query}
SQL: [/INST]"""
    
    def _validate_sql(self, sql: str) -> bool:
        """Простая валидация безопасности"""
        if not sql:
            return False
        
        sql_upper = sql.upper()
        
        # Ищем только отдельные слова
        dangerous = ["DROP ", "DELETE ", "UPDATE ", "INSERT ", 
                    "ALTER ", "TRUNCATE ", "CREATE ", "GRANT ",
                    "REVOKE ", "EXECUTE ", "DECLARE ", "CURSOR "]
        
        for cmd in dangerous:
            if cmd in sql_upper:
                logger.warning(f"Dangerous command detected: {cmd.strip()}")
                return False
        
        # Должен быть SELECT
        if "SELECT " not in sql_upper:
            logger.warning("SQL doesn't contain SELECT")
            return False
        
        return True
    
    def _extract_sql(self, response: str) -> Optional[str]:
        """Извлечение SQL из ответа LLM"""
        if not response:
            return None
        
        # Очищаем ответ
        response = response.strip()
        
        # Убираем markdown код
        response = re.sub(r'```(?:sql)?', '', response)
        response = re.sub(r'`', '', response)
        
        # Убираем комментарии
        response = re.sub(r'--.*', '', response)
        
        # Ищем SQL (от SELECT до ; или конца)
        sql_match = re.search(r'(SELECT .*?)(?:;|$)', response, re.IGNORECASE | re.DOTALL)
        if sql_match:
            sql = sql_match.group(1).strip()
            
            # Добавляем точку с запятой если нет
            if not sql.endswith(';'):
                sql += ';'
            
            # Нормализуем пробелы
            sql = re.sub(r'\s+', ' ', sql).strip()
            
            # Конвертируем русские даты
            for ru_month, num in self.month_map.items():
                pattern = rf'(\d+)\s+{ru_month}\s+(\d{{4}})'
                def replace_date(match):
                    day = match.group(1)
                    year = match.group(2)
                    return f"'{year}-{num:02d}-{int(day):02d}'"
                
                sql = re.sub(pattern, replace_date, sql, flags=re.IGNORECASE)
            
            logger.info(f"Extracted SQL: {sql}")
            return sql
        
        return None
    
    async def _call_ollama(self, prompt: str) -> str:
        """Вызов Ollama API"""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self.base_url}/api/generate",
                    json={
                        "model": self.model,
                        "prompt": prompt,
                        "stream": False,
                        "options": {
                            "temperature": 0.1,
                            "num_predict": 500,
                            "top_p": 0.9
                        }
                    },
                    timeout=30
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return data.get('response', '')
                    else:
                        error_text = await resp.text()
                        logger.error(f"Ollama API error: {resp.status} - {error_text}")
                        return ""
        except Exception as e:
            logger.error(f"Ollama call error: {e}")
            return ""
    
    async def ask(self, user_query: str) -> Optional[LLMResult]:
        """Основной метод обработки запроса"""
        logger.info(f"\n{'='*60}")
        logger.info(f"Query: '{user_query}'")
        
        # 1. Препроцессинг
        clean_query = self._preprocess_query(user_query)
        
        # 2. Проверяем МИНИМАЛЬНЫЕ правила (всего 5-6 случаев)
        rule_result = self._apply_minimal_rules(clean_query)
        if rule_result:
            logger.info(f"Used rule → {rule_result.sql}")
            return rule_result
        
        # 3. ОСНОВНОЙ ПУТЬ: LLM с мощным промптом
        logger.info("Using LLM with prompt")
        prompt = self._build_main_prompt(clean_query)
        
        try:
            response = await self._call_ollama(prompt)
            
            if not response:
                logger.error("Empty response from LLM")
                return None
            
            # 4. Извлекаем SQL
            sql = self._extract_sql(response)
            
            if not sql:
                logger.error("Failed to extract SQL from response")
                return None
            
            # 5. Простая валидация безопасности
            if not self._validate_sql(sql):
                logger.error(f"SQL failed safety check: {sql}")
                return None
            
            # 6. Возвращаем результат
            logger.info(f"Generated SQL: {sql}")
            return LLMResult(
                sql=sql,
                confidence=0.8,
                is_safe=True,
                source="prompt"
            )
            
        except Exception as e:
            logger.error(f"Exception in ask: {type(e).__name__}: {e}")
            return None