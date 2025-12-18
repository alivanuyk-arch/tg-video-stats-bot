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
    
    def __init__(self, model: str = "llama3.2:3b", base_url: str = "http://localhost:11434"):
        self.model = model
        self.base_url = base_url
        self.original_query = ""
        self.original_query_clean = ""
        self._init_month_map()
        
        # Пробуем вызвать с проверкой
        if hasattr(self, '_init_sql_rules'):
            self._init_sql_rules()
        else:
            print("⚠️  Метод _init_sql_rules не найден!")
            # Создаём пустые правила
            self.sql_rules = []
        
    def _init_month_map(self):
        """Словарь русских месяцев"""
        self.month_map = {
            'января': 1, 'февраля': 2, 'марта': 3, 'апреля': 4,
            'мая': 5, 'июня': 6, 'июля': 7, 'августа': 8,
            'сентября': 9, 'октября': 10, 'ноября': 11, 'декабря': 12,
            'январь': 1, 'февраль': 2, 'март': 3, 'апрель': 4,
            'май': 5, 'июнь': 6, 'июль': 7, 'август': 8,
            'сентябрь': 9, 'октябрь': 10, 'ноябрь': 11, 'декабрь': 12,
        }
    
    def _init_sql_rules(self):
        """Инициализация правил построения SQL"""
        self.sql_rules = []
        
        # Паттерн месяцев для дат (января, февраля...)
        month_pattern = '|'.join(self.month_map.keys())
        
        # Паттерн месяцев в предложном падеже (в январе, в феврале...)
        month_names = [
            'январе', 'феврале', 'марте', 'апреле', 'мае', 'июне',
            'июле', 'августе', 'сентябре', 'октябре', 'ноябре', 'декабре'
        ]
        month_names_pattern = '|'.join(month_names)
        
        # 1. Простые правила (самый высокий приоритет)
        self.sql_rules.append((
            r'сколько всего.*замеров',
            self._rule_negative_measurements, 
            1
        ))
        
        # 2. Диапазон времени с датой
        self.sql_rules.append((
            fr'с\s+(\d+):00\s+до\s+(\d+):00.*?\b(\d+\s+(?:{month_pattern})\s+\d{{4}})\b',
            self._rule_hours_range_date, 
            1
        ))
        
        # 3. Месяц и год ("в июне 2025 года")
        self.sql_rules.append((
            fr'в\s+({month_names_pattern})\s+(\d{{4}})\s+года',
            self._rule_month_year, 
            2
        ))
        
        # 4. Отрицательные замеры (разные формулировки)
        self.sql_rules.append((
            r'замеров.*отрицательным',
            self._rule_negative_measurements, 
            2
        ))
        self.sql_rules.append((
            r'отрицательным.*замером.*просмотров.*меньше',
            self._rule_negative_measurements, 
            2
        ))
        
        # 5. Полная дата
        self.sql_rules.append((
            fr'(\d+\s+(?:{month_pattern})\s+\d{{4}})',
            self._rule_full_date, 
            3
        ))
        
        self.sql_rules.append((
            fr'период\s+с\s+(\d+\s+(?:{month_pattern})\s+\d{{4}})\s+по\s+(\d+\s+(?:{month_pattern})\s+\d{{4}})',
            self._rule_period_count, 
            2  # Приоритет выше чем _rule_full_date
        ))

        # 6. Запросы с "больше N просмотров" и creator_id
        self.sql_rules.append((
            r'больше\s+([\d\s]+).*просмотров.*креатор.*id\s+([a-f0-9]{32})',
            self._rule_count_by_creator_views, 
            4
        ))
        self.sql_rules.append((
            r'сколько видео.*креатор.*id\s+([a-f0-9]{32}).*больше\s+([\d\s]+)',
            self._rule_count_by_creator_views_correct_order, 
            4
        ))
        
        # 7. Сколько всего замеров (общее)
        self.sql_rules.append((
            r'сколько всего.*замеров',
            self._rule_total_measurements, 
            5
        ))
        
        # 8. Год с "года" или "год"
        self.sql_rules.append((
            r'(\d{4})\s+(?:года|год)\b',
            self._rule_year_only, 
            6
        ))
        
        # 9. Просто год (отдельно стоящий)
        self.sql_rules.append((
            r'\b(\d{4})\b(?![\-\:\.])',
            self._rule_year_only, 
            7
        ))
        
        # 10. Отрицательные изменения (общее)
        self.sql_rules.append((
            r'отрицательные.*изменения',
            self._rule_negative_changes, 
            8
        ))
        
        # 11. Простые запросы по креатору (БЕЗ "больше N")
        self.sql_rules.append((
            r'сколько видео.*креатор.*id\s+([a-f0-9]{32})',
            self._rule_count_by_creator, 
            20
        ))
        
        # 12. Общие запросы
        self.sql_rules.append((r'сколько всего видео', self._rule_total_videos, 9))
        self.sql_rules.append((r'сумма.*лайков', self._rule_total_likes, 9))

    def _rule_videos_by_date_range(self, match):
        """Сколько видео опубликовано с X по Y"""
        date1_str = match.group(1)  # 1 ноября 2025
        date2_str = match.group(2)  # 5 ноября 2025
        
        date1_sql = self._convert_date(date1_str)
        date2_sql = self._convert_date(date2_str)
        
        # Ищем creator_id
        creator_match = re.search(r'id\s+([a-f0-9]{32})', self.original_query_clean)
        creator_condition = ""
        if creator_match:
            creator_condition = f" AND creator_id = '{creator_match.group(1)}'"
        
            return f"""SELECT COUNT(*) FROM videos 
                WHERE DATE(video_created_at) BETWEEN {date1_sql} AND {date2_sql}
                {creator_condition};"""

        # 2. Отрицательные замеры
        self.sql_rules.append((
            r'сколько всего.*замеров.*отрицательным',
            self._rule_negative_measurements, 
            2
        ))
        self.sql_rules.append((
            r'отрицательным.*замером.*просмотров.*меньше',
            self._rule_negative_measurements, 
            2
        ))
        
        # 3. Полная дата
        self.sql_rules.append((
            fr'(\d+\s+(?:{month_pattern})\s+\d{{4}})',
            self._rule_full_date, 
            3
        ))
        
        # 4. Запросы с "больше N просмотров" и creator_id (с числами с пробелами)
        self.sql_rules.append((
            r'больше\s+([\d\s]+).*просмотров.*креатор.*id\s+([a-f0-9]{32})',
            self._rule_count_by_creator_views, 
            4
        ))
        self.sql_rules.append((
            r'сколько видео.*креатор.*id\s+([a-f0-9]{32}).*больше\s+([\d\s]+)',
            self._rule_count_by_creator_views_correct_order, 
            4
        ))
        
        # 5. Сколько всего замеров
        self.sql_rules.append((
            r'сколько всего.*замеров',
            self._rule_total_measurements, 
            5
        ))
        
        # 6. Год с "года" или "год"
        self.sql_rules.append((
            r'(\d{4})\s+(?:года|год)\b',
            self._rule_year_only, 
            6
        ))
        
        # 7. Просто год (отдельно стоящий)
        self.sql_rules.append((
            r'\b(\d{4})\b(?![\-\:\.])',
            self._rule_year_only, 
            7
        ))
        
        # 8. Отрицательные изменения (общее)
        self.sql_rules.append((
            r'отрицательные.*изменения',
            self._rule_negative_changes, 
            8
        ))
        
        # 9. Простые запросы по креатору (БЕЗ "больше N")
        self.sql_rules.append((
            r'сколько видео.*креатор.*id\s+([a-f0-9]{32})',
            self._rule_count_by_creator, 
            20
        ))
        
        # 10. Общие запросы
        self.sql_rules.append((r'сколько всего видео', self._rule_total_videos, 9))
        self.sql_rules.append((r'сумма.*лайков', self._rule_total_likes, 9))
    
    # ===== ПРАВИЛА-ГЕНЕРАТОРЫ SQL =====
    
    def _rule_hours_range_date(self, match):
        """С 10:00 до 15:00 28 ноября 2025"""
        hour_start = match.group(1)  # 10
        hour_end = int(match.group(2)) - 1  # 15-1 = 14
        date_str = match.group(3)    # 28 ноября 2025
        
        date_sql = self._convert_date(date_str)
        
        # Ищем creator_id
        creator_match = re.search(r'id\s+([a-f0-9]{32})', self.original_query_clean)
        creator_condition = ""
        if creator_match:
            creator_condition = f" AND v.creator_id = '{creator_match.group(1)}'"
        
        return f"""SELECT SUM(vs.delta_views_count) 
                   FROM video_snapshots vs 
                   JOIN videos v ON vs.video_id = v.id 
                   WHERE DATE(vs.created_at) = {date_sql} 
                   AND EXTRACT(HOUR FROM vs.created_at) BETWEEN {hour_start} AND {hour_end}
                   {creator_condition};"""
    
    def _rule_negative_measurements(self, match):
        """Отрицательные замеры просмотров"""
        return "SELECT COUNT(*) FROM video_snapshots WHERE delta_views_count < 0;"
    
    def _rule_total_measurements(self, match):
        """Всего замеров статистики"""
        return "SELECT COUNT(*) FROM video_snapshots;"
    
    def _rule_count_by_creator_views(self, match):
        """Сколько видео у креатора X > N просмотров? (порядок: число, потом ID)"""
        views_raw = match.group(1).replace(' ', '')  # "10 000" → "10000"
        creator_id = match.group(2)
        return f"SELECT COUNT(*) FROM videos WHERE creator_id = '{creator_id}' AND views_count > {views_raw};"
    
    def _rule_count_by_creator_views_correct_order(self, match):
        """Правильный порядок: сначала ID, потом число"""
        creator_id = match.group(1)
        views_raw = match.group(2).replace(' ', '')
        return f"SELECT COUNT(*) FROM videos WHERE creator_id = '{creator_id}' AND views_count > {views_raw};"
    
    def _rule_full_date(self, match):
        """Обработка полной даты '28 ноября 2025' с возможным creator_id"""
        date_str = match.group(1)
        date_sql = self._convert_date(date_str)
        
        query = self.original_query_clean.lower()
        
        # Ищем creator_id
        creator_match = re.search(r'id\s+([a-f0-9]{32})', self.original_query_clean)
        creator_condition = ""
        if creator_match:
            creator_condition = f" AND v.creator_id = '{creator_match.group(1)}'"
        
        if "суммарно" in query or "выросли" in query or "прирост" in query:
            if creator_condition:
                # С JOIN если есть creator_id
                return f"""SELECT SUM(vs.delta_views_count) 
                           FROM video_snapshots vs 
                           JOIN videos v ON vs.video_id = v.id 
                           WHERE DATE(vs.created_at) = {date_sql}{creator_condition};"""
            else:
                # Без JOIN если нет creator_id
                return f"SELECT SUM(delta_views_count) FROM video_snapshots WHERE DATE(created_at) = {date_sql};"
        elif "отрицательные" in query:
            return f"SELECT COUNT(*) FROM video_snapshots WHERE delta_views_count < 0 AND DATE(created_at) = {date_sql};"
        else:
            return f"SELECT COUNT(*) FROM video_snapshots WHERE DATE(created_at) = {date_sql};"
    
    def _rule_year_only(self, match):
        """Обработка только года '2025' или '2025 года'"""
        year = match.group(1)
        query = self.original_query_clean.lower()
        
        # Проверяем особый случай "разных календарных дней"
        if "разных календарных днях" in query or "сколько разных дней" in query:
            # Определяем месяц
            month_num = None
            month_words = ['января', 'февраля', 'марта', 'апреля', 'мая', 'июня', 
                        'июля', 'августа', 'сентября', 'октября', 'ноября', 'декабря']
            for i, month in enumerate(month_words, 1):
                if month in query:
                    month_num = i
                    break
            
            # Ищем creator_id
            creator_match = re.search(r'id\s+([a-f0-9]{32})', self.original_query_clean)
            creator_condition = ""
            if creator_match:
                creator_condition = f" AND creator_id = '{creator_match.group(1)}'"
            
            # Формируем SQL
            month_condition = f" AND EXTRACT(MONTH FROM video_created_at) = {month_num}" if month_num else ""
            
            return f"""SELECT COUNT(DISTINCT DATE(video_created_at)) 
                    FROM videos 
                    WHERE EXTRACT(YEAR FROM video_created_at) = {year}
                    {month_condition}{creator_condition};"""
        
        # Обычная логика для года
        if "сколько видео" in query and "креатор" in query:
            creator_match = re.search(r'id\s+([a-f0-9]{32})', self.original_query_clean)
            creator_condition = ""
            if creator_match:
                creator_condition = f" AND creator_id = '{creator_match.group(1)}'"
            return f"SELECT COUNT(*) FROM videos WHERE EXTRACT(YEAR FROM video_created_at) = {year}{creator_condition};"
        
        elif "суммарно" in query or "сумма" in query or "выросли" in query:
            return f"SELECT SUM(delta_views_count) FROM video_snapshots WHERE EXTRACT(YEAR FROM created_at) = {year};"
        
        elif "отрицательные" in query:
            return f"SELECT COUNT(*) FROM video_snapshots WHERE delta_views_count < 0 AND EXTRACT(YEAR FROM created_at) = {year};"
        
        else:
            return f"SELECT COUNT(*) FROM videos WHERE EXTRACT(YEAR FROM video_created_at) = {year};"
    
    def _rule_count_by_creator(self, match):
        """Сколько видео у креатора X?"""
        creator_id = match.group(1)
        return f"SELECT COUNT(*) FROM videos WHERE creator_id = '{creator_id}';"
    
    def _rule_count_by_views(self, match):
        """Сколько видео > N просмотров?"""
        views = match.group(1)
        return f"SELECT COUNT(*) FROM videos WHERE views_count > {views};"
    
    def _rule_negative_changes(self, match):
        """Отрицательные изменения"""
        return "SELECT COUNT(*) FROM video_snapshots WHERE delta_views_count < 0;"
    
    def _rule_single_date(self, match):
        """Одна дата"""
        date_sql = self._convert_date(match.group(1))
        return f"SELECT SUM(delta_views_count) FROM video_snapshots WHERE DATE(created_at) = {date_sql};"
    
    def _rule_date_between(self, match):
        """С X по Y"""
        date1 = self._convert_date(match.group(1))
        date2 = self._convert_date(match.group(2))
        return f"SELECT SUM(delta_views_count) FROM video_snapshots WHERE DATE(created_at) BETWEEN {date1} AND {date2};"
    
    def _rule_total_videos(self, match):
        """Сколько всего видео"""
        return "SELECT COUNT(*) FROM videos;"
    
    def _rule_total_likes(self, match):
        """Сумма лайков всех видео"""
        return "SELECT SUM(likes_count) FROM videos;"
    
    def _rule_growth_by_date(self, match):
        """Прирост просмотров за дату"""
        date_str = match.group(1)
        date_sql = self._convert_date(date_str)
        return f"SELECT SUM(delta_views_count) FROM video_snapshots WHERE DATE(created_at) = {date_sql};"
    
    def _rule_month_year(self, match):
        """Обработка 'в июне 2025 года'"""
        month_ru = match.group(1)  # 'июне'
        year = match.group(2)      # '2025'
        
        # Преобразуем "июне" → "июнь"
        month_base = month_ru
        if month_ru.endswith('е'):
            month_base = month_ru[:-1] + 'ь'
        elif month_ru.endswith('те'):  # марте → март
            month_base = month_ru[:-2]
        
        # Получаем номер месяца из month_map
        month_num = None
        for ru_month, num in self.month_map.items():
            if ru_month.startswith(month_base[:3]):  # Сравниваем первые 3 буквы
                month_num = num
                break
        
        if month_num:
            return f"""SELECT SUM(views_count) FROM videos 
                       WHERE EXTRACT(YEAR FROM video_created_at) = {year}
                       AND EXTRACT(MONTH FROM video_created_at) = {month_num};"""
        
        # Если не нашли - возвращаем просто по году
        return f"SELECT SUM(views_count) FROM videos WHERE EXTRACT(YEAR FROM video_created_at) = {year};"

    def _rule_period_count(self, match):
        """Сколько видео опубликовано с X по Y"""
        date1_str = match.group(1)  # 1 ноября 2025
        date2_str = match.group(2)  # 5 ноября 2025
        
        date1_sql = self._convert_date(date1_str)
        date2_sql = self._convert_date(date2_str)
        
        # Ищем creator_id
        creator_match = re.search(r'id\s+([a-f0-9]{32})', self.original_query_clean)
        creator_condition = ""
        if creator_match:
            creator_condition = f" AND creator_id = '{creator_match.group(1)}'"
        
        return f"""SELECT COUNT(*) FROM videos 
                WHERE DATE(video_created_at) BETWEEN {date1_sql} AND {date2_sql}
                {creator_condition};"""

    def _convert_date(self, date_str: str) -> str:
        """Конвертирует русскую дату"""
        # ВРЕМЕННАЯ ОТЛАДКА
        print(f"🔥 _convert_date ВХОД: '{date_str}'")
        
        date_lower = date_str.lower()
        
        for ru_month, num in self.month_map.items():
            if ru_month in date_lower:
                print(f"🔥 Найден месяц '{ru_month}' -> {num}")
                date_lower = date_lower.replace(ru_month, f' {num} ')
                break
        
        import re
        parts = re.findall(r'\d+', date_lower)
        print(f"🔥 parts после re.findall: {parts}")
        
        if len(parts) == 3:
            day, month, year = parts[0], parts[1], parts[2]
            print(f"🔥 day='{day}', month='{month}', year='{year}'")
            
            # КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ
            # Убери int() чтобы увидеть строки
            result = f"'{year}-{month}-{day}'"  # БЕЗ int()
            print(f"🔥 РЕЗУЛЬТАТ (без int): {result}")
            
            # Теперь с int
            try:
                day_int = int(day)
                month_int = int(month) 
                year_int = int(year)
                result = f"'{year_int:04d}-{month_int:02d}-{day_int:02d}'"
                print(f"🔥 РЕЗУЛЬТАТ (с int): {result}")
            except Exception as e:
                print(f"🔥 ОШИБКА int(): {e}")
                result = f"'{year}-{month}-{day}'"
            
            return result
        
        return f"'{date_str}'"
    
    def _apply_rules(self, user_query: str) -> Optional[str]:
        """Проверка по правилам"""
        check_query = getattr(self, 'original_query_clean', user_query)
        
        print(f"\n🔍 DEBUG _apply_rules:")
        print(f"Запрос: '{check_query}'")
        
        matched = []
        for pattern, handler, priority in self.sql_rules:
            match = re.search(pattern, check_query, re.IGNORECASE)
            if match:
                print(f"✅ Правило '{pattern[:40]}...' СРАБОТАЛО")
                matched.append((priority, pattern, handler, match))
            else:
                print(f"❌ Правило '{pattern[:40]}...' НЕ СРАБОТАЛО")
        
        if matched:
            matched.sort(key=lambda x: x[0])
            priority, pattern, handler, match = matched[0]
            sql = handler(match)
            print(f"🎯 Используем правило приоритет {priority}: {sql}")
            return sql
        
        print("⚠️  Нет подходящих правил")
        return None

    
    def prepare_query_for_llm(self, user_query: str) -> str:
        """Подготовка запроса для LLM - оставляем только ДО 'то есть'"""
        stop_phrases = [
            'то есть',
            '— то есть',  
            '― то есть',  
            '– то есть',  
            '- то есть', 
            ', то есть',
            '; то есть',
            'другими словами',
            'иными словами',
            'то есть по сравнению',
            'то есть по отношению'
        ]
        
        query_for_rules = user_query
        query_for_llm = user_query
        
        for phrase in stop_phrases:
            if phrase in user_query.lower():
                query_for_rules = user_query.lower().split(phrase)[0].strip()
                if query_for_rules:
                    query_for_rules = query_for_rules[0].upper() + query_for_rules[1:]
                query_for_llm = query_for_rules
                break
        
        query_for_rules = query_for_rules.rstrip(' .,;—:-')
        query_for_llm = query_for_llm.rstrip(' .,;—:-')
        
        self.original_query_clean = query_for_rules
        self.original_query_full = user_query
        
        logger.info(f"Для правил: '{query_for_rules}'")
        logger.info(f"Для LLM: '{query_for_llm}'")
        
        return query_for_llm
    
    def _build_prompt(self, query: str) -> str:
        """English prompt with minimal structure"""
        return f"""<s>[INST] PostgreSQL SQL query.

    Database schema:
    1. videos table:
    - id (UUID, video identifier)
    - creator_id (string, 32 hex chars) 
    - views_count (integer, TOTAL views count)
    - video_created_at (timestamp, video publication date)
    - likes_count (integer)

    2. video_snapshots table:
    - video_id (references videos.id)
    - created_at (timestamp, hourly snapshots: 10:00, 11:00, etc.)
    - delta_views_count (integer, view change from previous hour)
        - Can be positive (views increased)
        - Can be negative (views decreased) 
        - Can be zero (no change)

    Key facts:
    - For "total views" or "gained X views" → use videos.views_count
    - For "view growth" or "change over period" → SUM(video_snapshots.delta_views_count)
    - For "negative changes" → WHERE delta_views_count < 0
    - For counting days → COUNT(DISTINCT DATE(column))
    - Hours: EXTRACT(HOUR FROM created_at) BETWEEN X AND Y
    - Dates: DATE(column) = 'YYYY-MM-DD'

    Write ONLY SQL code for this question: "{query}"

    SQL: [/INST] SELECT"""
    
    async def ask(self, user_query: str) -> Optional[LLMResult]:
        logger.info(f"\n{'='*60}")
        logger.info(f"Запрос: '{user_query}'")
        
        # Подготавливаем запрос (обрезаем "то есть")
        processed_query = self.prepare_query_for_llm(user_query)
        
        # ШАГ 1: Проверяем по правилам
        rule_sql = self._apply_rules(user_query)
        if rule_sql:
            logger.info(f"Использовано правило → {rule_sql}")
            return LLMResult(
                sql=rule_sql,
                confidence=1.0,
                is_safe=self._validate_sql(rule_sql)
            )
        
        # ШАГ 2: Если правил нет → LLM
        logger.info(f"Правил нет, идём в LLM")
        
        prompt = self._build_prompt(processed_query)
        
        try:
            response = await self._call_ollama(prompt)
            sql = self._extract_sql(response)
            
            if not sql:
                return None
            
            # Упрощаем SQL
            sql = self._simplify_sql(sql, processed_query)
            
            is_safe = self._validate_sql(sql)
            
            if not is_safe:
                return None
            
            return LLMResult(
                sql=sql,
                confidence=0.7,
                is_safe=is_safe
            )
            
        except Exception as e:
            logger.error(f"Исключение в ask: {type(e).__name__}: {e}")
            return None
    
    async def _call_ollama(self, prompt: str) -> str:
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
                    error_text = await resp.text()
                    raise Exception(f"API error: {resp.status} - {error_text}")
    
    def _extract_sql(self, response: str) -> Optional[str]:
        """Извлечение SQL с очисткой"""
        response = response.replace('```sql', '').replace('```', '').replace('`', '').strip()
        
        # Обрезаем после первой точки с запятой
        if ';' in response:
            response = response.split(';')[0] + ';'
        
        lines = []
        for line in response.split('\n'):
            line = line.strip()
            if line and not line.startswith('--'):
                lines.append(line)
        
        sql = ' '.join(lines)
        
        # Убираем плейсхолдеры
        sql = re.sub(r'\{[A-Z_]+\}', '', sql)
        
        # Убираем EXTRACT с числами в кавычках
        sql = re.sub(r"EXTRACT\(YEAR FROM '(\d{4})'\)", r"\1", sql, flags=re.IGNORECASE)
        sql = re.sub(r"EXTRACT\(MONTH FROM '(\d{1,2})'\)", r"\1", sql, flags=re.IGNORECASE)
        sql = re.sub(r"EXTRACT\(DAY FROM '(\d{1,2})'\)", r"\1", sql, flags=re.IGNORECASE)
        sql = re.sub(r"EXTRACT\(HOUR FROM '(\d{1,2})'\)", r"\1", sql, flags=re.IGNORECASE)
        
        # Русские месяцы → числа
        if hasattr(self, 'month_map'):
            for ru_month, num in self.month_map.items():
                if ru_month in sql.lower():
                    sql = re.sub(rf'\b{ru_month}\b', str(num), sql, flags=re.IGNORECASE)
        
        # Преобразуем полные русские даты
        full_date_pattern = r'(\d{1,2})\s+(\w+)\s+(\d{4})'
        def convert_full_date(match):
            day = match.group(1)
            month_ru = match.group(2).lower()
            year = match.group(3)
            
            if hasattr(self, 'month_map'):
                month_num = self.month_map.get(month_ru)
                if month_num:
                    return f"'{year}-{month_num:02d}-{int(day):02d}'"
            return match.group(0)
        
        sql = re.sub(full_date_pattern, convert_full_date, sql, flags=re.IGNORECASE)
        
        # Исправляем MySQL функции на PostgreSQL
        while 'YEAR(' in sql.upper():
            match = re.search(r'(?i)YEAR\(([^)]+)\)', sql)
            if match:
                inner = match.group(1)
                sql = sql.replace(match.group(0), f'EXTRACT(YEAR FROM {inner})')
        
        while 'MONTH(' in sql.upper():
            match = re.search(r'(?i)MONTH\(([^)]+)\)', sql)
            if match:
                inner = match.group(1)
                sql = sql.replace(match.group(0), f'EXTRACT(MONTH FROM {inner})')
        
        # Убираем лишние пробелы
        sql = ' '.join(sql.split())
        
        # Если SQL не начинается с SELECT - добавляем
        sql = sql.strip()
        if not sql.upper().startswith('SELECT'):
            if sql.upper().startswith('COUNT') or sql.upper().startswith('SUM') or \
            sql.upper().startswith('AVG') or 'FROM' in sql.upper():
                sql = f"SELECT {sql}"
        
        # Убедимся что есть точка с запятой в конце
        if not sql.endswith(';'):
            sql = sql + ';'
        
        # Проверяем что это похоже на SQL запрос
        if 'FROM' in sql.upper() and ('SELECT' in sql.upper() or 'COUNT' in sql.upper() or 
                                    'SUM' in sql.upper() or 'AVG' in sql.upper()):
            return sql
        
        return None
    
    def _simplify_sql(self, sql: str, user_query: str) -> str:
        """Упрощает сложные SQL конструкции"""
        sql_upper = sql.upper()
        
        # Убираем лишний GROUP BY для простых COUNT
        if "GROUP BY" in sql_upper and "сколько" in user_query.lower():
            sql = re.sub(r'\s+GROUP BY.*?(;|$)', ';', sql, flags=re.IGNORECASE)
        
        # Убираем HAVING если нет группировки
        if "HAVING" in sql_upper and "GROUP BY" not in sql_upper:
            sql = re.sub(r'\s+HAVING.*?(;|$)', ';', sql, flags=re.IGNORECASE)
        
        # Упрощаем BETWEEN если не нужно
        if "BETWEEN" in sql_upper and "по" not in user_query.lower():
            sql = re.sub(r'BETWEEN.*?AND', '=', sql, flags=re.IGNORECASE)
        
        return sql
    
    def _validate_sql(self, sql: str) -> bool:
        """Проверка безопасности SQL"""
        # Удаляем комментарии для проверки
        sql_without_comments = re.sub(r'--.*', '', sql)
        sql_upper = sql_without_comments.upper()
        
        # Опасные команды (без комментариев)
        dangerous = ["DROP", "DELETE", "UPDATE", "INSERT", 
                    "ALTER", "TRUNCATE", "/*", "*/"]
        
        for word in dangerous:
            if word in sql_upper:
                return False
        
        # Проверяем, что это SELECT-запрос
        has_select = "SELECT" in sql_upper
        has_from = "FROM" in sql_upper
        
        if not has_select or not has_from:
            return False
        
        # Проверка на вложенные агрегаты
        if re.search(r'SUM\(.*SUM\(', sql_upper) or \
           re.search(r'COUNT\(.*COUNT\(', sql_upper) or \
           re.search(r'AVG\(.*AVG\(', sql_upper):
            return False
        
        return True