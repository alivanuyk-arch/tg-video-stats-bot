import re
import json
import hashlib
import logging
from typing import Dict, List, Set, Optional, Any, Tuple
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass, asdict
from collections import defaultdict

logger = logging.getLogger(__name__)

@dataclass
class ConstructorStats:
    """Статистика конструктора"""
    total_patterns: int
    exact_hits: int
    pattern_hits: int
    llm_calls: int

class QueryConstructor:
    """Конструктор SQL запросов с обучением"""
    
    def __init__(self, llm_client=None):
        self.llm = llm_client
        
        # Структуры данных
        self.exact_cache: Dict[str, str] = {}
        self.patterns: Dict[str, Dict] = {}
        self.word_index: Dict[str, Set[str]] = defaultdict(set)
        
        # Конфигурация
        self.stop_words = {'и', 'в', 'с', 'по', 'за', 'у', 'о', 'от', 'есть', 'всего', 'x', 'id'}
        
        # Статистика
        self.stats = {
            'exact_hits': 0,
            'pattern_hits': 0,
            'llm_calls': 0,
            'new_patterns': 0
        }
        
        # Пути к файлам
        self.cache_file = Path("query_cache.json")
        self.patterns_file = Path("learned_patterns.json")
        
        # Загрузка сохранённых данных
        #self._load_data()
        
        # Предзагрузка примеров ТЗ
        #self._init_tz_patterns()  # ← ЭТОТ МЕТОД ВЫЗЫВАЕТСЯ
        
        logger.info(f"Конструктор инициализирован. Паттернов: {len(self.patterns)}")
    
    def _init_tz_patterns(self):  # ← ДОБАВЬ ЭТОТ МЕТОД!
        """Предзагрузка примеров из ТЗ"""
        tz_examples = [
        #("Сколько всего видео есть в системе?", 
       # "SELECT COUNT(*) FROM videos"),
        #("видео креатора с по ноября", 
       # "SELECT COUNT(*) FROM videos WHERE creator_id = '{ID}' AND DATE(video_created_at) BETWEEN '{DATE1}' AND '{DATE2}'"),
        #("Сколько видео набрало больше 100000 просмотров?", 
       # "SELECT COUNT(*) FROM videos WHERE views_count > {NUMBER}"),
        
        # Отрицательные дельты
       # ("Сколько замеров статистики с отрицательным приростом просмотров?", 
        #"SELECT COUNT(*) FROM video_snapshots WHERE delta_views_count < 0"),
        
        # Сумма просмотров за месяц
       # ("Какое суммарное количество просмотров набрали все видео, опубликованные в июне 2025 года?", 
        #"SELECT SUM(views_count) FROM videos WHERE EXTRACT(YEAR FROM video_created_at) = {YEAR} AND EXTRACT(MONTH FROM video_created_at) = {MONTH}"),
        
        # РАЗНЫЕ ДНИ (ГЛАВНЫЙ ПАТТЕРН!)
        #("Для креатора с id X посчитай разные дни ноября", 
        #"SELECT COUNT(DISTINCT DATE(video_created_at)) FROM videos WHERE creator_id = '{ID}' AND EXTRACT(YEAR FROM video_created_at) = {YEAR} AND EXTRACT(MONTH FROM video_created_at) = {MONTH}"),
        
        # Креатор с просмотрами > N
       # ("Сколько видео у креатора с id X набрали больше N просмотров", 
        #"SELECT COUNT(*) FROM videos WHERE creator_id = '{ID}' AND views_count > {NUMBER}"),
        ]
        
        for query, sql in tz_examples:
            words = self._extract_words(query)
            self._learn_from_example(query, sql, words, 'tz')
            self.exact_cache[query] = sql
    
    async def build_sql_async(self, user_query: str, use_llm: bool = True) -> str:
        """Асинхронная версия build_sql с LLM"""
        print(f"\n{'='*60}")
        print(f"DEBUG build_sql_async: Начало обработки запроса")
        print(f"Запрос: '{user_query}'")
        
                    # 1. Проверяем точный кэш
        if user_query in self.exact_cache:
           # print(f"DEBUG: Точное совпадение в кэше")
           # self.stats['exact_hits'] += 1
           # sql = self.exact_cache[user_query]
            #print(f"SQL из кэша: {sql}")
           # return sql
    
        # 2. Ищем похожий паттерн
            words = self._extract_words(user_query)
            print(f"DEBUG: Извлеченные слова: {words}")
        
            pattern = self._find_pattern(words)
        
        if pattern:
            print(f"DEBUG: Найден паттерн!")
            print(f"Шаблон: {pattern['template']}")
            print(f"Слова паттерна: {pattern['words']}")
            self.stats['pattern_hits'] += 1
            sql = self._fill_template(pattern['template'], user_query)
            print(f"SQL после заполнения шаблона: {sql}")
            
            # Если шаблон заполнен - сохраняем в кэш
            if '{' not in sql:
                self.exact_cache[user_query] = sql
                pattern['count'] += 1
                self._save_cache()
            return sql
        
        print(f"DEBUG: Паттерн не найден, переход к LLM")
        
        # 3. Если есть LLM и разрешено - используем её
        if use_llm and self.llm:
            print(f"DEBUG: Вызываю LLM для запроса")
            self.stats['llm_calls'] += 1
            logger.info(f"Запрос к LLM: {user_query[:50]}...")
            
            try:
                result = await self.llm.ask(user_query)
                if result and result.sql:
                    sql = result.sql
                    print(f"DEBUG: LLM вернул SQL: {sql}")
                    # Учимся на ответе LLM
                    words = self._extract_words(user_query)
                    self._learn_from_example(user_query, sql, words, 'llm')
                    self.exact_cache[user_query] = sql
                    self._save_all_data()
                    return sql
                else:
                    print(f"DEBUG: LLM не вернул результат")
            except Exception as e:
                logger.error(f"Ошибка LLM: {e}")
                print(f"DEBUG: Ошибка LLM: {e}")
        
        # 4. Fallback
        print(f"DEBUG: Использую fallback")
        return "SELECT COUNT(*) FROM videos"
    
    def _extract_words(self, query: str) -> Set[str]:
        """Извлекаем нормализованные слова"""
        print(f"DEBUG _extract_words: Начало, запрос='{query}'")
        query_lower = query.lower()
        clean = query_lower.replace('?', ' ').replace('.', ' ').replace(',', ' ')
        
        print(f"DEBUG _extract_words: До очистки: '{clean}'")
        
        # ВАЖНОЕ ИСПРАВЛЕНИЕ: сохраняем ID креатора (32 hex символа)
        clean = re.sub(r'[a-f0-9]{32}', ' IDCREATOR ', clean)
        
        # Сохраняем UUID видео
        clean = re.sub(r'[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}', ' IDVIDEO ', clean)
        
        # Удаляем обычные числа
        clean = re.sub(r'\b\d{1,4}\b', ' ', clean)
        clean = re.sub(r'\d{4}-\d{2}-\d{2}', ' ', clean)
        
        print(f"DEBUG _extract_words: После очистки: '{clean}'")
        
        # Разбиваем на слова
        all_words = clean.split()
        print(f"DEBUG _extract_words: Все слова: {all_words}")
        
        # Фильтруем
        filtered = set()
        for word in all_words:
            if word in self.stop_words:
                continue
            if len(word) < 3:
                continue
            filtered.add(word)
        
        print(f"DEBUG _extract_words: Итоговые слова: {filtered}")
        return filtered
    
    
    def _find_pattern(self, words: Set[str]) -> Optional[Dict]:
        """Ищет похожий паттерн - с приоритетом ключевых комбинаций"""
        print(f"\nDEBUG _find_pattern: Начало поиска")
        print(f"Слова запроса: {words}")
        print(f"Всего паттернов: {len(self.patterns)}")
        
        if not words:
            print("DEBUG _find_pattern: Нет слов для поиска")
            return None
        
        # ВАЖНОЕ ИСПРАВЛЕНИЕ: Проверяем наличие критических слов
        has_creator = any(word in words for word in ['IDCREATOR', 'креатора', 'креатор'])
        has_hours = any(word in words for word in ['промежутке', 'часов', 'часа', '10', '15'])
        has_date = any(word in words for word in ['ноября', 'декабря', 'января', 'дата'])
        
        print(f"DEBUG: Критические слова - креатор: {has_creator}, часы: {has_hours}, дата: {has_date}")
        
        # 1. Ищем паттерн с креатором И часами И датой
        if has_creator and has_hours and has_date:
            print("DEBUG: Ищем паттерн с креатором+часами+датой")
            for pattern_hash, pattern in self.patterns.items():
                template = pattern.get('template', '')
                pattern_words = set(pattern['words'])
                
                if 'креатора' in pattern_words and 'промежутке' in pattern_words:
                    print(f"DEBUG: Найден паттерн с креатором и промежутком: {pattern_hash[:8]}")
                    return pattern
        
        # 2. Ищем паттерн с креатором И датой
        if has_creator and has_date:
            print("DEBUG: Ищем паттерн с креатором+датой")
            for pattern_hash, pattern in self.patterns.items():
                pattern_words = set(pattern['words'])
                if 'креатора' in pattern_words and not 'промежутке' in pattern_words:
                    print(f"DEBUG: Найден паттерн с креатором: {pattern_hash[:8]}")
                    return pattern
        
        # 3. Ищем паттерн с часами И датой
        if has_hours and has_date:
            print("DEBUG: Ищем паттерн с часами+датой")
            for pattern_hash, pattern in self.patterns.items():
                pattern_words = set(pattern['words'])
                if 'промежутке' in pattern_words:
                    print(f"DEBUG: Найден паттерн с часами: {pattern_hash[:8]}")
                    return pattern
        
        # 4. Стандартный поиск (старая логика)
        print(f"DEBUG: Стандартный поиск паттернов")
        best_pattern = None
        best_score = 0
        
        for pattern_hash, pattern in self.patterns.items():
            pattern_words = set(pattern['words'])
            
            # Если все слова шаблона есть в запросе
            if pattern_words.issubset(words):
                common = words.intersection(pattern_words)
                total_in_pattern = len(pattern_words)
                coverage = len(common) / total_in_pattern if total_in_pattern > 0 else 0
                
                print(f"  Паттерн {pattern_hash[:8]}: покрытие {coverage:.2f}")
                
                if coverage >= 0.9:
                    score = coverage + (0.1 if len(common) == total_in_pattern else 0)
                    
                    if score > best_score:
                        best_score = score
                        best_pattern = pattern
                        print(f"    Новый лучший: score={score:.2f}")
        
        if best_pattern:
            print(f"DEBUG: Найден лучший паттерн со score={best_score:.2f}")
        else:
            print(f"DEBUG: Подходящий паттерн не найден")
        
        return best_pattern
    
    
    def _fill_template(self, template: str, query: str) -> str:
        """Заполняет шаблон параметрами"""
        print(f"\nDEBUG _fill_template: Начало")
        print(f"Шаблон: '{template}'")
        print(f"Запрос: '{query}'")
        
        sql = template
        query_lower = query.lower()
        
        # Извлекаем ВСЕ параметры
        params = {}
        dates = []
        found_range = False
        
        # ==================== 1. МЕСЯЦ И ГОД ИЗ ТЕКСТА ====================
        # Паттерн: "в ноябре 2025 года" или "ноября 2025 года"
        month_year_patterns = [
            r'в\s+(\w+)\s+(\d{4})\s+года',      # в ноябре 2025 года
            r'в\s+(\w+)\s+(\d{4})',             # в ноябре 2025
            r'за\s+(\w+)\s+(\d{4})',            # за ноябрь 2025
            r'(\w+)\s+(\d{4})\s+года',          # ноября 2025 года
            r'(\w+)\s+(\d{4})',                 # ноября 2025
            r'опубликованные\s+в\s+(\w+)\s+(\d{4})',
        ]
        
        for pattern in month_year_patterns:
            match = re.search(pattern, query_lower)
            if match:
                month_ru = match.group(1)
                year = match.group(2)
                month_num = self._rus_month_to_num(month_ru)
                
                if month_num:
                    params['{YEAR}'] = year
                    params['{MONTH}'] = str(month_num)
                    print(f"  Найден месяц и год: {month_ru} {year} -> месяц={month_num}")
                    break  # Нашли - выходим
        
        # ==================== 2. ДИАПАЗОНЫ ДАТ ====================
        date_range_patterns = [
            r'с\s+(\d{1,2})\s+(\w+)\s+(\d{4})\s+по\s+(\d{1,2})\s+(\w+)\s+(\d{4})',
            r'с\s+(\d{1,2})\s+(\w+)\s+(\d{4})\s+до\s+(\d{1,2})\s+(\w+)\s+(\d{4})',
            r'от\s+(\d{1,2})\s+(\w+)\s+(\d{4})\s+до\s+(\d{1,2})\s+(\w+)\s+(\d{4})',
            r'(\d{1,2})\s+(\w+)\s+(\d{4})\s+по\s+(\d{1,2})\s+(\w+)\s+(\d{4})',
            r'(\d{1,2})\s+(\w+)\s+(\d{4})\s+-\s+(\d{1,2})\s+(\w+)\s+(\d{4})',
            r'(\d{1,2})\s+(\w+)\s+(\d{4})\s+до\s+(\d{1,2})\s+(\w+)\s+(\d{4})',
        ]
        
        for pattern in date_range_patterns:
            match = re.search(pattern, query_lower)
            if match and len(match.groups()) == 6:
                day1, month1_ru, year1, day2, month2_ru, year2 = match.groups()
                month1_num = self._rus_month_to_num(month1_ru)
                month2_num = self._rus_month_to_num(month2_ru)
                
                if month1_num and month2_num:
                    date1 = f"{year1}-{month1_num:02d}-{int(day1):02d}"
                    date2 = f"{year2}-{month2_num:02d}-{int(day2):02d}"
                    
                    params['{DATE1}'] = date1
                    params['{DATE2}'] = date2
                    dates.extend([date1, date2])
                    print(f"  Найден диапазон дат: {date1} - {date2}")
                    found_range = True
                    break
        
        # ==================== 3. ОТДЕЛЬНЫЕ ДАТЫ ====================
        if not found_range:
            # Паттерн: "28 ноября 2025"
            date_matches = re.findall(r'(\d{1,2})\s+(\w+)\s+(\d{4})', query_lower)
            for day, month_ru, year in date_matches:
                month_num = self._rus_month_to_num(month_ru)
                if month_num:
                    sql_date = f"{year}-{month_num:02d}-{int(day):02d}"
                    dates.append(sql_date)
                    print(f"  Найдена дата: {sql_date}")
        
        # SQL даты: "2025-11-28"
        sql_dates = re.findall(r'\d{4}-\d{2}-\d{2}', query)
        dates.extend(sql_dates)
        
        # Сохраняем даты в params
        if dates:
            if len(dates) >= 2:
                params['{DATE1}'] = dates[0]
                params['{DATE2}'] = dates[1]
            if len(dates) >= 1:
                params['{DATE}'] = dates[0]
        
        # ==================== 4. ЧИСЛА ====================
        numbers = re.findall(r'\b\d+\b', query)
        filtered_numbers = []
        for num in numbers:
            if dates and any(num in date for date in dates):
                continue
            filtered_numbers.append(num)
        
        if filtered_numbers:
            print(f"  Найдены числа: {filtered_numbers}")
            for i, num in enumerate(filtered_numbers, 1):
                params[f'{{NUMBER{i}}}'] = num
            params['{NUMBER}'] = filtered_numbers[0]
        
        # ==================== 5. ID КРЕАТОРА ====================
        creator_id_match = re.search(r'креатор[ауе]?\s+(?:с\s+)?id\s+([a-f0-9]{32})', query_lower)
        if creator_id_match:
            params['{ID}'] = creator_id_match.group(1)
            print(f"  Найден ID креатора: {creator_id_match.group(1)}")
        else:
            id_patterns = [
                r'id\s+([a-f0-9]{32})',
                r'креатора\s+([a-f0-9]{32})',
            ]
            for pattern in id_patterns:
                id_match = re.search(pattern, query_lower)
                if id_match:
                    params['{ID}'] = id_match.group(1)
                    print(f"  Найден ID: {id_match.group(1)}")
                    break
        
        # ==================== 6. ЗАМЕНА ПЛЕЙСХОЛДЕРОВ ====================
        print(f"\nDEBUG _fill_template: Умная замена плейсхолдеров...")
        print(f"DEBUG: params перед заменой: {params}")
        print(f"DEBUG: SQL перед заменой: '{sql}'")
        
        # 6.1. Сначала специальные плейсхолдеры
        special_placeholders = ['{YEAR}', '{MONTH}', '{DATE1}', '{DATE2}']
        for placeholder in special_placeholders:
            if placeholder in sql:
                print(f"DEBUG: Проверяю {placeholder} -> в params: {placeholder in params}")
                if placeholder in params:
                    sql = sql.replace(placeholder, params[placeholder])
                    print(f"  Заменен {placeholder} -> {params[placeholder]}")
                else:
                    print(f"  ВНИМАНИЕ: {placeholder} в SQL, но нет в params!")
        
        # 6.2. {NUMBER1}, {NUMBER2} и т.д.
        for placeholder, value in params.items():
            if placeholder.startswith('{NUMBER') and placeholder in sql:
                sql = sql.replace(placeholder, value)
                print(f"  Заменен {placeholder} -> {value}")
        
        # 6.3. Остальные плейсхолдеры
        for placeholder, value in params.items():
            if placeholder in sql and placeholder not in special_placeholders and not placeholder.startswith('{NUMBER'):
                sql = sql.replace(placeholder, value)
                print(f"  Заменен {placeholder} -> {value}")
        
        # 6.4. Остались {NUMBER} - заменяем первым числом
        while '{NUMBER}' in sql:
            if filtered_numbers:
                sql = sql.replace('{NUMBER}', filtered_numbers[0], 1)
                print(f"  Заменен {{NUMBER}} -> {filtered_numbers[0]}")
            else:
                sql = sql.replace('{NUMBER}', '0', 1)
                print(f"  Заменен {{NUMBER}} -> 0")
        
        print(f"\nDEBUG _fill_template: Итоговый SQL:")
        print(f"  {sql}")
        return sql
    
    def _rus_month_to_num(self, month_ru: str) -> Optional[int]:
        """Конвертирует русский месяц в число"""
        month_map = {
            # родительный падеж
            'января': 1, 'февраля': 2, 'марта': 3, 'апреля': 4,
            'мая': 5, 'июня': 6, 'июля': 7, 'августа': 8,
            'сентября': 9, 'октября': 10, 'ноября': 11, 'декабря': 12,
            # именительный падеж  
            'январь': 1, 'февраль': 2, 'март': 3, 'апрель': 4,
            'май': 5, 'июнь': 6, 'июль': 7, 'август': 8,
            'сентябрь': 9, 'октябрь': 10, 'ноябрь': 11, 'декабрь': 12,
            # предложный падеж (В июне, В июле)
            'январе': 1, 'феврале': 2, 'марте': 3, 'апреле': 4,
            'мае': 5, 'июне': 6, 'июле': 7, 'августе': 8,
            'сентябре': 9, 'октябре': 10, 'ноябре': 11, 'декабре': 12,
        }
        
        month_lower = month_ru.lower()
        result = month_map.get(month_lower)
        print(f"DEBUG _rus_month_to_num: '{month_ru}' -> '{month_lower}' -> {result}")
        return result
        
    def _learn_from_example(self, query: str, sql: str, words: Set[str], source: str = 'manual'):
        """Сохраняет новый паттерн"""
        if not words:
            return
        
        template = self._generalize_sql(sql)
        
        pattern_key = self._make_pattern_key(words)
        
        if pattern_key in self.patterns:
            self.patterns[pattern_key]['count'] += 1
            self.patterns[pattern_key]['examples'].append(query)
        else:
            self.patterns[pattern_key] = {
                'words': list(words),
                'template': template,
                'count': 1,
                'examples': [query],
                'source': source,
                'created_at': datetime.now().isoformat()
            }
            
            for word in words:
                self.word_index[word].add(pattern_key)
            
            self.stats['new_patterns'] += 1
    
    def _generalize_sql(self, sql: str) -> str:
        """Создаёт шаблон из конкретного SQL"""
        print(f"\nDEBUG _generalize_sql: Создание шаблона")
        print(f"Исходный SQL: '{sql}'")
        
        template = sql
        
        # Заменяем MySQL функции на PostgreSQL
        template = template.replace("DATE_FORMAT(created_at, '%Y-%m-%d')", "DATE(created_at)")
        template = template.replace("HOUR(created_at)", "EXTRACT(HOUR FROM created_at)")
        
        # Даты в одинарных кавычках
        template = re.sub(r"'(\d{4}-\d{2}-\d{2})'", "'{DATE}'", template)
        
        # ВАЖНОЕ ИСПРАВЛЕНИЕ: Разные плейсхолдеры для BETWEEN
        # Находим все даты в шаблоне
        dates = re.findall(r"'\{DATE\}'", template)
        if len(dates) >= 2 and 'BETWEEN' in template:
            # Заменяем первую дату на {DATE1}, вторую на {DATE2}
            template = template.replace("'{DATE}'", "'{DATE1}'", 1)
            template = template.replace("'{DATE}'", "'{DATE2}'", 1)
        
        # Год и месяц в EXTRACT
        if 'EXTRACT(YEAR FROM video_created_at)' in template:
            template = re.sub(r'EXTRACT\(YEAR FROM video_created_at\)\s*=\s*\d{4}', 
                            'EXTRACT(YEAR FROM video_created_at) = {YEAR}', template)
        
        if 'EXTRACT(MONTH FROM video_created_at)' in template:
            template = re.sub(r'EXTRACT\(MONTH FROM video_created_at\)\s*=\s*\d{1,2}', 
                            'EXTRACT(MONTH FROM video_created_at) = {MONTH}', template)
        
        # ВАЖНОЕ ИСПРАВЛЕНИЕ: НЕ заменяем 0 в условиях delta_views_count
        # Сохраняем delta_views_count < 0 и delta_views_count > 0 как есть
        delta_patterns = [
            (r'delta_views_count\s*<\s*0', 'delta_views_count < 0'),
            (r'delta_views_count\s*>\s*0', 'delta_views_count > 0'),
            (r'delta_views_count\s*<=\s*0', 'delta_views_count <= 0'),
            (r'delta_views_count\s*>=\s*0', 'delta_views_count >= 0'),
        ]
        
        for pattern, replacement in delta_patterns:
            template = re.sub(pattern, replacement, template, flags=re.IGNORECASE)
        
        # Общие числа, но пропускаем уже обработанные
        # Находим все числа в условиях сравнения
        def replace_number(match):
            operator = match.group(1)
            number = match.group(2)
            
            # Если это 0 в delta_views_count - уже обработали выше
            if number == '0' and 'delta_views_count' in template:
                return f'{operator} 0'
            
            # Если это число в EXTRACT условиях - оставляем как есть
            if 'EXTRACT(YEAR' in template or 'EXTRACT(MONTH' in template:
                return match.group(0)
                
            return f'{operator} {{NUMBER}}'
        
        template = re.sub(r'([<>]=?|=)\s*(\d+)', replace_number, template)
        
        # ID (креаторы, видео)
        template = re.sub(r"'([a-f0-9]{32})'", "'{ID}'", template)  # 32 hex символа
        template = re.sub(r"'([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})'", "'{VIDEO_ID}'", template)
        
        # Часы в BETWEEN
        template = re.sub(r'BETWEEN\s+(\d+)\s+AND\s+(\d+)', r'BETWEEN {HOUR1} AND {HOUR2}', template)
        
        # Часы в >= и < условиях
        template = re.sub(r'EXTRACT\(HOUR FROM vs\.created_at\)\s*>=\s*(\d+)', 
                        r'EXTRACT(HOUR FROM vs.created_at) >= {HOUR1}', template)
        template = re.sub(r'EXTRACT\(HOUR FROM vs\.created_at\)\s*<\s*(\d+)', 
                        r'EXTRACT(HOUR FROM vs.created_at) < {HOUR2}', template)
        
        # Удаляем лишний текст после SQL (иногда LLM добавляет объяснения)
        if ';' in template:
            template = template.split(';')[0] + ';'
        
        print(f"Шаблон после обобщения: '{template}'")
        return template
    
    def _check_delta_zero(self, number: str, template: str) -> str:
        """Проверяет, является ли это 0 в условии delta_views_count"""
        # Если это 0 и в шаблоне есть delta_views_count, оставляем 0
        if number == '0' and 'delta_views_count' in template.lower():
            return '0'
        return '{NUMBER}'

    def _make_pattern_key(self, words: Set[str]) -> str:
        """Создаёт ключ паттерна"""
        sorted_words = sorted(words)
        key_str = " ".join(sorted_words)
        return hashlib.md5(key_str.encode()).hexdigest()[:16]
    
    def _load_data(self):
        """Загружает сохранённые данные"""
      #  try:
           # if self.cache_file.exists():
              #  with open(self.cache_file, 'r', encoding='utf-8') as f:
               #     data = json.load(f)
               #     self.exact_cache = data.get('exact_cache', {})
            
           # if self.patterns_file.exists():
                #with open(self.patterns_file, 'r', encoding='utf-8') as f:
                 #   data = json.load(f)
                 #   patterns_data = data.get('patterns', {})
                    
                  #  self.patterns.clear()
                  #  self.word_index.clear()
                    
                   # for pattern_key, pattern in patterns_data.items():
                    #    self.patterns[pattern_key] = pattern
                     #   for word in pattern.get('words', []):
                      #      self.word_index[word].add(pattern_key)
                    
       # except Exception as e:
           # logger.error(f"Ошибка загрузки: {e}")
        pass
    
    def _save_cache(self):
        """Сохраняет кэш"""
       # try:
          #  data = {
               # 'exact_cache': self.exact_cache,
                #'updated_at': datetime.now().isoformat()
           # }
            #with open(self.cache_file, 'w', encoding='utf-8') as f:
                #json.dump(data, f, ensure_ascii=False, indent=2)
       # except Exception as e:
           # logger.error(f"Ошибка сохранения кэша: {e}")
    
    def _save_all_data(self):
        """Сохраняет все данные"""
       # try:
            #patterns_data = {}
          #  for key, pattern in self.patterns.items():
            #    patterns_data[key] = pattern
            
           # data = {
          #      'patterns': patterns_data,
          #      'updated_at': datetime.now().isoformat()
          #  }
            
            # with open(self.patterns_file, 'w', encoding='utf-8') as f:
          #  json.dump(data, f, ensure_ascii=False, indent=2)
           # self._save_cache()
            
       # except Exception as e:
           # logger.error(f"Ошибка сохранения данных: {e}")
    
    def get_stats(self) -> ConstructorStats:
        """Возвращает статистику"""
        return ConstructorStats(
            total_patterns=len(self.patterns),
            exact_hits=self.stats['exact_hits'],
            pattern_hits=self.stats['pattern_hits'],
            llm_calls=self.stats['llm_calls']
        )
    
    def add_manual_pattern(self, query: str, sql: str):
        """Ручное добавление паттерна"""
        words = self._extract_words(query)
        self._learn_from_example(query, sql, words, 'manual')
        self.exact_cache[query] = sql
        self._save_all_data()
        logger.info(f"Добавлен ручной паттерн: {len(words)} слов")
    
    def clear_cache(self):
        """Очищает кэш"""
        self.exact_cache = {}
        self._save_cache()
        logger.info("Кэш очищен")