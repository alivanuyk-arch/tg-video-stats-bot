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
        self._load_data()
        
        # Предзагрузка примеров ТЗ
        self._init_tz_patterns()
        
        logger.info(f"Конструктор инициализирован. Паттернов: {len(self.patterns)}")
    
    def _init_tz_patterns(self):
        """Предзагрузка примеров из ТЗ"""
        tz_examples = [
            ("Сколько всего видео есть в системе?", 
             "SELECT COUNT(*) FROM videos"),
            
            ("Сколько видео набрало больше 100000 просмотров?", 
             "SELECT COUNT(*) FROM videos WHERE views_count > {NUMBER}"),
            
            ("На сколько просмотров выросли все видео 28 ноября 2025?", 
             "SELECT SUM(delta_views_count) FROM video_snapshots WHERE DATE(created_at) = '{DATE}'"),
            
            ("Сколько разных видео получали новые просмотры 27 ноября 2025?", 
             "SELECT COUNT(DISTINCT video_id) FROM video_snapshots WHERE DATE(created_at) = '{DATE}' AND delta_views_count > 0"),
            
            ("Сколько видео у креатора с id abc123?", 
             "SELECT COUNT(*) FROM videos WHERE creator_id = '{ID}'"),
            
            ("Сколько видео у креатора с id abc123 вышло с 1 по 5 ноября 2025?", 
             "SELECT COUNT(*) FROM videos WHERE creator_id = '{ID}' AND DATE(video_created_at) BETWEEN '{DATE1}' AND '{DATE2}'"),
            
            ("Сколько видео у креатора с id abc123 вышло с 1 ноября 2025 по 28 ноября 2025 включительно?", 
             "SELECT COUNT(*) FROM videos WHERE creator_id = '{ID}' AND DATE(video_created_at) BETWEEN '{DATE1}' AND '{DATE2}'"),
            ("Какое суммарное количество просмотров набрали все видео, опубликованные в июне 2025 года?", 
             "SELECT SUM(views_count) FROM videos WHERE EXTRACT(YEAR FROM video_created_at) = {YEAR} AND EXTRACT(MONTH FROM video_created_at) = {MONTH}"),
            
            # Альтернативный вариант:
            ("Какое суммарное количество просмотров набрали все видео, опубликованные в июне 2025 года?", 
             "SELECT SUM(views_count) FROM videos WHERE EXTRACT(YEAR FROM video_created_at) = 2025 AND EXTRACT(MONTH FROM video_created_at) = 6"),
            ("На сколько просмотров суммарно выросли все видео креатора с id X в промежутке с 10:00 до 15:00 28 ноября 2025 года?",
             "SELECT SUM(delta_views_count) FROM video_snapshots vs JOIN videos v ON vs.video_id = v.id WHERE v.creator_id = '{ID}' AND DATE(vs.created_at) = '{DATE}' AND EXTRACT(HOUR FROM vs.created_at) >= {HOUR1} AND EXTRACT(HOUR FROM vs.created_at) < {HOUR2}"),
            
            ("Сколько замеров статистики с отрицательным приростом просмотров?", "SELECT COUNT(*) FROM video_snapshots WHERE delta_views_count < 0"),
             ("На сколько просмотров суммарно выросли все видео креатора с id X в промежутке с 10:00 до 15:00 28 ноября 2025 года?",
             "SELECT SUM(delta_views_count) FROM video_snapshots vs JOIN videos v ON vs.video_id = v.id WHERE v.creator_id = '{ID}' AND DATE(vs.created_at) = '{DATE}' AND EXTRACT(HOUR FROM vs.created_at) >= {HOUR1} AND EXTRACT(HOUR FROM vs.created_at) < {HOUR2}"),
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
            print(f"DEBUG: Точное совпадение в кэше")
            self.stats['exact_hits'] += 1
            sql = self.exact_cache[user_query]
            print(f"SQL из кэша: {sql}")
            return sql
        
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
        
        # 1. ПРОВЕРКА КЛЮЧЕВЫХ КОМБИНАЦИЙ (ВЫСОКИЙ ПРИОРИТЕТ)
        key_combinations = [
            # Отрицательные дельты - ВАЖНО!
            ({'отрицательным', 'замеров', 'статистики'}, "negative_delta"),
            # Часы + креатор
            ({'промежутке', 'креатора', 'часов'}, "hours_creator"),
            # Сумма просмотров
            ({'суммарное', 'просмотров', 'количество'}, "sum_views"),
            # Количество видео  
            ({'сколько', 'видео', 'есть'}, "count_videos"),
            # Креаторы
            ({'сколько', 'креаторов'}, "count_creators"),
        ]
        
        for keyword_set, pattern_type in key_combinations:
            if keyword_set.issubset(words):
                print(f"DEBUG: Найдена ключевая комбинация: {keyword_set} → {pattern_type}")
                # Ищем паттерн по типу
                for pattern_hash, pattern in self.patterns.items():
                    template = pattern.get('template', '')
                    if pattern_type == "negative_delta" and 'delta_views_count < 0' in template:
                        print(f"DEBUG: Возвращаем паттерн для отрицательных дельт")
                        return pattern
                    elif pattern_type == "hours_creator" and 'EXTRACT(HOUR FROM' in template:
                        print(f"DEBUG: Возвращаем паттерн с часами и креатором")
                        return pattern
                    elif pattern_type == "sum_views" and 'SUM(views_count)' in template:
                        print(f"DEBUG: Возвращаем паттерн суммы просмотров")
                        return pattern
                    elif pattern_type == "count_videos" and 'COUNT(*) FROM videos' in template and 'WHERE' not in template:
                        print(f"DEBUG: Возвращаем паттерн количества видео")
                        return pattern
                    elif pattern_type == "count_creators" and 'COUNT(DISTINCT creator_id)' in template:
                        print(f"DEBUG: Возвращаем паттерн количества креаторов")
                        return pattern
        
        # 2. СТАНДАРТНЫЙ ПОИСК
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
                
                if coverage >= 0.8:
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
            
            # 1. Даты
            dates = []
            
            # Паттерн: "28 ноября 2025"
            date_matches = re.findall(r'(\d{1,2})\s+(\w+)\s+(\d{4})', query_lower)
            
            for day, month_ru, year in date_matches:
                month_num = self._rus_month_to_num(month_ru)
                if month_num:
                    sql_date = f"{year}-{month_num:02d}-{int(day):02d}"
                    dates.append(sql_date)
            
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
            
            # 2. Часы
            hour_patterns = [
                r'с\s+(\d{1,2}):00\s+до\s+(\d{1,2}):00',
                r'в\s+промежутке\s+с\s+(\d{1,2}):00\s+до\s+(\d{1,2}):00',
                r'между\s+(\d{1,2}):00\s+и\s+(\d{1,2}):00',
                r'(\d{1,2}):00\s*-\s*(\d{1,2}):00',
                r'(\d{1,2})\s+до\s+(\d{1,2})'
            ]
            
            hour_match = None
            for pattern in hour_patterns:
                hour_match = re.search(pattern, query_lower)
                if hour_match:
                    break
            
            if hour_match:
                try:
                    hour1 = hour_match.group(1)
                    hour2 = hour_match.group(2)
                    
                    # Для BETWEEN используем hour1 и hour2-1
                    params['{HOUR1}'] = hour1
                    params['{HOUR2}'] = hour2
                    
                    # Для условий >= и < используем как есть
                    params['{START_HOUR}'] = hour1
                    params['{END_HOUR}'] = hour2
                    
                except (ValueError, IndexError) as e:
                    print(f"  Ошибка обработки часов: {e}")
            
            # 3. Числа - ВАЖНОЕ ИСПРАВЛЕНИЕ: находим ВСЕ числа
            numbers = re.findall(r'\b\d+\b', query)
            
            if numbers:
                print(f"  Найдены числа: {numbers}")
                
                # Сохраняем все числа для последовательной замены
                # Если чисел больше чем нужно - используем первое и второе
                if len(numbers) >= 2:
                    # Первое число (скорее всего год)
                    params['{YEAR}'] = numbers[0]
                    # Второе число (или что-то другое)
                    params['{NUMBER2}'] = numbers[1]
                
                # Для простых {NUMBER} используем все найденные числа
                # Критическая часть: если есть месяц в тексте, заменяем его номером
                month_match = re.search(r'в\s+(\w+)\s+(\d{4})', query_lower)
                if month_match:
                    month_ru = month_match.group(1)
                    year = month_match.group(2)
                    month_num = self._rus_month_to_num(month_ru)
                    if month_num:
                        params['{YEAR}'] = year
                        params['{MONTH}'] = str(month_num)
                        print(f"  Обнаружен месяц и год: {month_ru} {year} -> месяц={month_num}")
            
               
            # 4. ВАЖНОЕ ИСПРАВЛЕНИЕ: поиск ID креатора (32 hex символа)
            creator_id_match = re.search(r'креатор[ауе]?\s+(?:с\s+)?id\s+([a-f0-9]{32})', query_lower)
            if creator_id_match:
                params['{ID}'] = creator_id_match.group(1)
                print(f"  Найден ID креатора: {creator_id_match.group(1)}")
            else:
                # Альтернативные паттерны
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
            
            # 5. УМНАЯ ЗАМЕНА ПЛЕЙСХОЛДЕРОВ
            print(f"\nDEBUG _fill_template: Умная замена плейсхолдеров...")
            original_sql = sql
            
            # 5.1. Сначала заменяем специальные плейсхолдеры
            if '{YEAR}' in sql and '{YEAR}' in params:
                sql = sql.replace('{YEAR}', params['{YEAR}'])
                print(f"  Заменен {{YEAR}} -> {params['{YEAR}']}")
            
            if '{MONTH}' in sql and '{MONTH}' in params:
                sql = sql.replace('{MONTH}', params['{MONTH}'])
                print(f"  Заменен {{MONTH}} -> {params['{MONTH}']}")
            
            if '{NUMBER2}' in sql and '{NUMBER2}' in params:
                sql = sql.replace('{NUMBER2}', params['{NUMBER2}'])
                print(f"  Заменен {{NUMBER2}} -> {params['{NUMBER2}']}")
            
            # 5.2. Последовательная замена {NUMBER} разными числами из запроса
            if '{NUMBER}' in sql and numbers:
                # Подсчитываем сколько {NUMBER} в SQL
                num_placeholders = sql.count('{NUMBER}')
                
                if num_placeholders == 1:
                    # Если только один плейсхолдер, используем первое число
                    sql = sql.replace('{NUMBER}', numbers[0])
                    print(f"  Заменен {{NUMBER}} -> {numbers[0]}")
                elif num_placeholders >= 2 and len(numbers) >= 2:
                    # Если два плейсхолдера, используем два разных числа
                    # Первая замена
                    sql = sql.replace('{NUMBER}', numbers[0], 1)
                    print(f"  Заменен {{NUMBER}}#1 -> {numbers[0]}")
                    # Вторая замена
                    sql = sql.replace('{NUMBER}', numbers[1], 1)
                    print(f"  Заменен {{NUMBER}}#2 -> {numbers[1]}")
                    # Если есть еще плейсхолдеры, используем последнее число
                    while '{NUMBER}' in sql:
                        sql = sql.replace('{NUMBER}', numbers[-1], 1)
                        print(f"  Заменен оставшийся {{NUMBER}} -> {numbers[-1]}")
            
            # 5.3. Остальные замены
            for placeholder, value in params.items():
                if placeholder in sql and placeholder not in ['{YEAR}', '{MONTH}', '{NUMBER2}']:
                    # Для {NUMBER} уже обработали выше
                    if placeholder != '{NUMBER}':
                        sql = sql.replace(placeholder, value)
                        print(f"  Заменен {placeholder} -> {value}")
            
            # 6. Если остались необработанные {NUMBER} и есть год/месяц в запросе
            if '{NUMBER}' in sql:
                # Пробуем найти год и месяц по тексту
                month_year_match = re.search(r'в\s+(\w+)\s+(\d{4})', query_lower)
                if month_year_match:
                    month_ru = month_year_match.group(1)
                    year = month_year_match.group(2)
                    month_num = self._rus_month_to_num(month_ru)
                    
                    if month_num:
                        # Подсчитываем сколько {NUMBER} осталось
                        remaining_placeholders = sql.count('{NUMBER}')
                        
                        if remaining_placeholders == 2:
                            # Должно быть: год и месяц
                            sql = sql.replace('{NUMBER}', year, 1)
                            sql = sql.replace('{NUMBER}', str(month_num), 1)
                            print(f"  Автозамена: год={year}, месяц={month_num}")
                        elif remaining_placeholders == 1:
                            # Только один - вероятно год
                            sql = sql.replace('{NUMBER}', year)
                            print(f"  Автозамена: год={year}")
            
            # 7. Исправляем часовые диапазоны
            if '{HOUR1}' in original_sql and '{HOUR2}' in original_sql:
                if '{HOUR1}' in params and '{HOUR2}' in params:
                    hour1 = int(params['{HOUR1}'])
                    hour2 = int(params['{HOUR2}'])
                    
                    # Для BETWEEN: hour1 AND hour2-1
                    if 'BETWEEN' in sql and hour2 > hour1:
                        sql = re.sub(
                            r'BETWEEN\s+' + str(hour1) + r'\s+AND\s+' + str(hour2),
                            f'BETWEEN {hour1} AND {hour2 - 1}',
                            sql
                        )
            
            print(f"\nDEBUG _fill_template: Итоговый SQL:")
            print(f"  {sql}")
            return sql
    
    def _rus_month_to_num(self, month_ru: str) -> Optional[int]:
        """Конвертирует русский месяц в число"""
        month_map = {
            'января': 1, 'февраля': 2, 'марта': 3, 'апреля': 4,
            'мая': 5, 'июня': 6, 'июля': 7, 'августа': 8,
            'сентября': 9, 'октября': 10, 'ноября': 11, 'декабря': 12,
            'январь': 1, 'февраль': 2, 'март': 3, 'апрель': 4,
            'май': 5, 'июнь': 6, 'июль': 7, 'август': 8,
            'сентябрь': 9, 'октябрь': 10, 'ноябрь': 11, 'декабрь': 12
        }
        return month_map.get(month_ru.lower())
    
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
            
            # Даты
            template = re.sub(r"'(\d{4}-\d{2}-\d{2})'", "'{DATE}'", template)
            
            # Числа (но сохраняем 2025 и 6 как {YEAR} и {MONTH} для специфических случаев)
            if 'EXTRACT(YEAR FROM video_created_at)' in template:
                template = re.sub(r'EXTRACT\(YEAR FROM video_created_at\)\s*=\s*\d{4}', 
                                'EXTRACT(YEAR FROM video_created_at) = {YEAR}', template)
            
            if 'EXTRACT(MONTH FROM video_created_at)' in template:
                template = re.sub(r'EXTRACT\(MONTH FROM video_created_at\)\s*=\s*\d{1,2}', 
                                'EXTRACT(MONTH FROM video_created_at) = {MONTH}', template)
            
            # Общие числа
            template = re.sub(r'([<>]=?|=)\s*\d+', r'\1 {NUMBER}', template)
            
            # ID
            template = re.sub(r"'([\w-]{15,}|\w{5,})'", "'{ID}'", template)
            
            # Часы в BETWEEN
            template = re.sub(r'BETWEEN\s+(\d+)\s+AND\s+(\d+)', r'BETWEEN {HOUR1} AND {HOUR2}', template)
            
            # Часы в >= и < условиях
            template = re.sub(r'EXTRACT\(HOUR FROM vs\.created_at\)\s*>=\s*(\d+)', 
                            r'EXTRACT(HOUR FROM vs.created_at) >= {HOUR1}', template)
            template = re.sub(r'EXTRACT\(HOUR FROM vs\.created_at\)\s*<\s*(\d+)', 
                            r'EXTRACT(HOUR FROM vs.created_at) < {HOUR2}', template)
            
            print(f"Шаблон после обобщения: '{template}'")
            return template
    
    def _make_pattern_key(self, words: Set[str]) -> str:
        """Создаёт ключ паттерна"""
        sorted_words = sorted(words)
        key_str = " ".join(sorted_words)
        return hashlib.md5(key_str.encode()).hexdigest()[:16]
    
    def _load_data(self):
        """Загружает сохранённые данные"""
        try:
            if self.cache_file.exists():
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.exact_cache = data.get('exact_cache', {})
            
            if self.patterns_file.exists():
                with open(self.patterns_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    patterns_data = data.get('patterns', {})
                    
                    self.patterns.clear()
                    self.word_index.clear()
                    
                    for pattern_key, pattern in patterns_data.items():
                        self.patterns[pattern_key] = pattern
                        for word in pattern.get('words', []):
                            self.word_index[word].add(pattern_key)
                    
        except Exception as e:
            logger.error(f"Ошибка загрузки: {e}")
    
    def _save_cache(self):
        """Сохраняет кэш"""
        try:
            data = {
                'exact_cache': self.exact_cache,
                'updated_at': datetime.now().isoformat()
            }
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Ошибка сохранения кэша: {e}")
    
    def _save_all_data(self):
        """Сохраняет все данные"""
        try:
            patterns_data = {}
            for key, pattern in self.patterns.items():
                patterns_data[key] = pattern
            
            data = {
                'patterns': patterns_data,
                'updated_at': datetime.now().isoformat()
            }
            
            with open(self.patterns_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            self._save_cache()
            
        except Exception as e:
            logger.error(f"Ошибка сохранения данных: {e}")
    
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