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

# ===================== ДАТАКЛАССЫ =====================

@dataclass
class ConstructorStats:
    """Статистика конструктора"""
    total_patterns: int
    exact_hits: int
    pattern_hits: int
    llm_calls: int

# ===================== ОСНОВНОЙ КЛАСС =====================

class QueryConstructor:
    """Конструктор SQL запросов с обучением"""
    
    def __init__(self, llm_client=None):
        self.llm = llm_client
        
        # Структуры данных
        self.exact_cache: Dict[str, str] = {}
        self.patterns: Dict[str, Dict] = {}
        self.word_index: Dict[str, Set[str]] = defaultdict(set)
        
        # Конфигурация
        self.stop_words = {'и', 'в', 'с', 'по', 'за', 'у', 'о', 'от', 'есть', 'всего'}
        
        # Статистика
        self.stats = {
            'exact_hits': 0,
            'pattern_hits': 0,
            'llm_calls': 0,
            'new_patterns': 0
        }
        
        # Загрузка сохранённых данных
        self._load_data()
        
        # Предзагрузка примеров ТЗ
        self._init_tz_patterns()
        
        logger.info(f"Конструктор инициализирован. Паттернов: {len(self.patterns)}")
    
    def _init_tz_patterns(self):
        """Предзагрузка примеров из ТЗ"""
        tz_examples = [
            ("Сколько всего видео есть в системе?", "SELECT COUNT(*) FROM videos"),
            ("Сколько видео набрало больше 100000 просмотров?", "SELECT COUNT(*) FROM videos WHERE views_count > {NUMBER}"),
            ("На сколько просмотров выросли все видео 28 ноября 2025?", "SELECT SUM(delta_views_count) FROM video_snapshots WHERE DATE(created_at) = '{DATE}'"),
            ("Сколько разных видео получали новые просмотры 27 ноября 2025?", "SELECT COUNT(DISTINCT video_id) FROM video_snapshots WHERE DATE(created_at) = '{DATE}' AND delta_views_count > 0"),
            ("Сколько видео у креатора с id abc123?", "SELECT COUNT(*) FROM videos WHERE creator_id = '{ID}'"),
            ("Сколько видео у креатора с id abc123 вышло с 1 по 5 ноября 2025?", "SELECT COUNT(*) FROM videos WHERE creator_id = '{ID}' AND DATE(video_created_at) BETWEEN '{DATE1}' AND '{DATE2}'"),
            ("Сколько видео у креатора с id abc123 вышло с 1 ноября 2025 по 28 ноября 2025 включительно?", "SELECT COUNT(*) FROM videos WHERE creator_id = '{ID}' AND DATE(video_created_at) BETWEEN '{DATE1}' AND '{DATE2}'"),
            ("Сколько видео у креатора с id abc123 вышло с 1 ноября 2025 по 28 ноября 2025 включительно?", "SELECT COUNT(*) FROM videos WHERE creator_id = '{ID}' AND DATE(video_created_at) BETWEEN '{DATE1}' AND '{DATE2}'"),
            ("Какое суммарное количество просмотров набрали все видео, опубликованные в июне 2025 года?", "SELECT SUM(views_count) FROM videos WHERE EXTRACT(YEAR FROM video_created_at) = 2025 AND EXTRACT(MONTH FROM video_created_at) = 6"),
        ]
        
        for query, sql in tz_examples:
            words = self._extract_words(query)
            self._learn_from_example(query, sql, words, 'tz')
            self.exact_cache[query] = sql
    
    # ===================== ОСНОВНОЙ МЕТОД =====================
    
    async def build_sql_async(self, user_query: str, use_llm: bool = True) -> str:
        """Асинхронная версия build_sql с LLM"""
        # 1. Проверяем точный кэш
        if user_query in self.exact_cache:
            self.stats['exact_hits'] += 1
            return self.exact_cache[user_query]
        
        # 2. Если есть LLM и разрешено - используем её
        if use_llm and self.llm:
            print(f"DEBUG: Иду в LLM для запроса: {user_query}")
            self.stats['llm_calls'] += 1
            logger.info(f"Запрос к LLM: {user_query[:50]}...")
            
            try:
                result = await self.llm.ask(user_query)
                if result and result.sql:
                    sql = result.sql
                    # Учимся на ответе LLM
                    words = self._extract_words(user_query)
                    self._learn_from_example(user_query, sql, words, 'llm')
                    self.exact_cache[user_query] = sql
                    self._save_all_data()
                    return sql
            except Exception as e:
                logger.error(f"Ошибка LLM: {e}")
        
        # 3. Ищем похожий паттерн
        words = self._extract_words(user_query)
        pattern = self._find_pattern(words)
        
        if pattern:
            self.stats['pattern_hits'] += 1
            sql = self._fill_template(pattern['template'], user_query)
            
            # Если шаблон заполнен - сохраняем в кэш
            if '{' not in sql:
                self.exact_cache[user_query] = sql
                pattern['count'] += 1
                self._save_cache()
                return sql
        
        # 4. Fallback
        return "SELECT COUNT(*) FROM videos"
    
    def _extract_words(self, query: str) -> Set[str]:
        """Извлекаем нормализованные слова"""
        query_lower = query.lower()
        clean = query_lower.replace('?', ' ').replace('.', ' ').replace(',', ' ')
        
        # Удаляем числа и даты
        clean = re.sub(r'\d{4}-\d{2}-\d{2}', ' ', clean)
        clean = re.sub(r'\d{1,2}\s+\w+\s+\d{4}', ' ', clean)
        clean = re.sub(r'\d+', ' ', clean)
        
        # Разбиваем на слова
        all_words = clean.split()
        
        # Фильтруем
        filtered = set()
        for word in all_words:
            if word in self.stop_words:
                continue
            if len(word) < 3:
                continue
            filtered.add(word)
        
        return filtered
    
    def _find_pattern(self, words: Set[str]) -> Optional[Dict]:
        """Ищет похожий паттерн"""
        if not words:
            return None
        
        best_pattern = None
        best_score = 0
        
        for pattern_hash, pattern in self.patterns.items():
            pattern_words = set(pattern['words'])
            
            # Если все слова шаблона есть в запросе
            if pattern_words.issubset(words):
                common = len(words.intersection(pattern_words))
                total_in_pattern = len(pattern_words)
                coverage = common / total_in_pattern if total_in_pattern > 0 else 0
                
                if coverage >= 0.8:
                    score = coverage + (0.1 if common == total_in_pattern else 0)
                    if score > best_score:
                        best_score = score
                        best_pattern = pattern
        
        return best_pattern
    
    def _fill_template(self, template: str, query: str) -> str:
        """Заполняет шаблон параметрами"""
        sql = template
        query_lower = query.lower()
        
        # Даты
        dates = []
        date_matches = re.findall(r'(\d{1,2})\s+(\w+)\s+(\d{4})', query_lower)
        for day, month_ru, year in date_matches:
            month_num = self._rus_month_to_num(month_ru)
            if month_num:
                sql_date = f"{year}-{month_num:02d}-{int(day):02d}"
                dates.append(sql_date)
        
        # SQL даты
        sql_dates = re.findall(r'\d{4}-\d{2}-\d{2}', query)
        dates.extend(sql_dates)
        
        # Заменяем DATE плейсхолдеры
        if dates:
            if len(dates) >= 2 and '{DATE1}' in sql and '{DATE2}' in sql:
                sql = sql.replace('{DATE1}', dates[0]).replace('{DATE2}', dates[1])
            elif len(dates) >= 1 and '{DATE}' in sql:
                sql = sql.replace('{DATE}', dates[0])
        
        # Числа
        numbers = re.findall(r'\d+', query)
        if numbers and '{NUMBER}' in sql:
            sql = sql.replace('{NUMBER}', max(numbers, key=int))
        
        # ID
        id_match = re.search(r'(?:креатор[ауе]?\s+с\s+)?id\s+([\w-]+)', query_lower, re.IGNORECASE)
        if id_match and '{ID}' in sql:
            sql = sql.replace('{ID}', id_match.group(1))
        
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
    
    # ===================== ОБУЧЕНИЕ =====================
    
    def _learn_from_example(self, query: str, sql: str, words: Set[str], source: str = 'manual'):
        """Сохраняет новый паттерн"""
        if not words:
            return
        
        # Создаём шаблон
        template = self._generalize_sql(sql)
        
        # Создаём ключ паттерна
        pattern_key = self._make_pattern_key(words)
        
        # Сохраняем паттерн
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
        template = sql
        
        # Даты
        template = re.sub(r"'(\d{4}-\d{2}-\d{2})'", "'{DATE}'", template)
        
        # Числа
        template = re.sub(r'([<>]=?|=)\s*\d+', r'\1 {NUMBER}', template)
        
        # ID
        template = re.sub(r"'([\w-]{15,}|\w{5,})'", "'{ID}'", template)
        
        return template
    
    def _make_pattern_key(self, words: Set[str]) -> str:
        """Создаёт ключ паттерна"""
        sorted_words = sorted(words)
        return hashlib.md5(" ".join(sorted_words).encode()).hexdigest()[:16]
    
    # ===================== СОХРАНЕНИЕ/ЗАГРУЗКА =====================
    
    def _load_data(self):
        """Загружает сохранённые данные"""
        try:
            cache_file = Path("query_cache.json")
            if cache_file.exists():
                with open(cache_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.exact_cache = data.get('exact_cache', {})
            
            patterns_file = Path("learned_patterns.json")
            if patterns_file.exists():
                with open(patterns_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    patterns_data = data.get('patterns', {})
                    
                    self.patterns.clear()
                    self.word_index.clear()
                    
                    for pattern_key, pattern in patterns_data.items():
                        self.patterns[pattern_key] = pattern
                        for word in pattern.get('words', []):
                            self.word_index[word].add(pattern_key)
                    
                    logger.info(f"Загружено {len(self.patterns)} паттернов")
                    
        except Exception as e:
            logger.error(f"Ошибка загрузки: {e}")
    
    def _save_cache(self):
        """Сохраняет кэш"""
        try:
            data = {
                'exact_cache': self.exact_cache,
                'updated_at': datetime.now().isoformat()
            }
            with open("query_cache.json", 'w', encoding='utf-8') as f:
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
            
            with open("learned_patterns.json", 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            self._save_cache()
            
        except Exception as e:
            logger.error(f"Ошибка сохранения данных: {e}")
    
    # ===================== УТИЛИТЫ =====================
    
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