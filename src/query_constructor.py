"""
Конструктор SQL запросов с битовыми масками
"""

import re
import json
import logging
from typing import Dict, List, Optional, Set, Any, Tuple
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass
import hashlib

logger = logging.getLogger(__name__)

@dataclass
class ConstructorStats:
    """Статистика конструктора"""
    total_words: int
    total_patterns: int
    cache_size: int
    fallback_patterns: int
    most_used_pattern: Optional[List[str]]
    most_used_count: int

class QueryConstructor:
    """Конструктор SQL запросов с битовыми масками"""
    
    def __init__(self, 
                 cache_file: str = "query_cache.json",
                 patterns_file: str = "learned_patterns.json",
                 llm_client = None):
        """
        Args:
            cache_file: файл для кэша точных запросов
            patterns_file: файл для обученных паттернов
            llm_client: клиент LLM (опционально)
        """
        self.cache_file = Path(cache_file)
        self.patterns_file = Path(patterns_file)
        self.llm_client = llm_client
        
        # Основные структуры
        self.word_to_bit: Dict[str, int] = {}
        self.bit_counter = 1
        
        # Кэши
        self.bit_combos: Dict[int, Dict] = {}  # mask → pattern data
        self.exact_cache: Dict[str, str] = {}  # exact query → SQL
        self.fallback_patterns: Dict[str, Dict] = {}  # hash → pattern
       
        # Инициализация
        self._init_vocabulary()
        self._learn_basic_patterns()
       
        # Загрузка данных
        self._load_data()
        
        
        
        logger.info(f"QueryConstructor инициализирован: {len(self.word_to_bit)} слов")
    
    # ===== ИНИЦИАЛИЗАЦИЯ =====
    
    def _init_vocabulary(self):
        """Инициализация словаря значимых слов"""
        # Локальный список
        vocab = [
            'сколько', 'сумма', 'среднее', 'максимум', 'минимум',
            'количество', 'число', 'итого',
            'видео', 'видеоролик', 'ролик',
            'просмотр', 'просмотры', 'просмотров',
            'лайк', 'лайки', 'лайков', 
            'комментарий', 'комментарии', 'комментариев',
            'жалоба', 'жалобы', 'жалоб',
            'креатор', 'креатора', 'креаторов',
            'автор', 'автора', 'авторов',
            'больше', 'меньше', 'более', 'менее', 'равно',
            'набрало', 'получило', 'вышло', 'создано',
            'январь', 'января', 'февраль', 'февраля',
            'март', 'марта', 'апрель', 'апреля',
            'май', 'мая', 'июнь', 'июня',
            'июль', 'июля', 'август', 'августа',
            'сентябрь', 'сентября', 'октябрь', 'октября',
            'ноябрь', 'ноября', 'декабрь', 'декабря',
            '2024', '2025', 'дата', 'период', 'месяц', 'год',
            'всего', 'разных', 'новые', 'прирост', 'рост',
            'с', 'на', 'за', 'по', 'в', 'у',
        ]
        
        # Добавляем слова
        for word in vocab:
            self._get_bit_for_word(word)
    
    def _learn_basic_patterns(self):
        """Обучение на базовых примерах"""
        examples = [
            ("Сколько всего видео есть в системе?", 
             "SELECT COUNT(*) FROM videos"),
            
            ("Сколько видео набрало больше 100000 просмотров?", 
             "SELECT COUNT(*) FROM videos WHERE views_count > 100000"),
            
            ("На сколько просмотров выросли все видео 2025-11-28?", 
             "SELECT SUM(delta_views_count) FROM video_snapshots WHERE DATE(created_at) = '2025-11-28'"),
            
            ("Сколько разных видео получали новые просмотры 2025-11-27?", 
             "SELECT COUNT(DISTINCT video_id) FROM video_snapshots WHERE DATE(created_at) = '2025-11-27' AND delta_views_count > 0"),
            
            ("Сумма просмотров", "SELECT SUM(views_count) FROM videos"),
            ("Средние лайки", "SELECT AVG(likes_count) FROM videos"),
        ]
        
        for query, sql in examples:
            self.learn_from_example(query, sql, 'manual')
    
    # ===== БИТОВАЯ ЛОГИКА =====
    
    def _get_bit_for_word(self, word: str) -> int:
        """Получаем бит для слова"""
        if word not in self.word_to_bit:
            self.word_to_bit[word] = self.bit_counter
            self.bit_counter <<= 1
        return self.word_to_bit[word]
    
    def _query_to_bits(self, query: str) -> Tuple[Set[int], List[str], List[str]]:
        """
        Анализ запроса
        Returns: (биты, известные слова, неизвестные слова)
        """
        query_lower = query.lower()
        words = query_lower.split()
        
        bits = set()
        known_words = []
        unknown_words = []
        
        # 1. Известные слова
        for word in words:
            if word in self.word_to_bit:
                bits.add(self.word_to_bit[word])
                known_words.append(word)
            elif len(word) > 2:  # Игнорируем предлоги и т.п.
                unknown_words.append(word)
        
        # 2. Специальные биты
        if re.search(r'\b\d+\b', query_lower):
            bits.add(1 << 30)  # Бит "есть число"
        
        if re.search(r'\d{4}-\d{2}-\d{2}', query_lower):
            bits.add(1 << 31)  # Бит "есть SQL дата"
        
        if re.search(r'\d{1,2}\s+\w+\s+\d{4}', query_lower):
            bits.add(1 << 29)  # Бит "есть русская дата"
        
        if re.search(r'[a-f0-9-]{36}', query_lower):
            bits.add(1 << 28)  # Бит "есть UUID"
        
        return bits, known_words, unknown_words
    
    def _bits_to_mask(self, bits: Set[int]) -> int:
        """Биты → маска"""
        mask = 0
        for bit in bits:
            mask |= bit
        return mask
    
    # ===== ОБУЧЕНИЕ =====
    
    def learn_from_example(self, query: str, sql: str, source: str = 'llm'):
        """Учимся на примере запрос→SQL"""
        bits, known_words, unknown_words = self._query_to_bits(query)
        mask = self._bits_to_mask(bits)
        
        # Создаём SQL шаблон
        sql_template = self._generalize_sql(sql)
        
        if known_words:
            # Сохраняем в битовые комбинации
            if mask in self.bit_combos:
                self.bit_combos[mask]['count'] += 1
                self.bit_combos[mask]['examples'].append(query)
            else:
                self.bit_combos[mask] = {
                    'template': sql_template,
                    'examples': [query],
                    'count': 1,
                    'words': known_words,
                    'unknown_words': unknown_words,
                    'created_at': datetime.now().isoformat(),
                    'source': source
                }
        else:
            # Fallback: сохраняем по хешу
            pattern_hash = self._create_pattern_hash(query)
            self.fallback_patterns[pattern_hash] = {
                'sql': sql,
                'template': sql_template,
                'query': query,
                'count': 1,
                'type': 'fallback',
                'source': source
            }
        
        # Сохраняем точный запрос
        self.exact_cache[query] = sql
        
        self._save_data()
        logger.info(f"Выучен паттерн: {known_words or ['fallback']}")
    
    def _generalize_sql(self, sql: str) -> str:
        """Обобщение SQL"""
        template = sql
        
        template = re.sub(r'\b\d+\b', '{N}', template)
        template = re.sub(r"'(\d{4}-\d{2}-\d{2})'", "'{DATE}'", template)
        template = re.sub(r"'([a-f0-9-]{36})'", "'{ID}'", template)
        
        return template
    
    def _create_pattern_hash(self, query: str) -> str:
        """Создаёт хеш для fallback паттернов"""
        normalized = re.sub(r'\s+', ' ', query.lower().strip())
        return hashlib.md5(normalized.encode()).hexdigest()[:16]
    
    # ===== ПОИСК =====
    
    def find_best_match(self, query: str) -> Optional[Dict]:  # ← ДОЛЖНО БЫТЬ 4 ПРОБЕЛА!
        """Поиск лучшего совпадения"""
        bits, known_words, unknown_words = self._query_to_bits(query)
        
        # 1. Проверяем точный кэш
        if query in self.exact_cache:
            return {'type': 'exact', 'sql': self.exact_cache[query]}
        
        # 2. Если есть известные слова - ищем по битам
        if known_words:
            mask = self._bits_to_mask(bits)
            
            for pattern_mask_str, data in self.bit_combos.items():
                # КОНВЕРТИРУЕМ pattern_mask в int если нужно
                try:
                    if isinstance(pattern_mask_str, str):
                        pattern_mask = int(pattern_mask_str)
                    else:
                        pattern_mask = pattern_mask_str
                except (ValueError, TypeError):
                    continue  # Пропускаем некорректные
                
                common = mask & pattern_mask
                if bin(common).count('1') >= max(2, len(known_words) // 2):
                    return {'type': 'bit_pattern', 'data': data, 'mask': mask}
        
        # 3. Fallback по хешу
        pattern_hash = self._create_pattern_hash(query)
        if pattern_hash in self.fallback_patterns:
            return {'type': 'fallback', 'data': self.fallback_patterns[pattern_hash]}
        
        return None
    
    # ===== ОСНОВНОЙ МЕТОД =====
    
    def build_sql(self, user_query: str, use_llm: bool = True) -> str:
        """
        Строит SQL для запроса
        
        Args:
            user_query: запрос на естественном языке
            use_llm: использовать LLM если не найден паттерн
            
        Returns:
            SQL запрос
        """
        logger.info(f"Обработка: {user_query}")
        
        # 1. Ищем совпадение
        match = self.find_best_match(user_query)
        
        if match:
            if match['type'] == 'exact':
                return match['sql']
            
            elif match['type'] == 'bit_pattern':
                data = match['data']
                sql = self._fill_template(data['template'], user_query)
                data['count'] += 1
                self.exact_cache[user_query] = sql
                return sql
            
            elif match['type'] == 'fallback':
                data = match['data']
                sql = self._fill_template(data['template'], user_query)
                data['count'] += 1
                self.exact_cache[user_query] = sql
                return sql
        
        # 2. LLM fallback
        if use_llm and self.llm_client:
            try:
                from .llm_fallback import LLMResult
                llm_result = self.llm_client.ask(user_query)
                
                if llm_result and llm_result.is_safe:
                    sql = llm_result.sql
                    
                    # Учимся на этом
                    self.learn_from_example(user_query, sql, 'llm')
                    
                    return sql
                    
            except Exception as e:
                logger.error(f"Ошибка LLM: {e}")
        
        # 3. Ultimate fallback
        return "SELECT COUNT(*) FROM videos"
    
    def _fill_template(self, template: str, query: str) -> str:
        """Заполнение шаблона"""
        sql = template
        
        # Извлекаем значения
        numbers = re.findall(r'\b\d+\b', query)
        dates = re.findall(r'\d{4}-\d{2}-\d{2}', query)
        uuids = re.findall(r'[a-f0-9-]{36}', query)
        
        # Русские даты
        rus_match = re.search(r'(\d{1,2})\s+(\w+)\s+(\d{4})', query)
        if rus_match:
            day, month_ru, year = rus_match.groups()
            month_num = self._rus_month_to_num(month_ru)
            if month_num:
                sql_date = f"{year}-{month_num:02d}-{int(day):02d}"
                sql = sql.replace('{DATE}', sql_date)
        
        # Замена плейсхолдеров
        if uuids and '{ID}' in sql:
            sql = sql.replace('{ID}', uuids[0])
        if dates and '{DATE}' in sql:
            sql = sql.replace('{DATE}', dates[0])
        if numbers and '{N}' in sql:
            sql = sql.replace('{N}', numbers[0])
        
        return sql
    
    def _rus_month_to_num(self, month_ru: str) -> Optional[int]:
        """Конвертация русского месяца"""
        month_map = {
            'января': 1, 'февраля': 2, 'марта': 3, 'апреля': 4,
            'мая': 5, 'июня': 6, 'июля': 7, 'августа': 8,
            'сентября': 9, 'октября': 10, 'ноября': 11, 'декабря': 12
        }
        return month_map.get(month_ru.lower())
    
    # ===== СОХРАНЕНИЕ/ЗАГРУЗКА =====
    
    def _load_data(self):
        """Загрузка данных"""
        try:
            if self.cache_file.exists():
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.exact_cache = data.get('exact_cache', {})
                    self.fallback_patterns = data.get('fallback_patterns', {})
            
            if self.patterns_file.exists():
                with open(self.patterns_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.word_to_bit = data.get('word_to_bit', {})
                    self.bit_combos = data.get('bit_combos', {})
                    
                    # Восстанавливаем bit_counter
                    max_bit = max(self.word_to_bit.values()) if self.word_to_bit else 0
                    self.bit_counter = 1
                    while self.bit_counter <= max_bit:
                        self.bit_counter <<= 1
                        
        except Exception as e:
            logger.error(f"Ошибка загрузки: {e}")
    
    def _save_data(self):
        """Сохранение данных"""
        try:
            # Кэш
            cache_data = {
                'exact_cache': self.exact_cache,
                'fallback_patterns': self.fallback_patterns,
                'updated_at': datetime.now().isoformat()
            }
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, ensure_ascii=False, indent=2)
            
            # Паттерны
            pattern_data = {
                'word_to_bit': self.word_to_bit,
                'bit_combos': self.bit_combos,
                'updated_at': datetime.now().isoformat()
            }
            with open(self.patterns_file, 'w', encoding='utf-8') as f:
                json.dump(pattern_data, f, ensure_ascii=False, indent=2)
                
        except Exception as e:
            logger.error(f"Ошибка сохранения: {e}")
    
    # ===== УТИЛИТЫ =====
    
    def add_manual_pattern(self, query: str, sql: str):
        """Ручное добавление паттерна"""
        self.learn_from_example(query, sql, 'manual')
    
    def clear_cache(self):
        """Очистка кэша"""
        self.exact_cache = {}
        self._save_data()
    
    def get_stats(self) -> ConstructorStats:
        """Статистика"""
        if self.bit_combos:
            most_used = max(self.bit_combos.values(), key=lambda x: x['count'])
        else:
            most_used = None
        
        return ConstructorStats(
            total_words=len(self.word_to_bit),
            total_patterns=len(self.bit_combos),
            cache_size=len(self.exact_cache),
            fallback_patterns=len(self.fallback_patterns),
            most_used_pattern=most_used['words'] if most_used else None,
            most_used_count=most_used['count'] if most_used else 0
        )


# Фабричная функция
def create_constructor(llm_client=None, enable_llm=True):
    """Создание конструктора"""
    if enable_llm and llm_client is None:
        try:
            from .llm_fallback import LLMTeacher
            llm_client = LLMTeacher()
        except ImportError:
            logger.warning("LLM недоступен")
            llm_client = None
    
    return QueryConstructor(
        cache_file="query_cache.json",
        patterns_file="learned_patterns.json",
        llm_client=llm_client
    )