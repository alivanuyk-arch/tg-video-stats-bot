#!/usr/bin/env python3
"""
Тестирование конструктора без бота - ИСПРАВЛЕННАЯ ВЕРСИЯ
"""

import asyncio
import sys
import os
from pathlib import Path

# Добавляем src в путь
current_dir = Path(__file__).parent
sys.path.append(str(current_dir / 'src'))

# Удаляем старые файлы если есть
cache_files = ['query_cache.json', 'learned_patterns.json']
for file in cache_files:
    if os.path.exists(file):
        os.remove(file)
        print(f"🗑️  Удалён {file}")

from query_constructor import create_constructor

async def test_basic():
    """Базовые тесты без LLM"""
    print("🧪 Тестируем конструктор БЕЗ LLM...")
    
    constructor = create_constructor(enable_llm=False)
    
    # Обучаем на примерах из ТЗ
    examples = [
        ("Сколько всего видео есть в системе?", 
         "SELECT COUNT(*) FROM videos"),
        ("Сколько видео набрало больше 100000 просмотров?", 
         "SELECT COUNT(*) FROM videos WHERE views_count > 100000"),
        ("На сколько просмотров выросли все видео 2025-11-28?", 
         "SELECT SUM(delta_views_count) FROM video_snapshots WHERE DATE(created_at) = '2025-11-28'"),
    ]
    
    for query, sql in examples:
        constructor.learn_from_example(query, sql, 'test')
    
    # Тестируем
    test_cases = [
        ("Сколько всего видео?", True, "SELECT COUNT(*) FROM videos"),
        ("Видео сколько всего?", True, "SELECT COUNT(*) FROM videos"),
      ("Сколько видео больше 50000 просмотров?", True, "SELECT COUNT(*) FROM videos WHERE views_count > 50000"),
        ("Больше 50000 просмотров видео сколько?", True, "SELECT COUNT(*) FROM videos WHERE views_count > 50000"),
        ("Сумма просмотров", True, "SELECT SUM(views_count) FROM videos"),
        ("Что там по статистике?", False, "SELECT COUNT(*) FROM videos"),  # Fallback
    ]
    
    for query, should_work, expected in test_cases:
        sql = constructor.build_sql(query, use_llm=False)
        
        if should_work:
            if sql == expected:
                print(f"✅ '{query}' → {sql}")
            else:
                print(f"❌ '{query}' → {sql} (ожидалось: {expected})")
        else:
            print(f"📦 '{query}' → {sql} (fallback)")
    
    return constructor

def test_pattern_matching(constructor):
    """Тест совпадения паттернов"""
    print("\n🎯 Тест совпадения паттернов...")
    
    # 1. Обучаем на ПРАВИЛЬНОМ примере (без "просмотров")
    constructor.learn_from_example(
        "Сколько видео набрало больше 100000",
        "SELECT COUNT(*) FROM videos WHERE views_count > 100000",
        'test'
    )
    
    print("1. Обучен паттерн: 'Сколько видео набрало больше 100000'")
    
    # 2. Проверяем что паттерн создался
    print("\n2. Проверка созданных паттернов:")
    for mask, data in constructor.bit_combos.items():
        words = data.get('words', [])
        if 'набрало' in words:
            print(f"   ✓ Нашёл паттерн: слова={words}")
            print(f"     SQL шаблон: {data['template']}")
    
    # 3. Тестируем разные формулировки
    print("\n3. Тестируем поиск:")
    test_queries = [
        "Сколько видео больше 50000 просмотров?",
        "Больше 50000 просмотров видео сколько?",
        "Видео с просмотрами больше 50000",
        "50000 просмотров видео сколько",
    ]
    
    for query in test_queries:
        print(f"\n   Запрос: '{query}'")
        
        # Отладка разбора
        bits, known, unknown = constructor._query_to_bits(query)
        print(f"     Известные слова: {known}")
        print(f"     Неизвестные: {unknown}")
        
        # Ищем
        match = constructor.find_best_match(query)
        if match:
            print(f"     ✓ Найдено! Тип: {match.get('type')}")
            if match['type'] == 'bit_pattern':
                print(f"       Слова паттерна: {match['data'].get('words', [])}")
        else:
            print(f"     ✗ Не найдено")

async def main():
    print("="*60)
    print("ТЕСТИРОВАНИЕ КОНСТРУКТОРА SQL - ИСПРАВЛЕННАЯ ВЕРСИЯ")
    print("="*60)
    
    # 1. Базовые тесты
    constructor = await test_basic()
    
    # 2. Тест паттернов
    test_pattern_matching(constructor)
    
    # 3. Статистика
    stats = constructor.get_stats()
    print("\n" + "="*60)
    print("СТАТИСТИКА КОНСТРУКТОРА:")
    print(f"• Слов в словаре: {stats.total_words}")
    print(f"• Паттернов: {stats.total_patterns}")
    print(f"• В кэше: {stats.cache_size}")
    print(f"• Fallback: {stats.fallback_patterns}")
    
    # 4. Выводим что выучили
    print("\n📚 Выученные паттерны:")
    for mask, data in constructor.bit_combos.items():
        words = data.get('words', [])
        count = data.get('count', 0)
    try:
        mask_int = int(mask) if isinstance(mask, str) else mask
        print(f"   Маска {mask_int:b}: {words} (использован {count} раз)")
    except:
        print(f"   Маска {mask}: {words} (использован {count} раз)")
    
    print("="*60)

if __name__ == "__main__":
    asyncio.run(main())