# test_fix.py
import sys
import os
sys.path.append('src')

from query_constructor import QueryConstructor

print("1. Создаю конструктор...")
constructor = QueryConstructor(enable_llm=False)

print(f"\n2. Словарь (первые 5):")
for word, bit in list(constructor.word_to_bit.items())[:5]:
    print(f"   '{word}' -> {bit}")

print("\n3. Проверяю 'сколько' и 'видео':")
print(f"   'сколько' в словаре: {'сколько' in constructor.word_to_bit}")
print(f"   'видео' в словаре: {'видео' in constructor.word_to_bit}")

print("\n4. Тестирую разбор запроса...")
query = "Сколько всего видео?"
print(f"   Запрос: '{query}'")
print(f"   Нижний регистр: '{query.lower()}'")

bits, known, unknown = constructor._query_to_bits(query)
print(f"   Известные слова: {known}")
print(f"   Биты: {bits}")

print("\n5. Обучаю...")
constructor.learn_from_example(
    "Сколько всего видео?",
    "SELECT COUNT(*) FROM videos",
    'test'
)

print("\n6. Ищу...")
match = constructor.find_best_match("Сколько всего видео?")
print(f"   Найдено: {match is not None}")
if match:
    print(f"   Тип: {match.get('type')}")
    if match['type'] == 'bit_pattern':
        print(f"   Слова паттерна: {match['data'].get('words', [])}")

print("\n7. Пробую build_sql...")
sql = constructor.build_sql("Сколько всего видео?", use_llm=False)
print(f"   SQL: {sql}")