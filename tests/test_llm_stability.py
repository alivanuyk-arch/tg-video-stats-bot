# test_llm_stability.py
import asyncio
import sys
sys.path.append('.')
from src.llm_fallback import LLMTeacher

async def stress_test():
    llm = LLMTeacher(model="phi")
    
    queries = [
        "Сколько всего видео?",
        "Видео с лайками более 5000",
        "Сумма комментариев за 29 ноября",
        "Сколько всего креаторов",
        "На сколько просмотров выросли все видео 28 ноября 2025?",
        "Сколько разных видео получали новые просмотры 27 ноября 2025?",
        "Сколько видео у креатора с id abc123?",
        "Видео с просмотрами от 1000 до 5000",
        "Среднее количество лайков",
        "Сумма всех просмотров",
    ]
    
    print("=== ТЕСТ LLM СТАБИЛЬНОСТИ ===\n")
    
    for i, query in enumerate(queries, 1):
        print(f"{i}. {query}")
        try:
            result = await llm.ask(query)
            if result and result.sql:
                print(f"   ✓ SQL: {result.sql[:80]}...")
                print(f"   Безопасный: {result.is_safe}")
            else:
                print(f"   ✗ LLM не ответила")
        except Exception as e:
            print(f"   ✗ ОШИБКА: {type(e).__name__}: {str(e)[:50]}")
    
    print("\n=== ТЕСТ ЗАВЕРШЕН ===")

if __name__ == "__main__":
    asyncio.run(stress_test())