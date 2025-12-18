import asyncpg
import asyncio

async def main():
    # Подключаемся к БД
    conn = await asyncpg.connect(
        host='localhost',
        user='postgres',
        password='postgres123',
        database='video_analytics',
        port=5432
    )
    
    print("=== Тест запросов ===")
    
    try:
        # 1. Все дельты с 10:00 до 15:00
        sql1 = """
        SELECT SUM(vs.delta_views_count)
        FROM video_snapshots vs
        JOIN videos v ON vs.video_id = v.id
        WHERE v.creator_id = 'cd87be38b50b4fdd8342bb3c383f3c7d'
          AND DATE(vs.created_at) = '2025-11-28'
          AND EXTRACT(HOUR FROM vs.created_at) BETWEEN 10 AND 15;
        """
        
        result1 = await conn.fetchval(sql1)
        print(f"1. Все дельты 10:00-15:00: {result1}")
        
        # 2. Только положительные дельты
        sql2 = """
        SELECT SUM(vs.delta_views_count)
        FROM video_snapshots vs
        JOIN videos v ON vs.video_id = v.id
        WHERE v.creator_id = 'cd87be38b50b4fdd8342bb3c383f3c7d'
          AND DATE(vs.created_at) = '2025-11-28'
          AND EXTRACT(HOUR FROM vs.created_at) BETWEEN 10 AND 15
          AND vs.delta_views_count > 0;
        """
        
        result2 = await conn.fetchval(sql2)
        print(f"2. Только положительные дельты: {result2}")
        
        # 3. Только >= 0 (положительные и нулевые)
        sql3 = """
        SELECT SUM(vs.delta_views_count)
        FROM video_snapshots vs
        JOIN videos v ON vs.video_id = v.id
        WHERE v.creator_id = 'cd87be38b50b4fdd8342bb3c383f3c7d'
          AND DATE(vs.created_at) = '2025-11-28'
          AND EXTRACT(HOUR FROM vs.created_at) BETWEEN 10 AND 15
          AND vs.delta_views_count >= 0;
        """
        
        result3 = await conn.fetchval(sql3)
        print(f"3. Дельты >= 0: {result3}")
        
        # 4. Без часа 10:00 (только 11-15)
        sql4 = """
        SELECT SUM(vs.delta_views_count)
        FROM video_snapshots vs
        JOIN videos v ON vs.video_id = v.id
        WHERE v.creator_id = 'cd87be38b50b4fdd8342bb3c383f3c7d'
          AND DATE(vs.created_at) = '2025-11-28'
          AND EXTRACT(HOUR FROM vs.created_at) BETWEEN 11 AND 15;
        """
        
        result4 = await conn.fetchval(sql4)
        print(f"4. Без 10:00 (только 11-15): {result4}")
        
        # 5. Без часа 15:00 (только 10-14)
        sql5 = """
        SELECT SUM(vs.delta_views_count)
        FROM video_snapshots vs
        JOIN videos v ON vs.video_id = v.id
        WHERE v.creator_id = 'cd87be38b50b4fdd8342bb3c383f3c7d'
          AND DATE(vs.created_at) = '2025-11-28'
          AND EXTRACT(HOUR FROM vs.created_at) BETWEEN 10 AND 14;
        """
        
        result5 = await conn.fetchval(sql5)
        print(f"5. Без 15:00 (только 10-14): {result5}")
        
        print(f"\nОжидалось: 757")
        
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(main())