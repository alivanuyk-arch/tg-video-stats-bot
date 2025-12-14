import sys
import os
import json
import asyncio
import asyncpg
import uuid
from pathlib import Path
from datetime import datetime

# Добавляем src в путь для импорта config
sys.path.append(str(Path(__file__).parent.parent / 'src'))

from config import config

# В load_data.py меняем обработку
async def load_json_data(json_filepath: str):
    # ... начало функции без изменений ...
    
    try:
        conn = await asyncpg.connect(
        host='localhost',
        port=5432,
        user='postgres',
        password='postgres123',  # ваш пароль из .env
        database='video_analytics')

        with open(json_filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Получаем список видео из словаря
        if isinstance(data, dict) and "videos" in data:
            videos_list = data["videos"]
            print(f"📁 Найдено {len(videos_list)} видео в ключе 'videos'")
        else:
            # На случай, если структура другая
            videos_list = data if isinstance(data, list) else []
            print(f"📁 Загружаем {len(videos_list)} видео")
        
        if not videos_list:
            print("⚠️ Нет данных для загрузки")
            return
        
        video_records = []
        snapshot_records = []
        
        # Теперь обрабатываем videos_list вместо data
        for video in videos_list:
            # Обработка данных видео
            try:
                video_id = uuid.UUID(video['id'])
            except (ValueError, KeyError):
                video_id = video.get('id', str(uuid.uuid4()))
            
            # КОНВЕРТАЦИЯ ДАТ в datetime объекты
            # ВАЖНО: эти строки ДОЛЖНЫ БЫТЬ ЗДЕСЬ, а не внутри except!
            video_created_at_str = video.get('video_created_at')
            created_at_str = video.get('created_at')
            updated_at_str = video.get('updated_at')
            
            # Преобразуем строки в datetime, если они не None
            video_created_at = None
            created_at = None
            updated_at = None
            
            if video_created_at_str:
                video_created_at = datetime.fromisoformat(video_created_at_str.replace('Z', '+00:00'))
            if created_at_str:
                created_at = datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
            if updated_at_str:
                updated_at = datetime.fromisoformat(updated_at_str.replace('Z', '+00:00'))
            
            video_records.append((
                video_id,
                video.get('creator_id'),
                video_created_at,  # ВАЖНО: используем переменную, а не video.get()
                video.get('views_count', 0),
                video.get('likes_count', 0),
                video.get('comments_count', 0),
                video.get('reports_count', 0),
                created_at,  # ВАЖНО: используем переменную, а не video.get()
                updated_at   # ВАЖНО: используем переменную, а не video.get()
            ))
            
            # Обработка снапшотов (конвертируем даты и тут)
            for snapshot in video.get('snapshots', []):
                snapshot_created_at_str = snapshot.get('created_at')
                snapshot_updated_at_str = snapshot.get('updated_at')
                
                snapshot_created_at = None
                snapshot_updated_at = None
                
                if snapshot_created_at_str:
                    snapshot_created_at = datetime.fromisoformat(snapshot_created_at_str.replace('Z', '+00:00'))
                if snapshot_updated_at_str:
                    snapshot_updated_at = datetime.fromisoformat(snapshot_updated_at_str.replace('Z', '+00:00'))
                
                snapshot_records.append((
                    snapshot.get('id', str(uuid.uuid4())),
                    video_id,
                    snapshot.get('views_count', 0),
                    snapshot.get('likes_count', 0),
                    snapshot.get('comments_count', 0),
                    snapshot.get('reports_count', 0),
                    snapshot.get('delta_views_count', 0),
                    snapshot.get('delta_likes_count', 0),
                    snapshot.get('delta_comments_count', 0),
                    snapshot.get('delta_reports_count', 0),
                    snapshot_created_at,  # ВАЖНО: используем переменную
                    snapshot_updated_at   # ВАЖНО: используем переменную
                ))
        
        # SQL для вставки
        video_insert_sql = """
        INSERT INTO videos 
        (id, creator_id, video_created_at, views_count, likes_count, 
         comments_count, reports_count, created_at, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        ON CONFLICT (id) DO NOTHING
        """
        
        snapshot_insert_sql = """
        INSERT INTO video_snapshots 
        (id, video_id, views_count, likes_count, comments_count, reports_count,
         delta_views_count, delta_likes_count, delta_comments_count, delta_reports_count,
         created_at, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        ON CONFLICT (id) DO NOTHING
        """
        
        # Вставка данных
        # Видео
        total_videos = 0
        batch_size = 1000
        for i in range(0, len(video_records), batch_size):
            batch = video_records[i:i+batch_size]
            await conn.executemany(video_insert_sql, batch)
            total_videos += len(batch)
            print(f"  Видео: {i+batch_size}/{len(video_records)}")
        
        # Снапшоты
        total_snapshots = 0
        for i in range(0, len(snapshot_records), batch_size):
            batch = snapshot_records[i:i+batch_size]
            await conn.executemany(snapshot_insert_sql, batch)
            total_snapshots += len(batch)
            print(f"  Снапшоты: {i+batch_size}/{len(snapshot_records)}")
        
        print(f"\n✅ Загрузка завершена!")
        print(f"📊 Видео загружено: {total_videos}")
        print(f"📈 Снапшотов загружено: {total_snapshots}")
        
        # Проверяем данные
        result = await conn.fetchval("SELECT COUNT(*) FROM videos")
        print(f"🔍 Проверка: в базе {result} видео")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        raise
    finally:
        await conn.close()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Использование: python scripts/load_data.py <путь_к_json>")
        sys.exit(1)
    
    json_path = sys.argv[1]
    asyncio.run(load_json_data(json_path))