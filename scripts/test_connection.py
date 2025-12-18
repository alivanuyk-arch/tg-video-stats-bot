import json

with open('data/videos.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

print("📊 Анализ структуры JSON файла:")
print(f"1. Тип корневого элемента: {type(data)}")
print(f"2. Ключи в корневом элементе: {list(data.keys())}")

if "videos" in data:
    videos = data["videos"]
    print(f"3. Тип 'videos': {type(videos)}")
    print(f"4. Количество видео: {len(videos)}")
    
    if videos:
        print(f"5. Первое видео имеет ключи: {list(videos[0].keys())}")
        print(f"6. Количество снапшотов в первом видео: {len(videos[0].get('snapshots', []))}")