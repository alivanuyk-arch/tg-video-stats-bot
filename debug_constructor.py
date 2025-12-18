import requests
prompt = '''Ты SQL генератор. Преобразуй вопрос в SQL.

БАЗА:
- videos: id, creator_id, video_created_at, views_count, likes_count, comments_count, reports_count
- video_snapshots: id, video_id, views_count, likes_count, comments_count, reports_count, delta_views_count, delta_likes_count, delta_comments_count, delta_reports_count, created_at

Вопрос: \"Сколько видео у креатора с id e5181a34f95b481789f99317332cc10d вышло с 1 ноября 2025 по 5 ноября 2025 включительно?\"
SQL:'''

response = requests.post('http://localhost:11434/api/generate', json={
    'model': 'gemma3:4b',
    'prompt': prompt,
    'stream': False,
    'options': {'temperature': 0.1}
})
print('Ответ:', response.json().get('response', '')[:200])