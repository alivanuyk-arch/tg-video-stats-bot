Конструктор перенесен в архив логика работает на боте с правилами

для будующего, если будет железо покрепче попробую

 def _build_prompt(self, query: str) -> str:
    """Умный промпт, который объясняет моделью логику данных"""
    return f"""<s>[INST] Ты — SQL-ассистент для аналитики видео. Сгенерируй точный PostgreSQL запрос.

БАЗА ДАННЫХ:
1. ТАБЛИЦА `videos` - основные данные о видео:
   - id (уникальный идентификатор видео)
   - creator_id (идентификатор создателя видео, 32 символа hex)
   - views_count (ИТОГОВОЕ количество просмотров видео на текущий момент)
   - video_created_at (дата и время ПУБЛИКАЦИИ видео)
   - likes_count (лайки)

2. ТАБЛИЦА `video_snapshots` - ежечасные замеры статистики:
   - video_id (ссылка на videos.id)
   - created_at (дата и время ЗАМЕРА, каждый час: 10:00, 11:00 и т.д.)
   - delta_views_count (изменение просмотров за ПРЕДЫДУЩИЙ час)
   - delta_views_count может быть: положительным (просмотры выросли), 
     отрицательным (просмотры упали), нулевым (без изменений)

КРИТИЧЕСКИЕ ПРАВИЛА:
1. Для вопросов про "ИТОГОВУЮ статистику", "всего просмотров", "набрали N просмотров" → 
   используй `videos.views_count`

2. Для вопросов про "ПРИРОСТ", "изменения за период", "выросли за час" → 
   используй SUM(`video_snapshots.delta_views_count`)

3. Для вопросов про "ОТРИЦАТЕЛЬНЫЕ изменения" → 
   WHERE `delta_views_count < 0`

4. Для вопросов про "СКОЛЬКО видео" → 
   COUNT(*) FROM `videos`

5. Для вопросов про "СКОЛЬКО замеров" → 
   COUNT(*) FROM `video_snapshots`

6. Для "РАЗНЫХ календарных дней" → 
   COUNT(DISTINCT DATE(column))

7. Даты и время:
   - Для фильтра по ДНЮ: DATE(`created_at`) = '2025-11-28'
   - Для фильтра по ЧАСАМ: EXTRACT(HOUR FROM `created_at`) BETWEEN 10 AND 14
   - Для фильтра по МЕСЯЦУ: EXTRACT(MONTH FROM `video_created_at`) = 11
   - Для фильтра по ГОДУ: EXTRACT(YEAR FROM `video_created_at`) = 2025
   - "с X по Y включительно": BETWEEN 'дата1' AND 'дата2'

8. JOIN таблиц нужен ТОЛЬКО когда:
   - В вопросе есть `creator_id` И нужны данные из `video_snapshots`
   - Пример: "прирост просмотров креатора X" → JOIN videos ON video_snapshots.video_id = videos.id

ПРИМЕРЫ ЗАПРОСОВ:
- "Сколько видео у креатора X?" → SELECT COUNT(*) FROM videos WHERE creator_id = 'X';
- "На сколько просмотров выросли видео с 10:00 до 15:00 28 ноября?" → SELECT SUM(delta_views_count) FROM video_snapshots WHERE DATE(created_at) = '2025-11-28' AND EXTRACT(HOUR FROM created_at) BETWEEN 10 AND 14;
- "В скольких днях ноября 2025 публиковал видео креатор X?" → SELECT COUNT(DISTINCT DATE(video_created_at)) FROM videos WHERE creator_id = 'X' AND EXTRACT(YEAR FROM video_created_at) = 2025 AND EXTRACT(MONTH FROM video_created_at) = 11;
- "Сколько отрицательных изменений просмотров?" → SELECT COUNT(*) FROM video_snapshots WHERE delta_views_count < 0;

ВАЖНО: Будь точен. Если сомневаешься — спроси, но сейчас просто верни SQL.

ВОПРОС ПОЛЬЗОВАТЕЛЯ (на русском): "{query}"

Сгенерируй правильный SQL запрос ТОЛЬКО на PostgreSQL: [/INST] SELECT"""