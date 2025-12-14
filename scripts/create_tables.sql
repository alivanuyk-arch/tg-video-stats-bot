-- Таблица videos
DROP TABLE IF EXISTS video_snapshots CASCADE;
DROP TABLE IF EXISTS videos CASCADE;

CREATE TABLE videos (
    id UUID PRIMARY KEY,
    creator_id VARCHAR(255),
    video_created_at TIMESTAMP WITH TIME ZONE,
    views_count INTEGER DEFAULT 0,
    likes_count INTEGER DEFAULT 0,
    comments_count INTEGER DEFAULT 0,
    reports_count INTEGER DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Таблица video_snapshots
CREATE TABLE video_snapshots (
    id VARCHAR(255) PRIMARY KEY,
    video_id UUID REFERENCES videos(id) ON DELETE CASCADE,
    views_count INTEGER DEFAULT 0,
    likes_count INTEGER DEFAULT 0,
    comments_count INTEGER DEFAULT 0,
    reports_count INTEGER DEFAULT 0,
    delta_views_count INTEGER DEFAULT 0,
    delta_likes_count INTEGER DEFAULT 0,
    delta_comments_count INTEGER DEFAULT 0,
    delta_reports_count INTEGER DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE
);

-- Индексы для производительности
CREATE INDEX idx_videos_creator_id ON videos(creator_id);
CREATE INDEX idx_videos_created_at ON videos(video_created_at);
CREATE INDEX idx_videos_views ON videos(views_count);
CREATE INDEX idx_snapshots_video_id ON video_snapshots(video_id);
CREATE INDEX idx_snapshots_created_at ON video_snapshots(created_at);
CREATE INDEX idx_snapshots_video_created ON video_snapshots(video_id, created_at);