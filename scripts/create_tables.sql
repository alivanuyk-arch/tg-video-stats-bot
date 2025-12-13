CREATE TABLE videos (
    id UUID PRIMARY KEY,
    video_created_at TIMESTAMPTZ,
    views_count INTEGER,
    likes_count INTEGER,
    reports_count INTEGER,
    comments_count INTEGER,
    creator_id VARCHAR(255),
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
);

CREATE TABLE video_snapshots (
    id VARCHAR(255) PRIMARY KEY,
    video_id UUID REFERENCES videos(id) ON DELETE CASCADE,
    views_count INTEGER,
    likes_count INTEGER,
    reports_count INTEGER,
    comments_count INTEGER,
    delta_views_count INTEGER,
    delta_likes_count INTEGER,
    delta_reports_count INTEGER,
    delta_comments_count INTEGER,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
);