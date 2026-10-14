-- Safe to run against both a new database and the existing TechPulse table.
CREATE TABLE IF NOT EXISTS news_articles (
    id SERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT,
    image_url TEXT,
    article_url TEXT UNIQUE NOT NULL,
    website_summary TEXT,
    source_domain TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE news_articles
    ADD COLUMN IF NOT EXISTS importance_score NUMERIC(5, 2) NOT NULL DEFAULT 0;
ALTER TABLE news_articles
    ADD COLUMN IF NOT EXISTS published_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_news_articles_created_at
    ON news_articles (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_news_articles_importance
    ON news_articles (importance_score DESC, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_news_articles_source
    ON news_articles (source_domain);

-- Backfill existing rows so the frontend's local Trending toggle has useful ordering.
-- This is a cheap rule-based fallback, not an additional Gemini request.
UPDATE news_articles
SET importance_score = CASE
    WHEN lower(concat_ws(' ', title, COALESCE(description, ''))) ~ '(security breach|critical vulnerability|zero.day|exploit|major outage|acquisition|acquires|series [a-f] funding|antitrust|recall)' THEN 75
    WHEN lower(concat_ws(' ', title, COALESCE(description, ''))) ~ '(announc|launch|release|unveil|open.source|funding|raises|research breakthrough)' THEN 60
    WHEN lower(concat_ws(' ', title, COALESCE(description, ''))) ~ '(artificial intelligence|(^|[^a-z])ai([^a-z]|$)|llm|nvidia|openai|google|microsoft|apple|meta|anthropic|amd|intel|chip|gpu)' THEN 50
    ELSE 40
END
WHERE COALESCE(importance_score, 0) = 0;

-- A briefing is generated once after a crawler batch (schedule the crawler hourly).
-- window_start/window_end record the actual batch collection interval.
CREATE TABLE IF NOT EXISTS hot_summaries (
    id BIGSERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    summary TEXT NOT NULL,
    key_developments JSONB NOT NULL DEFAULT '[]'::jsonb,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    article_count INTEGER NOT NULL DEFAULT 0 CHECK (article_count >= 0),
    generated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (window_end >= window_start)
);

CREATE INDEX IF NOT EXISTS idx_hot_summaries_generated_at
    ON hot_summaries (generated_at DESC);

CREATE TABLE IF NOT EXISTS hot_summary_articles (
    hot_summary_id BIGINT NOT NULL
        REFERENCES hot_summaries(id) ON DELETE CASCADE,
    article_id INTEGER NOT NULL
        REFERENCES news_articles(id) ON DELETE CASCADE,
    PRIMARY KEY (hot_summary_id, article_id)
);

CREATE INDEX IF NOT EXISTS idx_hot_summary_articles_article
    ON hot_summary_articles (article_id);
