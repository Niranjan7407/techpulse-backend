CREATE TABLE IF NOT EXISTS news_articles (
    id SERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT,
    image_url TEXT,
    article_url TEXT UNIQUE NOT NULL,
    website_summary TEXT, -- LLM summary goes here later
    source_domain TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);