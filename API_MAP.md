# TechPulse backend API map

Base URL: `http://localhost:8000`

## Public endpoints

### `GET /api/news`

Query parameters:

- `sort`: `recent` (default) or `hot`
- `limit`: integer 1–100, default 30
- `offset`: integer >= 0, default 0

Returns the existing PostgreSQL article fields. The frontend normalizer maps `website_summary` (preferred) or `description` into each card's `summary`, and maps `article_url`, `source_domain`, `image_url`, and `created_at` to the UI field names.

Example shape:

```json
{
  "sort": "recent",
  "limit": 30,
  "offset": 0,
  "total": 123,
  "articles": [
    {
      "id": 25,
      "title": "Example headline",
      "description": "Source description",
      "image_url": null,
      "article_url": "https://example.com/story",
      "website_summary": null,
      "source_domain": "techcrunch.com",
      "created_at": "2026-10-14T10:05:00Z",
      "published_at": null,
      "importance_score": 0
    }
  ]
}
```

### `GET /api/news/hot-summary`

Returns the latest persisted compiled briefing. It does not call Gemini.

```json
{
  "hot_summary": {
    "id": "7",
    "title": "The biggest technology signals this hour",
    "summary": "A consolidated multi-story briefing...",
    "windowStart": "2026-10-14T10:00:00Z",
    "windowEnd": "2026-10-14T10:06:00Z",
    "generatedAt": "2026-10-14T10:06:30Z",
    "articleCount": 21,
    "keyDevelopments": [
      {
        "headline": "Important development",
        "summary": "What happened, in concise terms.",
        "whyItMatters": "Why the change is significant.",
        "articleIds": ["2", "25"],
        "sources": [
          {"id": "25", "title": "Example headline", "source": "techcrunch.com", "url": "https://example.com/story"}
        ]
      }
    ],
    "sources": []
  }
}
```

Before the first successful briefing, the response is `{"hot_summary": null, "message": "..."}`. The frontend renders an explicit waiting state instead of inventing a live summary.

### `GET /api/news/hot-summaries?limit=24`

Returns recent briefing metadata for future history UI.

### `GET /api/sources`

Returns article count and latest ingest time for each `source_domain`.

### `GET /api/health`

Performs a lightweight `SELECT 1` and returns `{"status":"ok","service":"techpulse-api"}`.

## Protected ingestion endpoints

Pass `X-Admin-Key: <ADMIN_API_KEY>`.

- `POST /api/admin/ingestion/run`: starts the configured five-source crawl in a background task. Each source uses the existing Crawl4AI/Gemini extraction flow. One additional Gemini request compiles all unique database article records collected in that batch into one Hot Summary.
- `GET /api/admin/ingestion/status`: returns `idle`, `queued`, `running`, `succeeded`, or `failed`, plus timestamps and the last result.

The admin routes are disabled until `ADMIN_API_KEY` is configured. No default secret is used.
