# TechPulse FastAPI backend

This backend preserves the existing PostgreSQL `news_articles` table and Crawl4AI extraction pipeline, then generates one synthesized Hot Summary from the full per-run batch.

## Setup

1. Create a PostgreSQL database and copy `.env.example` to `.env.local`.
2. Set `DB_URL`, `GEMINI_API_KEY`, and a unique `ADMIN_API_KEY`.
3. Install dependencies and start the server:

```bash
python -m venv .venv
# Windows: .venv\\Scripts\\Activate.ps1
# macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt
uvicorn main:app --reload --host 127.0.0.1 --port 8000
```

The app runs `schema.sql` on startup. It adds only the required new columns to `news_articles` and creates the `hot_summaries` and `hot_summary_articles` tables. Existing news rows are not deleted.

## Run ingestion

```bash
curl -X POST http://127.0.0.1:8000/api/admin/ingestion/run \
  -H "X-Admin-Key: YOUR_ADMIN_API_KEY"
```

The crawler makes its existing extraction call for each configured source and then **at most one extra Gemini request for the entire batch**. That call generates the consolidated briefing. Generation errors are logged; the worker does not retry the model call. `GET /api/news/hot-summary` only reads the latest result from PostgreSQL and never invokes Gemini.

Schedule the ingestion job hourly with an OS scheduler to create hourly briefings. The stored `windowStart` and `windowEnd` capture the exact crawl batch interval. For a Unix cron entry, run from this folder so `.env.local` is loaded:

```cron
5 * * * * cd /absolute/path/to/techpulse-backend && /absolute/path/to/techpulse-backend/.venv/bin/python crawl.py >> ingestion.log 2>&1
```

API docs: `http://127.0.0.1:8000/docs`.

## Endpoints

- `GET /api/health`
- `GET /api/news?sort=recent&limit=30&offset=0`
- `GET /api/news/hot-summary`
- `GET /api/news/hot-summaries?limit=24`
- `GET /api/sources`
- `GET /api/admin/ingestion/status` (admin key required)
- `POST /api/admin/ingestion/run` (admin key required)

Configure frontend CORS via `FRONTEND_ORIGIN`. Multiple allowed origins can be comma-separated.
