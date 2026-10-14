"""FastAPI endpoints for the TechPulse Next.js frontend."""

import json
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

import asyncpg
from dotenv import load_dotenv
from fastapi import BackgroundTasks, Depends, FastAPI, Header, HTTPException, Query, Request, status
from fastapi.middleware.cors import CORSMiddleware

from crawl import run_pipeline_task

load_dotenv(".env.local")
load_dotenv()

DATABASE_URL = os.getenv("DB_URL", "")
ADMIN_SECRET = os.getenv("ADMIN_API_KEY", "").strip()
FRONTEND_ORIGINS = [
    origin.strip()
    for origin in os.getenv("FRONTEND_ORIGIN", "http://localhost:3000").split(",")
    if origin.strip()
]
SCHEMA_SQL = Path(__file__).with_name("schema.sql").read_text(encoding="utf-8")

_ingestion_status: dict[str, Any] = {
    "state": "idle",
    "started_at": None,
    "finished_at": None,
    "error": None,
    "result": None,
}


@asynccontextmanager
async def lifespan(app: FastAPI):
    if not DATABASE_URL:
        raise RuntimeError("DB_URL is missing. Set it in .env.local or the process environment.")
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10, command_timeout=30)
    async with pool.acquire() as conn:
        # Apply each DDL statement separately so the migration script works reliably with asyncpg.
        for statement in SCHEMA_SQL.split(";"):
            if statement.strip():
                await conn.execute(statement)
    app.state.db_pool = pool
    try:
        yield
    finally:
        await pool.close()


app = FastAPI(title="TechPulse News API", version="1.1.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=FRONTEND_ORIGINS,
    allow_credentials=False,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Content-Type", "X-Admin-Key", "Accept"],
)


async def get_db_pool(request: Request) -> asyncpg.Pool:
    return request.app.state.db_pool


def verify_admin(x_admin_key: str = Header(default="", alias="X-Admin-Key")) -> None:
    if not ADMIN_SECRET:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Admin endpoint disabled: configure ADMIN_API_KEY first.",
        )
    if x_admin_key != ADMIN_SECRET:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid admin credentials.")


def decode_json_field(value: Any, fallback: Any) -> Any:
    if isinstance(value, (list, dict)):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return fallback
    return fallback


def source_payload(row: asyncpg.Record) -> dict[str, Any]:
    return {
        "id": str(row["id"]),
        "title": row["title"],
        "source": row["source_domain"] or "Unknown source",
        "url": row["article_url"],
    }


@app.get("/api/health")
async def health(pool: asyncpg.Pool = Depends(get_db_pool)) -> dict[str, str]:
    async with pool.acquire() as conn:
        await conn.fetchval("SELECT 1")
    return {"status": "ok", "service": "techpulse-api"}


@app.get("/api/news")
async def get_news(
    sort: Literal["recent", "latest", "newest", "hot", "importance"] = "recent",
    limit: int = Query(default=30, ge=1, le=100),
    offset: int = Query(default=0, ge=0, le=10000),
    pool: asyncpg.Pool = Depends(get_db_pool),
) -> dict[str, Any]:
    normalized_sort = "hot" if sort in ("hot", "importance") else "recent"
    order_clause = (
        "ORDER BY importance_score DESC NULLS LAST, created_at DESC, id DESC"
        if normalized_sort == "hot"
        else "ORDER BY created_at DESC, id DESC"
    )
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT id, title, description, image_url, article_url, website_summary,
                   source_domain, created_at, published_at, importance_score
            FROM news_articles
            {order_clause}
            LIMIT $1 OFFSET $2;
            """,
            limit,
            offset,
        )
        total = await conn.fetchval("SELECT COUNT(*) FROM news_articles")
    return {
        "sort": normalized_sort,
        "limit": limit,
        "offset": offset,
        "total": int(total or 0),
        "articles": [dict(row) for row in rows],
    }


@app.get("/api/news/hot-summary")
async def get_hot_summary(pool: asyncpg.Pool = Depends(get_db_pool)) -> dict[str, Any]:
    """Return the latest stored multi-article briefing; this endpoint never calls Gemini."""
    async with pool.acquire() as conn:
        summary_row = await conn.fetchrow(
            """
            SELECT id, title, summary, key_developments, window_start, window_end,
                   article_count, generated_at
            FROM hot_summaries
            ORDER BY generated_at DESC, id DESC
            LIMIT 1;
            """
        )
        if summary_row is None:
            return {"hot_summary": None, "message": "No Hot Summary has been generated yet. Run ingestion to create one."}

        article_rows = await conn.fetch(
            """
            SELECT n.id, n.title, n.article_url, n.source_domain
            FROM hot_summary_articles hsa
            JOIN news_articles n ON n.id = hsa.article_id
            WHERE hsa.hot_summary_id = $1
            ORDER BY n.source_domain, n.id;
            """,
            summary_row["id"],
        )

    all_sources = [source_payload(row) for row in article_rows]
    sources_by_id = {source["id"]: source for source in all_sources}
    raw_developments = decode_json_field(summary_row["key_developments"], [])
    developments: list[dict[str, Any]] = []
    if isinstance(raw_developments, list):
        for development in raw_developments:
            if not isinstance(development, dict):
                continue
            article_ids = development.get("article_ids", development.get("articleIds", []))
            if not isinstance(article_ids, list):
                article_ids = []
            selected_sources = [sources_by_id[str(article_id)] for article_id in article_ids if str(article_id) in sources_by_id]
            developments.append({
                "headline": development.get("headline", development.get("title", "")),
                "summary": development.get("summary", ""),
                "whyItMatters": development.get("why_it_matters", development.get("whyItMatters")),
                "articleIds": [str(article_id) for article_id in article_ids],
                "sources": selected_sources,
            })

    summary = {
        "id": str(summary_row["id"]),
        "title": summary_row["title"],
        "summary": summary_row["summary"],
        "windowStart": summary_row["window_start"],
        "windowEnd": summary_row["window_end"],
        "generatedAt": summary_row["generated_at"],
        "articleCount": summary_row["article_count"],
        "keyDevelopments": developments,
        "sources": all_sources,
    }
    return {"hot_summary": summary}


@app.get("/api/news/hot-summaries")
async def get_hot_summary_history(
    limit: int = Query(default=24, ge=1, le=100),
    pool: asyncpg.Pool = Depends(get_db_pool),
) -> dict[str, Any]:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, title, summary, window_start, window_end, article_count, generated_at
            FROM hot_summaries
            ORDER BY generated_at DESC, id DESC
            LIMIT $1;
            """,
            limit,
        )
    return {"summaries": [dict(row) for row in rows]}


@app.get("/api/sources")
async def get_sources(pool: asyncpg.Pool = Depends(get_db_pool)) -> dict[str, Any]:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT source_domain, COUNT(*)::INTEGER AS article_count, MAX(created_at) AS last_seen_at
            FROM news_articles
            GROUP BY source_domain
            ORDER BY article_count DESC, source_domain ASC;
            """
        )
    return {
        "sources": [
            {
                "source": row["source_domain"] or "Unknown source",
                "articleCount": row["article_count"],
                "lastSeenAt": row["last_seen_at"],
            }
            for row in rows
        ]
    }


@app.get("/api/admin/ingestion/status", dependencies=[Depends(verify_admin)])
async def get_ingestion_status() -> dict[str, Any]:
    return dict(_ingestion_status)


async def _run_ingestion_job(pool: asyncpg.Pool) -> None:
    _ingestion_status.update({
        "state": "running",
        "started_at": datetime.now(timezone.utc).isoformat(),
        "finished_at": None,
        "error": None,
        "result": None,
    })
    try:
        result = await run_pipeline_task(pool)
        _ingestion_status.update({"state": "succeeded", "result": result})
    except Exception as exc:
        _ingestion_status.update({"state": "failed", "error": str(exc)})
        print(f"Ingestion job failed: {exc}")
    finally:
        _ingestion_status["finished_at"] = datetime.now(timezone.utc).isoformat()


@app.post(
    "/api/admin/ingestion/run",
    status_code=status.HTTP_202_ACCEPTED,
    dependencies=[Depends(verify_admin)],
)
async def trigger_ingestion(background_tasks: BackgroundTasks, pool: asyncpg.Pool = Depends(get_db_pool)) -> dict[str, str]:
    if _ingestion_status["state"] in ("queued", "running"):
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="An ingestion job is already queued or running.")
    _ingestion_status.update({"state": "queued", "error": None, "result": None})
    background_tasks.add_task(_run_ingestion_job, pool)
    return {"message": "Ingestion queued. One additional Gemini request will generate a Hot Summary after the crawl batch completes.", "state": "queued"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="127.0.0.1", port=int(os.getenv("PORT", "8000")), reload=False)
