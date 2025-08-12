from fastapi import FastAPI, Request, Query
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from sqlalchemy import select, func
from typing import Optional, List
import os

# ✅ FIXED: Add missing imports
from app.database import AsyncSessionLocal
from app.models.article import Article
from app.models.source import NewsSource

# Initialize FastAPI app
app = FastAPI()

templates = Jinja2Templates(directory="templates")

# Mount static files directory for CSS/JS
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/", response_class=HTMLResponse)
async def news_dashboard(request: Request):
    """Main news dashboard page"""
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/api/articles")
async def get_articles(
    category: Optional[str] = None,
    search: Optional[str] = None,
    source: Optional[str] = None,
    limit: int = Query(50, le=200),
    offset: int = 0
):
    """Get articles with optional filtering"""
    async with AsyncSessionLocal() as session:
        # ✅ FIXED: Use .desc() method instead of desc() function
        query = select(Article).order_by(Article.discovered_at.desc())
        
        # Apply filters
        if category and category != "all":
            query = query.filter(Article.primary_topic == category)
        
        if search:
            query = query.filter(
                Article.title.ilike(f"%{search}%") | 
                Article.content.ilike(f"%{search}%")
            )
        
        if source:
            query = query.filter(Article.source_name == source)
        
        # Apply pagination
        query = query.offset(offset).limit(limit)
        
        result = await session.execute(query)
        articles = result.scalars().all()
        
        # Convert to dict for JSON response
        articles_data = []
        for article in articles:
            articles_data.append({
                "id": article.id,
                "title": article.title,
                "content": (str(article.content or "")[:500] + "...") if len(str(article.content or "")) > 500 else str(article.content or ""),
                "summary": article.summary,
                "url": article.url,
                "source_name": article.source_name,
                "primary_topic": article.primary_topic,
                "secondary_topics": article.secondary_topics or [],
                "importance_level": article.importance_level,
                "primary_region": article.primary_region,
                "countries_mentioned": article.countries_mentioned or [],
                "quality_score": float(getattr(article, "quality_score", 0) or 0),
                "word_count": article.word_count,
                "reading_time_minutes": article.reading_time_minutes,
                "published_at": article.published_at.isoformat() if article.published_at is not None else None,
                "discovered_at": article.discovered_at.isoformat() if article.discovered_at is not None else None,
                "source_reliability": article.source_reliability
            })
        
        return {
            "articles": articles_data,
            "total": len(articles_data),
            "offset": offset,
            "limit": limit
        }

@app.get("/api/stats")
async def get_dashboard_stats():
    """Get dashboard statistics"""
    async with AsyncSessionLocal() as session:
        # Total articles
        total_articles = await session.scalar(select(func.count(Article.id)))
        
        # Articles by topic
        # ✅ FIXED: Use func.count().desc() instead of desc(func.count())
        topic_stats = await session.execute(
            select(Article.primary_topic, func.count(Article.id))
            .group_by(Article.primary_topic)
            .order_by(func.count(Article.id).desc())
        )
        topics = dict([tuple(row) for row in topic_stats.fetchall()])
        
        # Articles by source
        # ✅ FIXED: Use func.count().desc() instead of desc(func.count())
        source_stats = await session.execute(
            select(Article.source_name, func.count(Article.id))
            .group_by(Article.source_name)
            .order_by(func.count(Article.id).desc())
            .limit(10)
        )
        top_sources = dict([ (row[0], row[1]) for row in source_stats.fetchall() ])
        
        # Recent activity (last 24 hours)
        from datetime import datetime, timedelta
        yesterday = datetime.utcnow() - timedelta(days=1)
        recent_articles = await session.scalar(
            select(func.count(Article.id))
            .filter(Article.discovered_at >= yesterday)
        )
        
        return {
            "total_articles": total_articles,
            "topics": topics,
            "top_sources": top_sources,
            "recent_articles": recent_articles
        }

@app.get("/api/sources")
async def get_sources():
    """Get all news sources with their stats"""
    async with AsyncSessionLocal() as session:
        # ✅ FIXED: Use column.desc() instead of desc(column)
        result = await session.execute(
            select(NewsSource).order_by(NewsSource.reliability_score.desc()) # type: ignore[attr-defined]
        )
        sources = result.scalars().all()
        
        sources_data = []
        for source in sources:
            sources_data.append({
                "name": source.name,
                "url": source.url,
                "reliability_score": source.reliability_score,
                "total_articles_collected": source.total_articles_collected or 0,
                "successful_polls": source.successful_polls or 0,
                "failed_polls": source.failed_polls or 0,
                "primary_region": source.primary_region,
                "last_successful_poll_at": source.last_successful_poll_at.isoformat() if source.last_successful_poll_at is not None else None
            })
        
        return {"sources": sources_data}
